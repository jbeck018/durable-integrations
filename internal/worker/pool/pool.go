// Package pool provides a generic worker pool with configurable concurrency,
// backpressure handling, and atomic stats tracking for the FlowForge pipeline.
package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PoolStats contains live counters for a worker pool.
type PoolStats struct {
	Active    int64 `json:"active"`
	Pending   int64 `json:"pending"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
}

// Pool is a generic, bounded worker pool that processes items of type T with
// configurable concurrency. It uses buffered channels for backpressure and
// atomic counters for lock-free stats.
type Pool[T any] struct {
	name        string
	concurrency int
	processor   func(context.Context, T) error

	input  chan T
	done   chan struct{}
	wg     sync.WaitGroup
	cancel context.CancelFunc

	active    atomic.Int64
	pending   atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64

	started atomic.Bool
	stopped atomic.Bool

	mu sync.Mutex
}

// NewPool creates a new worker pool. The input channel buffer is sized at
// 2x concurrency to allow submitters to stay ahead of workers without
// unbounded growth.
func NewPool[T any](name string, concurrency int, processor func(context.Context, T) error) *Pool[T] {
	if concurrency < 1 {
		concurrency = 1
	}
	bufSize := concurrency * 2
	return &Pool[T]{
		name:        name,
		concurrency: concurrency,
		processor:   processor,
		input:       make(chan T, bufSize),
		done:        make(chan struct{}),
	}
}

// Start launches the worker goroutines. It is safe to call only once.
func (p *Pool[T]) Start(ctx context.Context) {
	if p.started.Swap(true) {
		return
	}
	ctx, p.cancel = context.WithCancel(ctx)

	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

// worker is the main loop for a single pool goroutine. It pulls items from the
// input channel, processes them, and updates atomic stats counters.
func (p *Pool[T]) worker(ctx context.Context, _ int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			// Drain remaining items in the channel before exiting.
			for {
				select {
				case item, ok := <-p.input:
					if !ok {
						return
					}
					p.pending.Add(-1)
					p.processItem(ctx, item)
				default:
					return
				}
			}
		case item, ok := <-p.input:
			if !ok {
				return
			}
			p.pending.Add(-1)
			p.processItem(ctx, item)
		}
	}
}

// processItem executes the processor function with stats bookkeeping.
func (p *Pool[T]) processItem(ctx context.Context, item T) {
	p.active.Add(1)
	defer p.active.Add(-1)

	if err := p.processor(ctx, item); err != nil {
		p.failed.Add(1)
	} else {
		p.completed.Add(1)
	}
}

// Submit enqueues an item for processing. It blocks if the input channel is
// full (backpressure) and returns an error if the pool has been shut down
// or the provided context is cancelled.
func (p *Pool[T]) Submit(item T) error {
	if p.stopped.Load() {
		return errors.New("pool is shut down")
	}
	if !p.started.Load() {
		return errors.New("pool not started")
	}

	p.pending.Add(1)
	select {
	case p.input <- item:
		return nil
	case <-p.done:
		p.pending.Add(-1)
		return errors.New("pool is shut down")
	}
}

// SubmitWithContext enqueues an item with a caller-controlled context for
// cancellation-aware submission.
func (p *Pool[T]) SubmitWithContext(ctx context.Context, item T) error {
	if p.stopped.Load() {
		return errors.New("pool is shut down")
	}
	if !p.started.Load() {
		return errors.New("pool not started")
	}

	p.pending.Add(1)
	select {
	case p.input <- item:
		return nil
	case <-ctx.Done():
		p.pending.Add(-1)
		return ctx.Err()
	case <-p.done:
		p.pending.Add(-1)
		return errors.New("pool is shut down")
	}
}

// Shutdown gracefully drains the pool. It closes the input channel, then waits
// for all in-flight items to complete or the context deadline to pass.
func (p *Pool[T]) Shutdown(ctx context.Context) error {
	if p.stopped.Swap(true) {
		return nil
	}

	// Signal no more submits accepted.
	close(p.done)
	close(p.input)

	// Wait for workers to finish or context to expire.
	finished := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		// Force-cancel remaining work.
		if p.cancel != nil {
			p.cancel()
		}
		return fmt.Errorf("pool %s: shutdown timed out: %w", p.name, ctx.Err())
	}
}

// Stats returns a snapshot of the pool's current statistics.
func (p *Pool[T]) Stats() PoolStats {
	return PoolStats{
		Active:    p.active.Load(),
		Pending:   p.pending.Load(),
		Completed: p.completed.Load(),
		Failed:    p.failed.Load(),
	}
}

// Name returns the pool's name.
func (p *Pool[T]) Name() string {
	return p.name
}

// Concurrency returns the configured number of workers.
func (p *Pool[T]) Concurrency() int {
	return p.concurrency
}

// Resize dynamically changes the pool concurrency. New workers are started;
// excess workers finish their current item then exit. Only increases are
// immediate; decreases take effect as current workers complete.
func (p *Pool[T]) Resize(ctx context.Context, newConcurrency int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newConcurrency < 1 {
		newConcurrency = 1
	}
	if newConcurrency > p.concurrency {
		delta := newConcurrency - p.concurrency
		for i := 0; i < delta; i++ {
			p.wg.Add(1)
			go p.worker(ctx, p.concurrency+i)
		}
	}
	p.concurrency = newConcurrency
}

// QueueDepth returns the number of items waiting to be processed.
func (p *Pool[T]) QueueDepth() int {
	return int(p.pending.Load())
}

// WaitIdle blocks until all submitted items have been processed. It polls at
// the given interval. Useful for testing and controlled shutdown sequences.
func (p *Pool[T]) WaitIdle(ctx context.Context, pollInterval time.Duration) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if p.active.Load() == 0 && p.pending.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
