// Package load provides the load phase of the FlowForge ETL pipeline.
// It batches records and writes them to destination connectors with retry
// and dead letter queue support.
package load

import (
	"errors"
	"sync"
	"time"

	"github.com/flowforge/flowforge/pkg/protocol"
)

// Batcher accumulates records and flushes them in batches when the buffer
// reaches maxSize or the flushInterval elapses. It is safe for concurrent use.
type Batcher struct {
	maxSize       int
	flushInterval time.Duration
	flushFn       func([]protocol.Record) error

	mu      sync.Mutex
	buffer  []protocol.Record
	closed  bool
	done    chan struct{}
	timer   *time.Timer
}

// NewBatcher creates a Batcher that flushes when maxSize records accumulate
// or flushInterval elapses, whichever comes first.
func NewBatcher(maxSize int, flushInterval time.Duration, flushFn func([]protocol.Record) error) *Batcher {
	if maxSize <= 0 {
		maxSize = 1000
	}
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}
	b := &Batcher{
		maxSize:       maxSize,
		flushInterval: flushInterval,
		flushFn:       flushFn,
		buffer:        make([]protocol.Record, 0, maxSize),
		done:          make(chan struct{}),
	}
	b.timer = time.AfterFunc(flushInterval, b.timerFlush)
	return b
}

// Add appends a record to the buffer. When the buffer reaches maxSize, a
// synchronous flush is triggered.
func (b *Batcher) Add(record protocol.Record) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errors.New("batcher is closed")
	}
	b.buffer = append(b.buffer, record)
	if len(b.buffer) >= b.maxSize {
		batch := b.drainLocked()
		b.mu.Unlock()
		return b.flushFn(batch)
	}
	b.mu.Unlock()
	return nil
}

// Flush forces an immediate flush of all buffered records.
func (b *Batcher) Flush() error {
	b.mu.Lock()
	if len(b.buffer) == 0 {
		b.mu.Unlock()
		return nil
	}
	batch := b.drainLocked()
	b.mu.Unlock()
	return b.flushFn(batch)
}

// Close flushes remaining records and stops the background timer. After Close
// returns, Add will return an error.
func (b *Batcher) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.timer.Stop()
	var batch []protocol.Record
	if len(b.buffer) > 0 {
		batch = b.drainLocked()
	}
	b.mu.Unlock()
	close(b.done)
	if len(batch) > 0 {
		return b.flushFn(batch)
	}
	return nil
}

// Len returns the number of records currently buffered.
func (b *Batcher) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer)
}

// drainLocked moves the current buffer into a new slice and resets the buffer.
// The caller must hold b.mu.
func (b *Batcher) drainLocked() []protocol.Record {
	batch := b.buffer
	b.buffer = make([]protocol.Record, 0, b.maxSize)
	b.resetTimerLocked()
	return batch
}

// resetTimerLocked resets the flush timer. The caller must hold b.mu.
func (b *Batcher) resetTimerLocked() {
	b.timer.Reset(b.flushInterval)
}

// timerFlush is invoked by the background timer when the flush interval elapses.
func (b *Batcher) timerFlush() {
	b.mu.Lock()
	if b.closed || len(b.buffer) == 0 {
		if !b.closed {
			b.resetTimerLocked()
		}
		b.mu.Unlock()
		return
	}
	batch := b.drainLocked()
	b.mu.Unlock()
	// Ignore the error from timer-triggered flushes; the next explicit Flush
	// or Add will surface errors.
	_ = b.flushFn(batch)
}
