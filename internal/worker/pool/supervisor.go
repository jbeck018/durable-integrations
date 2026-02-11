package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// HealthStatus represents the health of a pool.
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// PoolHealth contains health information for a single pool.
type PoolHealth struct {
	Name       string       `json:"name"`
	Status     HealthStatus `json:"status"`
	Stats      PoolStats    `json:"stats"`
	QueueDepth int          `json:"queue_depth"`
}

// ScaleConfig defines auto-scaling thresholds for a pool.
type ScaleConfig struct {
	MinWorkers        int
	MaxWorkers        int
	ScaleUpThreshold  int // queue depth above which to scale up
	ScaleDownThreshold int // queue depth below which to scale down
	CooldownPeriod    time.Duration
}

// managedPool wraps a pool-like interface so the supervisor can manage pools
// of any item type through a common abstraction.
type managedPool struct {
	name        string
	statsFunc   func() PoolStats
	resizeFunc  func(ctx context.Context, n int)
	shutdownFn  func(ctx context.Context) error
	startFn     func(ctx context.Context)
	concurrency func() int
	queueDepth  func() int
	scaleConfig *ScaleConfig
	lastScale   time.Time
}

// Supervisor manages multiple named worker pools, provides health checks,
// and handles automatic scaling based on queue depth thresholds.
type Supervisor struct {
	mu    sync.RWMutex
	pools map[string]*managedPool

	ctx    context.Context
	cancel context.CancelFunc

	autoScaleInterval time.Duration
	running           bool
}

// NewSupervisor creates a new pool supervisor. The autoScaleInterval controls
// how often the supervisor checks queue depths for scaling decisions.
func NewSupervisor(autoScaleInterval time.Duration) *Supervisor {
	if autoScaleInterval <= 0 {
		autoScaleInterval = 5 * time.Second
	}
	return &Supervisor{
		pools:             make(map[string]*managedPool),
		autoScaleInterval: autoScaleInterval,
	}
}

// Register adds a pool to the supervisor. The pool is wrapped via closures so
// the supervisor can work with Pool[T] for any T.
func Register[T any](s *Supervisor, p *Pool[T], scaleConfig *ScaleConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pools[p.Name()]; exists {
		return fmt.Errorf("pool %q already registered", p.Name())
	}
	mp := &managedPool{
		name:        p.Name(),
		statsFunc:   p.Stats,
		resizeFunc:  p.Resize,
		shutdownFn:  p.Shutdown,
		startFn:     p.Start,
		concurrency: p.Concurrency,
		queueDepth:  p.QueueDepth,
		scaleConfig: scaleConfig,
	}
	s.pools[p.Name()] = mp
	return nil
}

// Start starts all registered pools and begins the auto-scaling loop.
func (s *Supervisor) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.running = true

	for _, mp := range s.pools {
		mp.startFn(s.ctx)
	}
	s.mu.Unlock()

	go s.autoScaleLoop()
}

// Stop shuts down all managed pools with the given context deadline.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	if s.cancel != nil {
		s.cancel()
	}
	poolsCopy := make(map[string]*managedPool, len(s.pools))
	for k, v := range s.pools {
		poolsCopy[k] = v
	}
	s.mu.Unlock()

	var errs []error
	for name, mp := range poolsCopy {
		if err := mp.shutdownFn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("pool %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// ScaleUp increases the concurrency of a named pool by the given delta.
func (s *Supervisor) ScaleUp(name string, delta int) error {
	s.mu.RLock()
	mp, ok := s.pools[name]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pool %q not found", name)
	}
	newSize := mp.concurrency() + delta
	if mp.scaleConfig != nil && newSize > mp.scaleConfig.MaxWorkers {
		newSize = mp.scaleConfig.MaxWorkers
	}
	mp.resizeFunc(s.ctx, newSize)
	mp.lastScale = time.Now()
	return nil
}

// ScaleDown decreases the concurrency of a named pool by the given delta.
func (s *Supervisor) ScaleDown(name string, delta int) error {
	s.mu.RLock()
	mp, ok := s.pools[name]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("pool %q not found", name)
	}
	newSize := mp.concurrency() - delta
	if mp.scaleConfig != nil && newSize < mp.scaleConfig.MinWorkers {
		newSize = mp.scaleConfig.MinWorkers
	}
	if newSize < 1 {
		newSize = 1
	}
	mp.resizeFunc(s.ctx, newSize)
	mp.lastScale = time.Now()
	return nil
}

// HealthCheck returns the health status of all managed pools.
func (s *Supervisor) HealthCheck() map[string]PoolHealth {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]PoolHealth, len(s.pools))
	for name, mp := range s.pools {
		stats := mp.statsFunc()
		depth := mp.queueDepth()
		status := HealthStatusHealthy

		if mp.scaleConfig != nil {
			if depth > mp.scaleConfig.ScaleUpThreshold*2 {
				status = HealthStatusUnhealthy
			} else if depth > mp.scaleConfig.ScaleUpThreshold {
				status = HealthStatusDegraded
			}
		} else {
			concurrency := mp.concurrency()
			if concurrency > 0 && depth > concurrency*4 {
				status = HealthStatusUnhealthy
			} else if concurrency > 0 && depth > concurrency*2 {
				status = HealthStatusDegraded
			}
		}

		if stats.Failed > 0 && stats.Completed > 0 {
			failRate := float64(stats.Failed) / float64(stats.Failed+stats.Completed)
			if failRate > 0.5 {
				status = HealthStatusUnhealthy
			} else if failRate > 0.1 {
				status = HealthStatusDegraded
			}
		}

		result[name] = PoolHealth{
			Name:       name,
			Status:     status,
			Stats:      stats,
			QueueDepth: depth,
		}
	}
	return result
}

// PoolNames returns the names of all registered pools.
func (s *Supervisor) PoolNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.pools))
	for n := range s.pools {
		names = append(names, n)
	}
	return names
}

// autoScaleLoop periodically checks queue depths and adjusts pool sizes.
func (s *Supervisor) autoScaleLoop() {
	ticker := time.NewTicker(s.autoScaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.evaluateScaling()
		}
	}
}

// evaluateScaling checks each pool's queue depth against its scale config
// and adjusts worker counts when thresholds are crossed and cooldown has elapsed.
func (s *Supervisor) evaluateScaling() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	for _, mp := range s.pools {
		if mp.scaleConfig == nil {
			continue
		}
		if now.Sub(mp.lastScale) < mp.scaleConfig.CooldownPeriod {
			continue
		}

		depth := mp.queueDepth()
		current := mp.concurrency()

		if depth > mp.scaleConfig.ScaleUpThreshold && current < mp.scaleConfig.MaxWorkers {
			// Scale up by 50% or to max, whichever is smaller.
			increase := current / 2
			if increase < 1 {
				increase = 1
			}
			newSize := current + increase
			if newSize > mp.scaleConfig.MaxWorkers {
				newSize = mp.scaleConfig.MaxWorkers
			}
			mp.resizeFunc(s.ctx, newSize)
			mp.lastScale = now
		} else if depth < mp.scaleConfig.ScaleDownThreshold && current > mp.scaleConfig.MinWorkers {
			// Scale down by 25% or to min, whichever is larger.
			decrease := current / 4
			if decrease < 1 {
				decrease = 1
			}
			newSize := current - decrease
			if newSize < mp.scaleConfig.MinWorkers {
				newSize = mp.scaleConfig.MinWorkers
			}
			mp.resizeFunc(s.ctx, newSize)
			mp.lastScale = now
		}
	}
}
