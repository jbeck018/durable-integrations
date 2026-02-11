// Package scheduler provides cron-based scheduling of FlowForge sync workflows.
// It manages a set of scheduled syncs, parses standard 5-field cron expressions,
// and uses Temporal for durable execution of the scheduled runs.
package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/observability/logging"
	"github.com/flowforge/flowforge/internal/observability/metrics"
)

// TemporalScheduler is the interface for starting scheduled sync workflows.
// This decouples the scheduler from the concrete Temporal client.
type TemporalScheduler interface {
	StartScheduledSync(ctx context.Context, syncID, tenantID, cronExpr string) error
	CancelScheduledSync(ctx context.Context, syncID, tenantID string) error
}

// SyncRepository is the interface for reading sync configuration.
type SyncRepository interface {
	GetScheduledSyncs(ctx context.Context) ([]SyncRecord, error)
}

// SyncRecord represents a sync with schedule information from the repository.
type SyncRecord struct {
	SyncID   string `json:"sync_id"`
	TenantID string `json:"tenant_id"`
	CronExpr string `json:"cron_expr"`
	Status   string `json:"status"`
}

// CronSchedule describes the schedule for a sync workflow.
type CronSchedule struct {
	Expression string    `json:"expression"`
	Timezone   string    `json:"timezone,omitempty"`
	NextRun    time.Time `json:"next_run"`
	LastRun    time.Time `json:"last_run,omitempty"`
}

// ScheduledSync pairs a sync identifier with its schedule.
type ScheduledSync struct {
	SyncID   string       `json:"sync_id"`
	TenantID string       `json:"tenant_id"`
	Schedule CronSchedule `json:"schedule"`
}

// Scheduler manages cron-scheduled sync workflows. It maintains an in-memory
// registry of scheduled syncs and periodically fires due syncs via Temporal.
type Scheduler struct {
	temporal TemporalScheduler
	syncRepo SyncRepository
	metrics  *metrics.Metrics
	logger   *logging.Logger

	mu        sync.RWMutex
	schedules map[string]*ScheduledSync // keyed by syncID
	stopCh    chan struct{}
	stopped   bool
}

// NewScheduler creates a Scheduler. Pass nil for any optional dependency
// (syncRepo, metrics) if it is not available at construction time.
func NewScheduler(temporal TemporalScheduler, syncRepo SyncRepository, m *metrics.Metrics) *Scheduler {
	return &Scheduler{
		temporal:  temporal,
		syncRepo:  syncRepo,
		metrics:   m,
		logger:    logging.Global().WithField("component", "scheduler"),
		schedules: make(map[string]*ScheduledSync),
		stopCh:    make(chan struct{}),
	}
}

// Start begins the scheduling loop. It checks for due syncs every 30 seconds.
// It blocks until the context is cancelled or Stop is called, returning nil
// in the normal shutdown case.
func (s *Scheduler) Start(ctx context.Context) error {
	s.logger.Info("scheduler starting")

	// Load initial schedules from the repository if available.
	if s.syncRepo != nil {
		if err := s.loadFromRepo(ctx); err != nil {
			s.logger.Warn("failed to load initial schedules from repository", "error", err)
		}
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler shutting down via context cancellation")
			return nil
		case <-s.stopCh:
			s.logger.Info("scheduler stopped")
			return nil
		case now := <-ticker.C:
			s.fireDueSyncs(ctx, now)
		}
	}
}

// Stop signals the scheduling loop to terminate.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopped {
		s.stopped = true
		close(s.stopCh)
	}
}

// ScheduleSync registers or updates a sync schedule using a cron expression.
// The cron expression must be a valid 5-field expression (minute hour dom month dow).
func (s *Scheduler) ScheduleSync(syncID string, schedule CronSchedule) error {
	if syncID == "" {
		return fmt.Errorf("syncID is required")
	}
	if schedule.Expression == "" {
		return fmt.Errorf("cron expression is required")
	}

	// Validate the cron expression by computing the next run.
	loc := time.UTC
	if schedule.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(schedule.Timezone)
		if err != nil {
			return fmt.Errorf("invalid timezone %q: %w", schedule.Timezone, err)
		}
	}

	nextRun, err := NextCronTime(schedule.Expression, time.Now().In(loc))
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", schedule.Expression, err)
	}
	schedule.NextRun = nextRun

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.schedules[syncID]
	if exists {
		existing.Schedule = schedule
	} else {
		s.schedules[syncID] = &ScheduledSync{
			SyncID:   syncID,
			Schedule: schedule,
		}
	}

	s.logger.Info("sync scheduled",
		"sync_id", syncID,
		"cron", schedule.Expression,
		"next_run", nextRun.Format(time.RFC3339),
	)

	return nil
}

// UnscheduleSync removes a sync from the schedule registry. If the sync
// is not currently scheduled, this is a no-op.
func (s *Scheduler) UnscheduleSync(syncID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.schedules[syncID]; !exists {
		return nil
	}
	delete(s.schedules, syncID)

	s.logger.Info("sync unscheduled", "sync_id", syncID)
	return nil
}

// ListScheduled returns a snapshot of all currently scheduled syncs,
// sorted by next run time.
func (s *Scheduler) ListScheduled() []ScheduledSync {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]ScheduledSync, 0, len(s.schedules))
	for _, ss := range s.schedules {
		result = append(result, *ss)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Schedule.NextRun.Before(result[j].Schedule.NextRun)
	})
	return result
}

// fireDueSyncs iterates through scheduled syncs and triggers any that are due.
func (s *Scheduler) fireDueSyncs(ctx context.Context, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for syncID, ss := range s.schedules {
		if ss.Schedule.NextRun.After(now) {
			continue
		}

		// Fire the sync via Temporal.
		if s.temporal != nil {
			if err := s.temporal.StartScheduledSync(ctx, syncID, ss.TenantID, ss.Schedule.Expression); err != nil {
				s.logger.Error("failed to start scheduled sync",
					"sync_id", syncID,
					"error", err,
				)
				if s.metrics != nil {
					s.metrics.RecordError(ss.TenantID, "", "scheduler_fire_failed")
				}
				continue
			}
		}

		// Update timing: record last run and compute next run.
		ss.Schedule.LastRun = now

		loc := time.UTC
		if ss.Schedule.Timezone != "" {
			if parsedLoc, err := time.LoadLocation(ss.Schedule.Timezone); err == nil {
				loc = parsedLoc
			}
		}
		nextRun, err := NextCronTime(ss.Schedule.Expression, now.In(loc))
		if err != nil {
			s.logger.Error("failed to compute next run time",
				"sync_id", syncID,
				"error", err,
			)
			continue
		}
		ss.Schedule.NextRun = nextRun

		s.logger.Info("scheduled sync fired",
			"sync_id", syncID,
			"next_run", nextRun.Format(time.RFC3339),
		)
	}
}

// loadFromRepo populates the schedule registry from the persistent store.
func (s *Scheduler) loadFromRepo(ctx context.Context) error {
	syncs, err := s.syncRepo.GetScheduledSyncs(ctx)
	if err != nil {
		return fmt.Errorf("failed to load scheduled syncs: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sr := range syncs {
		if sr.CronExpr == "" || sr.Status != "active" {
			continue
		}
		nextRun, parseErr := NextCronTime(sr.CronExpr, time.Now())
		if parseErr != nil {
			s.logger.Warn("skipping sync with invalid cron expression",
				"sync_id", sr.SyncID,
				"cron", sr.CronExpr,
				"error", parseErr,
			)
			continue
		}
		s.schedules[sr.SyncID] = &ScheduledSync{
			SyncID:   sr.SyncID,
			TenantID: sr.TenantID,
			Schedule: CronSchedule{
				Expression: sr.CronExpr,
				NextRun:    nextRun,
			},
		}
	}

	s.logger.Info("loaded schedules from repository", "count", len(s.schedules))
	return nil
}

// ---------------------------------------------------------------------------
// 5-field Cron Parser
//
// Supports: minute (0-59), hour (0-23), day-of-month (1-31), month (1-12),
// day-of-week (0-6, 0=Sunday). Each field supports: single value, wildcard (*),
// range (1-5), step (*/5 or 1-10/2), and comma-separated lists (1,3,5).
// ---------------------------------------------------------------------------

// cronField stores the set of valid values for one cron field.
type cronField struct {
	values map[int]bool
}

// parseCronExpr parses a 5-field cron expression into five cronField sets.
func parseCronExpr(expr string) ([5]cronField, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != 5 {
		return [5]cronField{}, fmt.Errorf("expected 5 fields, got %d", len(fields))
	}

	ranges := [5][2]int{
		{0, 59}, // minute
		{0, 23}, // hour
		{1, 31}, // day of month
		{1, 12}, // month
		{0, 6},  // day of week
	}

	var result [5]cronField
	for i, field := range fields {
		parsed, err := parseSingleField(field, ranges[i][0], ranges[i][1])
		if err != nil {
			return [5]cronField{}, fmt.Errorf("field %d (%q): %w", i, field, err)
		}
		result[i] = parsed
	}
	return result, nil
}

// parseSingleField parses one cron field (e.g. "*/5", "1,3,5", "10-20/2", "*").
func parseSingleField(field string, min, max int) (cronField, error) {
	cf := cronField{values: make(map[int]bool)}

	// Handle comma-separated list.
	parts := strings.Split(field, ",")
	for _, part := range parts {
		if err := parsePart(part, min, max, cf.values); err != nil {
			return cf, err
		}
	}
	return cf, nil
}

// parsePart parses a single component of a cron field (no commas).
func parsePart(part string, min, max int, values map[int]bool) error {
	// Check for step: "X/Y" or "*/Y" or "A-B/Y"
	stepParts := strings.SplitN(part, "/", 2)
	step := 1
	if len(stepParts) == 2 {
		var err error
		step, err = strconv.Atoi(stepParts[1])
		if err != nil || step <= 0 {
			return fmt.Errorf("invalid step %q", stepParts[1])
		}
		part = stepParts[0]
	}

	// Wildcard.
	if part == "*" {
		for v := min; v <= max; v += step {
			values[v] = true
		}
		return nil
	}

	// Range: "A-B"
	if strings.Contains(part, "-") {
		rangeParts := strings.SplitN(part, "-", 2)
		start, err := strconv.Atoi(rangeParts[0])
		if err != nil {
			return fmt.Errorf("invalid range start %q", rangeParts[0])
		}
		end, err := strconv.Atoi(rangeParts[1])
		if err != nil {
			return fmt.Errorf("invalid range end %q", rangeParts[1])
		}
		if start < min || end > max || start > end {
			return fmt.Errorf("range %d-%d out of bounds [%d, %d]", start, end, min, max)
		}
		for v := start; v <= end; v += step {
			values[v] = true
		}
		return nil
	}

	// Single value.
	val, err := strconv.Atoi(part)
	if err != nil {
		return fmt.Errorf("invalid value %q", part)
	}
	if val < min || val > max {
		return fmt.Errorf("value %d out of bounds [%d, %d]", val, min, max)
	}
	values[val] = true
	return nil
}

// NextCronTime computes the next time at or after 'after' that matches the
// given 5-field cron expression. It searches forward minute-by-minute up to
// 4 years to handle all valid cron patterns, including leap years.
func NextCronTime(expr string, after time.Time) (time.Time, error) {
	fields, err := parseCronExpr(expr)
	if err != nil {
		return time.Time{}, err
	}

	// Start from the next minute boundary.
	t := after.Truncate(time.Minute).Add(time.Minute)

	// Search up to ~4 years forward.
	maxIterations := 4 * 366 * 24 * 60
	for i := 0; i < maxIterations; i++ {
		if fields[3].values[int(t.Month())] &&
			fields[2].values[t.Day()] &&
			fields[4].values[int(t.Weekday())] &&
			fields[1].values[t.Hour()] &&
			fields[0].values[t.Minute()] {
			return t, nil
		}

		// Optimize: if month doesn't match, skip to next month.
		if !fields[3].values[int(t.Month())] {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		// If day doesn't match, skip to next day.
		if !fields[2].values[t.Day()] || !fields[4].values[int(t.Weekday())] {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		// If hour doesn't match, skip to next hour.
		if !fields[1].values[t.Hour()] {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}
		// Otherwise increment by one minute.
		t = t.Add(time.Minute)
	}

	return time.Time{}, fmt.Errorf("no matching time found within 4 years for expression %q", expr)
}
