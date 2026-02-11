// Package signals defines Temporal signal types and helpers for controlling
// sync workflows at runtime. Signals provide a mechanism for external callers
// to pause, resume, cancel, or modify running workflows without termination.
package signals

import (
	"encoding/json"
)

// Signal channel names used across all workflows.
const (
	SignalPause  = "sync:pause"
	SignalResume = "sync:resume"
	SignalCancel = "sync:cancel"
	SignalModify = "sync:modify"
)

// PauseSignal requests a workflow to pause at its next safe checkpoint.
type PauseSignal struct {
	Reason    string `json:"reason"`
	RequestID string `json:"request_id"`
}

// ResumeSignal requests a paused workflow to resume execution.
type ResumeSignal struct {
	RequestID string `json:"request_id"`
}

// CancelSignal requests a workflow to perform graceful cancellation with cleanup.
type CancelSignal struct {
	Reason    string `json:"reason"`
	RequestID string `json:"request_id"`
	SaveState bool   `json:"save_state"`
}

// ModifySignal requests a running workflow to change its parameters.
// Only fields present in the Changes map will be updated.
type ModifySignal struct {
	Changes   map[string]json.RawMessage `json:"changes"`
	RequestID string                     `json:"request_id"`
}

// SignalState tracks pending signal state within a workflow.
type SignalState struct {
	PauseRequested  bool
	CancelRequested bool
	PauseReason     string
	CancelReason    string
	SaveStateOnCancel bool
	PendingModify   *ModifySignal
}

// NewSignalState creates a zero-valued SignalState ready for use.
func NewSignalState() *SignalState {
	return &SignalState{}
}

// ApplyPause records that a pause signal was received.
func (s *SignalState) ApplyPause(sig PauseSignal) {
	s.PauseRequested = true
	s.PauseReason = sig.Reason
}

// ApplyResume clears the paused state.
func (s *SignalState) ApplyResume() {
	s.PauseRequested = false
	s.PauseReason = ""
}

// ApplyCancel records that a cancel signal was received.
func (s *SignalState) ApplyCancel(sig CancelSignal) {
	s.CancelRequested = true
	s.CancelReason = sig.Reason
	s.SaveStateOnCancel = sig.SaveState
}

// ApplyModify stores a pending modification for the workflow to consume.
func (s *SignalState) ApplyModify(sig ModifySignal) {
	s.PendingModify = &sig
}

// ConsumeModify returns the pending modification and clears it, or returns nil
// if no modification is pending.
func (s *SignalState) ConsumeModify() *ModifySignal {
	m := s.PendingModify
	s.PendingModify = nil
	return m
}

// IsPaused returns true if the workflow should be in a paused state.
func (s *SignalState) IsPaused() bool {
	return s.PauseRequested
}

// IsCancelled returns true if the workflow should perform graceful cancellation.
func (s *SignalState) IsCancelled() bool {
	return s.CancelRequested
}
