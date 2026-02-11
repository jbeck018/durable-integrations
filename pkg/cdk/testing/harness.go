// Package testing provides a test harness for connector developers to validate
// their Source and Destination implementations against the FlowForge protocol.
package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// SourceTestResult holds the results of testing a source connector.
type SourceTestResult struct {
	SpecValid   bool
	CheckPassed bool
	CheckResult *protocol.CheckResult
	Catalog     *protocol.Catalog
	StreamCount int
	Records     []protocol.Record
	States      []protocol.State
	Logs        []protocol.Log
	RecordCount int
	Errors      []error
}

// DestTestResult holds the results of testing a destination connector.
type DestTestResult struct {
	SpecValid    bool
	CheckPassed  bool
	CheckResult  *protocol.CheckResult
	WriteResult  *protocol.WriteResult
	Capabilities []protocol.DestinationSyncMode
	Errors       []error
}

// TestHarness provides a structured way to test connector implementations.
// It validates protocol compliance and collects test results.
type TestHarness struct {
	t       *testing.T
	timeout time.Duration
}

// NewTestHarness creates a TestHarness bound to the given test instance.
func NewTestHarness(t *testing.T) *TestHarness {
	return &TestHarness{
		t:       t,
		timeout: 30 * time.Second,
	}
}

// WithTimeout sets the timeout for all operations.
func (h *TestHarness) WithTimeout(d time.Duration) *TestHarness {
	h.timeout = d
	return h
}

// TestSource runs a comprehensive test suite against a source connector.
func (h *TestHarness) TestSource(source cdk.Source, config json.RawMessage) *SourceTestResult {
	h.t.Helper()
	result := &SourceTestResult{}

	// Test Spec.
	spec, err := source.Spec()
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("Spec() failed: %w", err))
	} else if spec != nil && len(spec.ConfigSchema) > 0 {
		result.SpecValid = true
	} else {
		result.Errors = append(result.Errors, fmt.Errorf("Spec() returned nil or empty config schema"))
	}

	// Test Check.
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	checkResult, err := source.Check(ctx, config)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("Check() failed: %w", err))
	} else {
		result.CheckResult = checkResult
		result.CheckPassed = checkResult.Status == protocol.CheckStatusSucceeded
	}

	// Test Discover.
	ctx2, cancel2 := context.WithTimeout(context.Background(), h.timeout)
	defer cancel2()

	catalog, err := source.Discover(ctx2, config)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("Discover() failed: %w", err))
	} else {
		result.Catalog = catalog
		result.StreamCount = len(catalog.Streams)
	}

	// Test Read (if we have a catalog).
	if catalog != nil && len(catalog.Streams) > 0 {
		configured := &protocol.ConfiguredCatalog{
			Streams: make([]protocol.ConfiguredStream, 0, len(catalog.Streams)),
		}
		for _, s := range catalog.Streams {
			mode := protocol.SyncModeFullRefresh
			if len(s.SupportedSyncModes) > 0 {
				mode = s.SupportedSyncModes[0]
			}
			configured.Streams = append(configured.Streams, protocol.ConfiguredStream{
				Stream:   s,
				SyncMode: mode,
			})
		}

		ctx3, cancel3 := context.WithTimeout(context.Background(), h.timeout)
		defer cancel3()

		output := make(chan protocol.Message, 1000)
		readErr := make(chan error, 1)
		go func() {
			readErr <- source.Read(ctx3, config, configured, nil, output)
			close(output)
		}()

		for msg := range output {
			switch {
			case msg.Record != nil:
				result.Records = append(result.Records, *msg.Record)
				result.RecordCount++
			case msg.State != nil:
				result.States = append(result.States, *msg.State)
			case msg.Log != nil:
				result.Logs = append(result.Logs, *msg.Log)
			}
		}

		if rErr := <-readErr; rErr != nil {
			result.Errors = append(result.Errors, fmt.Errorf("Read() failed: %w", rErr))
		}
	}

	return result
}

// TestDestination runs a comprehensive test suite against a destination connector.
func (h *TestHarness) TestDestination(dest cdk.Destination, config json.RawMessage) *DestTestResult {
	h.t.Helper()
	result := &DestTestResult{}

	// Test Spec.
	spec, err := dest.Spec()
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("Spec() failed: %w", err))
	} else if spec != nil && len(spec.ConfigSchema) > 0 {
		result.SpecValid = true
	} else {
		result.Errors = append(result.Errors, fmt.Errorf("Spec() returned nil or empty config schema"))
	}

	// Test Check.
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	checkResult, err := dest.Check(ctx, config)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("Check() failed: %w", err))
	} else {
		result.CheckResult = checkResult
		result.CheckPassed = checkResult.Status == protocol.CheckStatusSucceeded
	}

	// Test Capabilities.
	result.Capabilities = dest.Capabilities()

	// Test Write with a sample record.
	ctx2, cancel2 := context.WithTimeout(context.Background(), h.timeout)
	defer cancel2()

	sampleData, _ := json.Marshal(map[string]interface{}{
		"id":   "test-1",
		"name": "Test Record",
	})

	catalog := &protocol.ConfiguredCatalog{
		Streams: []protocol.ConfiguredStream{
			{
				Stream: protocol.Stream{
					Name:               "test_stream",
					SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh},
				},
				SyncMode:            protocol.SyncModeFullRefresh,
				DestinationSyncMode: protocol.DestSyncModeAppend,
			},
		},
	}

	input := make(chan protocol.Message, 1)
	input <- protocol.Message{
		Type: protocol.MessageTypeRecord,
		Record: &protocol.Record{
			Stream:    "test_stream",
			Data:      sampleData,
			EmittedAt: time.Now(),
		},
	}
	close(input)

	writeResult, err := dest.Write(ctx2, config, catalog, input)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("Write() failed: %w", err))
	} else {
		result.WriteResult = writeResult
	}

	return result
}

// AssertValidSpec asserts that the source or destination has a valid spec.
func (h *TestHarness) AssertValidSpec(source cdk.Source) {
	h.t.Helper()
	spec, err := source.Spec()
	if err != nil {
		h.t.Fatalf("Spec() returned error: %v", err)
	}
	if spec == nil {
		h.t.Fatal("Spec() returned nil")
	}
	if len(spec.ConfigSchema) == 0 {
		h.t.Fatal("Spec() returned empty config schema")
	}
}

// AssertCheckSucceeds asserts that Check returns a success status.
func (h *TestHarness) AssertCheckSucceeds(source cdk.Source, config json.RawMessage) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	result, err := source.Check(ctx, config)
	if err != nil {
		h.t.Fatalf("Check() returned error: %v", err)
	}
	if result.Status != protocol.CheckStatusSucceeded {
		h.t.Fatalf("Check() failed: %s", result.Message)
	}
}

// AssertDiscoverReturnsStreams asserts that Discover returns at least one stream.
func (h *TestHarness) AssertDiscoverReturnsStreams(source cdk.Source, config json.RawMessage) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	catalog, err := source.Discover(ctx, config)
	if err != nil {
		h.t.Fatalf("Discover() returned error: %v", err)
	}
	if catalog == nil || len(catalog.Streams) == 0 {
		h.t.Fatal("Discover() returned no streams")
	}
}

// AssertReadProducesRecords asserts that Read produces at least one record.
func (h *TestHarness) AssertReadProducesRecords(source cdk.Source, config json.RawMessage, catalog *protocol.ConfiguredCatalog) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	output := make(chan protocol.Message, 1000)
	readErr := make(chan error, 1)
	go func() {
		readErr <- source.Read(ctx, config, catalog, nil, output)
		close(output)
	}()

	recordCount := 0
	for msg := range output {
		if msg.Record != nil {
			recordCount++
		}
	}

	if rErr := <-readErr; rErr != nil {
		h.t.Fatalf("Read() returned error: %v", rErr)
	}
	if recordCount == 0 {
		h.t.Fatal("Read() produced no records")
	}
}

// AssertWriteSucceeds asserts that Write completes without error.
func (h *TestHarness) AssertWriteSucceeds(dest cdk.Destination, config json.RawMessage, catalog *protocol.ConfiguredCatalog, records []protocol.Message) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	input := make(chan protocol.Message, len(records))
	for _, r := range records {
		input <- r
	}
	close(input)

	result, err := dest.Write(ctx, config, catalog, input)
	if err != nil {
		h.t.Fatalf("Write() returned error: %v", err)
	}
	if result == nil {
		h.t.Fatal("Write() returned nil result")
	}
	if len(result.Errors) > 0 {
		h.t.Fatalf("Write() returned %d errors, first: %s", len(result.Errors), result.Errors[0].Message)
	}
}

// MockSource is a test double for cdk.Source that returns configured responses.
type MockSource struct {
	SpecFn     func() (*protocol.Spec, error)
	CheckFn    func(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error)
	DiscoverFn func(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error)
	ReadFn     func(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error
}

// Spec implements cdk.Source.
func (m *MockSource) Spec() (*protocol.Spec, error) {
	if m.SpecFn != nil {
		return m.SpecFn()
	}
	return &protocol.Spec{
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
	}, nil
}

// Check implements cdk.Source.
func (m *MockSource) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	if m.CheckFn != nil {
		return m.CheckFn(ctx, config)
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded}, nil
}

// Discover implements cdk.Source.
func (m *MockSource) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	if m.DiscoverFn != nil {
		return m.DiscoverFn(ctx, config)
	}
	return &protocol.Catalog{
		Streams: []protocol.Stream{
			{
				Name:               "mock_stream",
				SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh},
				Schema:             json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}}}`),
			},
		},
	}, nil
}

// Read implements cdk.Source.
func (m *MockSource) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	if m.ReadFn != nil {
		return m.ReadFn(ctx, config, catalog, state, output)
	}
	data, _ := json.Marshal(map[string]interface{}{"id": "mock-1", "value": "test"})
	output <- protocol.Message{
		Type: protocol.MessageTypeRecord,
		Record: &protocol.Record{
			Stream:    "mock_stream",
			Data:      data,
			EmittedAt: time.Now(),
		},
	}
	return nil
}

// MockDestination is a test double for cdk.Destination.
type MockDestination struct {
	SpecFn         func() (*protocol.Spec, error)
	CheckFn        func(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error)
	WriteFn        func(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error)
	CapabilitiesFn func() []protocol.DestinationSyncMode
}

// Spec implements cdk.Destination.
func (m *MockDestination) Spec() (*protocol.Spec, error) {
	if m.SpecFn != nil {
		return m.SpecFn()
	}
	return &protocol.Spec{
		ConfigSchema: json.RawMessage(`{"type":"object"}`),
	}, nil
}

// Check implements cdk.Destination.
func (m *MockDestination) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	if m.CheckFn != nil {
		return m.CheckFn(ctx, config)
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded}, nil
}

// Write implements cdk.Destination.
func (m *MockDestination) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	if m.WriteFn != nil {
		return m.WriteFn(ctx, config, catalog, input)
	}
	var count int64
	for msg := range input {
		if msg.Record != nil {
			count++
		}
	}
	return &protocol.WriteResult{RecordsWritten: count}, nil
}

// Capabilities implements cdk.Destination.
func (m *MockDestination) Capabilities() []protocol.DestinationSyncMode {
	if m.CapabilitiesFn != nil {
		return m.CapabilitiesFn()
	}
	return []protocol.DestinationSyncMode{
		protocol.DestSyncModeAppend,
		protocol.DestSyncModeUpsert,
	}
}
