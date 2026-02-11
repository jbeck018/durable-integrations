// Package protocol provides the message handler that routes and processes
// protocol messages flowing between connectors and the FlowForge platform.
package protocol

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/flowforge/flowforge/pkg/protocol"
)

// RecordCallback is invoked for each RECORD message.
type RecordCallback func(record *protocol.Record)

// StateCallback is invoked for each STATE message.
type StateCallback func(state *protocol.State)

// LogCallback is invoked for each LOG message.
type LogCallback func(log *protocol.Log)

// ControlCallback is invoked for each CONTROL message.
type ControlCallback func(control *protocol.Control)

// SchemaCallback is invoked for each SCHEMA message.
type SchemaCallback func(schema *protocol.SchemaMessage)

// MessageHandler processes protocol messages from connectors, dispatching each
// message type to the appropriate registered callback. It supports buffered
// batch accumulation for records and provides metrics on processed messages.
type MessageHandler struct {
	mu sync.RWMutex

	onRecord  RecordCallback
	onState   StateCallback
	onLog     LogCallback
	onControl ControlCallback
	onSchema  SchemaCallback

	// Batch accumulation
	batchSize    int
	batchTimeout time.Duration
	batchBuf     []protocol.Record
	batchTimer   *time.Timer
	batchFlush   func(records []protocol.Record)

	// Metrics
	recordCount  int64
	stateCount   int64
	logCount     int64
	controlCount int64
	schemaCount  int64
	errorCount   int64
}

// HandlerOption configures a MessageHandler.
type HandlerOption func(*MessageHandler)

// WithRecordHandler sets the callback for RECORD messages.
func WithRecordHandler(fn RecordCallback) HandlerOption {
	return func(h *MessageHandler) { h.onRecord = fn }
}

// WithStateHandler sets the callback for STATE messages.
func WithStateHandler(fn StateCallback) HandlerOption {
	return func(h *MessageHandler) { h.onState = fn }
}

// WithLogHandler sets the callback for LOG messages.
func WithLogHandler(fn LogCallback) HandlerOption {
	return func(h *MessageHandler) { h.onLog = fn }
}

// WithControlHandler sets the callback for CONTROL messages.
func WithControlHandler(fn ControlCallback) HandlerOption {
	return func(h *MessageHandler) { h.onControl = fn }
}

// WithSchemaHandler sets the callback for SCHEMA messages.
func WithSchemaHandler(fn SchemaCallback) HandlerOption {
	return func(h *MessageHandler) { h.onSchema = fn }
}

// WithBatchAccumulation enables record batching. Records are accumulated until
// batchSize records are collected or batchTimeout elapses, whichever comes
// first, then flushFn is called with the batch.
func WithBatchAccumulation(batchSize int, batchTimeout time.Duration, flushFn func(records []protocol.Record)) HandlerOption {
	return func(h *MessageHandler) {
		h.batchSize = batchSize
		h.batchTimeout = batchTimeout
		h.batchFlush = flushFn
		h.batchBuf = make([]protocol.Record, 0, batchSize)
	}
}

// NewMessageHandler creates a MessageHandler with the given options.
func NewMessageHandler(opts ...HandlerOption) *MessageHandler {
	h := &MessageHandler{
		batchSize: 0, // batching disabled by default
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// MessageStats holds counters for processed message types.
type MessageStats struct {
	Records  int64 `json:"records"`
	States   int64 `json:"states"`
	Logs     int64 `json:"logs"`
	Controls int64 `json:"controls"`
	Schemas  int64 `json:"schemas"`
	Errors   int64 `json:"errors"`
}

// Stats returns a snapshot of message processing counters.
func (h *MessageHandler) Stats() MessageStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return MessageStats{
		Records:  h.recordCount,
		States:   h.stateCount,
		Logs:     h.logCount,
		Controls: h.controlCount,
		Schemas:  h.schemaCount,
		Errors:   h.errorCount,
	}
}

// RouteMessage dispatches a protocol message to the appropriate handler
// based on its type. Returns an error if the message type is unknown or
// if the payload is missing.
func (h *MessageHandler) RouteMessage(msg protocol.Message) error {
	switch msg.Type {
	case protocol.MessageTypeRecord:
		return h.handleRecord(msg)
	case protocol.MessageTypeState:
		return h.handleState(msg)
	case protocol.MessageTypeLog:
		return h.handleLog(msg)
	case protocol.MessageTypeControl:
		return h.handleControl(msg)
	case protocol.MessageTypeSchema:
		return h.handleSchema(msg)
	default:
		h.mu.Lock()
		h.errorCount++
		h.mu.Unlock()
		return fmt.Errorf("unhandled message type: %s", msg.Type)
	}
}

// ProcessChannel reads all messages from the channel and routes each one.
// It returns the first routing error encountered, or nil if the channel
// closes cleanly.
func (h *MessageHandler) ProcessChannel(ch <-chan protocol.Message) error {
	for msg := range ch {
		if err := h.RouteMessage(msg); err != nil {
			return err
		}
	}
	// Flush any remaining buffered records.
	h.FlushBatch()
	return nil
}

// FlushBatch forces a flush of any accumulated record batch.
func (h *MessageHandler) FlushBatch() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.flushBatchLocked()
}

func (h *MessageHandler) flushBatchLocked() {
	if h.batchFlush == nil || len(h.batchBuf) == 0 {
		return
	}
	if h.batchTimer != nil {
		h.batchTimer.Stop()
		h.batchTimer = nil
	}
	batch := make([]protocol.Record, len(h.batchBuf))
	copy(batch, h.batchBuf)
	h.batchBuf = h.batchBuf[:0]
	// Release lock during flush callback to avoid blocking producers.
	h.mu.Unlock()
	h.batchFlush(batch)
	h.mu.Lock()
}

func (h *MessageHandler) handleRecord(msg protocol.Message) error {
	if msg.Record == nil {
		h.mu.Lock()
		h.errorCount++
		h.mu.Unlock()
		return fmt.Errorf("RECORD message has nil record payload")
	}

	h.mu.Lock()
	h.recordCount++

	// If batch accumulation is enabled, buffer the record.
	if h.batchFlush != nil && h.batchSize > 0 {
		h.batchBuf = append(h.batchBuf, *msg.Record)
		if len(h.batchBuf) >= h.batchSize {
			h.flushBatchLocked()
		} else if h.batchTimer == nil && h.batchTimeout > 0 {
			h.batchTimer = time.AfterFunc(h.batchTimeout, func() {
				h.FlushBatch()
			})
		}
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()

	if h.onRecord != nil {
		h.onRecord(msg.Record)
	}
	return nil
}

func (h *MessageHandler) handleState(msg protocol.Message) error {
	if msg.State == nil {
		h.mu.Lock()
		h.errorCount++
		h.mu.Unlock()
		return fmt.Errorf("STATE message has nil state payload")
	}
	h.mu.Lock()
	h.stateCount++
	h.mu.Unlock()

	if h.onState != nil {
		h.onState(msg.State)
	}
	return nil
}

func (h *MessageHandler) handleLog(msg protocol.Message) error {
	if msg.Log == nil {
		h.mu.Lock()
		h.errorCount++
		h.mu.Unlock()
		return fmt.Errorf("LOG message has nil log payload")
	}
	h.mu.Lock()
	h.logCount++
	h.mu.Unlock()

	if h.onLog != nil {
		h.onLog(msg.Log)
	}
	return nil
}

func (h *MessageHandler) handleControl(msg protocol.Message) error {
	if msg.Control == nil {
		h.mu.Lock()
		h.errorCount++
		h.mu.Unlock()
		return fmt.Errorf("CONTROL message has nil control payload")
	}
	h.mu.Lock()
	h.controlCount++
	h.mu.Unlock()

	if h.onControl != nil {
		h.onControl(msg.Control)
	}
	return nil
}

func (h *MessageHandler) handleSchema(msg protocol.Message) error {
	if msg.Schema == nil {
		h.mu.Lock()
		h.errorCount++
		h.mu.Unlock()
		return fmt.Errorf("SCHEMA message has nil schema payload")
	}
	h.mu.Lock()
	h.schemaCount++
	h.mu.Unlock()

	if h.onSchema != nil {
		h.onSchema(msg.Schema)
	}
	return nil
}

// DecodeMessages decodes newline-delimited JSON protocol messages from raw
// bytes, returning the parsed messages. This is useful when reading from
// subprocess stdout or file-based transports.
func DecodeMessages(data []byte) ([]protocol.Message, error) {
	var messages []protocol.Message
	dec := json.NewDecoder(jsonBytesReader(data))
	for dec.More() {
		var msg protocol.Message
		if err := dec.Decode(&msg); err != nil {
			return messages, fmt.Errorf("decode protocol message: %w", err)
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// jsonBytesReader wraps a byte slice as an io.Reader for json.NewDecoder.
type jsonReader struct {
	data []byte
	pos  int
}

func jsonBytesReader(data []byte) *jsonReader {
	return &jsonReader{data: data}
}

func (r *jsonReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
