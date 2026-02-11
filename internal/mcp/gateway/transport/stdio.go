package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

const stdioMaxLineSize = 1 << 20 // 1 MB

// StdioTransport implements newline-delimited JSON-RPC transport over stdin/stdout.
// This is the standard transport for local connector processes spawned by the gateway.
type StdioTransport struct {
	reader  io.Reader
	writer  io.Writer
	writeMu sync.Mutex // serializes writes to stdout
}

// NewStdioTransport creates a stdio transport using os.Stdin and os.Stdout.
func NewStdioTransport() *StdioTransport {
	return &StdioTransport{
		reader: os.Stdin,
		writer: os.Stdout,
	}
}

// NewStdioTransportWithIO creates a stdio transport with custom reader/writer,
// useful for testing or wrapping subprocess pipes.
func NewStdioTransportWithIO(r io.Reader, w io.Writer) *StdioTransport {
	return &StdioTransport{
		reader: r,
		writer: w,
	}
}

// Name returns the transport identifier.
func (t *StdioTransport) Name() string { return "stdio" }

// Start reads JSON-RPC requests from stdin line by line, dispatches them to
// the handler, and writes responses to stdout. It blocks until the context
// is cancelled or stdin reaches EOF.
func (t *StdioTransport) Start(ctx context.Context, handler RequestHandler) error {
	scanner := bufio.NewScanner(t.reader)
	scanner.Buffer(make([]byte, 0, stdioMaxLineSize), stdioMaxLineSize)

	lineCh := make(chan []byte, 16)
	errCh := make(chan error, 1)

	// Read lines in a separate goroutine so we can respect context cancellation
	go func() {
		defer close(lineCh)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			// Copy the line since scanner reuses its buffer
			cp := make([]byte, len(line))
			copy(cp, line)
			select {
			case lineCh <- cp:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errCh <- err
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return fmt.Errorf("stdio read error: %w", err)
		case line, ok := <-lineCh:
			if !ok {
				// stdin closed / EOF
				return nil
			}

			var req JSONRPCRequest
			if err := json.Unmarshal(line, &req); err != nil {
				resp := NewErrorResponse(nil, CodeParseError, "invalid JSON", nil)
				if writeErr := t.writeResponse(resp); writeErr != nil {
					return fmt.Errorf("stdio write error: %w", writeErr)
				}
				continue
			}

			if req.JSONRPC != JSONRPCVersion {
				resp := NewErrorResponse(req.ID, CodeInvalidRequest, "invalid JSON-RPC version", nil)
				if writeErr := t.writeResponse(resp); writeErr != nil {
					return fmt.Errorf("stdio write error: %w", writeErr)
				}
				continue
			}

			resp, err := handler(ctx, &req)
			if err != nil {
				resp = NewErrorResponse(req.ID, CodeInternalError, err.Error(), nil)
			}

			if err := t.writeResponse(resp); err != nil {
				return fmt.Errorf("stdio write error: %w", err)
			}
		}
	}
}

// Stop is a no-op for stdio transport; the process lifecycle manages cleanup.
func (t *StdioTransport) Stop(_ context.Context) error {
	return nil
}

// writeResponse serializes a JSON-RPC response and writes it as a single line to stdout.
func (t *StdioTransport) writeResponse(resp *JSONRPCResponse) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	// Write the JSON followed by a newline delimiter
	if _, err := t.writer.Write(data); err != nil {
		return err
	}
	_, err = t.writer.Write([]byte{'\n'})
	return err
}
