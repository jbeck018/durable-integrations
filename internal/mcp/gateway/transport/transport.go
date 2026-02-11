// Package transport defines the transport layer for the MCP gateway.
// It provides the Transport interface and JSON-RPC 2.0 message types used
// by all transport implementations (SSE, WebSocket, stdio).
package transport

import (
	"context"
	"encoding/json"
)

// JSONRPCVersion is the JSON-RPC protocol version.
const JSONRPCVersion = "2.0"

// Standard JSON-RPC error codes per the specification.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// JSONRPCRequest represents a JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// JSONRPCResponse represents a JSON-RPC 2.0 response.
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC 2.0 error object.
type JSONRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// JSONRPCNotification represents a JSON-RPC 2.0 notification (no ID).
type JSONRPCNotification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// NewResponse creates a successful JSON-RPC response with the given result.
func NewResponse(id json.RawMessage, result interface{}) (*JSONRPCResponse, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &JSONRPCResponse{
		JSONRPC: JSONRPCVersion,
		ID:      id,
		Result:  data,
	}, nil
}

// NewErrorResponse creates a JSON-RPC error response.
func NewErrorResponse(id json.RawMessage, code int, message string, data interface{}) *JSONRPCResponse {
	resp := &JSONRPCResponse{
		JSONRPC: JSONRPCVersion,
		ID:      id,
		Error: &JSONRPCError{
			Code:    code,
			Message: message,
		},
	}
	if data != nil {
		if raw, err := json.Marshal(data); err == nil {
			resp.Error.Data = raw
		}
	}
	return resp
}

// RequestHandler is the callback invoked by a transport when it receives a request.
// The handler processes the request and returns a response.
type RequestHandler func(ctx context.Context, req *JSONRPCRequest) (*JSONRPCResponse, error)

// Transport defines the interface that all MCP transport implementations must satisfy.
type Transport interface {
	// Start begins listening for incoming connections and dispatching requests
	// to the provided handler. It blocks until the context is cancelled or
	// a fatal error occurs.
	Start(ctx context.Context, handler RequestHandler) error

	// Stop gracefully shuts down the transport, closing all active connections.
	Stop(ctx context.Context) error

	// Name returns the transport type identifier (e.g., "sse", "websocket", "stdio").
	Name() string
}
