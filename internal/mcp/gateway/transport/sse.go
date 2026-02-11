package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	sseHeartbeatInterval = 30 * time.Second
	sseWriteTimeout      = 10 * time.Second
	sseMaxRequestBody    = 1 << 20 // 1 MB
)

// sseClient tracks a single SSE connection.
type sseClient struct {
	id        string
	events    chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func (c *sseClient) close() {
	c.closeOnce.Do(func() {
		close(c.done)
	})
}

// SSETransport implements Server-Sent Events transport for the MCP gateway.
// It exposes two HTTP endpoints: GET /sse for establishing the event stream,
// and POST /message for receiving JSON-RPC requests from clients.
type SSETransport struct {
	addr    string
	server  *http.Server
	handler RequestHandler

	mu      sync.RWMutex
	clients map[string]*sseClient
	nextID  atomic.Uint64
}

// NewSSETransport creates a new SSE transport listening on the given address.
func NewSSETransport(addr string) *SSETransport {
	return &SSETransport{
		addr:    addr,
		clients: make(map[string]*sseClient),
	}
}

// Name returns the transport identifier.
func (t *SSETransport) Name() string { return "sse" }

// Start begins the SSE HTTP server. It blocks until the context is cancelled.
func (t *SSETransport) Start(ctx context.Context, handler RequestHandler) error {
	t.handler = handler

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", t.handleSSE)
	mux.HandleFunc("/message", t.handleMessage)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","transport":"sse"}`))
	})

	t.server = &http.Server{
		Addr:              t.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := t.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("sse transport failed: %w", err)
	case <-ctx.Done():
		return t.Stop(context.Background())
	}
}

// Stop gracefully shuts down the SSE transport and closes all client connections.
func (t *SSETransport) Stop(ctx context.Context) error {
	t.mu.Lock()
	for _, c := range t.clients {
		c.close()
	}
	t.clients = make(map[string]*sseClient)
	t.mu.Unlock()

	if t.server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return t.server.Shutdown(shutdownCtx)
	}
	return nil
}

// handleSSE establishes a new SSE connection for a client.
func (t *SSETransport) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	clientID := fmt.Sprintf("sse_%d", t.nextID.Add(1))
	client := &sseClient{
		id:     clientID,
		events: make(chan []byte, 64),
		done:   make(chan struct{}),
	}

	t.mu.Lock()
	t.clients[clientID] = client
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		delete(t.clients, clientID)
		t.mu.Unlock()
		client.close()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-SSE-Client-ID", clientID)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Send the endpoint event so the client knows where to POST messages
	_, _ = fmt.Fprintf(w, "event: endpoint\ndata: /message?client_id=%s\n\n", clientID)
	flusher.Flush()

	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-client.done:
			return
		case data := <-client.events:
			_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
			flusher.Flush()
		case <-heartbeat.C:
			_, _ = fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

// handleMessage processes incoming JSON-RPC requests POSTed by SSE clients.
func (t *SSETransport) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clientID := r.URL.Query().Get("client_id")

	body, err := io.ReadAll(io.LimitReader(r.Body, sseMaxRequestBody))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	defer r.Body.Close()

	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON-RPC request")
		return
	}

	if req.JSONRPC != JSONRPCVersion {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON-RPC version")
		return
	}

	resp, err := t.handler(r.Context(), &req)
	if err != nil {
		resp = NewErrorResponse(req.ID, CodeInternalError, err.Error(), nil)
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to marshal response")
		return
	}

	// If the client has an SSE connection, also push the response over the stream
	if clientID != "" {
		t.mu.RLock()
		client, ok := t.clients[clientID]
		t.mu.RUnlock()
		if ok {
			select {
			case client.events <- respBytes:
			default:
				log.Printf("SSE client %s event buffer full, dropping response", clientID)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp := NewErrorResponse(nil, CodeInvalidRequest, message, nil)
	data, _ := json.Marshal(resp)
	_, _ = w.Write(data)
}
