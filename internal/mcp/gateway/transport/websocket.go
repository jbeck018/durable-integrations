package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsPingInterval  = 30 * time.Second
	wsPongTimeout   = 10 * time.Second
	wsWriteTimeout  = 10 * time.Second
	wsReadLimit     = 1 << 20 // 1 MB
)

// wsConn tracks a single WebSocket connection.
type wsConn struct {
	id   string
	conn *websocket.Conn
	mu   sync.Mutex // serializes writes
	done chan struct{}
	closeOnce sync.Once
}

func (c *wsConn) writeJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	return c.conn.WriteJSON(v)
}

func (c *wsConn) writePing() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	return c.conn.WriteMessage(websocket.PingMessage, nil)
}

func (c *wsConn) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

// WSTransport implements full-duplex WebSocket transport for the MCP gateway.
type WSTransport struct {
	addr     string
	server   *http.Server
	handler  RequestHandler
	upgrader websocket.Upgrader

	mu      sync.RWMutex
	conns   map[string]*wsConn
	nextID  atomic.Uint64
}

// NewWSTransport creates a new WebSocket transport listening on the given address.
func NewWSTransport(addr string) *WSTransport {
	return &WSTransport{
		addr:  addr,
		conns: make(map[string]*wsConn),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

// Name returns the transport identifier.
func (t *WSTransport) Name() string { return "websocket" }

// Start begins the WebSocket HTTP server. It blocks until the context is cancelled.
func (t *WSTransport) Start(ctx context.Context, handler RequestHandler) error {
	t.handler = handler

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", t.handleWS)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","transport":"websocket"}`))
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
		return fmt.Errorf("websocket transport failed: %w", err)
	case <-ctx.Done():
		return t.Stop(context.Background())
	}
}

// Stop gracefully shuts down the WebSocket transport and closes all connections.
func (t *WSTransport) Stop(ctx context.Context) error {
	t.mu.Lock()
	for _, c := range t.conns {
		c.close()
	}
	t.conns = make(map[string]*wsConn)
	t.mu.Unlock()

	if t.server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return t.server.Shutdown(shutdownCtx)
	}
	return nil
}

// handleWS upgrades an HTTP connection to WebSocket and starts processing.
func (t *WSTransport) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := t.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}

	connID := fmt.Sprintf("ws_%d", t.nextID.Add(1))
	wc := &wsConn{
		id:   connID,
		conn: conn,
		done: make(chan struct{}),
	}

	t.mu.Lock()
	t.conns[connID] = wc
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		delete(t.conns, connID)
		t.mu.Unlock()
		wc.close()
	}()

	conn.SetReadLimit(wsReadLimit)
	_ = conn.SetReadDeadline(time.Now().Add(wsPingInterval + wsPongTimeout))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(wsPingInterval + wsPongTimeout))
		return nil
	})

	// Start ping loop
	go t.pingLoop(wc)

	// Read loop
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("websocket %s read error: %v", connID, err)
			}
			return
		}

		var req JSONRPCRequest
		if err := json.Unmarshal(message, &req); err != nil {
			errResp := NewErrorResponse(nil, CodeParseError, "invalid JSON", nil)
			_ = wc.writeJSON(errResp)
			continue
		}

		if req.JSONRPC != JSONRPCVersion {
			errResp := NewErrorResponse(req.ID, CodeInvalidRequest, "invalid JSON-RPC version", nil)
			_ = wc.writeJSON(errResp)
			continue
		}

		// Process request asynchronously to avoid blocking the read loop
		go func(request JSONRPCRequest) {
			resp, handleErr := t.handler(r.Context(), &request)
			if handleErr != nil {
				resp = NewErrorResponse(request.ID, CodeInternalError, handleErr.Error(), nil)
			}
			if writeErr := wc.writeJSON(resp); writeErr != nil {
				log.Printf("websocket %s write error: %v", connID, writeErr)
			}
		}(req)
	}
}

// pingLoop sends periodic ping frames to keep the connection alive.
func (t *WSTransport) pingLoop(wc *wsConn) {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-wc.done:
			return
		case <-ticker.C:
			if err := wc.writePing(); err != nil {
				wc.close()
				return
			}
		}
	}
}
