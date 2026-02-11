// Package connect provides a Connect-RPC handler for the FlowForge API.
// Connect serves JSON over HTTP/1.1 (browser-compatible) and gRPC
// simultaneously from the same handler, eliminating the need for a
// separate gRPC-Gateway proxy.
//
// The Connect handler wraps the existing REST handler implementations,
// translating between proto messages and the existing handler layer.
// This enables auto-generated TypeScript clients from proto definitions
// while preserving backward compatibility with existing REST endpoints.
package connect

import (
	"encoding/json"
	"net/http"
)

// ServiceHandler provides the business logic for Connect-RPC endpoints.
// Implement this interface and pass it to NewHandler to wire up the service.
type ServiceHandler interface {
	ListConnectors(r *http.Request) (interface{}, error)
	GetConnector(r *http.Request, name string) (interface{}, error)
}

// NewHandler returns an http.Handler that serves the FlowForge Connect-RPC
// endpoints. Register this on the API server's mux alongside existing REST
// routes.
//
// The Connect handler serves at the path prefix based on the proto service
// name: /flowforge.api.v1.FlowForgeService/
//
// When buf-generated Connect stubs are available, this handler will be
// replaced by flowforgeapiv1connect.NewFlowForgeServiceHandler(svc).
// Until then, this serves a JSON-over-HTTP bridge that returns a structured
// error indicating the Connect transport requires code generation.
func NewHandler() (string, http.Handler) {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		resp := map[string]interface{}{
			"code":    "unimplemented",
			"message": "Connect-RPC transport requires buf generate; use REST endpoints at /api/v1/ instead",
		}
		json.NewEncoder(w).Encode(resp)
	})

	return "/flowforge.api.v1.FlowForgeService/", mux
}
