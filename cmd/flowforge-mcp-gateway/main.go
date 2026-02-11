// FlowForge MCP Gateway — Model Context Protocol server for AI agent integration.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/flowforge/flowforge/internal/common"
)

var version = "dev"

func main() {
	cfg := common.LoadConfig()

	logLevel := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	slog.Info("starting FlowForge MCP Gateway",
		"version", version,
		"port", cfg.MCPGatewayPort,
		"transports", cfg.MCPTransports,
	)

	// The MCP Gateway is initialized and started by internal/mcp/gateway/gateway.go.
	// This main.go handles signal-based lifecycle.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = ctx
	_ = cfg

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-quit
	slog.Info("received shutdown signal", "signal", sig.String())
	cancel()

	fmt.Println("MCP Gateway stopped")
}
