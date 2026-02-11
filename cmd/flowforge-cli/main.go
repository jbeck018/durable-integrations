// Command flowforge-cli is the command-line interface for managing FlowForge
// syncs, connectors, tenants, and schemas. It communicates with the FlowForge
// API via HTTP and supports subcommands in a cobra-style structure implemented
// with the standard library flag package.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// version is set at build time via ldflags.
var version = "dev"

// defaultAPIBase is the default FlowForge API URL.
const defaultAPIBase = "http://localhost:8080"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}

	switch args[0] {
	case "sync":
		return handleSync(args[1:])
	case "connector":
		return handleConnector(args[1:])
	case "tenant":
		return handleTenant(args[1:])
	case "schema":
		return handleSchema(args[1:])
	case "version":
		fmt.Printf("flowforge-cli %s\n", version)
		return nil
	case "help", "--help", "-h":
		printUsage()
		return nil
	default:
		return fmt.Errorf("unknown command %q. Run 'flowforge-cli help' for usage", args[0])
	}
}

func printUsage() {
	fmt.Println(`flowforge-cli - FlowForge command-line interface

Usage:
  flowforge-cli <command> <subcommand> [flags]

Commands:
  sync        Manage data syncs (list, create, trigger, pause, resume, status)
  connector   Manage connectors (list, test)
  tenant      Manage tenants (list, create)
  schema      Manage schemas (discover, diff)
  version     Print version information
  help        Show this help message

Environment:
  FLOWFORGE_API_URL   Base URL of the FlowForge API (default: http://localhost:8080)
  FLOWFORGE_API_TOKEN API authentication token`)
}

// ---------------------------------------------------------------------------
// sync subcommands
// ---------------------------------------------------------------------------

func handleSync(args []string) error {
	if len(args) == 0 {
		fmt.Println(`Usage: flowforge-cli sync <subcommand>

Subcommands:
  list      List all syncs
  create    Create a new sync
  trigger   Trigger a sync run
  pause     Pause a running sync
  resume    Resume a paused sync
  status    Get sync status`)
		return nil
	}

	switch args[0] {
	case "list":
		return syncList(args[1:])
	case "create":
		return syncCreate(args[1:])
	case "trigger":
		return syncTrigger(args[1:])
	case "pause":
		return syncPause(args[1:])
	case "resume":
		return syncResume(args[1:])
	case "status":
		return syncStatus(args[1:])
	default:
		return fmt.Errorf("unknown sync subcommand %q", args[0])
	}
}

func syncList(args []string) error {
	tenantID := flagValue(args, "--tenant", "")
	path := "/api/v1/syncs"
	if tenantID != "" {
		path += "?tenant_id=" + tenantID
	}

	resp, err := apiGet(path)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func syncCreate(args []string) error {
	sourceID := flagValue(args, "--source", "")
	destID := flagValue(args, "--dest", "")
	tenantID := flagValue(args, "--tenant", "")
	schedule := flagValue(args, "--schedule", "")

	if sourceID == "" || destID == "" || tenantID == "" {
		return fmt.Errorf("usage: flowforge-cli sync create --source <id> --dest <id> --tenant <id> [--schedule <cron>]")
	}

	body := map[string]interface{}{
		"source_connection_id": sourceID,
		"dest_connection_id":   destID,
		"tenant_id":            tenantID,
	}
	if schedule != "" {
		body["schedule"] = map[string]string{"expression": schedule}
	}

	resp, err := apiPost("/api/v1/syncs", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func syncTrigger(args []string) error {
	syncID := flagValue(args, "--id", "")
	if syncID == "" && len(args) > 0 && !strings.HasPrefix(args[0], "--") {
		syncID = args[0]
	}
	if syncID == "" {
		return fmt.Errorf("usage: flowforge-cli sync trigger --id <sync-id>")
	}

	resp, err := apiPost(fmt.Sprintf("/api/v1/syncs/%s/trigger", syncID), nil)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func syncPause(args []string) error {
	syncID := flagValue(args, "--id", "")
	if syncID == "" && len(args) > 0 && !strings.HasPrefix(args[0], "--") {
		syncID = args[0]
	}
	if syncID == "" {
		return fmt.Errorf("usage: flowforge-cli sync pause --id <sync-id>")
	}

	resp, err := apiPost(fmt.Sprintf("/api/v1/syncs/%s/pause", syncID), nil)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func syncResume(args []string) error {
	syncID := flagValue(args, "--id", "")
	if syncID == "" && len(args) > 0 && !strings.HasPrefix(args[0], "--") {
		syncID = args[0]
	}
	if syncID == "" {
		return fmt.Errorf("usage: flowforge-cli sync resume --id <sync-id>")
	}

	resp, err := apiPost(fmt.Sprintf("/api/v1/syncs/%s/resume", syncID), nil)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func syncStatus(args []string) error {
	syncID := flagValue(args, "--id", "")
	if syncID == "" && len(args) > 0 && !strings.HasPrefix(args[0], "--") {
		syncID = args[0]
	}
	if syncID == "" {
		return fmt.Errorf("usage: flowforge-cli sync status --id <sync-id>")
	}

	resp, err := apiGet(fmt.Sprintf("/api/v1/syncs/%s/status", syncID))
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

// ---------------------------------------------------------------------------
// connector subcommands
// ---------------------------------------------------------------------------

func handleConnector(args []string) error {
	if len(args) == 0 {
		fmt.Println(`Usage: flowforge-cli connector <subcommand>

Subcommands:
  list   List all connectors
  test   Test a connector connection`)
		return nil
	}

	switch args[0] {
	case "list":
		return connectorList(args[1:])
	case "test":
		return connectorTest(args[1:])
	default:
		return fmt.Errorf("unknown connector subcommand %q", args[0])
	}
}

func connectorList(args []string) error {
	tenantID := flagValue(args, "--tenant", "")
	path := "/api/v1/connectors"
	if tenantID != "" {
		path += "?tenant_id=" + tenantID
	}

	resp, err := apiGet(path)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func connectorTest(args []string) error {
	connectorID := flagValue(args, "--id", "")
	if connectorID == "" && len(args) > 0 && !strings.HasPrefix(args[0], "--") {
		connectorID = args[0]
	}
	if connectorID == "" {
		return fmt.Errorf("usage: flowforge-cli connector test --id <connector-id>")
	}

	resp, err := apiPost(fmt.Sprintf("/api/v1/connectors/%s/check", connectorID), nil)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

// ---------------------------------------------------------------------------
// tenant subcommands
// ---------------------------------------------------------------------------

func handleTenant(args []string) error {
	if len(args) == 0 {
		fmt.Println(`Usage: flowforge-cli tenant <subcommand>

Subcommands:
  list     List all tenants
  create   Create a new tenant`)
		return nil
	}

	switch args[0] {
	case "list":
		return tenantList(args[1:])
	case "create":
		return tenantCreate(args[1:])
	default:
		return fmt.Errorf("unknown tenant subcommand %q", args[0])
	}
}

func tenantList(_ []string) error {
	resp, err := apiGet("/api/v1/tenants")
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func tenantCreate(args []string) error {
	name := flagValue(args, "--name", "")
	namespace := flagValue(args, "--namespace", "")

	if name == "" || namespace == "" {
		return fmt.Errorf("usage: flowforge-cli tenant create --name <name> --namespace <namespace>")
	}

	body := map[string]interface{}{
		"name":      name,
		"namespace": namespace,
	}

	resp, err := apiPost("/api/v1/tenants", body)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

// ---------------------------------------------------------------------------
// schema subcommands
// ---------------------------------------------------------------------------

func handleSchema(args []string) error {
	if len(args) == 0 {
		fmt.Println(`Usage: flowforge-cli schema <subcommand>

Subcommands:
  discover   Discover schemas for a connection
  diff       Compare two schema versions`)
		return nil
	}

	switch args[0] {
	case "discover":
		return schemaDiscover(args[1:])
	case "diff":
		return schemaDiff(args[1:])
	default:
		return fmt.Errorf("unknown schema subcommand %q", args[0])
	}
}

func schemaDiscover(args []string) error {
	connectionID := flagValue(args, "--connection", "")
	if connectionID == "" && len(args) > 0 && !strings.HasPrefix(args[0], "--") {
		connectionID = args[0]
	}
	if connectionID == "" {
		return fmt.Errorf("usage: flowforge-cli schema discover --connection <connection-id>")
	}

	resp, err := apiPost(fmt.Sprintf("/api/v1/connections/%s/discover", connectionID), nil)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func schemaDiff(args []string) error {
	streamID := flagValue(args, "--stream", "")
	v1 := flagValue(args, "--v1", "")
	v2 := flagValue(args, "--v2", "")

	if streamID == "" || v1 == "" || v2 == "" {
		return fmt.Errorf("usage: flowforge-cli schema diff --stream <stream-id> --v1 <version> --v2 <version>")
	}

	resp, err := apiGet(fmt.Sprintf("/api/v1/streams/%s/schema/diff?v1=%s&v2=%s", streamID, v1, v2))
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

// ---------------------------------------------------------------------------
// HTTP client helpers
// ---------------------------------------------------------------------------

func apiBase() string {
	base := os.Getenv("FLOWFORGE_API_URL")
	if base == "" {
		base = defaultAPIBase
	}
	return strings.TrimRight(base, "/")
}

func apiToken() string {
	return os.Getenv("FLOWFORGE_API_TOKEN")
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
	}
}

func apiGet(path string) (json.RawMessage, error) {
	url := apiBase() + path
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	return doRequest(req)
}

func apiPost(path string, body interface{}) (json.RawMessage, error) {
	url := apiBase() + path
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(http.MethodPost, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return doRequest(req)
}

func doRequest(req *http.Request) (json.RawMessage, error) {
	req.Header.Set("Accept", "application/json")
	if token := apiToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := newHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return json.RawMessage(respBody), nil
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

// flagValue scans args for a --key value pair and returns the value.
// Returns defaultVal if the flag is not present.
func flagValue(args []string, key, defaultVal string) string {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == key {
			return args[i+1]
		}
		// Support --key=value syntax.
		if strings.HasPrefix(args[i], key+"=") {
			return strings.TrimPrefix(args[i], key+"=")
		}
	}
	// Check last element for --key=value.
	if len(args) > 0 {
		last := args[len(args)-1]
		if strings.HasPrefix(last, key+"=") {
			return strings.TrimPrefix(last, key+"=")
		}
	}
	return defaultVal
}

// printJSON pretty-prints a JSON value to stdout.
func printJSON(data json.RawMessage) {
	var buf bytes.Buffer
	if err := json.Indent(&buf, data, "", "  "); err != nil {
		// Fallback to raw output.
		fmt.Println(string(data))
		return
	}
	fmt.Println(buf.String())
}
