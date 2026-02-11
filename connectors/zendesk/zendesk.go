// Package zendesk implements a bidirectional FlowForge connector for Zendesk Support.
// It uses the Zendesk REST API v2 for reading/writing, the incremental export API
// for large-volume reads, and the job API for batch writes.
package zendesk

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	restcommon "github.com/flowforge/flowforge/connectors/common"
	errs "github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

const (
	connectorName    = "zendesk"
	connectorVersion = "1.0.0"
	maxJobBatchSize  = 100
)

// streamDefs maps stream names to their Zendesk API details.
var streamDefs = map[string]zdStreamDef{
	"tickets": {
		listPath:          "/api/v2/tickets.json",
		incrementalPath:   "/api/v2/incremental/tickets/cursor.json",
		resultsField:      "tickets",
		pk:                "id",
		supportsIncremental: true,
		writePath:         "/api/v2/tickets/create_many.json",
		updatePath:        "/api/v2/tickets/update_many.json",
	},
	"users": {
		listPath:          "/api/v2/users.json",
		incrementalPath:   "/api/v2/incremental/users/cursor.json",
		resultsField:      "users",
		pk:                "id",
		supportsIncremental: true,
		writePath:         "/api/v2/users/create_many.json",
		updatePath:        "/api/v2/users/update_many.json",
	},
	"organizations": {
		listPath:          "/api/v2/organizations.json",
		incrementalPath:   "/api/v2/incremental/organizations/cursor.json",
		resultsField:      "organizations",
		pk:                "id",
		supportsIncremental: true,
		writePath:         "/api/v2/organizations/create_many.json",
		updatePath:        "/api/v2/organizations/update_many.json",
	},
	"groups": {
		listPath:     "/api/v2/groups.json",
		resultsField: "groups",
		pk:           "id",
		supportsIncremental: false,
	},
	"ticket_comments": {
		listPath:     "/api/v2/ticket_audits.json",
		resultsField: "audits",
		pk:           "id",
		supportsIncremental: false,
	},
	"ticket_fields": {
		listPath:     "/api/v2/ticket_fields.json",
		resultsField: "ticket_fields",
		pk:           "id",
		supportsIncremental: false,
	},
	"satisfaction_ratings": {
		listPath:     "/api/v2/satisfaction_ratings.json",
		resultsField: "satisfaction_ratings",
		pk:           "id",
		supportsIncremental: false,
	},
}

type zdStreamDef struct {
	listPath            string
	incrementalPath     string
	resultsField        string
	pk                  string
	supportsIncremental bool
	writePath           string
	updatePath          string
}

// zdConfig holds parsed Zendesk connector configuration.
type zdConfig struct {
	Subdomain string                  `json:"subdomain"`
	OAuth     *restcommon.OAuthConfig `json:"oauth,omitempty"`
	Email     string                  `json:"email,omitempty"`
	APIToken  string                  `json:"api_token,omitempty"`
}

func parseZDConfig(raw json.RawMessage) (*zdConfig, error) {
	var cfg zdConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, errs.NewConnectorError(connectorName, "parse_config", err)
	}
	if cfg.Subdomain == "" {
		return nil, errs.NewConnectorError(connectorName, "parse_config",
			fmt.Errorf("subdomain is required"))
	}
	if cfg.OAuth == nil && cfg.APIToken == "" {
		return nil, errs.NewConnectorError(connectorName, "parse_config",
			fmt.Errorf("either oauth or api_token+email must be provided"))
	}
	return &cfg, nil
}

func (cfg *zdConfig) baseURL() string {
	return fmt.Sprintf("https://%s.zendesk.com", cfg.Subdomain)
}

// Connector implements cdk.Bidirectional for Zendesk.
type Connector struct{}

func (c *Connector) buildClient(ctx context.Context, cfg *zdConfig) *restcommon.RESTClient {
	opts := []restcommon.ClientOption{
		restcommon.WithRetry(3, 500*time.Millisecond),
		restcommon.WithRateLimit(10),
	}
	if cfg.OAuth != nil {
		if cfg.OAuth.TokenURL == "" {
			cfg.OAuth.TokenURL = cfg.baseURL() + "/oauth/tokens"
		}
		ts := cfg.OAuth.TokenSource(ctx)
		opts = append(opts, restcommon.WithOAuth2(ts))
	} else if cfg.APIToken != "" {
		// Zendesk API token auth uses email/token:{api_token} as basic auth.
		opts = append(opts, restcommon.WithAPIKey(
			"Bearer "+cfg.APIToken, "Authorization"))
	}
	return restcommon.NewRESTClient(cfg.baseURL(), opts...)
}

// Spec returns the connector's configuration JSON Schema.
func (c *Connector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["subdomain"],
		"properties": {
			"subdomain": {
				"type": "string",
				"description": "Zendesk subdomain (e.g. 'mycompany' for mycompany.zendesk.com)"
			},
			"oauth": {
				"type": "object",
				"properties": {
					"client_id": {"type": "string"},
					"client_secret": {"type": "string"},
					"access_token": {"type": "string"},
					"refresh_token": {"type": "string"}
				}
			},
			"email": {
				"type": "string",
				"description": "Agent email for API token auth"
			},
			"api_token": {
				"type": "string",
				"description": "Zendesk API token"
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.flowforge.io/connectors/zendesk",
		ConfigSchema:     schema,
	}, nil
}

// Check validates the Zendesk connection.
func (c *Connector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseZDConfig(config)
	if err != nil {
		return &protocol.CheckResult{Status: protocol.CheckStatusFailed, Message: err.Error()}, nil
	}
	client := c.buildClient(ctx, cfg)
	_, err = client.Get(ctx, "/api/v2/users/me.json")
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("Failed to connect to Zendesk: %v", err),
		}, nil
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded, Message: "Connected to Zendesk successfully"}, nil
}

// Discover returns a catalog of Zendesk streams.
func (c *Connector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	streams := make([]protocol.Stream, 0, len(streamDefs))
	for name, def := range streamDefs {
		schemaJSON, _ := json.Marshal(map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"id": map[string]string{"type": "integer"},
			},
		})

		syncModes := []protocol.SyncMode{protocol.SyncModeFullRefresh}
		var cursorField []string
		if def.supportsIncremental {
			syncModes = append(syncModes, protocol.SyncModeIncremental)
			cursorField = []string{"updated_at"}
		}

		streams = append(streams, protocol.Stream{
			Name:               name,
			DisplayName:        formatDisplayName(name),
			Schema:             schemaJSON,
			SupportedSyncModes: syncModes,
			DefaultCursorField: cursorField,
			SourceDefinedPK:    true,
			PrimaryKey:         [][]string{{def.pk}},
		})
	}
	return &protocol.Catalog{Streams: streams}, nil
}

// Read emits records from configured Zendesk streams.
func (c *Connector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseZDConfig(config)
	if err != nil {
		return err
	}
	client := c.buildClient(ctx, cfg)

	for _, cs := range catalog.Streams {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		def, ok := streamDefs[cs.Stream.Name]
		if !ok {
			continue
		}

		if err := c.readStream(ctx, client, cs, def, state, output); err != nil {
			output <- protocol.Message{
				Type: protocol.MessageTypeLog,
				Log: &protocol.Log{
					Level:     protocol.LogLevelError,
					Message:   fmt.Sprintf("Error reading stream %s: %v", cs.Stream.Name, err),
					Timestamp: time.Now().UTC(),
				},
			}
		}
	}
	return nil
}

func (c *Connector) readStream(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def zdStreamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	if cs.SyncMode == protocol.SyncModeIncremental && def.supportsIncremental {
		return c.readIncremental(ctx, client, cs, def, state, output)
	}
	return c.readFullRefresh(ctx, client, cs, def, output)
}

// readIncremental uses the Zendesk incremental cursor export API.
func (c *Connector) readIncremental(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def zdStreamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	// Get start_time from state, default to 0 (Unix epoch = full history).
	startTime := "0"
	var cursor string
	if raw, ok := state[cs.Stream.Name]; ok {
		var s struct {
			Cursor    string `json:"cursor"`
			StartTime string `json:"start_time"`
		}
		if json.Unmarshal(raw, &s) == nil {
			if s.Cursor != "" {
				cursor = s.Cursor
			} else if s.StartTime != "" {
				startTime = s.StartTime
			}
		}
	}

	var maxUpdated string
	var lastCursor string

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var path string
		if cursor != "" {
			path = def.incrementalPath + "?cursor=" + cursor
		} else {
			path = def.incrementalPath + "?start_time=" + startTime
		}

		resp, err := client.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("incremental export: %w", err)
		}

		results := extractArray(resp.Body, def.resultsField)
		for _, item := range results {
			rec, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			data, _ := json.Marshal(rec)
			output <- protocol.Message{
				Type: protocol.MessageTypeRecord,
				Record: &protocol.Record{
					Stream:    cs.Stream.Name,
					Data:      data,
					EmittedAt: time.Now().UTC(),
				},
			}

			if ts, ok := rec["updated_at"].(string); ok && ts > maxUpdated {
				maxUpdated = ts
			}
		}

		// The cursor-based incremental API returns end_of_stream and after_cursor.
		endOfStream, _ := resp.Body["end_of_stream"].(bool)
		afterCursor, _ := resp.Body["after_cursor"].(string)

		if afterCursor != "" {
			lastCursor = afterCursor
		}

		if endOfStream || afterCursor == "" {
			break
		}
		cursor = afterCursor
	}

	// Emit state with cursor for resumption.
	stateMap := map[string]string{}
	if lastCursor != "" {
		stateMap["cursor"] = lastCursor
	}
	if maxUpdated != "" {
		stateMap["start_time"] = maxUpdated
	}
	if len(stateMap) > 0 {
		stateData, _ := json.Marshal(stateMap)
		output <- protocol.Message{
			Type: protocol.MessageTypeState,
			State: &protocol.State{
				Type:   protocol.StateTypeStream,
				Stream: cs.Stream.Name,
				Data:   stateData,
			},
		}
	}
	return nil
}

// readFullRefresh reads all records using standard pagination (next_page URLs).
func (c *Connector) readFullRefresh(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def zdStreamDef, output chan<- protocol.Message) error {
	path := def.listPath
	for path != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("list API: %w", err)
		}

		results := extractArray(resp.Body, def.resultsField)
		for _, item := range results {
			rec, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			data, _ := json.Marshal(rec)
			output <- protocol.Message{
				Type: protocol.MessageTypeRecord,
				Record: &protocol.Record{
					Stream:    cs.Stream.Name,
					Data:      data,
					EmittedAt: time.Now().UTC(),
				},
			}
		}

		// Zendesk returns next_page as a full URL; we need just the path portion.
		path = ""
		if nextPage, ok := resp.Body["next_page"].(string); ok && nextPage != "" {
			// Extract the path from the full URL.
			path = extractPath(nextPage)
		}
	}
	return nil
}

// Write uses the Zendesk batch/job API for efficient writes.
func (c *Connector) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	cfg, err := parseZDConfig(config)
	if err != nil {
		return nil, err
	}
	client := c.buildClient(ctx, cfg)

	result := &protocol.WriteResult{}
	buffers := make(map[string][]map[string]interface{})

	for msg := range input {
		if msg.Record == nil {
			continue
		}
		var rec map[string]interface{}
		if err := json.Unmarshal(msg.Record.Data, &rec); err != nil {
			result.Errors = append(result.Errors, protocol.WriteError{
				Message: fmt.Sprintf("unmarshal record: %v", err),
				Record:  msg.Record.Data,
			})
			continue
		}
		streamName := msg.Record.Stream
		buffers[streamName] = append(buffers[streamName], rec)

		if len(buffers[streamName]) >= maxJobBatchSize {
			written, writeErrs := c.flushBatch(ctx, client, streamName, buffers[streamName])
			result.RecordsWritten += int64(written)
			result.Errors = append(result.Errors, writeErrs...)
			buffers[streamName] = nil
		}
	}

	for streamName, recs := range buffers {
		if len(recs) == 0 {
			continue
		}
		written, writeErrs := c.flushBatch(ctx, client, streamName, recs)
		result.RecordsWritten += int64(written)
		result.Errors = append(result.Errors, writeErrs...)
	}

	return result, nil
}

// flushBatch sends a batch of records to Zendesk using the job API.
func (c *Connector) flushBatch(ctx context.Context, client *restcommon.RESTClient, streamName string, records []map[string]interface{}) (int, []protocol.WriteError) {
	def, ok := streamDefs[streamName]
	if !ok || def.writePath == "" {
		writeErrs := make([]protocol.WriteError, len(records))
		for i, rec := range records {
			data, _ := json.Marshal(rec)
			writeErrs[i] = protocol.WriteError{
				Message: fmt.Sprintf("stream %s does not support writes", streamName),
				Record:  data,
			}
		}
		return 0, writeErrs
	}

	// Separate records into creates (no id) and updates (has id).
	var creates, updates []map[string]interface{}
	for _, rec := range records {
		if _, hasID := rec["id"]; hasID {
			updates = append(updates, rec)
		} else {
			creates = append(creates, rec)
		}
	}

	written := 0
	var writeErrs []protocol.WriteError

	if len(creates) > 0 {
		w, e := c.submitJob(ctx, client, def.writePath, def.resultsField, creates)
		written += w
		writeErrs = append(writeErrs, e...)
	}

	if len(updates) > 0 && def.updatePath != "" {
		w, e := c.submitJob(ctx, client, def.updatePath, def.resultsField, updates)
		written += w
		writeErrs = append(writeErrs, e...)
	}

	return written, writeErrs
}

// submitJob submits a batch job to Zendesk and polls for completion.
func (c *Connector) submitJob(ctx context.Context, client *restcommon.RESTClient, path string, resourceKey string, records []map[string]interface{}) (int, []protocol.WriteError) {
	body := map[string]interface{}{
		resourceKey: records,
	}

	resp, err := client.Post(ctx, path, body)
	if err != nil {
		writeErrs := make([]protocol.WriteError, len(records))
		for i, rec := range records {
			data, _ := json.Marshal(rec)
			writeErrs[i] = protocol.WriteError{
				Message: fmt.Sprintf("job submit error: %v", err),
				Record:  data,
			}
		}
		return 0, writeErrs
	}

	// For Zendesk, create_many returns a job_status with a URL to poll.
	jobStatus, ok := resp.Body["job_status"].(map[string]interface{})
	if !ok {
		// Some endpoints return results directly.
		return len(records), nil
	}

	statusURL, _ := jobStatus["url"].(string)
	if statusURL == "" {
		return len(records), nil
	}

	// Poll the job status until completion or timeout.
	statusPath := extractPath(statusURL)
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return 0, []protocol.WriteError{{Message: "context cancelled while waiting for job"}}
		case <-time.After(2 * time.Second):
		}

		statusResp, err := client.Get(ctx, statusPath)
		if err != nil {
			continue
		}

		js, ok := statusResp.Body["job_status"].(map[string]interface{})
		if !ok {
			continue
		}

		status, _ := js["status"].(string)
		if status == "completed" {
			total, _ := js["total"].(float64)
			return int(total), nil
		}
		if status == "failed" || status == "killed" {
			msg, _ := js["message"].(string)
			return 0, []protocol.WriteError{{Message: fmt.Sprintf("job %s: %s", status, msg)}}
		}
	}

	return 0, []protocol.WriteError{{Message: "job timed out after 5 minutes"}}
}

// Capabilities returns the destination sync modes Zendesk supports.
func (c *Connector) Capabilities() []protocol.DestinationSyncMode {
	return []protocol.DestinationSyncMode{
		protocol.DestSyncModeAppend,
		protocol.DestSyncModeUpsert,
	}
}

// Resolve handles conflicts using last-write-wins.
func (c *Connector) Resolve(ctx context.Context, conflicts []cdk.Conflict) ([]cdk.Resolution, error) {
	resolutions := make([]cdk.Resolution, len(conflicts))
	for i, conflict := range conflicts {
		strategy := cdk.ResolutionLastWrite
		var merged json.RawMessage
		if conflict.SourceTS >= conflict.DestTS {
			merged = conflict.SourceRecord
		} else {
			merged = conflict.DestRecord
		}
		resolutions[i] = cdk.Resolution{
			PrimaryKey:   conflict.PrimaryKey,
			Strategy:     strategy,
			MergedRecord: merged,
		}
	}
	return resolutions, nil
}

// WebhookHandler processes Zendesk webhook trigger events.
func (c *Connector) WebhookHandler(ctx context.Context, event cdk.WebhookEvent) error {
	var payload struct {
		TicketID   interface{} `json:"ticket_id"`
		EventType  string      `json:"event_type"`
	}
	if err := json.Unmarshal(event.Body, &payload); err != nil {
		return errs.NewConnectorError(connectorName, "webhook_parse", err)
	}
	if payload.EventType == "" {
		return errs.NewConnectorError(connectorName, "webhook_validate",
			fmt.Errorf("missing event_type in webhook payload"))
	}
	return nil
}

// extractArray extracts a []interface{} from a map by key.
func extractArray(body map[string]interface{}, key string) []interface{} {
	if body == nil {
		return nil
	}
	val, ok := body[key]
	if !ok {
		return nil
	}
	arr, ok := val.([]interface{})
	if !ok {
		return nil
	}
	return arr
}

// extractPath extracts the path portion from a full URL string.
func extractPath(fullURL string) string {
	// Find the path after the host.
	if idx := strings.Index(fullURL, ".com"); idx >= 0 {
		return fullURL[idx+4:]
	}
	if idx := strings.Index(fullURL, ".zendesk.com"); idx >= 0 {
		return fullURL[idx+12:]
	}
	return fullURL
}

// formatDisplayName converts a snake_case name to Title Case.
func formatDisplayName(name string) string {
	parts := strings.Split(name, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

func init() {
	cdk.RegisterBidirectional(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Zendesk",
		Version:     connectorVersion,
		Category:    "Support",
	}, &Connector{})
}
