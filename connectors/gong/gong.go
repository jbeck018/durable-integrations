// Package gong implements a source FlowForge connector for the Gong conversation
// intelligence platform. It reads calls, users, meetings, scorecards, trackers,
// and stats using the Gong API v2 with basic auth and cursor-based pagination.
package gong

import (
	"context"
	"encoding/base64"
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
	baseURL          = "https://api.gong.io"
	connectorName    = "gong"
	connectorVersion = "1.0.0"
	dailyLimit       = 10000 // Gong API daily request limit
)

// streamDefs maps stream names to their Gong API endpoint details.
var streamDefs = map[string]gongStreamDef{
	"calls": {
		listPath:     "/v2/calls",
		cursorField:  "records.cursor",
		resultsField: "calls",
		pk:           "id",
		usePOST:      true,
		bodyKey:      "filter",
	},
	"users": {
		listPath:     "/v2/users",
		cursorField:  "records.cursor",
		resultsField: "users",
		pk:           "id",
		usePOST:      false,
	},
	"meetings": {
		listPath:     "/v2/meetings",
		cursorField:  "records.cursor",
		resultsField: "meetings",
		pk:           "id",
		usePOST:      true,
		bodyKey:      "filter",
	},
	"scorecards": {
		listPath:     "/v2/settings/scorecards",
		cursorField:  "",
		resultsField: "scorecards",
		pk:           "scorecardId",
		usePOST:      false,
	},
	"trackers": {
		listPath:     "/v2/settings/trackers",
		cursorField:  "",
		resultsField: "trackers",
		pk:           "trackerId",
		usePOST:      false,
	},
	"stats": {
		listPath:     "/v2/stats/activity/aggregate",
		cursorField:  "records.cursor",
		resultsField: "stats",
		pk:           "",
		usePOST:      true,
		bodyKey:      "filter",
	},
}

type gongStreamDef struct {
	listPath     string
	cursorField  string
	resultsField string
	pk           string
	usePOST      bool
	bodyKey      string
}

// gongConfig holds parsed Gong connector configuration.
type gongConfig struct {
	AccessKey    string `json:"access_key"`
	AccessSecret string `json:"access_secret"`
}

func parseGongConfig(raw json.RawMessage) (*gongConfig, error) {
	var cfg gongConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, errs.NewConnectorError(connectorName, "parse_config", err)
	}
	if cfg.AccessKey == "" || cfg.AccessSecret == "" {
		return nil, errs.NewConnectorError(connectorName, "parse_config",
			fmt.Errorf("access_key and access_secret are required"))
	}
	return &cfg, nil
}

// DailyLimiter tracks daily API request counts. Implementations should use a
// shared store (e.g. Redis) so that all workers for the same tenant share one
// counter that resets at midnight UTC.
type DailyLimiter interface {
	// Increment adds delta to today's counter and returns the new total.
	// Returns an error if the daily limit would be exceeded.
	Increment(ctx context.Context, key string, delta int, limit int) (int, error)
}

// Connector implements cdk.Source for Gong.
type Connector struct {
	// DailyLimiter is an optional daily request counter. When set, every API
	// request increments the counter and requests are rejected once the daily
	// limit is reached.
	DailyLimiter DailyLimiter
}

func (c *Connector) buildClient(cfg *gongConfig) *restcommon.RESTClient {
	// Gong uses HTTP Basic Auth with access_key:access_secret.
	creds := base64.StdEncoding.EncodeToString([]byte(cfg.AccessKey + ":" + cfg.AccessSecret))
	return restcommon.NewRESTClient(baseURL,
		restcommon.WithAPIKey("Basic "+creds, "Authorization"),
		restcommon.WithRetry(3, 500*time.Millisecond),
		restcommon.WithRateLimit(3),
	)
}

// checkDailyLimit increments the daily counter and returns an error if the
// limit has been exceeded. If no DailyLimiter is configured, it is a no-op.
func (c *Connector) checkDailyLimit(ctx context.Context) error {
	if c.DailyLimiter == nil {
		return nil
	}
	count, err := c.DailyLimiter.Increment(ctx, "gong:daily", 1, dailyLimit)
	if err != nil {
		return fmt.Errorf("gong daily limit exceeded (%d/%d): %w", count, dailyLimit, err)
	}
	return nil
}

// Spec returns the connector's configuration JSON Schema.
func (c *Connector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["access_key", "access_secret"],
		"properties": {
			"access_key": {
				"type": "string",
				"description": "Gong API access key"
			},
			"access_secret": {
				"type": "string",
				"description": "Gong API access key secret"
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.flowforge.io/connectors/gong",
		ConfigSchema:     schema,
	}, nil
}

// Check validates the Gong connection by listing users.
func (c *Connector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseGongConfig(config)
	if err != nil {
		return &protocol.CheckResult{Status: protocol.CheckStatusFailed, Message: err.Error()}, nil
	}
	client := c.buildClient(cfg)
	_, err = client.Get(ctx, "/v2/users")
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("Failed to connect to Gong: %v", err),
		}, nil
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded, Message: "Connected to Gong successfully"}, nil
}

// Discover returns a catalog of all Gong streams.
func (c *Connector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	streams := make([]protocol.Stream, 0, len(streamDefs))
	for name, def := range streamDefs {
		schemaJSON, _ := json.Marshal(map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"id": map[string]string{"type": "string"},
			},
		})

		syncModes := []protocol.SyncMode{protocol.SyncModeFullRefresh}
		var cursorField []string
		if def.usePOST {
			syncModes = append(syncModes, protocol.SyncModeIncremental)
			cursorField = []string{"started"}
		}

		var pk [][]string
		if def.pk != "" {
			pk = [][]string{{def.pk}}
		}

		streams = append(streams, protocol.Stream{
			Name:               name,
			DisplayName:        formatDisplayName(name),
			Schema:             schemaJSON,
			SupportedSyncModes: syncModes,
			DefaultCursorField: cursorField,
			SourceDefinedPK:    def.pk != "",
			PrimaryKey:         pk,
		})
	}
	return &protocol.Catalog{Streams: streams}, nil
}

// Read emits records from configured Gong streams.
func (c *Connector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseGongConfig(config)
	if err != nil {
		return err
	}
	client := c.buildClient(cfg)

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

func (c *Connector) readStream(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def gongStreamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	// Determine the fromDateTime filter for incremental sync.
	var fromDateTime string
	if cs.SyncMode == protocol.SyncModeIncremental {
		if raw, ok := state[cs.Stream.Name]; ok {
			var s struct {
				FromDateTime string `json:"fromDateTime"`
			}
			if json.Unmarshal(raw, &s) == nil && s.FromDateTime != "" {
				fromDateTime = s.FromDateTime
			}
		}
	}

	if def.usePOST {
		return c.readViaPOST(ctx, client, cs, def, fromDateTime, output)
	}
	return c.readViaGET(ctx, client, cs, def, output)
}

// readViaPOST reads a stream that uses POST-based list endpoints (calls, meetings, stats).
func (c *Connector) readViaPOST(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def gongStreamDef, fromDateTime string, output chan<- protocol.Message) error {
	cursor := ""
	var maxTimestamp string

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		body := map[string]interface{}{}
		filter := map[string]interface{}{}
		if fromDateTime != "" {
			filter["fromDateTime"] = fromDateTime
		}
		// Always set a toDateTime to avoid unbounded queries.
		filter["toDateTime"] = time.Now().UTC().Format(time.RFC3339)
		body[def.bodyKey] = filter

		if cursor != "" {
			body["cursor"] = cursor
		}

		if err := c.checkDailyLimit(ctx); err != nil {
			return err
		}
		resp, err := client.Post(ctx, def.listPath, body)
		if err != nil {
			return fmt.Errorf("POST %s: %w", def.listPath, err)
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

			// Track max timestamp for state.
			if ts, ok := rec["started"].(string); ok && ts > maxTimestamp {
				maxTimestamp = ts
			}
		}

		// Check for next cursor in records metadata.
		records, ok := resp.Body["records"].(map[string]interface{})
		if !ok {
			break
		}
		nextCursor, _ := records["cursor"].(string)
		if nextCursor == "" || nextCursor == cursor {
			break
		}
		cursor = nextCursor
	}

	if maxTimestamp != "" {
		stateData, _ := json.Marshal(map[string]string{"fromDateTime": maxTimestamp})
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

// readViaGET reads a stream that uses GET-based list endpoints (users, scorecards, trackers).
func (c *Connector) readViaGET(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def gongStreamDef, output chan<- protocol.Message) error {
	cursor := ""

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := def.listPath
		if cursor != "" {
			sep := "?"
			if strings.Contains(path, "?") {
				sep = "&"
			}
			path += sep + "cursor=" + cursor
		}

		if err := c.checkDailyLimit(ctx); err != nil {
			return err
		}
		resp, err := client.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("GET %s: %w", path, err)
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

		// Check for cursor in records metadata.
		if def.cursorField == "" {
			break
		}
		records, ok := resp.Body["records"].(map[string]interface{})
		if !ok {
			break
		}
		nextCursor, _ := records["cursor"].(string)
		if nextCursor == "" || nextCursor == cursor {
			break
		}
		cursor = nextCursor
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
	cdk.RegisterSource(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Gong",
		Version:     connectorVersion,
		Category:    "Sales Intelligence",
	}, &Connector{}) // DailyLimiter is nil by default; injected by worker at runtime
}
