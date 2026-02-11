// Package chorus implements a source FlowForge connector for Chorus.ai (now ZoomInfo Chorus),
// a conversation intelligence platform. It reads meetings, calls, trackers, insights,
// and participants using the Chorus REST API with token-based auth and date filtering.
package chorus

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
	baseURL          = "https://chorus.ai/api/v1"
	connectorName    = "chorus"
	connectorVersion = "1.0.0"
	pageSize         = 100
)

// streamDefs maps stream names to their Chorus API endpoint details.
var streamDefs = map[string]chorusStreamDef{
	"meetings": {
		listPath:     "/meetings",
		resultsField: "meetings",
		pk:           "id",
		dateFilter:   "updatedAfter",
		hasPagination: true,
	},
	"calls": {
		listPath:     "/calls",
		resultsField: "calls",
		pk:           "id",
		dateFilter:   "updatedAfter",
		hasPagination: true,
	},
	"trackers": {
		listPath:      "/trackers",
		resultsField:  "trackers",
		pk:            "id",
		dateFilter:    "",
		hasPagination: false,
	},
	"insights": {
		listPath:     "/insights",
		resultsField: "insights",
		pk:           "id",
		dateFilter:   "fromDate",
		hasPagination: true,
	},
	"participants": {
		listPath:     "/participants",
		resultsField: "participants",
		pk:           "id",
		dateFilter:   "updatedAfter",
		hasPagination: true,
	},
}

type chorusStreamDef struct {
	listPath      string
	resultsField  string
	pk            string
	dateFilter    string
	hasPagination bool
}

// chorusConfig holds parsed Chorus connector configuration.
type chorusConfig struct {
	APIToken string `json:"api_token"`
}

func parseChorusConfig(raw json.RawMessage) (*chorusConfig, error) {
	var cfg chorusConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, errs.NewConnectorError(connectorName, "parse_config", err)
	}
	if cfg.APIToken == "" {
		return nil, errs.NewConnectorError(connectorName, "parse_config",
			fmt.Errorf("api_token is required"))
	}
	return &cfg, nil
}

// Connector implements cdk.Source for Chorus.ai.
type Connector struct{}

func (c *Connector) buildClient(cfg *chorusConfig) *restcommon.RESTClient {
	return restcommon.NewRESTClient(baseURL,
		restcommon.WithAPIKey("Bearer "+cfg.APIToken, "Authorization"),
		restcommon.WithRetry(3, time.Second),
		restcommon.WithRateLimit(5),
	)
}

// Spec returns the connector's configuration JSON Schema.
func (c *Connector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["api_token"],
		"properties": {
			"api_token": {
				"type": "string",
				"description": "Chorus.ai API authentication token"
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.flowforge.io/connectors/chorus",
		ConfigSchema:     schema,
	}, nil
}

// Check validates the Chorus connection by listing trackers (lightweight endpoint).
func (c *Connector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseChorusConfig(config)
	if err != nil {
		return &protocol.CheckResult{Status: protocol.CheckStatusFailed, Message: err.Error()}, nil
	}
	client := c.buildClient(cfg)
	_, err = client.Get(ctx, "/trackers")
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("Failed to connect to Chorus: %v", err),
		}, nil
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded, Message: "Connected to Chorus successfully"}, nil
}

// Discover returns a catalog of all Chorus streams.
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
		if def.dateFilter != "" {
			syncModes = append(syncModes, protocol.SyncModeIncremental)
			cursorField = []string{"updatedAt"}
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

// Read emits records from configured Chorus streams.
func (c *Connector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseChorusConfig(config)
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

func (c *Connector) readStream(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def chorusStreamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	// Build the query parameters.
	var dateParam string
	if cs.SyncMode == protocol.SyncModeIncremental && def.dateFilter != "" {
		if raw, ok := state[cs.Stream.Name]; ok {
			var s struct {
				UpdatedAt string `json:"updatedAt"`
			}
			if json.Unmarshal(raw, &s) == nil && s.UpdatedAt != "" {
				dateParam = s.UpdatedAt
			}
		}
	}

	var maxUpdated string
	page := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := def.listPath
		params := []string{}
		if def.hasPagination {
			params = append(params, fmt.Sprintf("offset=%d", page*pageSize))
			params = append(params, fmt.Sprintf("limit=%d", pageSize))
		}
		if dateParam != "" && def.dateFilter != "" {
			params = append(params, fmt.Sprintf("%s=%s", def.dateFilter, dateParam))
		}
		if len(params) > 0 {
			path += "?" + strings.Join(params, "&")
		}

		resp, err := client.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("GET %s: %w", path, err)
		}

		results := extractArray(resp.Body, def.resultsField)
		if len(results) == 0 {
			break
		}

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

			if ts, ok := rec["updatedAt"].(string); ok && ts > maxUpdated {
				maxUpdated = ts
			}
		}

		if !def.hasPagination || len(results) < pageSize {
			break
		}
		page++
	}

	if maxUpdated != "" {
		stateData, _ := json.Marshal(map[string]string{"updatedAt": maxUpdated})
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
		DisplayName: "Chorus.ai",
		Version:     connectorVersion,
		Category:    "Sales Intelligence",
	}, &Connector{})
}
