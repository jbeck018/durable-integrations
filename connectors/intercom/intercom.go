// Package intercom implements a bidirectional FlowForge connector for the Intercom
// customer messaging platform. It reads contacts, companies, conversations, admins,
// tags, segments, and articles using the Intercom API v2.10 with scroll-based and
// cursor-based pagination, and writes via the batch contacts API.
package intercom

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
	baseURL          = "https://api.intercom.io"
	connectorName    = "intercom"
	connectorVersion = "1.0.0"
	scrollPageSize   = 50
)

// streamDefs maps stream names to their Intercom API details.
var streamDefs = map[string]icStreamDef{
	"contacts": {
		listPath:     "/contacts",
		resultsField: "data",
		pk:           "id",
		useScroll:    true,
		searchPath:   "/contacts/search",
		batchWrite:   true,
	},
	"companies": {
		listPath:     "/companies",
		resultsField: "data",
		pk:           "id",
		useScroll:    true,
		searchPath:   "",
		batchWrite:   false,
		writePath:    "/companies",
	},
	"conversations": {
		listPath:     "/conversations",
		resultsField: "conversations",
		pk:           "id",
		useScroll:    false,
		batchWrite:   false,
	},
	"admins": {
		listPath:     "/admins",
		resultsField: "admins",
		pk:           "id",
		useScroll:    false,
		batchWrite:   false,
	},
	"tags": {
		listPath:     "/tags",
		resultsField: "data",
		pk:           "id",
		useScroll:    false,
		batchWrite:   false,
	},
	"segments": {
		listPath:     "/segments",
		resultsField: "segments",
		pk:           "id",
		useScroll:    false,
		batchWrite:   false,
	},
	"articles": {
		listPath:     "/articles",
		resultsField: "data",
		pk:           "id",
		useScroll:    false,
		batchWrite:   false,
		writePath:    "/articles",
	},
}

type icStreamDef struct {
	listPath     string
	resultsField string
	pk           string
	useScroll    bool
	searchPath   string
	batchWrite   bool
	writePath    string
}

// icConfig holds parsed Intercom connector configuration.
type icConfig struct {
	OAuth       *restcommon.OAuthConfig `json:"oauth,omitempty"`
	AccessToken string                  `json:"access_token,omitempty"`
}

func parseICConfig(raw json.RawMessage) (*icConfig, error) {
	var cfg icConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, errs.NewConnectorError(connectorName, "parse_config", err)
	}
	if cfg.OAuth == nil && cfg.AccessToken == "" {
		return nil, errs.NewConnectorError(connectorName, "parse_config",
			fmt.Errorf("either oauth or access_token must be provided"))
	}
	return &cfg, nil
}

// Connector implements cdk.Bidirectional for Intercom.
type Connector struct{}

func (c *Connector) buildClient(ctx context.Context, cfg *icConfig) *restcommon.RESTClient {
	opts := []restcommon.ClientOption{
		restcommon.WithRetry(3, 500*time.Millisecond),
		restcommon.WithRateLimit(15),
	}
	if cfg.OAuth != nil {
		if cfg.OAuth.TokenURL == "" {
			cfg.OAuth.TokenURL = "https://api.intercom.io/auth/eagle/token"
		}
		ts := cfg.OAuth.TokenSource(ctx)
		opts = append(opts, restcommon.WithOAuth2(ts))
	} else if cfg.AccessToken != "" {
		opts = append(opts, restcommon.WithAPIKey("Bearer "+cfg.AccessToken, "Authorization"))
	}
	return restcommon.NewRESTClient(baseURL, opts...)
}

// Spec returns the connector's configuration JSON Schema.
func (c *Connector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"properties": {
			"oauth": {
				"type": "object",
				"properties": {
					"client_id": {"type": "string"},
					"client_secret": {"type": "string"},
					"access_token": {"type": "string"},
					"refresh_token": {"type": "string"}
				}
			},
			"access_token": {
				"type": "string",
				"description": "Intercom access token for authentication"
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.flowforge.io/connectors/intercom",
		ConfigSchema:     schema,
	}, nil
}

// Check validates the Intercom connection by fetching the authenticated admin.
func (c *Connector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseICConfig(config)
	if err != nil {
		return &protocol.CheckResult{Status: protocol.CheckStatusFailed, Message: err.Error()}, nil
	}
	client := c.buildClient(ctx, cfg)
	_, err = client.Get(ctx, "/me")
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("Failed to connect to Intercom: %v", err),
		}, nil
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded, Message: "Connected to Intercom successfully"}, nil
}

// Discover returns a catalog of Intercom streams.
func (c *Connector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	streams := make([]protocol.Stream, 0, len(streamDefs))
	for name, def := range streamDefs {
		schemaJSON, _ := json.Marshal(map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"id":   map[string]string{"type": "string"},
				"type": map[string]string{"type": "string"},
			},
		})

		syncModes := []protocol.SyncMode{protocol.SyncModeFullRefresh}
		var cursorField []string
		if def.useScroll || def.searchPath != "" {
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

// Read emits records from configured Intercom streams.
func (c *Connector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseICConfig(config)
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

func (c *Connector) readStream(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def icStreamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	// For incremental sync on contacts, use the search API.
	if cs.SyncMode == protocol.SyncModeIncremental && def.searchPath != "" {
		return c.readViaSearch(ctx, client, cs, def, state, output)
	}
	// For scroll-supported streams, use scroll API for full refresh.
	if def.useScroll {
		return c.readViaScroll(ctx, client, cs, def, output)
	}
	// Standard cursor pagination.
	return c.readViaCursorPagination(ctx, client, cs, def, output)
}

// readViaSearch uses the Intercom search API for incremental contact syncs.
func (c *Connector) readViaSearch(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def icStreamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	var lastUpdated int64
	if raw, ok := state[cs.Stream.Name]; ok {
		var s struct {
			UpdatedAt int64 `json:"updated_at"`
		}
		if json.Unmarshal(raw, &s) == nil && s.UpdatedAt > 0 {
			lastUpdated = s.UpdatedAt
		}
	}

	var maxUpdated int64
	startingAfter := ""

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		searchBody := map[string]interface{}{
			"query": map[string]interface{}{
				"field":    "updated_at",
				"operator": ">",
				"value":    lastUpdated,
			},
			"sort": map[string]interface{}{
				"field": "updated_at",
				"order": "ascending",
			},
			"pagination": map[string]interface{}{
				"per_page": scrollPageSize,
			},
		}
		if startingAfter != "" {
			pagination := searchBody["pagination"].(map[string]interface{})
			pagination["starting_after"] = startingAfter
		}

		resp, err := client.Post(ctx, def.searchPath, searchBody)
		if err != nil {
			return fmt.Errorf("search API: %w", err)
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

			if ts, ok := rec["updated_at"].(float64); ok && int64(ts) > maxUpdated {
				maxUpdated = int64(ts)
			}
		}

		// Check for next page via pages.next.starting_after.
		pages, ok := resp.Body["pages"].(map[string]interface{})
		if !ok {
			break
		}
		next, ok := pages["next"].(map[string]interface{})
		if !ok {
			break
		}
		sa, ok := next["starting_after"].(string)
		if !ok || sa == "" {
			break
		}
		startingAfter = sa
	}

	if maxUpdated > 0 {
		stateData, _ := json.Marshal(map[string]int64{"updated_at": maxUpdated})
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

// readViaScroll uses the Intercom scroll API for full-refresh contact reads.
func (c *Connector) readViaScroll(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def icStreamDef, output chan<- protocol.Message) error {
	scrollParam := ""

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := def.listPath + "/scroll"
		if scrollParam != "" {
			path += "?scroll_param=" + scrollParam
		}

		resp, err := client.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("scroll API: %w", err)
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
		}

		newScroll, _ := resp.Body["scroll_param"].(string)
		if newScroll == "" || newScroll == scrollParam {
			break
		}
		scrollParam = newScroll
	}
	return nil
}

// readViaCursorPagination uses standard cursor-based pagination for non-scroll streams.
func (c *Connector) readViaCursorPagination(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def icStreamDef, output chan<- protocol.Message) error {
	startingAfter := ""

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := def.listPath
		if startingAfter != "" {
			sep := "?"
			if strings.Contains(path, "?") {
				sep = "&"
			}
			path += sep + "starting_after=" + startingAfter
		}

		resp, err := client.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("list API: %w", err)
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
		}

		// Check for next page.
		pages, ok := resp.Body["pages"].(map[string]interface{})
		if !ok {
			break
		}
		next, ok := pages["next"].(map[string]interface{})
		if !ok {
			break
		}
		sa, ok := next["starting_after"].(string)
		if !ok || sa == "" {
			break
		}
		startingAfter = sa
	}
	return nil
}

// Write handles batch writes for contacts and individual writes for others.
func (c *Connector) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	cfg, err := parseICConfig(config)
	if err != nil {
		return nil, err
	}
	client := c.buildClient(ctx, cfg)

	result := &protocol.WriteResult{}
	contactBuf := make([]map[string]interface{}, 0)

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
		def, ok := streamDefs[streamName]
		if !ok {
			result.Errors = append(result.Errors, protocol.WriteError{
				Message: fmt.Sprintf("unknown stream: %s", streamName),
				Record:  msg.Record.Data,
			})
			continue
		}

		if def.batchWrite {
			contactBuf = append(contactBuf, rec)
			if len(contactBuf) >= scrollPageSize {
				w, e := c.flushContactBatch(ctx, client, contactBuf)
				result.RecordsWritten += int64(w)
				result.Errors = append(result.Errors, e...)
				contactBuf = contactBuf[:0]
			}
		} else if def.writePath != "" {
			w, e := c.writeIndividual(ctx, client, streamName, def, rec)
			result.RecordsWritten += int64(w)
			result.Errors = append(result.Errors, e...)
		} else {
			result.Errors = append(result.Errors, protocol.WriteError{
				Message: fmt.Sprintf("stream %s does not support writes", streamName),
				Record:  msg.Record.Data,
			})
		}
	}

	// Flush remaining contacts.
	if len(contactBuf) > 0 {
		w, e := c.flushContactBatch(ctx, client, contactBuf)
		result.RecordsWritten += int64(w)
		result.Errors = append(result.Errors, e...)
	}

	return result, nil
}

// flushContactBatch sends a batch of contacts to Intercom.
func (c *Connector) flushContactBatch(ctx context.Context, client *restcommon.RESTClient, contacts []map[string]interface{}) (int, []protocol.WriteError) {
	// Intercom bulk user create: POST /contacts with items array.
	items := make([]map[string]interface{}, len(contacts))
	for i, contact := range contacts {
		item := map[string]interface{}{
			"data": contact,
		}
		// If the contact has an ID, set method to put (update), otherwise post (create).
		if _, hasID := contact["id"]; hasID {
			item["method"] = "put"
			item["data_type"] = "contact"
		} else {
			item["method"] = "post"
			item["data_type"] = "contact"
		}
		items[i] = item
	}

	body := map[string]interface{}{
		"items": items,
	}

	resp, err := client.Post(ctx, "/bulk/contacts", body)
	if err != nil {
		writeErrs := make([]protocol.WriteError, len(contacts))
		for i, rec := range contacts {
			data, _ := json.Marshal(rec)
			writeErrs[i] = protocol.WriteError{
				Message: fmt.Sprintf("bulk API error: %v", err),
				Record:  data,
			}
		}
		return 0, writeErrs
	}

	// Intercom bulk API returns a job ID; it processes asynchronously.
	// For simplicity in the connector, we treat the accepted response as success.
	if resp.StatusCode == 200 || resp.StatusCode == 202 {
		return len(contacts), nil
	}
	return 0, []protocol.WriteError{{Message: fmt.Sprintf("unexpected status: %d", resp.StatusCode)}}
}

// writeIndividual writes a single record to a non-batch-supported stream.
func (c *Connector) writeIndividual(ctx context.Context, client *restcommon.RESTClient, streamName string, def icStreamDef, rec map[string]interface{}) (int, []protocol.WriteError) {
	// If the record has an ID, use PUT for update; otherwise POST for create.
	if id, hasID := rec["id"]; hasID {
		path := fmt.Sprintf("%s/%v", def.writePath, id)
		_, err := client.Put(ctx, path, rec)
		if err != nil {
			data, _ := json.Marshal(rec)
			return 0, []protocol.WriteError{{
				Message: fmt.Sprintf("update %s: %v", streamName, err),
				Record:  data,
			}}
		}
		return 1, nil
	}

	_, err := client.Post(ctx, def.writePath, rec)
	if err != nil {
		data, _ := json.Marshal(rec)
		return 0, []protocol.WriteError{{
			Message: fmt.Sprintf("create %s: %v", streamName, err),
			Record:  data,
		}}
	}
	return 1, nil
}

// Capabilities returns the destination sync modes Intercom supports.
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

// WebhookHandler processes Intercom webhook events.
func (c *Connector) WebhookHandler(ctx context.Context, event cdk.WebhookEvent) error {
	var payload struct {
		Type  string                 `json:"type"`
		Topic string                 `json:"topic"`
		Data  map[string]interface{} `json:"data"`
		AppID string                 `json:"app_id"`
	}
	if err := json.Unmarshal(event.Body, &payload); err != nil {
		return errs.NewConnectorError(connectorName, "webhook_parse", err)
	}
	if payload.Topic == "" {
		return errs.NewConnectorError(connectorName, "webhook_validate",
			fmt.Errorf("missing topic in webhook payload"))
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
	cdk.RegisterBidirectional(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Intercom",
		Version:     connectorVersion,
		Category:    "Customer Messaging",
	}, &Connector{})
}
