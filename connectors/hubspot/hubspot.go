// Package hubspot implements a bidirectional FlowForge connector for HubSpot CRM.
// It uses the HubSpot CRM v3 API for reading/writing objects, the v3 search API
// for incremental reads with cursor pagination, and the batch API for efficient writes.
package hubspot

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
	baseURL          = "https://api.hubapi.com"
	maxBatchSize     = 100
	connectorName    = "hubspot"
	connectorVersion = "1.0.0"
)

// streamDefs maps stream names to HubSpot CRM object types and their primary key fields.
var streamDefs = map[string]streamDef{
	"contacts":    {objectType: "contacts", pk: "id", hasSearch: true},
	"companies":   {objectType: "companies", pk: "id", hasSearch: true},
	"deals":       {objectType: "deals", pk: "id", hasSearch: true},
	"tickets":     {objectType: "tickets", pk: "id", hasSearch: true},
	"line_items":  {objectType: "line_items", pk: "id", hasSearch: true},
	"products":    {objectType: "products", pk: "id", hasSearch: true},
	"engagements": {objectType: "engagements", pk: "id", hasSearch: false},
	"owners":      {objectType: "owners", pk: "id", hasSearch: false},
}

type streamDef struct {
	objectType string
	pk         string
	hasSearch  bool
}

// hsConfig holds parsed HubSpot connector configuration.
type hsConfig struct {
	OAuth  *restcommon.OAuthConfig `json:"oauth,omitempty"`
	APIKey string                  `json:"api_key,omitempty"`
}

func parseHSConfig(raw json.RawMessage) (*hsConfig, error) {
	var cfg hsConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, errs.NewConnectorError(connectorName, "parse_config", err)
	}
	if cfg.OAuth == nil && cfg.APIKey == "" {
		return nil, errs.NewConnectorError(connectorName, "parse_config",
			fmt.Errorf("either oauth or api_key must be provided"))
	}
	if cfg.OAuth != nil && cfg.OAuth.TokenURL == "" {
		cfg.OAuth.TokenURL = "https://api.hubapi.com/oauth/v1/token"
	}
	return &cfg, nil
}

// Connector implements cdk.Bidirectional for HubSpot.
type Connector struct{}

func (c *Connector) buildClient(ctx context.Context, cfg *hsConfig) *restcommon.RESTClient {
	opts := []restcommon.ClientOption{
		restcommon.WithRetry(3, 500*time.Millisecond),
		restcommon.WithRateLimit(10),
	}
	if cfg.OAuth != nil {
		ts := cfg.OAuth.TokenSource(ctx)
		opts = append(opts, restcommon.WithOAuth2(ts))
	} else if cfg.APIKey != "" {
		opts = append(opts, restcommon.WithAPIKey("Bearer "+cfg.APIKey, "Authorization"))
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
					"refresh_token": {"type": "string"},
					"access_token": {"type": "string"}
				}
			},
			"api_key": {
				"type": "string",
				"description": "HubSpot private app access token"
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.flowforge.io/connectors/hubspot",
		ConfigSchema:     schema,
	}, nil
}

// Check validates the HubSpot connection.
func (c *Connector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseHSConfig(config)
	if err != nil {
		return &protocol.CheckResult{Status: protocol.CheckStatusFailed, Message: err.Error()}, nil
	}
	client := c.buildClient(ctx, cfg)
	_, err = client.Get(ctx, "/crm/v3/objects/contacts?limit=1")
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("Failed to connect to HubSpot: %v", err),
		}, nil
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded, Message: "Connected to HubSpot successfully"}, nil
}

// Discover returns a catalog by fetching object schemas from HubSpot.
func (c *Connector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	cfg, err := parseHSConfig(config)
	if err != nil {
		return nil, err
	}
	client := c.buildClient(ctx, cfg)

	var streams []protocol.Stream
	for streamName, def := range streamDefs {
		schemaJSON := c.fetchObjectSchema(ctx, client, def.objectType)
		streams = append(streams, protocol.Stream{
			Name:               streamName,
			DisplayName:        formatDisplayName(streamName),
			Schema:             schemaJSON,
			SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh, protocol.SyncModeIncremental},
			DefaultCursorField: []string{"hs_lastmodifieddate"},
			SourceDefinedPK:    true,
			PrimaryKey:         [][]string{{def.pk}},
		})
	}
	return &protocol.Catalog{Streams: streams}, nil
}

// fetchObjectSchema fetches the CRM object schema from HubSpot and converts it to JSON Schema.
func (c *Connector) fetchObjectSchema(ctx context.Context, client *restcommon.RESTClient, objectType string) json.RawMessage {
	path := fmt.Sprintf("/crm/v3/schemas/%s", objectType)
	resp, err := client.Get(ctx, path)
	if err != nil {
		// If schema endpoint fails, try properties endpoint for standard objects.
		return c.fetchPropertiesSchema(ctx, client, objectType)
	}

	properties := make(map[string]interface{})
	if propsRaw, ok := resp.Body["properties"].([]interface{}); ok {
		for _, p := range propsRaw {
			prop, ok := p.(map[string]interface{})
			if !ok {
				continue
			}
			name, _ := prop["name"].(string)
			hsType, _ := prop["type"].(string)
			if name != "" {
				properties[name] = map[string]interface{}{
					"type": hsTypeToJSON(hsType),
				}
			}
		}
	}

	schemaJSON, _ := json.Marshal(map[string]interface{}{
		"type":       "object",
		"properties": properties,
	})
	return schemaJSON
}

// fetchPropertiesSchema uses the CRM v3 properties endpoint as a fallback.
func (c *Connector) fetchPropertiesSchema(ctx context.Context, client *restcommon.RESTClient, objectType string) json.RawMessage {
	path := fmt.Sprintf("/crm/v3/properties/%s", objectType)
	resp, err := client.Get(ctx, path)
	if err != nil {
		defaultSchema, _ := json.Marshal(map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{"id": map[string]string{"type": "string"}},
		})
		return defaultSchema
	}

	properties := map[string]interface{}{
		"id": map[string]interface{}{"type": "string"},
	}
	if results, ok := resp.Body["results"].([]interface{}); ok {
		for _, r := range results {
			prop, ok := r.(map[string]interface{})
			if !ok {
				continue
			}
			name, _ := prop["name"].(string)
			hsType, _ := prop["type"].(string)
			if name != "" {
				properties[name] = map[string]interface{}{
					"type": hsTypeToJSON(hsType),
				}
			}
		}
	}

	schemaJSON, _ := json.Marshal(map[string]interface{}{
		"type":       "object",
		"properties": properties,
	})
	return schemaJSON
}

// hsTypeToJSON maps HubSpot property types to JSON Schema types.
func hsTypeToJSON(hsType string) string {
	switch hsType {
	case "number":
		return "number"
	case "bool":
		return "boolean"
	case "enumeration", "string", "date", "datetime", "phone_number":
		return "string"
	default:
		return "string"
	}
}

// Read emits records from configured streams using the HubSpot CRM v3 search API
// for incremental sync, or the list API for full refresh.
func (c *Connector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseHSConfig(config)
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

func (c *Connector) readStream(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def streamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	// For incremental sync with search-capable objects, use the search API.
	if cs.SyncMode == protocol.SyncModeIncremental && def.hasSearch {
		return c.readViaSearch(ctx, client, cs, def, state, output)
	}
	// Otherwise use the list API with cursor pagination.
	return c.readViaList(ctx, client, cs, def, output)
}

// readViaSearch uses the HubSpot CRM v3 search API with lastModifiedDate filter.
func (c *Connector) readViaSearch(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def streamDef, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	var lastModified string
	if raw, ok := state[cs.Stream.Name]; ok {
		var s struct {
			LastModified string `json:"hs_lastmodifieddate"`
		}
		if json.Unmarshal(raw, &s) == nil && s.LastModified != "" {
			lastModified = s.LastModified
		}
	}

	properties := extractPropertyNames(cs.Stream.Schema)
	var maxModified string
	after := ""

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		searchBody := buildSearchRequest(properties, lastModified, after)
		path := fmt.Sprintf("/crm/v3/objects/%s/search", def.objectType)
		resp, err := client.Post(ctx, path, searchBody)
		if err != nil {
			return fmt.Errorf("search API: %w", err)
		}

		results, ok := resp.Body["results"].([]interface{})
		if !ok || len(results) == 0 {
			break
		}

		for _, item := range results {
			rec, ok := item.(map[string]interface{})
			if !ok {
				continue
			}

			// Flatten: merge properties into top-level with id.
			flat := map[string]interface{}{"id": rec["id"]}
			if props, ok := rec["properties"].(map[string]interface{}); ok {
				for k, v := range props {
					flat[k] = v
				}
			}

			data, _ := json.Marshal(flat)
			output <- protocol.Message{
				Type: protocol.MessageTypeRecord,
				Record: &protocol.Record{
					Stream:    cs.Stream.Name,
					Data:      data,
					EmittedAt: time.Now().UTC(),
				},
			}

			if lm, ok := flat["hs_lastmodifieddate"].(string); ok && lm > maxModified {
				maxModified = lm
			}
		}

		// Check for next page cursor.
		paging, ok := resp.Body["paging"].(map[string]interface{})
		if !ok {
			break
		}
		next, ok := paging["next"].(map[string]interface{})
		if !ok {
			break
		}
		nextAfter, ok := next["after"].(string)
		if !ok || nextAfter == "" {
			break
		}
		after = nextAfter
	}

	if maxModified != "" {
		stateData, _ := json.Marshal(map[string]string{"hs_lastmodifieddate": maxModified})
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

// buildSearchRequest constructs a HubSpot CRM v3 search request body.
func buildSearchRequest(properties []string, lastModified string, after string) map[string]interface{} {
	body := map[string]interface{}{
		"limit":      100,
		"properties": properties,
		"sorts": []map[string]interface{}{
			{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"},
		},
	}

	if lastModified != "" {
		body["filterGroups"] = []map[string]interface{}{
			{
				"filters": []map[string]interface{}{
					{
						"propertyName": "hs_lastmodifieddate",
						"operator":     "GTE",
						"value":        lastModified,
					},
				},
			},
		}
	}

	if after != "" {
		body["after"] = after
	}
	return body
}

// readViaList uses the HubSpot CRM v3 list endpoint with cursor pagination.
func (c *Connector) readViaList(ctx context.Context, client *restcommon.RESTClient, cs protocol.ConfiguredStream, def streamDef, output chan<- protocol.Message) error {
	properties := extractPropertyNames(cs.Stream.Schema)
	propParam := ""
	if len(properties) > 0 {
		propParam = "&properties=" + strings.Join(properties, ",")
	}

	after := ""
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := fmt.Sprintf("/crm/v3/objects/%s?limit=100%s", def.objectType, propParam)
		if after != "" {
			path += "&after=" + after
		}

		resp, err := client.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("list API: %w", err)
		}

		results, ok := resp.Body["results"].([]interface{})
		if !ok || len(results) == 0 {
			break
		}

		for _, item := range results {
			rec, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			flat := map[string]interface{}{"id": rec["id"]}
			if props, ok := rec["properties"].(map[string]interface{}); ok {
				for k, v := range props {
					flat[k] = v
				}
			}

			data, _ := json.Marshal(flat)
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
		paging, ok := resp.Body["paging"].(map[string]interface{})
		if !ok {
			break
		}
		next, ok := paging["next"].(map[string]interface{})
		if !ok {
			break
		}
		nextAfter, ok := next["after"].(string)
		if !ok || nextAfter == "" {
			break
		}
		after = nextAfter
	}
	return nil
}

// Write uses the HubSpot batch API for efficient writes.
func (c *Connector) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	cfg, err := parseHSConfig(config)
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

		if len(buffers[streamName]) >= maxBatchSize {
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

// flushBatch sends a batch of records to HubSpot using the batch upsert API.
func (c *Connector) flushBatch(ctx context.Context, client *restcommon.RESTClient, streamName string, records []map[string]interface{}) (int, []protocol.WriteError) {
	def, ok := streamDefs[streamName]
	if !ok {
		writeErrs := make([]protocol.WriteError, len(records))
		for i, rec := range records {
			data, _ := json.Marshal(rec)
			writeErrs[i] = protocol.WriteError{Message: fmt.Sprintf("unknown stream: %s", streamName), Record: data}
		}
		return 0, writeErrs
	}

	// Build batch request. HubSpot batch upsert expects an inputs array
	// with id (for updates) and properties.
	inputs := make([]map[string]interface{}, 0, len(records))
	for _, rec := range records {
		input := map[string]interface{}{}

		// Extract id for upsert; the rest go into properties.
		id, hasID := rec["id"]
		props := make(map[string]interface{})
		for k, v := range rec {
			if k == "id" {
				continue
			}
			props[k] = v
		}
		input["properties"] = props
		if hasID {
			input["id"] = id
		}
		inputs = append(inputs, input)
	}

	path := fmt.Sprintf("/crm/v3/objects/%s/batch/create", def.objectType)
	body := map[string]interface{}{"inputs": inputs}

	resp, err := client.Post(ctx, path, body)
	if err != nil {
		writeErrs := make([]protocol.WriteError, len(records))
		for i, rec := range records {
			data, _ := json.Marshal(rec)
			writeErrs[i] = protocol.WriteError{
				Message: fmt.Sprintf("batch API error: %v", err),
				Record:  data,
			}
		}
		return 0, writeErrs
	}

	return parseHSBatchResults(resp.Body, records)
}

// parseHSBatchResults interprets HubSpot batch API response.
func parseHSBatchResults(body map[string]interface{}, records []map[string]interface{}) (int, []protocol.WriteError) {
	status, _ := body["status"].(string)
	if status == "COMPLETE" {
		results, ok := body["results"].([]interface{})
		if ok {
			return len(results), nil
		}
		return len(records), nil
	}

	// Handle partial errors.
	var writeErrs []protocol.WriteError
	written := 0
	if results, ok := body["results"].([]interface{}); ok {
		written = len(results)
	}
	if errorsRaw, ok := body["errors"].([]interface{}); ok {
		for _, e := range errorsRaw {
			errObj, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			msg, _ := errObj["message"].(string)
			writeErrs = append(writeErrs, protocol.WriteError{Message: msg})
		}
	}
	return written, writeErrs
}

// Capabilities returns the destination sync modes HubSpot supports.
func (c *Connector) Capabilities() []protocol.DestinationSyncMode {
	return []protocol.DestinationSyncMode{
		protocol.DestSyncModeAppend,
		protocol.DestSyncModeUpsert,
	}
}

// Resolve handles conflicts with configurable strategy (defaults to last-write-wins).
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

// WebhookHandler processes HubSpot webhook events.
func (c *Connector) WebhookHandler(ctx context.Context, event cdk.WebhookEvent) error {
	var payload []struct {
		EventID          int64  `json:"eventId"`
		SubscriptionType string `json:"subscriptionType"`
		ObjectID         int64  `json:"objectId"`
		PropertyName     string `json:"propertyName"`
		ChangeSource     string `json:"changeSource"`
		OccurredAt       int64  `json:"occurredAt"`
	}
	if err := json.Unmarshal(event.Body, &payload); err != nil {
		return errs.NewConnectorError(connectorName, "webhook_parse", err)
	}
	if len(payload) == 0 {
		return errs.NewConnectorError(connectorName, "webhook_validate",
			fmt.Errorf("empty webhook payload"))
	}
	return nil
}

// extractPropertyNames returns property names from a JSON Schema.
func extractPropertyNames(schema json.RawMessage) []string {
	var s struct {
		Properties map[string]interface{} `json:"properties"`
	}
	if json.Unmarshal(schema, &s) != nil || s.Properties == nil {
		return nil
	}
	names := make([]string, 0, len(s.Properties))
	for k := range s.Properties {
		names = append(names, k)
	}
	return names
}

// formatDisplayName converts a snake_case stream name to Title Case.
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
		DisplayName: "HubSpot",
		Version:     connectorVersion,
		Category:    "CRM",
	}, &Connector{})
}
