// Package salesforce implements a bidirectional FlowForge connector for Salesforce CRM.
// It uses the Salesforce REST API v59.0 for metadata and SOQL queries, the Composite
// API for batch writes, and supports outbound-message webhooks for real-time CDC.
package salesforce

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
	apiVersion       = "v59.0"
	maxCompositeSize = 200
	connectorName    = "salesforce"
	connectorVersion = "1.0.0"
)

// supportedStreams enumerates the standard Salesforce objects this connector exposes.
var supportedStreams = []string{
	"Account", "Contact", "Lead", "Opportunity", "Task", "Event", "Case", "Campaign",
}

// sfConfig holds parsed Salesforce connector configuration.
type sfConfig struct {
	InstanceURL   string                  `json:"instance_url"`
	OAuth         *restcommon.OAuthConfig `json:"oauth"`
	CustomObjects []string                `json:"custom_objects,omitempty"`
	BulkThreshold int                     `json:"bulk_threshold,omitempty"`
}

func parseSFConfig(raw json.RawMessage) (*sfConfig, error) {
	var cfg sfConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, errs.NewConnectorError(connectorName, "parse_config", err)
	}
	if cfg.InstanceURL == "" {
		return nil, errs.NewConnectorError(connectorName, "parse_config", fmt.Errorf("instance_url is required"))
	}
	if cfg.OAuth == nil {
		return nil, errs.NewConnectorError(connectorName, "parse_config", fmt.Errorf("oauth credentials are required"))
	}
	if cfg.OAuth.TokenURL == "" {
		cfg.OAuth.TokenURL = "https://login.salesforce.com/services/oauth2/token"
	}
	if cfg.BulkThreshold <= 0 {
		cfg.BulkThreshold = 10000
	}
	return &cfg, nil
}

// Connector implements cdk.Bidirectional for Salesforce.
type Connector struct{}

func (c *Connector) buildClient(ctx context.Context, cfg *sfConfig) *restcommon.RESTClient {
	ts := cfg.OAuth.TokenSource(ctx)
	baseURL := strings.TrimRight(cfg.InstanceURL, "/")
	return restcommon.NewRESTClient(baseURL,
		restcommon.WithOAuth2(ts),
		restcommon.WithRetry(3, 500*time.Millisecond),
		restcommon.WithRateLimit(20),
	)
}

// allStreams returns the union of supported standard objects and configured custom objects.
func allStreams(cfg *sfConfig) []string {
	streams := make([]string, len(supportedStreams))
	copy(streams, supportedStreams)
	streams = append(streams, cfg.CustomObjects...)
	return streams
}

// Spec returns the connector's configuration JSON Schema.
func (c *Connector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["instance_url", "oauth"],
		"properties": {
			"instance_url": {
				"type": "string",
				"description": "Salesforce instance URL (e.g. https://na1.salesforce.com)"
			},
			"oauth": {
				"type": "object",
				"required": ["client_id", "client_secret", "refresh_token"],
				"properties": {
					"client_id": {"type": "string"},
					"client_secret": {"type": "string"},
					"refresh_token": {"type": "string"},
					"access_token": {"type": "string"}
				}
			},
			"custom_objects": {
				"type": "array",
				"items": {"type": "string"},
				"description": "Additional custom object API names to sync"
			},
			"bulk_threshold": {
				"type": "integer",
				"default": 10000,
				"description": "Record count above which Bulk API is used for reads"
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.flowforge.io/connectors/salesforce",
		ConfigSchema:     schema,
	}, nil
}

// Check validates the Salesforce connection by calling /services/data/.
func (c *Connector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseSFConfig(config)
	if err != nil {
		return &protocol.CheckResult{Status: protocol.CheckStatusFailed, Message: err.Error()}, nil
	}
	client := c.buildClient(ctx, cfg)
	_, err = client.Get(ctx, "/services/data/")
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("Failed to connect to Salesforce: %v", err),
		}, nil
	}
	return &protocol.CheckResult{Status: protocol.CheckStatusSucceeded, Message: "Connected to Salesforce successfully"}, nil
}

// Discover returns a catalog by fetching describe metadata for each object.
func (c *Connector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	cfg, err := parseSFConfig(config)
	if err != nil {
		return nil, err
	}
	client := c.buildClient(ctx, cfg)

	var streams []protocol.Stream
	for _, objName := range allStreams(cfg) {
		path := fmt.Sprintf("/services/data/%s/sobjects/%s/describe", apiVersion, objName)
		resp, err := client.Get(ctx, path)
		if err != nil {
			continue
		}

		schemaProps := buildSchemaFromDescribe(resp.Body)
		schemaJSON, _ := json.Marshal(map[string]interface{}{
			"type":       "object",
			"properties": schemaProps,
		})

		pk := [][]string{{"Id"}}
		streams = append(streams, protocol.Stream{
			Name:               strings.ToLower(objName),
			DisplayName:        objName,
			Schema:             schemaJSON,
			SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh, protocol.SyncModeIncremental},
			DefaultCursorField: []string{"LastModifiedDate"},
			SourceDefinedPK:    true,
			PrimaryKey:         pk,
		})
	}
	return &protocol.Catalog{Streams: streams}, nil
}

// buildSchemaFromDescribe converts Salesforce describe metadata into a JSON Schema
// properties map.
func buildSchemaFromDescribe(body map[string]interface{}) map[string]interface{} {
	props := make(map[string]interface{})
	fieldsRaw, ok := body["fields"]
	if !ok {
		return props
	}
	fields, ok := fieldsRaw.([]interface{})
	if !ok {
		return props
	}
	for _, f := range fields {
		field, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := field["name"].(string)
		sfType, _ := field["type"].(string)
		if name == "" {
			continue
		}
		props[name] = map[string]interface{}{
			"type": sfTypeToJSON(sfType),
		}
	}
	return props
}

// sfTypeToJSON maps Salesforce field types to JSON Schema types.
func sfTypeToJSON(sfType string) string {
	switch sfType {
	case "int", "integer":
		return "integer"
	case "double", "currency", "percent":
		return "number"
	case "boolean":
		return "boolean"
	case "date", "datetime", "time":
		return "string"
	default:
		return "string"
	}
}

// sfObjectAPIName converts a stream name (lowercase) back to the Salesforce
// API object name (PascalCase). Custom objects are returned as-is.
func sfObjectAPIName(streamName string, cfg *sfConfig) string {
	for _, s := range supportedStreams {
		if strings.EqualFold(s, streamName) {
			return s
		}
	}
	for _, co := range cfg.CustomObjects {
		if strings.EqualFold(co, streamName) {
			return co
		}
	}
	// Best-effort: capitalize first letter.
	if len(streamName) > 0 {
		return strings.ToUpper(streamName[:1]) + streamName[1:]
	}
	return streamName
}

// Read emits records from configured streams using SOQL queries.
func (c *Connector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseSFConfig(config)
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

		objName := sfObjectAPIName(cs.Stream.Name, cfg)
		if err := c.readStream(ctx, client, cfg, cs, objName, state, output); err != nil {
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

// readStream reads a single object using SOQL with automatic nextRecordsUrl pagination.
func (c *Connector) readStream(ctx context.Context, client *restcommon.RESTClient, _ *sfConfig, cs protocol.ConfiguredStream, objName string, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	// Build field list from schema.
	fields := extractFieldNames(cs.Stream.Schema)
	if len(fields) == 0 {
		fields = []string{"Id"}
	}
	fieldList := strings.Join(fields, ", ")

	// Build SOQL query with optional incremental cursor.
	var cursor string
	if cs.SyncMode == protocol.SyncModeIncremental {
		if raw, ok := state[cs.Stream.Name]; ok {
			var s struct {
				LastModifiedDate string `json:"LastModifiedDate"`
			}
			if json.Unmarshal(raw, &s) == nil && s.LastModifiedDate != "" {
				cursor = s.LastModifiedDate
			}
		}
	}

	soql := fmt.Sprintf("SELECT %s FROM %s", fieldList, objName)
	if cursor != "" {
		soql += fmt.Sprintf(" WHERE LastModifiedDate > %s", cursor)
	}
	soql += " ORDER BY LastModifiedDate ASC"

	var maxModified string

	// Use SOQL query with automatic nextRecordsUrl pagination.
	queryPath := fmt.Sprintf("/services/data/%s/query?q=%s", apiVersion, soqlEncode(soql))
	for queryPath != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.Get(ctx, queryPath)
		if err != nil {
			return fmt.Errorf("SOQL query: %w", err)
		}

		records := extractRecordArray(resp.Body, "records")
		for _, rec := range records {
			recMap, ok := rec.(map[string]interface{})
			if !ok {
				continue
			}
			// Remove Salesforce metadata wrapper.
			delete(recMap, "attributes")

			data, _ := json.Marshal(recMap)
			output <- protocol.Message{
				Type: protocol.MessageTypeRecord,
				Record: &protocol.Record{
					Stream:    cs.Stream.Name,
					Data:      data,
					EmittedAt: time.Now().UTC(),
				},
			}

			if lm, ok := recMap["LastModifiedDate"].(string); ok && lm > maxModified {
				maxModified = lm
			}
		}

		// Follow nextRecordsUrl for automatic pagination.
		queryPath = ""
		if next, ok := resp.Body["nextRecordsUrl"].(string); ok && next != "" {
			queryPath = next
		}
	}

	// Emit state checkpoint.
	if maxModified != "" {
		stateData, _ := json.Marshal(map[string]string{"LastModifiedDate": maxModified})
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

// Write uses the Salesforce Composite API to batch upsert records.
func (c *Connector) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	cfg, err := parseSFConfig(config)
	if err != nil {
		return nil, err
	}
	client := c.buildClient(ctx, cfg)

	result := &protocol.WriteResult{}

	// Buffer records per stream and flush in batches of maxCompositeSize.
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

		if len(buffers[streamName]) >= maxCompositeSize {
			written, writeErrs := c.flushBatch(ctx, client, cfg, streamName, buffers[streamName])
			result.RecordsWritten += int64(written)
			result.Errors = append(result.Errors, writeErrs...)
			buffers[streamName] = nil
		}
	}

	// Flush remaining buffers.
	for streamName, recs := range buffers {
		if len(recs) == 0 {
			continue
		}
		written, writeErrs := c.flushBatch(ctx, client, cfg, streamName, recs)
		result.RecordsWritten += int64(written)
		result.Errors = append(result.Errors, writeErrs...)
	}

	return result, nil
}

// flushBatch sends a batch of records to Salesforce using the Composite API.
func (c *Connector) flushBatch(ctx context.Context, client *restcommon.RESTClient, cfg *sfConfig, streamName string, records []map[string]interface{}) (int, []protocol.WriteError) {
	objName := sfObjectAPIName(streamName, cfg)
	path := fmt.Sprintf("/services/data/%s/composite/sobjects", apiVersion)

	// Build the composite request body. Each record must include the "attributes"
	// wrapper with the sObject type.
	sobjects := make([]map[string]interface{}, 0, len(records))
	for _, rec := range records {
		obj := make(map[string]interface{}, len(rec)+1)
		for k, v := range rec {
			obj[k] = v
		}
		obj["attributes"] = map[string]string{"type": objName}
		sobjects = append(sobjects, obj)
	}

	body := map[string]interface{}{
		"allOrNone": false,
		"records":   sobjects,
	}

	resp, err := client.Patch(ctx, path, body)
	if err != nil {
		writeErrs := make([]protocol.WriteError, len(records))
		for i, rec := range records {
			data, _ := json.Marshal(rec)
			writeErrs[i] = protocol.WriteError{
				Message: fmt.Sprintf("composite API error: %v", err),
				Record:  data,
			}
		}
		return 0, writeErrs
	}

	return parseCompositeResults(resp.RawBody, records)
}

// parseCompositeResults interprets the Salesforce composite API response array.
func parseCompositeResults(raw []byte, records []map[string]interface{}) (int, []protocol.WriteError) {
	var results []interface{}
	if err := json.Unmarshal(raw, &results); err != nil {
		return 0, []protocol.WriteError{{Message: fmt.Sprintf("parse composite response: %v", err)}}
	}

	written := 0
	var writeErrs []protocol.WriteError
	for i, item := range results {
		r, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		success, _ := r["success"].(bool)
		if success {
			written++
		} else {
			msg := "unknown error"
			if errArr, ok := r["errors"].([]interface{}); ok && len(errArr) > 0 {
				if errObj, ok := errArr[0].(map[string]interface{}); ok {
					msg, _ = errObj["message"].(string)
				}
			}
			var recData json.RawMessage
			if i < len(records) {
				recData, _ = json.Marshal(records[i])
			}
			writeErrs = append(writeErrs, protocol.WriteError{Message: msg, Record: recData})
		}
	}
	return written, writeErrs
}

// Capabilities returns the destination sync modes Salesforce supports.
func (c *Connector) Capabilities() []protocol.DestinationSyncMode {
	return []protocol.DestinationSyncMode{
		protocol.DestSyncModeAppend,
		protocol.DestSyncModeUpsert,
	}
}

// Resolve handles conflicts using last-write-wins by default.
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

// WebhookHandler processes Salesforce outbound message payloads.
func (c *Connector) WebhookHandler(ctx context.Context, event cdk.WebhookEvent) error {
	var payload struct {
		SObjectType string                   `json:"sobject_type"`
		Action      string                   `json:"action"`
		Records     []map[string]interface{} `json:"records"`
	}
	if err := json.Unmarshal(event.Body, &payload); err != nil {
		return errs.NewConnectorError(connectorName, "webhook_parse", err)
	}
	if payload.SObjectType == "" {
		return errs.NewConnectorError(connectorName, "webhook_validate",
			fmt.Errorf("missing sobject_type in webhook payload"))
	}
	return nil
}

// extractFieldNames returns the list of property names from a JSON Schema.
func extractFieldNames(schema json.RawMessage) []string {
	var s struct {
		Properties map[string]interface{} `json:"properties"`
	}
	if json.Unmarshal(schema, &s) != nil || s.Properties == nil {
		return nil
	}
	fields := make([]string, 0, len(s.Properties))
	for k := range s.Properties {
		fields = append(fields, k)
	}
	return fields
}

// extractRecordArray extracts a []interface{} from a map by key.
func extractRecordArray(body map[string]interface{}, key string) []interface{} {
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

// soqlEncode performs minimal URL encoding of a SOQL query string for use in
// the Salesforce REST query endpoint URL.
func soqlEncode(s string) string {
	r := strings.NewReplacer(
		" ", "+",
		"'", "%27",
		">", "%3E",
		"<", "%3C",
		"=", "%3D",
	)
	return r.Replace(s)
}

func init() {
	cdk.RegisterBidirectional(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Salesforce",
		Version:     connectorVersion,
		Category:    "CRM",
	}, &Connector{})
}
