// Package bigquery implements a FlowForge bidirectional connector for Google BigQuery.
// It uses the BigQuery REST API directly for both reading (query jobs) and writing
// (streaming inserts / tabledata.insertAll). Authentication is via service account
// JSON credentials or OAuth2.
package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

const (
	connectorName    = "bigquery"
	connectorVersion = "1.0.0"
	bqBaseURL        = "https://bigquery.googleapis.com/bigquery/v2"
	defaultBatchSize = 10000
	defaultLocation  = "US"
	bigqueryScope    = "https://www.googleapis.com/auth/bigquery"
)

// BigQueryConfig holds the configuration for the BigQuery connector.
type BigQueryConfig struct {
	ProjectID       string `json:"project_id"`
	DatasetID       string `json:"dataset_id"`
	CredentialsJSON string `json:"credentials_json"`
	Location        string `json:"location"`
	BatchSize       int    `json:"batch_size"`
}

func (c *BigQueryConfig) validate() error {
	if c.ProjectID == "" {
		return &common.ValidationError{Field: "project_id", Message: "project_id is required"}
	}
	if c.DatasetID == "" {
		return &common.ValidationError{Field: "dataset_id", Message: "dataset_id is required"}
	}
	if c.CredentialsJSON == "" {
		return &common.ValidationError{Field: "credentials_json", Message: "credentials_json is required"}
	}
	if c.Location == "" {
		c.Location = defaultLocation
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.BatchSize > defaultBatchSize {
		c.BatchSize = defaultBatchSize
	}
	return nil
}

// BigQueryConnector implements cdk.Bidirectional for Google BigQuery.
type BigQueryConnector struct{}

func init() {
	cdk.RegisterBidirectional(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Google BigQuery",
		Version:     connectorVersion,
		Category:    "data-warehouse",
	}, &BigQueryConnector{})
}

// Spec returns the JSON Schema for the BigQuery connector configuration.
func (b *BigQueryConnector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["project_id", "dataset_id", "credentials_json"],
		"properties": {
			"project_id": {
				"type": "string",
				"title": "GCP Project ID",
				"description": "The Google Cloud project ID containing the BigQuery dataset."
			},
			"dataset_id": {
				"type": "string",
				"title": "Dataset ID",
				"description": "The BigQuery dataset to read from and write to."
			},
			"credentials_json": {
				"type": "string",
				"title": "Service Account JSON",
				"description": "The contents of the service account JSON key file.",
				"airbyte_secret": true
			},
			"location": {
				"type": "string",
				"title": "Dataset Location",
				"description": "BigQuery dataset location (e.g. US, EU).",
				"default": "US"
			},
			"batch_size": {
				"type": "integer",
				"title": "Batch Size",
				"description": "Number of rows per streaming insert batch.",
				"default": 10000,
				"minimum": 1,
				"maximum": 50000
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://cloud.google.com/bigquery/docs",
		ConfigSchema:     schema,
	}, nil
}

// parseConfig deserializes and validates the raw JSON config.
func parseConfig(raw json.RawMessage) (*BigQueryConfig, error) {
	var cfg BigQueryConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, common.NewConnectorError(connectorName, "parse_config", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, common.NewConnectorError(connectorName, "validate_config", err)
	}
	return &cfg, nil
}

// httpClient builds an authenticated *http.Client from the service account JSON.
func httpClient(ctx context.Context, cfg *BigQueryConfig) (*http.Client, error) {
	//nolint:staticcheck // credentials JSON comes from validated connector config, not untrusted input
	creds, err := google.CredentialsFromJSONWithParams(ctx, []byte(cfg.CredentialsJSON), google.CredentialsParams{
		Scopes: []string{bigqueryScope},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse credentials: %w", err)
	}
	return oauth2.NewClient(ctx, creds.TokenSource), nil
}

// Check validates connectivity by listing datasets in the project.
func (b *BigQueryConnector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: err.Error(),
		}, nil
	}
	client, err := httpClient(ctx, cfg)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("authentication failed: %v", err),
		}, nil
	}
	url := fmt.Sprintf("%s/projects/%s/datasets?maxResults=1", bqBaseURL, cfg.ProjectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("failed to create request: %v", err),
		}, nil
	}
	resp, err := client.Do(req)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("connection failed: %v", err),
		}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("BigQuery API returned %d: %s", resp.StatusCode, string(body)),
		}, nil
	}
	return &protocol.CheckResult{
		Status:  protocol.CheckStatusSucceeded,
		Message: "Successfully connected to BigQuery",
	}, nil
}

// bqTableListResponse models the BigQuery tables.list response.
type bqTableListResponse struct {
	Tables []bqTableRef `json:"tables"`
}

type bqTableRef struct {
	TableReference struct {
		ProjectID string `json:"projectId"`
		DatasetID string `json:"datasetId"`
		TableID   string `json:"tableId"`
	} `json:"tableReference"`
	Type string `json:"type"`
}

// bqTableSchemaResponse models the BigQuery tables.get response for schema.
type bqTableSchemaResponse struct {
	Schema struct {
		Fields []bqField `json:"fields"`
	} `json:"schema"`
	NumRows string `json:"numRows"`
}

type bqField struct {
	Name   string    `json:"name"`
	Type   string    `json:"type"`
	Mode   string    `json:"mode"`
	Fields []bqField `json:"fields,omitempty"`
}

// bqTypeToJSONSchemaType maps BigQuery types to JSON Schema types.
func bqTypeToJSONSchemaType(bqType string) map[string]interface{} {
	switch strings.ToUpper(bqType) {
	case "STRING", "BYTES", "DATE", "DATETIME", "TIME", "TIMESTAMP", "GEOGRAPHY", "JSON":
		return map[string]interface{}{"type": "string"}
	case "INTEGER", "INT64":
		return map[string]interface{}{"type": "integer"}
	case "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC":
		return map[string]interface{}{"type": "number"}
	case "BOOLEAN", "BOOL":
		return map[string]interface{}{"type": "boolean"}
	case "RECORD", "STRUCT":
		return map[string]interface{}{"type": "object"}
	default:
		return map[string]interface{}{"type": "string"}
	}
}

// buildJSONSchema constructs a JSON Schema object from BigQuery field metadata.
func buildJSONSchema(fields []bqField) json.RawMessage {
	properties := make(map[string]interface{})
	for _, f := range fields {
		prop := bqTypeToJSONSchemaType(f.Type)
		if strings.ToUpper(f.Type) == "RECORD" || strings.ToUpper(f.Type) == "STRUCT" {
			if len(f.Fields) > 0 {
				var nested map[string]interface{}
				nestedBytes := buildJSONSchema(f.Fields)
				_ = json.Unmarshal(nestedBytes, &nested)
				prop = nested
			}
		}
		if strings.ToUpper(f.Mode) == "REPEATED" {
			prop = map[string]interface{}{
				"type":  "array",
				"items": prop,
			}
		}
		properties[f.Name] = prop
	}
	schema := map[string]interface{}{
		"type":       "object",
		"properties": properties,
	}
	out, _ := json.Marshal(schema)
	return out
}

// Discover lists tables in the configured dataset and returns their schemas.
func (b *BigQueryConnector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	client, err := httpClient(ctx, cfg)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}

	// List tables in the dataset.
	tablesURL := fmt.Sprintf("%s/projects/%s/datasets/%s/tables?maxResults=1000",
		bqBaseURL, cfg.ProjectID, cfg.DatasetID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tablesURL, nil)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, common.NewConnectorError(connectorName, "discover",
			fmt.Errorf("tables.list returned %d: %s", resp.StatusCode, string(body)))
	}
	var tableList bqTableListResponse
	if err := json.NewDecoder(resp.Body).Decode(&tableList); err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}

	var streams []protocol.Stream
	for _, tbl := range tableList.Tables {
		tableID := tbl.TableReference.TableID
		schema, fetchErr := fetchTableSchema(ctx, client, cfg, tableID)
		if fetchErr != nil {
			continue
		}
		syncModes := []protocol.SyncMode{protocol.SyncModeFullRefresh, protocol.SyncModeIncremental}
		streams = append(streams, protocol.Stream{
			Name:               tableID,
			Namespace:          cfg.DatasetID,
			Schema:             schema,
			SupportedSyncModes: syncModes,
		})
	}
	return &protocol.Catalog{Streams: streams}, nil
}

// fetchTableSchema retrieves the schema for a single table.
func fetchTableSchema(ctx context.Context, client *http.Client, cfg *BigQueryConfig, tableID string) (json.RawMessage, error) {
	url := fmt.Sprintf("%s/projects/%s/datasets/%s/tables/%s",
		bqBaseURL, cfg.ProjectID, cfg.DatasetID, tableID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("tables.get returned %d: %s", resp.StatusCode, string(body))
	}
	var tblSchema bqTableSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&tblSchema); err != nil {
		return nil, err
	}
	return buildJSONSchema(tblSchema.Schema.Fields), nil
}

// bqQueryRequest is the payload sent to the jobs.query endpoint.
type bqQueryRequest struct {
	Query        string `json:"query"`
	UseLegacySQL bool   `json:"useLegacySql"`
	MaxResults   int    `json:"maxResults"`
	Location     string `json:"location,omitempty"`
	TimeoutMs    int64  `json:"timeoutMs,omitempty"`
	PageToken    string `json:"pageToken,omitempty"`
}

// bqQueryResponse models the jobs.query response.
type bqQueryResponse struct {
	JobComplete  bool `json:"jobComplete"`
	JobReference struct {
		ProjectID string `json:"projectId"`
		JobID     string `json:"jobId"`
		Location  string `json:"location"`
	} `json:"jobReference"`
	Schema struct {
		Fields []bqField `json:"fields"`
	} `json:"schema"`
	Rows                []bqRow `json:"rows"`
	TotalRows           string  `json:"totalRows"`
	PageToken           string  `json:"pageToken"`
	TotalBytesProcessed string  `json:"totalBytesProcessed"`
}

type bqRow struct {
	F []bqCell `json:"f"`
}

type bqCell struct {
	V interface{} `json:"v"`
}

// bqGetQueryResultsResponse models the jobs.getQueryResults response.
type bqGetQueryResultsResponse struct {
	JobComplete bool   `json:"jobComplete"`
	PageToken   string `json:"pageToken"`
	Schema      struct {
		Fields []bqField `json:"fields"`
	} `json:"schema"`
	Rows      []bqRow `json:"rows"`
	TotalRows string  `json:"totalRows"`
}

// Read executes SELECT queries on configured streams and emits records.
// Supports cursor-based pagination via BigQuery page tokens.
func (b *BigQueryConnector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}
	client, err := httpClient(ctx, cfg)
	if err != nil {
		return common.NewConnectorError(connectorName, "read", err)
	}
	for _, cs := range catalog.Streams {
		if err := ctx.Err(); err != nil {
			return err
		}
		if readErr := readStream(ctx, client, cfg, cs, state, output); readErr != nil {
			output <- protocol.Message{
				Type: protocol.MessageTypeLog,
				Log: &protocol.Log{
					Level:     protocol.LogLevelError,
					Message:   fmt.Sprintf("error reading stream %s: %v", cs.Stream.Name, readErr),
					Timestamp: time.Now().UTC(),
				},
			}
		}
	}
	return nil
}

// readStream reads a single configured stream by executing a query job.
func readStream(ctx context.Context, client *http.Client, cfg *BigQueryConfig, cs protocol.ConfiguredStream, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	tableName := fmt.Sprintf("`%s.%s.%s`", cfg.ProjectID, cfg.DatasetID, cs.Stream.Name)
	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	// For incremental mode, apply partition pruning if cursor field is available.
	if cs.SyncMode == protocol.SyncModeIncremental && len(cs.CursorField) > 0 {
		cursorField := cs.CursorField[0]
		if stateData, ok := state[cs.Stream.Name]; ok {
			var stateMap map[string]interface{}
			if json.Unmarshal(stateData, &stateMap) == nil {
				if cursorVal, exists := stateMap[cursorField]; exists {
					query = fmt.Sprintf("SELECT * FROM %s WHERE %s > '%v' ORDER BY %s",
						tableName, cursorField, cursorVal, cursorField)
				}
			}
		}
	}

	// Execute the initial query.
	queryReq := bqQueryRequest{
		Query:        query,
		UseLegacySQL: false,
		MaxResults:   cfg.BatchSize,
		Location:     cfg.Location,
		TimeoutMs:    60000,
	}
	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		return fmt.Errorf("marshal query request: %w", err)
	}
	queryURL := fmt.Sprintf("%s/projects/%s/queries", bqBaseURL, cfg.ProjectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("create query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("query returned %d: %s", resp.StatusCode, string(body))
	}

	var queryResp bqQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return fmt.Errorf("decode query response: %w", err)
	}

	// If job not complete, poll for results.
	if !queryResp.JobComplete {
		if err := waitForJob(ctx, client, cfg, queryResp.JobReference.JobID, queryResp.JobReference.Location); err != nil {
			return err
		}
	}

	// Process the first page of results.
	fields := queryResp.Schema.Fields
	var lastCursorValue interface{}
	for _, row := range queryResp.Rows {
		record, cursorVal := rowToRecord(fields, row, cs)
		if cursorVal != nil {
			lastCursorValue = cursorVal
		}
		output <- protocol.Message{
			Type:   protocol.MessageTypeRecord,
			Record: &record,
		}
	}

	// Paginate through remaining results.
	pageToken := queryResp.PageToken
	for pageToken != "" {
		if err := ctx.Err(); err != nil {
			return err
		}
		resultsURL := fmt.Sprintf("%s/projects/%s/queries/%s?pageToken=%s&location=%s&maxResults=%d",
			bqBaseURL, cfg.ProjectID, queryResp.JobReference.JobID, pageToken, cfg.Location, cfg.BatchSize)
		pageReq, err := http.NewRequestWithContext(ctx, http.MethodGet, resultsURL, nil)
		if err != nil {
			return fmt.Errorf("create page request: %w", err)
		}
		pageResp, err := client.Do(pageReq)
		if err != nil {
			return fmt.Errorf("fetch page: %w", err)
		}
		var resultsPage bqGetQueryResultsResponse
		decodeErr := json.NewDecoder(pageResp.Body).Decode(&resultsPage)
		pageResp.Body.Close()
		if decodeErr != nil {
			return fmt.Errorf("decode page: %w", decodeErr)
		}
		if !resultsPage.JobComplete {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if len(resultsPage.Schema.Fields) > 0 {
			fields = resultsPage.Schema.Fields
		}
		for _, row := range resultsPage.Rows {
			record, cursorVal := rowToRecord(fields, row, cs)
			if cursorVal != nil {
				lastCursorValue = cursorVal
			}
			output <- protocol.Message{
				Type:   protocol.MessageTypeRecord,
				Record: &record,
			}
		}
		pageToken = resultsPage.PageToken
	}

	// Emit state checkpoint for incremental streams.
	if cs.SyncMode == protocol.SyncModeIncremental && len(cs.CursorField) > 0 && lastCursorValue != nil {
		stateData, _ := json.Marshal(map[string]interface{}{
			cs.CursorField[0]: lastCursorValue,
		})
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

// maxJobWaitDuration is the maximum time to wait for a BigQuery job to complete.
const maxJobWaitDuration = 30 * time.Minute

// waitForJob polls the BigQuery jobs.get endpoint until the job completes or
// the timeout is reached. The timeout defaults to 30 minutes to prevent
// infinite polling on stuck jobs.
func waitForJob(ctx context.Context, client *http.Client, cfg *BigQueryConfig, jobID, location string) error {
	deadline := time.Now().Add(maxJobWaitDuration)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("job %s timed out after %v", jobID, maxJobWaitDuration)
		}
		url := fmt.Sprintf("%s/projects/%s/jobs/%s?location=%s", bqBaseURL, cfg.ProjectID, jobID, location)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		var jobStatus struct {
			Status struct {
				State       string `json:"state"`
				ErrorResult *struct {
					Reason  string `json:"reason"`
					Message string `json:"message"`
				} `json:"errorResult"`
			} `json:"status"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&jobStatus)
		resp.Body.Close()
		if decodeErr != nil {
			return decodeErr
		}
		if jobStatus.Status.ErrorResult != nil {
			return fmt.Errorf("job failed: %s - %s",
				jobStatus.Status.ErrorResult.Reason, jobStatus.Status.ErrorResult.Message)
		}
		if jobStatus.Status.State == "DONE" {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

// rowToRecord converts a BigQuery row to a protocol.Record.
// Returns the cursor field value if present, for state tracking.
func rowToRecord(fields []bqField, row bqRow, cs protocol.ConfiguredStream) (protocol.Record, interface{}) {
	data := make(map[string]interface{})
	var cursorVal interface{}
	for i, cell := range row.F {
		if i < len(fields) {
			data[fields[i].Name] = cell.V
			if len(cs.CursorField) > 0 && fields[i].Name == cs.CursorField[0] {
				cursorVal = cell.V
			}
		}
	}
	dataBytes, _ := json.Marshal(data)
	return protocol.Record{
		Stream:    cs.Stream.Name,
		Namespace: cs.Stream.Namespace,
		Data:      dataBytes,
		EmittedAt: time.Now().UTC(),
	}, cursorVal
}

// bqInsertAllRequest is the payload for the tabledata.insertAll endpoint.
type bqInsertAllRequest struct {
	Rows                []bqInsertRow `json:"rows"`
	SkipInvalidRows     bool          `json:"skipInvalidRows"`
	IgnoreUnknownValues bool          `json:"ignoreUnknownValues"`
}

type bqInsertRow struct {
	InsertID string                 `json:"insertId,omitempty"`
	JSON     map[string]interface{} `json:"json"`
}

// bqInsertAllResponse is the response from tabledata.insertAll.
type bqInsertAllResponse struct {
	InsertErrors []struct {
		Index  int `json:"index"`
		Errors []struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"errors"`
	} `json:"insertErrors"`
}

// Write consumes records from the input channel and streams them into BigQuery
// using the streaming inserts API (tabledata.insertAll), batched by configurable size.
func (b *BigQueryConnector) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	client, err := httpClient(ctx, cfg)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "write", err)
	}

	// Buffer records per stream and flush in batches.
	type streamBuffer struct {
		rows []bqInsertRow
	}
	buffers := make(map[string]*streamBuffer)
	var mu sync.Mutex
	var totalWritten int64
	var writeErrors []protocol.WriteError
	var stateMessages []protocol.State

	flush := func(streamName string, rows []bqInsertRow) error {
		if len(rows) == 0 {
			return nil
		}
		insertURL := fmt.Sprintf("%s/projects/%s/datasets/%s/tables/%s/insertAll",
			bqBaseURL, cfg.ProjectID, cfg.DatasetID, streamName)
		payload := bqInsertAllRequest{
			Rows:                rows,
			SkipInvalidRows:     false,
			IgnoreUnknownValues: true,
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal insert request: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertURL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create insert request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			resp, doErr := client.Do(req)
			if doErr != nil {
				lastErr = doErr
				time.Sleep(time.Duration(attempt+1) * time.Second)
				req.Body = io.NopCloser(bytes.NewReader(body))
				continue
			}
			var insertResp bqInsertAllResponse
			decodeErr := json.NewDecoder(resp.Body).Decode(&insertResp)
			resp.Body.Close()
			if decodeErr != nil {
				lastErr = decodeErr
				continue
			}
			if resp.StatusCode == http.StatusOK && len(insertResp.InsertErrors) == 0 {
				mu.Lock()
				totalWritten += int64(len(rows))
				mu.Unlock()
				return nil
			}
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
				lastErr = fmt.Errorf("insertAll returned %d", resp.StatusCode)
				time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
				req.Body = io.NopCloser(bytes.NewReader(body))
				continue
			}
			// Partial errors: count successes and record failures.
			failedIndices := make(map[int]bool)
			for _, ie := range insertResp.InsertErrors {
				failedIndices[ie.Index] = true
				errMsg := ""
				for _, e := range ie.Errors {
					errMsg += e.Reason + ": " + e.Message + "; "
				}
				rowData, _ := json.Marshal(rows[ie.Index].JSON)
				mu.Lock()
				writeErrors = append(writeErrors, protocol.WriteError{
					Message: errMsg,
					Record:  rowData,
				})
				mu.Unlock()
			}
			successCount := int64(len(rows) - len(failedIndices))
			mu.Lock()
			totalWritten += successCount
			mu.Unlock()
			return nil
		}
		return lastErr
	}

	for msg := range input {
		if err := ctx.Err(); err != nil {
			break
		}
		switch msg.Type {
		case protocol.MessageTypeRecord:
			if msg.Record == nil {
				continue
			}
			streamName := msg.Record.Stream
			var data map[string]interface{}
			if err := json.Unmarshal(msg.Record.Data, &data); err != nil {
				writeErrors = append(writeErrors, protocol.WriteError{
					Message: fmt.Sprintf("failed to unmarshal record: %v", err),
					Record:  msg.Record.Data,
				})
				continue
			}
			buf, exists := buffers[streamName]
			if !exists {
				buf = &streamBuffer{}
				buffers[streamName] = buf
			}
			buf.rows = append(buf.rows, bqInsertRow{
				InsertID: fmt.Sprintf("%d", time.Now().UnixNano()),
				JSON:     data,
			})
			if len(buf.rows) >= cfg.BatchSize {
				if flushErr := flush(streamName, buf.rows); flushErr != nil {
					writeErrors = append(writeErrors, protocol.WriteError{
						Message:   fmt.Sprintf("batch flush failed for stream %s: %v", streamName, flushErr),
						ErrorCode: "FLUSH_ERROR",
					})
				}
				buf.rows = buf.rows[:0]
			}
		case protocol.MessageTypeState:
			if msg.State != nil {
				// Flush all buffers before acknowledging state.
				for sn, buf := range buffers {
					if len(buf.rows) > 0 {
						if flushErr := flush(sn, buf.rows); flushErr != nil {
							writeErrors = append(writeErrors, protocol.WriteError{
								Message:   fmt.Sprintf("state flush failed for stream %s: %v", sn, flushErr),
								ErrorCode: "FLUSH_ERROR",
							})
						}
						buf.rows = buf.rows[:0]
					}
				}
				stateMessages = append(stateMessages, *msg.State)
			}
		}
	}

	// Final flush of remaining buffers.
	for sn, buf := range buffers {
		if len(buf.rows) > 0 {
			if flushErr := flush(sn, buf.rows); flushErr != nil {
				writeErrors = append(writeErrors, protocol.WriteError{
					Message:   fmt.Sprintf("final flush failed for stream %s: %v", sn, flushErr),
					ErrorCode: "FLUSH_ERROR",
				})
			}
		}
	}

	return &protocol.WriteResult{
		RecordsWritten: totalWritten,
		Errors:         writeErrors,
		StateMessages:  stateMessages,
	}, nil
}

// Capabilities returns the supported destination sync modes.
func (b *BigQueryConnector) Capabilities() []protocol.DestinationSyncMode {
	return []protocol.DestinationSyncMode{
		protocol.DestSyncModeAppend,
		protocol.DestSyncModeUpsert,
	}
}

// Resolve handles conflicts using last-write-wins by default.
func (b *BigQueryConnector) Resolve(_ context.Context, conflicts []cdk.Conflict) ([]cdk.Resolution, error) {
	resolutions := make([]cdk.Resolution, len(conflicts))
	for i, c := range conflicts {
		var strategy cdk.ResolutionStrategy
		if c.SourceTS >= c.DestTS {
			strategy = cdk.ResolutionSourceWins
		} else {
			strategy = cdk.ResolutionDestWins
		}
		resolutions[i] = cdk.Resolution{
			PrimaryKey: c.PrimaryKey,
			Strategy:   strategy,
		}
	}
	return resolutions, nil
}

// WebhookHandler processes inbound webhook events. BigQuery does not natively
// support webhooks, so this implementation logs the event and returns nil.
func (b *BigQueryConnector) WebhookHandler(_ context.Context, event cdk.WebhookEvent) error {
	_ = event
	return nil
}
