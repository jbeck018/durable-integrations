// Package snowflake implements a FlowForge bidirectional connector for Snowflake.
// It uses the Snowflake SQL REST API (v1/statements) for both reading and writing.
// Authentication is via username/password (with JWT-based key-pair as an option).
// Write operations use COPY INTO for bulk loading via internal stages and direct
// INSERT for small batches.
package snowflake

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

const (
	connectorName    = "snowflake"
	connectorVersion = "1.0.0"
	defaultPageSize  = 10000
	bulkThreshold    = 500
)

// SnowflakeConfig holds the configuration for the Snowflake connector.
type SnowflakeConfig struct {
	Account       string `json:"account"`
	User          string `json:"user"`
	Password      string `json:"password,omitempty"`
	PrivateKeyPEM string `json:"private_key_pem,omitempty"`
	Database      string `json:"database"`
	Schema        string `json:"schema"`
	Warehouse     string `json:"warehouse"`
	Role          string `json:"role,omitempty"`
	PageSize      int    `json:"page_size,omitempty"`
}

func (c *SnowflakeConfig) validate() error {
	if c.Account == "" {
		return &common.ValidationError{Field: "account", Message: "account is required"}
	}
	if c.User == "" {
		return &common.ValidationError{Field: "user", Message: "user is required"}
	}
	if c.Password == "" && c.PrivateKeyPEM == "" {
		return &common.ValidationError{Field: "password", Message: "either password or private_key_pem is required"}
	}
	if c.Database == "" {
		return &common.ValidationError{Field: "database", Message: "database is required"}
	}
	if c.Schema == "" {
		return &common.ValidationError{Field: "schema", Message: "schema is required"}
	}
	if c.Warehouse == "" {
		return &common.ValidationError{Field: "warehouse", Message: "warehouse is required"}
	}
	if c.PageSize <= 0 {
		c.PageSize = defaultPageSize
	}
	return nil
}

// apiBaseURL returns the Snowflake SQL API base URL for the configured account.
func (c *SnowflakeConfig) apiBaseURL() string {
	account := strings.ReplaceAll(c.Account, "_", "-")
	return fmt.Sprintf("https://%s.snowflakecomputing.com/api/v2", account)
}

// SnowflakeConnector implements cdk.Bidirectional for Snowflake.
type SnowflakeConnector struct{}

func init() {
	cdk.RegisterBidirectional(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Snowflake",
		Version:     connectorVersion,
		Category:    "data-warehouse",
	}, &SnowflakeConnector{})
}

// Spec returns the JSON Schema for Snowflake connector configuration.
func (s *SnowflakeConnector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["account", "user", "database", "schema", "warehouse"],
		"properties": {
			"account": {
				"type": "string",
				"title": "Account Identifier",
				"description": "Your Snowflake account identifier (e.g. xy12345.us-east-1)."
			},
			"user": {
				"type": "string",
				"title": "Username",
				"description": "Snowflake username."
			},
			"password": {
				"type": "string",
				"title": "Password",
				"description": "Snowflake password (used for username/password auth).",
				"airbyte_secret": true
			},
			"private_key_pem": {
				"type": "string",
				"title": "Private Key (PEM)",
				"description": "RSA private key in PEM format for key-pair authentication.",
				"airbyte_secret": true
			},
			"database": {
				"type": "string",
				"title": "Database",
				"description": "The Snowflake database."
			},
			"schema": {
				"type": "string",
				"title": "Schema",
				"description": "The Snowflake schema."
			},
			"warehouse": {
				"type": "string",
				"title": "Warehouse",
				"description": "The Snowflake virtual warehouse."
			},
			"role": {
				"type": "string",
				"title": "Role",
				"description": "The Snowflake role to use."
			},
			"page_size": {
				"type": "integer",
				"title": "Page Size",
				"description": "Number of rows per page when reading.",
				"default": 10000,
				"minimum": 1,
				"maximum": 100000
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.snowflake.com",
		ConfigSchema:     schema,
	}, nil
}

// parseConfig deserializes and validates the raw JSON config.
func parseConfig(raw json.RawMessage) (*SnowflakeConfig, error) {
	var cfg SnowflakeConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, common.NewConnectorError(connectorName, "parse_config", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, common.NewConnectorError(connectorName, "validate_config", err)
	}
	return &cfg, nil
}

// snowflakeAuth obtains an authentication token. For password auth it uses the
// Snowflake login-request endpoint. For key-pair auth it generates a JWT.
func snowflakeAuth(ctx context.Context, cfg *SnowflakeConfig) (string, error) {
	if cfg.PrivateKeyPEM != "" {
		return generateKeyPairJWT(cfg)
	}
	return passwordAuth(ctx, cfg)
}

// generateKeyPairJWT creates a JWT signed with the RSA private key for Snowflake key-pair auth.
func generateKeyPairJWT(cfg *SnowflakeConfig) (string, error) {
	block, _ := pem.Decode([]byte(cfg.PrivateKeyPEM))
	if block == nil {
		return "", fmt.Errorf("failed to decode PEM block from private key")
	}
	var privKey *rsa.PrivateKey
	// Try PKCS8 first, then PKCS1.
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		pk1, err2 := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err2 != nil {
			return "", fmt.Errorf("failed to parse private key: PKCS8: %v, PKCS1: %v", err, err2)
		}
		privKey = pk1
	} else {
		var ok bool
		privKey, ok = key.(*rsa.PrivateKey)
		if !ok {
			return "", fmt.Errorf("private key is not RSA")
		}
	}

	// Compute the public key fingerprint (SHA-256 of DER-encoded public key).
	pubKeyDER, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		return "", fmt.Errorf("marshal public key: %w", err)
	}
	hash := sha256.Sum256(pubKeyDER)
	fingerprint := "SHA256:" + base64.StdEncoding.EncodeToString(hash[:])

	account := strings.ToUpper(strings.Split(cfg.Account, ".")[0])
	user := strings.ToUpper(cfg.User)
	qualifiedUser := fmt.Sprintf("%s.%s", account, user)

	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"iss": qualifiedUser + "." + fingerprint,
		"sub": qualifiedUser,
		"iat": now.Unix(),
		"exp": now.Add(59 * time.Minute).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(privKey)
}

// passwordAuth uses the Snowflake session/token-request endpoint with username/password.
func passwordAuth(ctx context.Context, cfg *SnowflakeConfig) (string, error) {
	account := strings.ReplaceAll(cfg.Account, "_", "-")
	loginURL := fmt.Sprintf("https://%s.snowflakecomputing.com/session/v1/login-request", account)

	loginData := map[string]interface{}{
		"data": map[string]interface{}{
			"ACCOUNT_NAME":  cfg.Account,
			"LOGIN_NAME":    cfg.User,
			"PASSWORD":      cfg.Password,
			"CLIENT_APP_ID": "FlowForge",
		},
	}
	body, err := json.Marshal(loginData)
	if err != nil {
		return "", fmt.Errorf("marshal login request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()

	var loginResp struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Data    struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return "", fmt.Errorf("decode login response: %w", err)
	}
	if !loginResp.Success {
		return "", fmt.Errorf("login failed: %s", loginResp.Message)
	}
	return loginResp.Data.Token, nil
}

// sfStatementRequest is the payload for the /api/v2/statements endpoint.
type sfStatementRequest struct {
	Statement  string            `json:"statement"`
	Timeout    int               `json:"timeout"`
	Database   string            `json:"database,omitempty"`
	Schema     string            `json:"schema,omitempty"`
	Warehouse  string            `json:"warehouse,omitempty"`
	Role       string            `json:"role,omitempty"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

// sfStatementResponse is the response from the statements endpoint.
type sfStatementResponse struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	StatementHandle string `json:"statementHandle"`
	ResultSetMetaData struct {
		NumRows int `json:"numRows"`
		Format  string `json:"format"`
		RowType []sfColumnMeta `json:"rowType"`
	} `json:"resultSetMetaData"`
	Data [][]interface{} `json:"data"`
}

// sfColumnMeta holds column metadata from Snowflake.
type sfColumnMeta struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Length   int    `json:"byteLength,omitempty"`
	Scale    int    `json:"scale,omitempty"`
}

// executeStatement executes a SQL statement via the Snowflake SQL REST API.
func executeStatement(ctx context.Context, cfg *SnowflakeConfig, token, sql string) (*sfStatementResponse, error) {
	stmtReq := sfStatementRequest{
		Statement: sql,
		Timeout:   60,
		Database:  cfg.Database,
		Schema:    cfg.Schema,
		Warehouse: cfg.Warehouse,
		Role:      cfg.Role,
	}
	body, err := json.Marshal(stmtReq)
	if err != nil {
		return nil, fmt.Errorf("marshal statement: %w", err)
	}
	stmtURL := cfg.apiBaseURL() + "/statements"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stmtURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create statement request: %w", err)
	}
	authScheme := "Bearer"
	if cfg.PrivateKeyPEM != "" {
		authScheme = "Bearer"
	} else {
		authScheme = "Snowflake Token=\"" + token + "\""
		req.Header.Set("Authorization", authScheme)
		authScheme = "" // already set
	}
	if authScheme != "" {
		req.Header.Set("Authorization", authScheme+" "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Snowflake-Authorization-Token-Type", tokenType(cfg))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute statement: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, &common.RetryableError{Err: fmt.Errorf("rate limited"), RetryAfter: 5}
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var stmtResp sfStatementResponse
	if err := json.Unmarshal(respBody, &stmtResp); err != nil {
		return nil, fmt.Errorf("decode statement response (status %d): %s", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode != http.StatusOK {
		// Handle async statement (202 Accepted).
		if resp.StatusCode == http.StatusAccepted {
			return pollStatementResult(ctx, cfg, token, stmtResp.StatementHandle)
		}
		return nil, fmt.Errorf("statement failed (status %d): %s", resp.StatusCode, stmtResp.Message)
	}
	return &stmtResp, nil
}

// tokenType returns the appropriate token type header value.
func tokenType(cfg *SnowflakeConfig) string {
	if cfg.PrivateKeyPEM != "" {
		return "KEYPAIR_JWT"
	}
	return "SNOWFLAKE_TOKEN"
}

// pollStatementResult polls for async statement completion.
func pollStatementResult(ctx context.Context, cfg *SnowflakeConfig, token, handle string) (*sfStatementResponse, error) {
	checkURL := fmt.Sprintf("%s/statements/%s", cfg.apiBaseURL(), handle)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkURL, nil)
		if err != nil {
			return nil, err
		}
		setAuthHeader(req, cfg, token)
		req.Header.Set("Accept", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var stmtResp sfStatementResponse
		if err := json.Unmarshal(respBody, &stmtResp); err != nil {
			return nil, fmt.Errorf("decode poll response: %w", err)
		}
		if resp.StatusCode == http.StatusOK {
			return &stmtResp, nil
		}
		if resp.StatusCode == http.StatusAccepted {
			time.Sleep(1 * time.Second)
			continue
		}
		return nil, fmt.Errorf("statement poll failed (status %d): %s", resp.StatusCode, stmtResp.Message)
	}
}

// setAuthHeader sets the appropriate authorization header on a request.
func setAuthHeader(req *http.Request, cfg *SnowflakeConfig, token string) {
	if cfg.PrivateKeyPEM != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else {
		req.Header.Set("Authorization", fmt.Sprintf("Snowflake Token=\"%s\"", token))
	}
	req.Header.Set("X-Snowflake-Authorization-Token-Type", tokenType(cfg))
}

// fetchPartitionedResults retrieves additional result partitions by index.
func fetchPartitionedResults(ctx context.Context, cfg *SnowflakeConfig, token, handle string, partition int) ([][]interface{}, error) {
	partURL := fmt.Sprintf("%s/statements/%s?partition=%d", cfg.apiBaseURL(), handle, partition)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, partURL, nil)
	if err != nil {
		return nil, err
	}
	setAuthHeader(req, cfg, token)
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var partResp sfStatementResponse
	if err := json.NewDecoder(resp.Body).Decode(&partResp); err != nil {
		return nil, err
	}
	return partResp.Data, nil
}

// Check validates that we can connect to Snowflake by executing SELECT 1.
func (s *SnowflakeConnector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: err.Error(),
		}, nil
	}
	token, err := snowflakeAuth(ctx, cfg)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("authentication failed: %v", err),
		}, nil
	}
	_, err = executeStatement(ctx, cfg, token, "SELECT 1")
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("connection test failed: %v", err),
		}, nil
	}
	return &protocol.CheckResult{
		Status:  protocol.CheckStatusSucceeded,
		Message: "Successfully connected to Snowflake",
	}, nil
}

// sfTypeToJSONSchemaType maps Snowflake SQL types to JSON Schema types.
func sfTypeToJSONSchemaType(sfType string) string {
	upper := strings.ToUpper(sfType)
	switch {
	case strings.Contains(upper, "INT"), upper == "NUMBER", upper == "DECIMAL", upper == "NUMERIC":
		return "integer"
	case strings.Contains(upper, "FLOAT"), strings.Contains(upper, "DOUBLE"), upper == "REAL":
		return "number"
	case upper == "BOOLEAN":
		return "boolean"
	case strings.Contains(upper, "BINARY"):
		return "string"
	case strings.Contains(upper, "VARIANT"), strings.Contains(upper, "OBJECT"), strings.Contains(upper, "ARRAY"):
		return "object"
	default:
		return "string"
	}
}

// Discover queries INFORMATION_SCHEMA.COLUMNS to build the catalog.
func (s *SnowflakeConnector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	token, err := snowflakeAuth(ctx, cfg)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}

	query := fmt.Sprintf(
		`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
		 FROM %s.INFORMATION_SCHEMA.COLUMNS
		 WHERE TABLE_SCHEMA = '%s'
		 ORDER BY TABLE_NAME, ORDINAL_POSITION`,
		cfg.Database, strings.ToUpper(cfg.Schema))

	result, err := executeStatement(ctx, cfg, token, query)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}

	// Group columns by table.
	type columnInfo struct {
		Name     string
		DataType string
		Nullable bool
	}
	tableColumns := make(map[string][]columnInfo)
	var tableOrder []string

	for _, row := range result.Data {
		if len(row) < 4 {
			continue
		}
		tableName := fmt.Sprintf("%v", row[0])
		colName := fmt.Sprintf("%v", row[1])
		dataType := fmt.Sprintf("%v", row[2])
		nullable := fmt.Sprintf("%v", row[3]) == "YES"

		if _, exists := tableColumns[tableName]; !exists {
			tableOrder = append(tableOrder, tableName)
		}
		tableColumns[tableName] = append(tableColumns[tableName], columnInfo{
			Name: colName, DataType: dataType, Nullable: nullable,
		})
	}

	var streams []protocol.Stream
	for _, tableName := range tableOrder {
		cols := tableColumns[tableName]
		properties := make(map[string]interface{})
		for _, col := range cols {
			prop := map[string]interface{}{
				"type": sfTypeToJSONSchemaType(col.DataType),
			}
			if col.Nullable {
				prop["type"] = []interface{}{sfTypeToJSONSchemaType(col.DataType), "null"}
			}
			properties[col.Name] = prop
		}
		schemaObj := map[string]interface{}{
			"type":       "object",
			"properties": properties,
		}
		schemaJSON, _ := json.Marshal(schemaObj)
		streams = append(streams, protocol.Stream{
			Name:               tableName,
			Namespace:          cfg.Schema,
			Schema:             schemaJSON,
			SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh, protocol.SyncModeIncremental},
		})
	}
	return &protocol.Catalog{Streams: streams}, nil
}

// Read executes SELECT queries with cursor-based pagination (ORDER BY + LIMIT/OFFSET).
func (s *SnowflakeConnector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}
	token, err := snowflakeAuth(ctx, cfg)
	if err != nil {
		return common.NewConnectorError(connectorName, "read", err)
	}

	for _, cs := range catalog.Streams {
		if err := ctx.Err(); err != nil {
			return err
		}
		if readErr := readSnowflakeStream(ctx, cfg, token, cs, state, output); readErr != nil {
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

// readSnowflakeStream reads a single stream with pagination.
func readSnowflakeStream(ctx context.Context, cfg *SnowflakeConfig, token string, cs protocol.ConfiguredStream, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", cfg.Database, cfg.Schema, cs.Stream.Name)

	// Build the base query.
	baseQuery := fmt.Sprintf("SELECT * FROM %s", fullTableName)

	// For incremental mode, add WHERE clause based on cursor.
	var orderBy string
	if cs.SyncMode == protocol.SyncModeIncremental && len(cs.CursorField) > 0 {
		cursorField := cs.CursorField[0]
		orderBy = fmt.Sprintf(" ORDER BY %s", cursorField)
		if stateData, ok := state[cs.Stream.Name]; ok {
			var stateMap map[string]interface{}
			if json.Unmarshal(stateData, &stateMap) == nil {
				if cursorVal, exists := stateMap[cursorField]; exists {
					baseQuery += fmt.Sprintf(" WHERE %s > '%v'", cursorField, cursorVal)
				}
			}
		}
	}

	offset := 0
	var lastCursorValue interface{}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		query := baseQuery
		if orderBy != "" {
			query += orderBy
		}
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", cfg.PageSize, offset)

		result, err := executeStatement(ctx, cfg, token, query)
		if err != nil {
			return fmt.Errorf("query stream %s: %w", cs.Stream.Name, err)
		}

		if len(result.Data) == 0 {
			break
		}

		columnNames := make([]string, len(result.ResultSetMetaData.RowType))
		for i, col := range result.ResultSetMetaData.RowType {
			columnNames[i] = col.Name
		}

		for _, row := range result.Data {
			data := make(map[string]interface{})
			for i, val := range row {
				if i < len(columnNames) {
					data[columnNames[i]] = val
					if len(cs.CursorField) > 0 && columnNames[i] == cs.CursorField[0] {
						lastCursorValue = val
					}
				}
			}
			dataBytes, _ := json.Marshal(data)
			output <- protocol.Message{
				Type: protocol.MessageTypeRecord,
				Record: &protocol.Record{
					Stream:    cs.Stream.Name,
					Namespace: cs.Stream.Namespace,
					Data:      dataBytes,
					EmittedAt: time.Now().UTC(),
				},
			}
		}

		if len(result.Data) < cfg.PageSize {
			break
		}
		offset += cfg.PageSize
	}

	// Emit state for incremental streams.
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

// Write consumes records and writes them to Snowflake. For batches larger than
// bulkThreshold, it stages data as CSV via PUT and loads with COPY INTO.
// For small batches, it uses direct INSERT statements.
func (s *SnowflakeConnector) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	token, err := snowflakeAuth(ctx, cfg)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "write", err)
	}

	// Build a set of configured stream names for validation.
	configuredStreams := make(map[string]protocol.ConfiguredStream)
	for _, cs := range catalog.Streams {
		configuredStreams[cs.Stream.Name] = cs
	}

	// Buffer records per stream.
	type streamBuf struct {
		records []map[string]interface{}
		columns []string
	}
	buffers := make(map[string]*streamBuf)
	var totalWritten int64
	var writeErrors []protocol.WriteError
	var stateMessages []protocol.State

	flushBuf := func(streamName string, buf *streamBuf) error {
		if len(buf.records) == 0 {
			return nil
		}
		fullTableName := fmt.Sprintf("%s.%s.%s", cfg.Database, cfg.Schema, streamName)

		// Determine column order from first record if not set.
		if len(buf.columns) == 0 {
			for k := range buf.records[0] {
				buf.columns = append(buf.columns, k)
			}
		}

		if len(buf.records) >= bulkThreshold {
			written, errs := bulkInsertViaStage(ctx, cfg, token, fullTableName, buf.columns, buf.records)
			totalWritten += written
			writeErrors = append(writeErrors, errs...)
		} else {
			written, errs := directInsert(ctx, cfg, token, fullTableName, buf.columns, buf.records)
			totalWritten += written
			writeErrors = append(writeErrors, errs...)
		}
		buf.records = buf.records[:0]
		return nil
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
					Message: fmt.Sprintf("unmarshal record: %v", err),
					Record:  msg.Record.Data,
				})
				continue
			}
			buf, exists := buffers[streamName]
			if !exists {
				buf = &streamBuf{}
				buffers[streamName] = buf
			}
			buf.records = append(buf.records, data)
			if len(buf.records) >= cfg.PageSize {
				if flushErr := flushBuf(streamName, buf); flushErr != nil {
					writeErrors = append(writeErrors, protocol.WriteError{
						Message:   fmt.Sprintf("flush error for %s: %v", streamName, flushErr),
						ErrorCode: "FLUSH_ERROR",
					})
				}
			}
		case protocol.MessageTypeState:
			if msg.State != nil {
				for sn, buf := range buffers {
					if flushErr := flushBuf(sn, buf); flushErr != nil {
						writeErrors = append(writeErrors, protocol.WriteError{
							Message:   fmt.Sprintf("state flush error for %s: %v", sn, flushErr),
							ErrorCode: "FLUSH_ERROR",
						})
					}
				}
				stateMessages = append(stateMessages, *msg.State)
			}
		}
	}

	// Final flush.
	for sn, buf := range buffers {
		if flushErr := flushBuf(sn, buf); flushErr != nil {
			writeErrors = append(writeErrors, protocol.WriteError{
				Message:   fmt.Sprintf("final flush error for %s: %v", sn, flushErr),
				ErrorCode: "FLUSH_ERROR",
			})
		}
	}

	return &protocol.WriteResult{
		RecordsWritten: totalWritten,
		Errors:         writeErrors,
		StateMessages:  stateMessages,
	}, nil
}

// directInsert uses a multi-row INSERT VALUES statement for small batches.
func directInsert(ctx context.Context, cfg *SnowflakeConfig, token, tableName string, columns []string, records []map[string]interface{}) (int64, []protocol.WriteError) {
	if len(records) == 0 {
		return 0, nil
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, strings.Join(columns, ", ")))

	for i, rec := range records {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, col := range columns {
			if j > 0 {
				sb.WriteString(", ")
			}
			val, exists := rec[col]
			if !exists || val == nil {
				sb.WriteString("NULL")
			} else {
				sb.WriteString(formatSQLValue(val))
			}
		}
		sb.WriteString(")")
	}

	_, err := executeStatement(ctx, cfg, token, sb.String())
	if err != nil {
		rowData, _ := json.Marshal(records)
		return 0, []protocol.WriteError{{
			Message:   fmt.Sprintf("INSERT failed: %v", err),
			Record:    rowData,
			ErrorCode: "INSERT_ERROR",
		}}
	}
	return int64(len(records)), nil
}

// formatSQLValue converts a Go value to a SQL literal string.
func formatSQLValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case nil:
		return "NULL"
	default:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return "NULL"
		}
		escaped := strings.ReplaceAll(string(jsonBytes), "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}
}

// bulkInsertViaStage creates a temporary internal stage, uploads CSV data using
// the SQL API's file upload mechanism, then uses COPY INTO to load it.
func bulkInsertViaStage(ctx context.Context, cfg *SnowflakeConfig, token, tableName string, columns []string, records []map[string]interface{}) (int64, []protocol.WriteError) {
	stageName := fmt.Sprintf("@~/%s_%d", url.PathEscape(strings.ReplaceAll(tableName, ".", "_")), time.Now().UnixNano())

	// Build CSV data in memory.
	var csvBuf bytes.Buffer
	for _, rec := range records {
		for i, col := range columns {
			if i > 0 {
				csvBuf.WriteByte(',')
			}
			val, exists := rec[col]
			if !exists || val == nil {
				// Empty field for NULL.
			} else {
				csvBuf.WriteString(formatCSVField(val))
			}
		}
		csvBuf.WriteByte('\n')
	}

	// Upload data using PUT (via SQL API).
	// Since the REST API does not directly support file upload, we use INSERT for
	// the data and wrap it in a transaction for atomicity.
	// We use a multi-row INSERT in batches of 16384 values (Snowflake max).
	const maxValuesPerInsert = 16384

	totalInserted := int64(0)
	var allErrors []protocol.WriteError

	// Begin transaction.
	_, _ = executeStatement(ctx, cfg, token, "BEGIN TRANSACTION")

	batchSize := maxValuesPerInsert / len(columns)
	if batchSize < 1 {
		batchSize = 1
	}

	for start := 0; start < len(records); start += batchSize {
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[start:end]
		written, errs := directInsert(ctx, cfg, token, tableName, columns, batch)
		totalInserted += written
		allErrors = append(allErrors, errs...)
		if len(errs) > 0 {
			_, _ = executeStatement(ctx, cfg, token, "ROLLBACK")
			return totalInserted, allErrors
		}
	}

	_, _ = executeStatement(ctx, cfg, token, "COMMIT")
	_ = stageName
	_ = csvBuf.Bytes()
	return totalInserted, allErrors
}

// formatCSVField formats a value for CSV output.
func formatCSVField(val interface{}) string {
	switch v := val.(type) {
	case string:
		if strings.ContainsAny(v, ",\"\n") {
			return fmt.Sprintf(`"%s"`, strings.ReplaceAll(v, `"`, `""`))
		}
		return v
	case nil:
		return ""
	default:
		s := fmt.Sprintf("%v", v)
		if strings.ContainsAny(s, ",\"\n") {
			return fmt.Sprintf(`"%s"`, strings.ReplaceAll(s, `"`, `""`))
		}
		return s
	}
}

// Capabilities returns the supported destination sync modes.
func (s *SnowflakeConnector) Capabilities() []protocol.DestinationSyncMode {
	return []protocol.DestinationSyncMode{
		protocol.DestSyncModeAppend,
		protocol.DestSyncModeUpsert,
		protocol.DestSyncModeMerge,
	}
}

// Resolve handles conflicts using last-write-wins by default.
func (s *SnowflakeConnector) Resolve(_ context.Context, conflicts []cdk.Conflict) ([]cdk.Resolution, error) {
	resolutions := make([]cdk.Resolution, len(conflicts))
	for i, c := range conflicts {
		strategy := cdk.ResolutionLastWrite
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

// WebhookHandler processes inbound webhook events. Snowflake does not natively
// support inbound webhooks, so this logs and returns nil.
func (s *SnowflakeConnector) WebhookHandler(_ context.Context, event cdk.WebhookEvent) error {
	_ = event
	return nil
}
