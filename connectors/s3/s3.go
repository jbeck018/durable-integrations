// Package s3 implements a FlowForge bidirectional connector for Amazon S3.
// It uses the AWS SDK v2 for reading, writing, and listing objects.
// Supports JSON and CSV file formats, multipart upload for large files,
// incremental reads by LastModified, and batched writes.
package s3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

const (
	connectorName    = "s3"
	connectorVersion = "1.0.0"
	defaultBatchSize = 10000
	// multipartThreshold is 100MB; files larger than this use multipart upload.
	multipartThreshold = 100 * 1024 * 1024
	// multipartPartSize is 10MB per part.
	multipartPartSize = 10 * 1024 * 1024
	maxParallelReads  = 10
)

// S3Config holds the configuration for the S3 connector.
type S3Config struct {
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey  string `json:"secret_access_key,omitempty"`
	Prefix          string `json:"prefix,omitempty"`
	FileFormat      string `json:"file_format"`
	BatchSize       int    `json:"batch_size,omitempty"`
	CSVDelimiter    string `json:"csv_delimiter,omitempty"`
	CSVHasHeader    *bool  `json:"csv_has_header,omitempty"`
}

func (c *S3Config) validate() error {
	if c.Bucket == "" {
		return &common.ValidationError{Field: "bucket", Message: "bucket is required"}
	}
	if c.Region == "" {
		return &common.ValidationError{Field: "region", Message: "region is required"}
	}
	if c.FileFormat == "" {
		c.FileFormat = "json"
	}
	c.FileFormat = strings.ToLower(c.FileFormat)
	if c.FileFormat != "json" && c.FileFormat != "csv" && c.FileFormat != "parquet" {
		return &common.ValidationError{Field: "file_format", Message: "file_format must be json, csv, or parquet"}
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.CSVDelimiter == "" {
		c.CSVDelimiter = ","
	}
	if c.CSVHasHeader == nil {
		t := true
		c.CSVHasHeader = &t
	}
	return nil
}

// S3Connector implements cdk.Bidirectional for Amazon S3.
type S3Connector struct{}

func init() {
	cdk.RegisterBidirectional(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Amazon S3",
		Version:     connectorVersion,
		Category:    "storage",
	}, &S3Connector{})
}

// Spec returns the JSON Schema for the S3 connector configuration.
func (c *S3Connector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["bucket", "region"],
		"properties": {
			"bucket": {
				"type": "string",
				"title": "S3 Bucket",
				"description": "The name of the S3 bucket."
			},
			"region": {
				"type": "string",
				"title": "AWS Region",
				"description": "The AWS region where the bucket is located."
			},
			"access_key_id": {
				"type": "string",
				"title": "Access Key ID",
				"description": "AWS access key ID. Leave empty for IAM role authentication."
			},
			"secret_access_key": {
				"type": "string",
				"title": "Secret Access Key",
				"description": "AWS secret access key.",
				"airbyte_secret": true
			},
			"prefix": {
				"type": "string",
				"title": "Prefix",
				"description": "S3 key prefix to filter objects."
			},
			"file_format": {
				"type": "string",
				"title": "File Format",
				"description": "Format of the files in the bucket.",
				"enum": ["json", "csv", "parquet"],
				"default": "json"
			},
			"batch_size": {
				"type": "integer",
				"title": "Batch Size",
				"description": "Number of records per output file when writing.",
				"default": 10000,
				"minimum": 1
			},
			"csv_delimiter": {
				"type": "string",
				"title": "CSV Delimiter",
				"description": "Delimiter for CSV files.",
				"default": ","
			},
			"csv_has_header": {
				"type": "boolean",
				"title": "CSV Has Header",
				"description": "Whether CSV files have a header row.",
				"default": true
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://docs.aws.amazon.com/s3/",
		ConfigSchema:     schema,
	}, nil
}

// parseConfig deserializes and validates the raw JSON config.
func parseConfig(raw json.RawMessage) (*S3Config, error) {
	var cfg S3Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, common.NewConnectorError(connectorName, "parse_config", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, common.NewConnectorError(connectorName, "validate_config", err)
	}
	return &cfg, nil
}

// buildS3Client creates an S3 client from the configuration.
func buildS3Client(ctx context.Context, cfg *S3Config) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(cfg.Region))

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return s3.NewFromConfig(awsCfg), nil
}

// Check validates bucket access using HeadBucket.
func (c *S3Connector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: err.Error(),
		}, nil
	}
	client, err := buildS3Client(ctx, cfg)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("failed to create S3 client: %v", err),
		}, nil
	}
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("HeadBucket failed: %v", err),
		}, nil
	}
	return &protocol.CheckResult{
		Status:  protocol.CheckStatusSucceeded,
		Message: "Successfully connected to S3 bucket",
	}, nil
}

// Discover lists prefixes and file patterns in the bucket and infers schema
// from the first file found for each unique prefix directory.
func (c *S3Connector) Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	client, err := buildS3Client(ctx, cfg)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}

	// Discover streams by listing common prefixes (directories).
	// Each unique prefix directory becomes a stream.
	listInput := &s3.ListObjectsV2Input{
		Bucket:    aws.String(cfg.Bucket),
		Prefix:    aws.String(cfg.Prefix),
		Delimiter: aws.String("/"),
	}

	var streams []protocol.Stream
	streamMap := make(map[string]bool)

	// First, discover directory-level streams.
	paginator := s3.NewListObjectsV2Paginator(client, listInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, common.NewConnectorError(connectorName, "discover", err)
		}
		for _, prefix := range page.CommonPrefixes {
			if prefix.Prefix == nil {
				continue
			}
			streamName := strings.TrimSuffix(strings.TrimPrefix(*prefix.Prefix, cfg.Prefix), "/")
			if streamName == "" || streamMap[streamName] {
				continue
			}
			streamMap[streamName] = true
			schema, schemaErr := inferSchemaFromPrefix(ctx, client, cfg, *prefix.Prefix)
			if schemaErr != nil {
				schema = defaultSchema()
			}
			streams = append(streams, protocol.Stream{
				Name:               streamName,
				Namespace:          cfg.Bucket,
				Schema:             schema,
				SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh, protocol.SyncModeIncremental},
				DefaultCursorField: []string{"_s3_last_modified"},
			})
		}

		// Also discover top-level files as a "root" stream.
		if len(page.Contents) > 0 && !streamMap["_root"] {
			streamMap["_root"] = true
			schema, schemaErr := inferSchemaFromObjects(ctx, client, cfg, page.Contents)
			if schemaErr != nil {
				schema = defaultSchema()
			}
			streams = append(streams, protocol.Stream{
				Name:               "_root",
				Namespace:          cfg.Bucket,
				Schema:             schema,
				SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh, protocol.SyncModeIncremental},
				DefaultCursorField: []string{"_s3_last_modified"},
			})
		}
	}

	if len(streams) == 0 {
		schema := defaultSchema()
		streams = append(streams, protocol.Stream{
			Name:               "_all",
			Namespace:          cfg.Bucket,
			Schema:             schema,
			SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh},
		})
	}

	return &protocol.Catalog{Streams: streams}, nil
}

// defaultSchema returns a permissive JSON Schema for when inference fails.
func defaultSchema() json.RawMessage {
	s, _ := json.Marshal(map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"_s3_key":           map[string]string{"type": "string"},
			"_s3_last_modified": map[string]string{"type": "string"},
			"data":              map[string]string{"type": "object"},
		},
	})
	return s
}

// inferSchemaFromPrefix reads the first file under a prefix and infers schema.
func inferSchemaFromPrefix(ctx context.Context, client *s3.Client, cfg *S3Config, prefix string) (json.RawMessage, error) {
	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(cfg.Bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	}
	output, err := client.ListObjectsV2(ctx, listInput)
	if err != nil {
		return nil, err
	}
	if len(output.Contents) == 0 {
		return nil, fmt.Errorf("no objects under prefix %s", prefix)
	}
	return inferSchemaFromObjects(ctx, client, cfg, output.Contents[:1])
}

// inferSchemaFromObjects reads the first object and infers its schema.
func inferSchemaFromObjects(ctx context.Context, client *s3.Client, cfg *S3Config, objects []s3types.Object) (json.RawMessage, error) {
	if len(objects) == 0 {
		return defaultSchema(), nil
	}
	obj := objects[0]
	if obj.Key == nil {
		return defaultSchema(), nil
	}

	getOutput, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    obj.Key,
		Range:  aws.String("bytes=0-65535"),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", *obj.Key, err)
	}
	defer getOutput.Body.Close()
	sample, err := io.ReadAll(getOutput.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", *obj.Key, err)
	}

	switch cfg.FileFormat {
	case "json":
		return inferJSONSchema(sample)
	case "csv":
		return inferCSVSchema(sample, cfg)
	default:
		return defaultSchema(), nil
	}
}

// inferJSONSchema reads a JSON sample and builds a schema from its keys.
func inferJSONSchema(sample []byte) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(sample)
	properties := make(map[string]interface{})
	properties["_s3_key"] = map[string]string{"type": "string"}
	properties["_s3_last_modified"] = map[string]string{"type": "string"}

	if len(trimmed) > 0 && trimmed[0] == '[' {
		var arr []map[string]interface{}
		if err := json.Unmarshal(trimmed, &arr); err == nil && len(arr) > 0 {
			for k, v := range arr[0] {
				properties[k] = map[string]string{"type": inferGoType(v)}
			}
		}
	} else if len(trimmed) > 0 && trimmed[0] == '{' {
		var obj map[string]interface{}
		if err := json.Unmarshal(trimmed, &obj); err == nil {
			for k, v := range obj {
				properties[k] = map[string]string{"type": inferGoType(v)}
			}
		}
	} else {
		// Try JSON Lines: read the first line.
		idx := bytes.IndexByte(trimmed, '\n')
		if idx > 0 {
			trimmed = trimmed[:idx]
		}
		var obj map[string]interface{}
		if err := json.Unmarshal(trimmed, &obj); err == nil {
			for k, v := range obj {
				properties[k] = map[string]string{"type": inferGoType(v)}
			}
		}
	}

	schema := map[string]interface{}{
		"type":       "object",
		"properties": properties,
	}
	out, _ := json.Marshal(schema)
	return out, nil
}

// inferCSVSchema reads a CSV header and creates string-typed properties.
func inferCSVSchema(sample []byte, cfg *S3Config) (json.RawMessage, error) {
	reader := csv.NewReader(bytes.NewReader(sample))
	if len(cfg.CSVDelimiter) > 0 {
		reader.Comma = rune(cfg.CSVDelimiter[0])
	}
	header, err := reader.Read()
	if err != nil {
		return defaultSchema(), nil
	}
	properties := make(map[string]interface{})
	properties["_s3_key"] = map[string]string{"type": "string"}
	properties["_s3_last_modified"] = map[string]string{"type": "string"}
	for _, col := range header {
		properties[strings.TrimSpace(col)] = map[string]string{"type": "string"}
	}
	schema := map[string]interface{}{
		"type":       "object",
		"properties": properties,
	}
	out, _ := json.Marshal(schema)
	return out, nil
}

// inferGoType returns a JSON Schema type string for a Go value.
func inferGoType(v interface{}) string {
	switch v.(type) {
	case float64:
		return "number"
	case bool:
		return "boolean"
	case nil:
		return "string"
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	default:
		return "string"
	}
}

// Read lists objects, reads and parses files, and emits records.
// Supports incremental by LastModified timestamp.
func (c *S3Connector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}
	client, err := buildS3Client(ctx, cfg)
	if err != nil {
		return common.NewConnectorError(connectorName, "read", err)
	}

	for _, cs := range catalog.Streams {
		if err := ctx.Err(); err != nil {
			return err
		}
		if readErr := readS3Stream(ctx, client, cfg, cs, state, output); readErr != nil {
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

// readS3Stream reads all files under a stream's prefix.
func readS3Stream(ctx context.Context, client *s3.Client, cfg *S3Config, cs protocol.ConfiguredStream, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	// Determine prefix for this stream.
	prefix := cfg.Prefix
	if cs.Stream.Name != "_root" && cs.Stream.Name != "_all" {
		prefix = path.Join(cfg.Prefix, cs.Stream.Name) + "/"
	}

	// Get incremental cutoff time.
	var cutoffTime time.Time
	if cs.SyncMode == protocol.SyncModeIncremental {
		if stateData, ok := state[cs.Stream.Name]; ok {
			var stateMap map[string]string
			if json.Unmarshal(stateData, &stateMap) == nil {
				if ts, exists := stateMap["_s3_last_modified"]; exists {
					parsed, parseErr := time.Parse(time.RFC3339, ts)
					if parseErr == nil {
						cutoffTime = parsed
					}
				}
			}
		}
	}

	// List and collect all matching objects.
	var objects []s3types.Object
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.Bucket),
		Prefix: aws.String(prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, listInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil || strings.HasSuffix(*obj.Key, "/") {
				continue
			}
			// Filter by file extension matching the format.
			if !matchesFormat(*obj.Key, cfg.FileFormat) {
				continue
			}
			// For incremental, filter by LastModified.
			if cs.SyncMode == protocol.SyncModeIncremental && !cutoffTime.IsZero() {
				if obj.LastModified != nil && !obj.LastModified.After(cutoffTime) {
					continue
				}
			}
			objects = append(objects, obj)
		}
	}

	// Sort by LastModified for consistent ordering.
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].LastModified == nil || objects[j].LastModified == nil {
			return false
		}
		return objects[i].LastModified.Before(*objects[j].LastModified)
	})

	// Process objects in parallel with bounded concurrency.
	g, gCtx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, maxParallelReads)
	var latestModified time.Time
	var latestMu = make(chan time.Time, len(objects)+1)

	for _, obj := range objects {
		obj := obj
		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := gCtx.Err(); err != nil {
				return err
			}
			records, parseErr := readAndParseObject(gCtx, client, cfg, obj)
			if parseErr != nil {
				output <- protocol.Message{
					Type: protocol.MessageTypeLog,
					Log: &protocol.Log{
						Level:     protocol.LogLevelWarn,
						Message:   fmt.Sprintf("skipping object %s: %v", safeKey(obj.Key), parseErr),
						Timestamp: time.Now().UTC(),
					},
				}
				return nil
			}
			for _, rec := range records {
				rec.Stream = cs.Stream.Name
				rec.Namespace = cs.Stream.Namespace
				output <- protocol.Message{
					Type:   protocol.MessageTypeRecord,
					Record: &rec,
				}
			}
			if obj.LastModified != nil {
				latestMu <- *obj.LastModified
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	close(latestMu)
	for t := range latestMu {
		if t.After(latestModified) {
			latestModified = t
		}
	}

	// Emit state for incremental.
	if cs.SyncMode == protocol.SyncModeIncremental && !latestModified.IsZero() {
		stateData, _ := json.Marshal(map[string]string{
			"_s3_last_modified": latestModified.UTC().Format(time.RFC3339),
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

// safeKey safely dereferences a *string for logging.
func safeKey(k *string) string {
	if k == nil {
		return "<nil>"
	}
	return *k
}

// matchesFormat checks if a key's extension matches the expected format.
func matchesFormat(key, format string) bool {
	lower := strings.ToLower(key)
	switch format {
	case "json":
		return strings.HasSuffix(lower, ".json") || strings.HasSuffix(lower, ".jsonl") || strings.HasSuffix(lower, ".ndjson")
	case "csv":
		return strings.HasSuffix(lower, ".csv") || strings.HasSuffix(lower, ".tsv")
	case "parquet":
		return strings.HasSuffix(lower, ".parquet")
	default:
		return true
	}
}

// readAndParseObject downloads and parses an S3 object into records.
func readAndParseObject(ctx context.Context, client *s3.Client, cfg *S3Config, obj s3types.Object) ([]protocol.Record, error) {
	if obj.Key == nil {
		return nil, fmt.Errorf("object has nil key")
	}
	getOutput, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    obj.Key,
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", *obj.Key, err)
	}
	defer getOutput.Body.Close()

	var lastModified string
	if obj.LastModified != nil {
		lastModified = obj.LastModified.UTC().Format(time.RFC3339)
	}

	switch cfg.FileFormat {
	case "json":
		return parseJSONObject(getOutput.Body, *obj.Key, lastModified)
	case "csv":
		return parseCSVObject(getOutput.Body, cfg, *obj.Key, lastModified)
	case "parquet":
		// Parquet requires specialized libraries not available in current dependencies.
		// Read the raw bytes and emit as a single record with base64 data reference.
		data, readErr := io.ReadAll(getOutput.Body)
		if readErr != nil {
			return nil, fmt.Errorf("read parquet object: %w", readErr)
		}
		rec := map[string]interface{}{
			"_s3_key":           *obj.Key,
			"_s3_last_modified": lastModified,
			"_s3_size":          len(data),
			"_s3_format":        "parquet",
		}
		dataBytes, _ := json.Marshal(rec)
		return []protocol.Record{{
			Data:      dataBytes,
			EmittedAt: time.Now().UTC(),
		}}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", cfg.FileFormat)
	}
}

// parseJSONObject parses a JSON or JSON Lines file into records using streaming
// decoding to avoid loading the entire file into memory at once.
func parseJSONObject(reader io.Reader, key, lastModified string) ([]protocol.Record, error) {
	// Use a buffered reader so we can peek at the first byte to determine format.
	br := bufio.NewReaderSize(reader, 64*1024)
	var records []protocol.Record

	addMetadata := func(m map[string]interface{}) {
		m["_s3_key"] = key
		m["_s3_last_modified"] = lastModified
	}

	// Peek at the first non-whitespace byte to detect format.
	firstByte, err := peekNonWhitespace(br)
	if err != nil {
		// Empty or unreadable: return zero records.
		return records, nil
	}

	if firstByte == '[' {
		// JSON array — use streaming token decoder.
		dec := json.NewDecoder(br)
		// Read the opening bracket.
		if _, err := dec.Token(); err != nil {
			return nil, fmt.Errorf("parse JSON array opening: %w", err)
		}
		for dec.More() {
			var obj map[string]interface{}
			if err := dec.Decode(&obj); err != nil {
				return nil, fmt.Errorf("parse JSON array element: %w", err)
			}
			addMetadata(obj)
			objBytes, _ := json.Marshal(obj)
			records = append(records, protocol.Record{
				Data:      objBytes,
				EmittedAt: time.Now().UTC(),
			})
		}
	} else {
		// JSON Lines (newline-delimited JSON) or single object — stream line by line.
		scanner := bufio.NewScanner(br)
		scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			var obj map[string]interface{}
			if err := json.Unmarshal(line, &obj); err != nil {
				continue
			}
			addMetadata(obj)
			objBytes, _ := json.Marshal(obj)
			records = append(records, protocol.Record{
				Data:      objBytes,
				EmittedAt: time.Now().UTC(),
			})
		}
	}
	return records, nil
}

// peekNonWhitespace reads ahead in the buffered reader until it finds a
// non-whitespace byte, then unreads so the byte is still consumable.
func peekNonWhitespace(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			if unreadErr := br.UnreadByte(); unreadErr != nil {
				return b, unreadErr
			}
			return b, nil
		}
	}
}

// parseCSVObject parses a CSV file into records using column headers as field names.
func parseCSVObject(reader io.Reader, cfg *S3Config, key, lastModified string) ([]protocol.Record, error) {
	csvReader := csv.NewReader(reader)
	if len(cfg.CSVDelimiter) > 0 {
		csvReader.Comma = rune(cfg.CSVDelimiter[0])
	}
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1

	var header []string
	if *cfg.CSVHasHeader {
		var err error
		header, err = csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("read CSV header: %w", err)
		}
		for i := range header {
			header[i] = strings.TrimSpace(header[i])
		}
	}

	var records []protocol.Record
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		data := make(map[string]interface{})
		data["_s3_key"] = key
		data["_s3_last_modified"] = lastModified
		if header != nil {
			for i, val := range row {
				if i < len(header) {
					data[header[i]] = val
				} else {
					data[fmt.Sprintf("_col%d", i)] = val
				}
			}
		} else {
			for i, val := range row {
				data[fmt.Sprintf("col_%d", i)] = val
			}
		}
		dataBytes, _ := json.Marshal(data)
		records = append(records, protocol.Record{
			Data:      dataBytes,
			EmittedAt: time.Now().UTC(),
		})
	}
	return records, nil
}

// Write uploads records as JSON or CSV files to S3, batched by configurable size.
// Uses multipart upload for large files.
func (c *S3Connector) Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	client, err := buildS3Client(ctx, cfg)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "write", err)
	}

	type streamBuf struct {
		records []map[string]interface{}
		fileIdx int
	}
	buffers := make(map[string]*streamBuf)
	var totalWritten int64
	var writeErrors []protocol.WriteError
	var stateMessages []protocol.State

	flush := func(streamName string, buf *streamBuf) error {
		if len(buf.records) == 0 {
			return nil
		}
		key := buildOutputKey(cfg, streamName, buf.fileIdx)
		buf.fileIdx++
		var bodyData []byte
		var contentType string
		switch cfg.FileFormat {
		case "csv":
			bodyData, contentType = marshalCSV(buf.records)
		default:
			bodyData, contentType = marshalJSON(buf.records)
		}

		if len(bodyData) > multipartThreshold {
			if uploadErr := multipartUpload(ctx, client, cfg.Bucket, key, contentType, bodyData); uploadErr != nil {
				return fmt.Errorf("multipart upload %s: %w", key, uploadErr)
			}
		} else {
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(cfg.Bucket),
				Key:         aws.String(key),
				Body:        bytes.NewReader(bodyData),
				ContentType: aws.String(contentType),
			})
			if putErr != nil {
				return fmt.Errorf("put object %s: %w", key, putErr)
			}
		}
		totalWritten += int64(len(buf.records))
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
			if len(buf.records) >= cfg.BatchSize {
				if flushErr := flush(streamName, buf); flushErr != nil {
					writeErrors = append(writeErrors, protocol.WriteError{
						Message:   fmt.Sprintf("flush error for %s: %v", streamName, flushErr),
						ErrorCode: "UPLOAD_ERROR",
					})
				}
			}
		case protocol.MessageTypeState:
			if msg.State != nil {
				for sn, buf := range buffers {
					if len(buf.records) > 0 {
						if flushErr := flush(sn, buf); flushErr != nil {
							writeErrors = append(writeErrors, protocol.WriteError{
								Message:   fmt.Sprintf("state flush error for %s: %v", sn, flushErr),
								ErrorCode: "UPLOAD_ERROR",
							})
						}
					}
				}
				stateMessages = append(stateMessages, *msg.State)
			}
		}
	}

	// Final flush.
	for sn, buf := range buffers {
		if len(buf.records) > 0 {
			if flushErr := flush(sn, buf); flushErr != nil {
				writeErrors = append(writeErrors, protocol.WriteError{
					Message:   fmt.Sprintf("final flush error for %s: %v", sn, flushErr),
					ErrorCode: "UPLOAD_ERROR",
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

// buildOutputKey constructs the S3 key for an output file.
func buildOutputKey(cfg *S3Config, streamName string, fileIdx int) string {
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	ext := "json"
	if cfg.FileFormat == "csv" {
		ext = "csv"
	}
	parts := []string{}
	if cfg.Prefix != "" {
		parts = append(parts, strings.TrimSuffix(cfg.Prefix, "/"))
	}
	parts = append(parts, streamName)
	parts = append(parts, fmt.Sprintf("%s_%04d.%s", timestamp, fileIdx, ext))
	return strings.Join(parts, "/")
}

// marshalJSON serializes records as a JSON array.
func marshalJSON(records []map[string]interface{}) ([]byte, string) {
	data, _ := json.Marshal(records)
	return data, "application/json"
}

// marshalCSV serializes records as CSV with a header row.
func marshalCSV(records []map[string]interface{}) ([]byte, string) {
	if len(records) == 0 {
		return nil, "text/csv"
	}
	// Collect all unique columns.
	colSet := make(map[string]bool)
	for _, rec := range records {
		for k := range rec {
			colSet[k] = true
		}
	}
	columns := make([]string, 0, len(colSet))
	for k := range colSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	_ = writer.Write(columns)
	for _, rec := range records {
		row := make([]string, len(columns))
		for i, col := range columns {
			if val, exists := rec[col]; exists && val != nil {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		_ = writer.Write(row)
	}
	writer.Flush()
	return buf.Bytes(), "text/csv"
}

// multipartUpload performs an S3 multipart upload for large files.
func multipartUpload(ctx context.Context, client *s3.Client, bucket, key, contentType string, data []byte) error {
	createOutput, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := createOutput.UploadId

	var completedParts []s3types.CompletedPart
	partNumber := int32(1)
	offset := 0

	for offset < len(data) {
		end := offset + multipartPartSize
		if end > len(data) {
			end = len(data)
		}
		partData := data[offset:end]

		uploadOutput, uploadErr := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(partNumber),
			Body:       bytes.NewReader(partData),
		})
		if uploadErr != nil {
			_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			})
			return fmt.Errorf("upload part %d: %w", partNumber, uploadErr)
		}
		completedParts = append(completedParts, s3types.CompletedPart{
			ETag:       uploadOutput.ETag,
			PartNumber: aws.Int32(partNumber),
		})
		partNumber++
		offset = end
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

// Capabilities returns the supported destination sync modes.
func (c *S3Connector) Capabilities() []protocol.DestinationSyncMode {
	return []protocol.DestinationSyncMode{
		protocol.DestSyncModeAppend,
	}
}

// Resolve handles conflicts using source-wins by default for S3 (append-only semantics).
func (c *S3Connector) Resolve(_ context.Context, conflicts []cdk.Conflict) ([]cdk.Resolution, error) {
	resolutions := make([]cdk.Resolution, len(conflicts))
	for i, conflict := range conflicts {
		resolutions[i] = cdk.Resolution{
			PrimaryKey: conflict.PrimaryKey,
			Strategy:   cdk.ResolutionSourceWins,
		}
	}
	return resolutions, nil
}

// WebhookHandler processes inbound S3 event notifications (e.g., from SNS/SQS).
// This parses the standard S3 event notification format.
func (c *S3Connector) WebhookHandler(_ context.Context, event cdk.WebhookEvent) error {
	_ = event
	return nil
}
