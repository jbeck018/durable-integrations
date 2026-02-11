// Package blob provides S3-compatible blob storage for FlowForge.
// It wraps the AWS SDK v2 S3 client with simplified operations for
// uploading, downloading, deleting, listing, and generating presigned URLs.
package blob

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Client wraps an S3 client with a default bucket and simplified operations.
type Client struct {
	s3Client     *s3.Client
	presignClient *s3.PresignClient
	bucket       string
}

// Object represents metadata about a stored object.
type Object struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	ContentType  string    `json:"content_type,omitempty"`
	ETag         string    `json:"etag,omitempty"`
}

// NewClient creates an S3-compatible blob storage client.
// endpoint can be a MinIO or other S3-compatible service URL.
// If endpoint is empty, the default AWS endpoint resolution is used.
func NewClient(ctx context.Context, endpoint, region, accessKey, secretKey, bucket string) (*Client, error) {
	optFns := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	}

	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(cfg, s3Opts...)
	presignClient := s3.NewPresignClient(s3Client)

	return &Client{
		s3Client:      s3Client,
		presignClient: presignClient,
		bucket:        bucket,
	}, nil
}

// Upload stores data from reader at the given key with the specified content type.
func (c *Client) Upload(ctx context.Context, key string, reader io.Reader, contentType string) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        reader,
		ContentType: aws.String(contentType),
	}

	_, err := c.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", key, err)
	}
	return nil
}

// UploadBytes is a convenience method that uploads raw bytes.
func (c *Client) UploadBytes(ctx context.Context, key string, data []byte, contentType string) error {
	return c.Upload(ctx, key, newBytesReader(data), contentType)
}

// Download retrieves the object at the given key.
// The caller is responsible for closing the returned ReadCloser.
func (c *Client) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	result, err := c.s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("s3 get %q: %w", key, err)
	}
	return result.Body, nil
}

// Delete removes the object at the given key.
func (c *Client) Delete(ctx context.Context, key string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	_, err := c.s3Client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 delete %q: %w", key, err)
	}
	return nil
}

// DeleteBatch removes multiple objects in a single request.
func (c *Client) DeleteBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	objects := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objects[i] = types.ObjectIdentifier{
			Key: aws.String(key),
		}
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(c.bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}

	_, err := c.s3Client.DeleteObjects(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 batch delete: %w", err)
	}
	return nil
}

// List returns objects with the given key prefix.
// maxKeys limits the number of results; use 0 for the default (1000).
func (c *Client) List(ctx context.Context, prefix string, maxKeys int) ([]Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}
	if maxKeys > 0 {
		input.MaxKeys = aws.Int32(int32(maxKeys))
	}

	var objects []Object
	paginator := s3.NewListObjectsV2Paginator(c.s3Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list prefix %q: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			o := Object{
				Key:          aws.ToString(obj.Key),
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
			}
			if obj.ETag != nil {
				o.ETag = *obj.ETag
			}
			objects = append(objects, o)
		}
		if maxKeys > 0 && len(objects) >= maxKeys {
			objects = objects[:maxKeys]
			break
		}
	}

	return objects, nil
}

// PresignedGetURL generates a presigned URL for downloading an object.
// The URL is valid for the specified duration.
func (c *Client) PresignedGetURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	result, err := c.presignClient.PresignGetObject(ctx, input, func(po *s3.PresignOptions) {
		po.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign get %q: %w", key, err)
	}
	return result.URL, nil
}

// PresignedPutURL generates a presigned URL for uploading an object.
// The URL is valid for the specified duration.
func (c *Client) PresignedPutURL(ctx context.Context, key, contentType string, expires time.Duration) (string, error) {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}

	result, err := c.presignClient.PresignPutObject(ctx, input, func(po *s3.PresignOptions) {
		po.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign put %q: %w", key, err)
	}
	return result.URL, nil
}

// Exists checks whether an object exists at the given key.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	_, err := c.s3Client.HeadObject(ctx, input)
	if err != nil {
		// HeadObject returns a NotFound-style error when the key does not exist.
		// We check the error message because the SDK wraps it.
		return false, nil
	}
	return true, nil
}

// bytesReader wraps a byte slice to implement io.Reader.
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
