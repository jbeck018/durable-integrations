package common

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all FlowForge service configuration.
// Loaded from environment variables with sensible defaults.
type Config struct {
	// Service
	ServiceName string
	Environment string // "development", "staging", "production"
	LogLevel    string

	// API
	APIPort    int
	APIHost    string
	GRPCPort   int

	// Database
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string
	DBMaxConns int

	// Redis
	RedisHost     string
	RedisPort     int
	RedisPassword string
	RedisDB       int

	// Temporal
	TemporalHost      string
	TemporalPort      int
	TemporalNamespace string

	// S3 / Object Storage
	S3Endpoint  string
	S3Bucket    string
	S3Region    string
	S3AccessKey string
	S3SecretKey string

	// Vault / Secrets
	VaultAddr  string
	VaultToken string

	// MCP Gateway
	MCPGatewayPort int
	MCPTransports  string // "sse,websocket"

	// Encryption
	EncryptionKey string

	// Performance
	WorkerConcurrency    int
	BatchSize            int
	ConnectionPoolSize   int
	RequestTimeout       time.Duration
	ShutdownGracePeriod  time.Duration
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() *Config {
	return &Config{
		ServiceName:         envStr("FLOWFORGE_SERVICE_NAME", "flowforge"),
		Environment:         envStr("FLOWFORGE_ENV", "development"),
		LogLevel:            envStr("FLOWFORGE_LOG_LEVEL", "info"),
		APIPort:             envInt("FLOWFORGE_API_PORT", 8080),
		APIHost:             envStr("FLOWFORGE_API_HOST", "0.0.0.0"),
		GRPCPort:            envInt("FLOWFORGE_GRPC_PORT", 9090),
		DBHost:              envStr("POSTGRES_HOST", "localhost"),
		DBPort:              envInt("POSTGRES_PORT", 5432),
		DBUser:              envStr("POSTGRES_USER", "flowforge"),
		DBPassword:          envStr("POSTGRES_PASSWORD", ""),
		DBName:              envStr("POSTGRES_DB", "flowforge"),
		DBSSLMode:           envStr("POSTGRES_SSL_MODE", "disable"),
		DBMaxConns:          envInt("POSTGRES_MAX_CONNS", 20),
		RedisHost:           envStr("REDIS_HOST", "localhost"),
		RedisPort:           envInt("REDIS_PORT", 6379),
		RedisPassword:       envStr("REDIS_PASSWORD", ""),
		RedisDB:             envInt("REDIS_DB", 0),
		TemporalHost:        envStr("TEMPORAL_HOST", "localhost"),
		TemporalPort:        envInt("TEMPORAL_PORT", 7233),
		TemporalNamespace:   envStr("TEMPORAL_NAMESPACE", "flowforge"),
		S3Endpoint:          envStr("S3_ENDPOINT", "http://localhost:9000"),
		S3Bucket:            envStr("S3_BUCKET", "flowforge-staging"),
		S3Region:            envStr("S3_REGION", "us-east-1"),
		S3AccessKey:         envStr("S3_ACCESS_KEY", ""),
		S3SecretKey:         envStr("S3_SECRET_KEY", ""),
		VaultAddr:           envStr("VAULT_ADDR", "http://localhost:8200"),
		VaultToken:          envStr("VAULT_TOKEN", ""),
		MCPGatewayPort:      envInt("MCP_GATEWAY_PORT", 8090),
		MCPTransports:       envStr("MCP_GATEWAY_TRANSPORT", "sse,websocket"),
		EncryptionKey:       envStr("FLOWFORGE_ENCRYPTION_KEY", ""),
		WorkerConcurrency:   envInt("FLOWFORGE_WORKER_CONCURRENCY", 100),
		BatchSize:           envInt("FLOWFORGE_BATCH_SIZE", 1000),
		ConnectionPoolSize:  envInt("FLOWFORGE_CONN_POOL_SIZE", 10),
		RequestTimeout:      envDuration("FLOWFORGE_REQUEST_TIMEOUT", 30*time.Second),
		ShutdownGracePeriod: envDuration("FLOWFORGE_SHUTDOWN_GRACE", 15*time.Second),
	}
}

// DSN returns the PostgreSQL connection string.
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.DBHost, c.DBPort, c.DBUser, c.DBPassword, c.DBName, c.DBSSLMode,
	)
}

// TemporalAddr returns the Temporal server address.
func (c *Config) TemporalAddr() string {
	return fmt.Sprintf("%s:%d", c.TemporalHost, c.TemporalPort)
}

// RedisAddr returns the Redis address.
func (c *Config) RedisAddr() string {
	return fmt.Sprintf("%s:%d", c.RedisHost, c.RedisPort)
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
