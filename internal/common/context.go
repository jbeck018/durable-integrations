package common

import (
	"context"
)

type contextKey string

const (
	tenantIDKey     contextKey = "tenant_id"
	correlationKey  contextKey = "correlation_id"
	userIDKey       contextKey = "user_id"
	connectorKey    contextKey = "connector_name"
)

// WithTenantID attaches a tenant ID to the context.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantIDKey, tenantID)
}

// TenantIDFrom extracts the tenant ID from the context.
func TenantIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(tenantIDKey).(string)
	return v
}

// WithCorrelationID attaches a correlation ID for distributed tracing.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey, id)
}

// CorrelationIDFrom extracts the correlation ID from the context.
func CorrelationIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(correlationKey).(string)
	return v
}

// WithUserID attaches a user ID to the context.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// UserIDFrom extracts the user ID from the context.
func UserIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(userIDKey).(string)
	return v
}

// WithConnectorName attaches the connector name to the context.
func WithConnectorName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, connectorKey, name)
}

// ConnectorNameFrom extracts the connector name from the context.
func ConnectorNameFrom(ctx context.Context) string {
	v, _ := ctx.Value(connectorKey).(string)
	return v
}
