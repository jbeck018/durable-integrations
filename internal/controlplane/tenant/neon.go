// Package tenant — Neon provisioner for project-per-tenant database isolation.
// Each tenant gets an isolated Neon Postgres project, providing full database
// separation, scale-to-zero compute, and branch-based dev environments.
package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	neonAPIBase    = "https://console.neon.tech/api/v2"
	neonAPITimeout = 30 * time.Second
)

// TenantDatabase holds the provisioned Neon project details for a tenant.
type TenantDatabase struct {
	ProjectID     string `json:"project_id"`
	ConnectionURI string `json:"connection_uri"`
	Host          string `json:"host"`
	DBName        string `json:"db_name"`
	RoleName      string `json:"role_name"`
	Region        string `json:"region"`
}

// NeonProvisioner manages Neon project lifecycle for tenant isolation.
type NeonProvisioner struct {
	apiKey    string
	region    string
	pgVersion int
	client    *http.Client
}

// NeonProvisionerConfig configures the NeonProvisioner.
type NeonProvisionerConfig struct {
	APIKey    string
	Region    string // e.g., "aws-us-east-1"
	PGVersion int    // e.g., 16
}

// NewNeonProvisioner creates a NeonProvisioner.
func NewNeonProvisioner(cfg NeonProvisionerConfig) *NeonProvisioner {
	if cfg.Region == "" {
		cfg.Region = "aws-us-east-1"
	}
	if cfg.PGVersion == 0 {
		cfg.PGVersion = 16
	}

	return &NeonProvisioner{
		apiKey:    cfg.APIKey,
		region:    cfg.Region,
		pgVersion: cfg.PGVersion,
		client: &http.Client{
			Timeout: neonAPITimeout,
		},
	}
}

// neonCreateProjectRequest is the Neon API request body for creating a project.
type neonCreateProjectRequest struct {
	Project neonProjectSpec `json:"project"`
}

type neonProjectSpec struct {
	Name            string               `json:"name"`
	RegionID        string               `json:"region_id"`
	PGVersion       int                  `json:"pg_version"`
	StorePasswords  bool                 `json:"store_passwords"`
	DefaultEndpoint neonEndpointSettings `json:"default_endpoint_settings"`
}

type neonEndpointSettings struct {
	AutoscalingLimitMinCU float64 `json:"autoscaling_limit_min_cu"`
	AutoscalingLimitMaxCU float64 `json:"autoscaling_limit_max_cu"`
	SuspendTimeoutSeconds int     `json:"suspend_timeout_seconds"`
}

// neonCreateProjectResponse is the Neon API response for creating a project.
type neonCreateProjectResponse struct {
	Project struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		RegionID  string `json:"region_id"`
		CreatedAt string `json:"created_at"`
	} `json:"project"`
	ConnectionURIs []struct {
		ConnectionURI string `json:"connection_uri"`
	} `json:"connection_uris"`
	Databases []struct {
		ID      int    `json:"id"`
		Name    string `json:"name"`
		OwnerID int    `json:"owner_id"`
	} `json:"databases"`
	Roles []struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	} `json:"roles"`
	Endpoints []struct {
		Host string `json:"host"`
	} `json:"endpoints"`
}

// ProvisionDatabase creates an isolated Neon project for a tenant.
// The project includes a default branch, endpoint, database, and role.
func (p *NeonProvisioner) ProvisionDatabase(ctx context.Context, tenantID, tenantName string) (*TenantDatabase, error) {
	reqBody := neonCreateProjectRequest{
		Project: neonProjectSpec{
			Name:           fmt.Sprintf("flowforge-tenant-%s", tenantName),
			RegionID:       p.region,
			PGVersion:      p.pgVersion,
			StorePasswords: true,
			DefaultEndpoint: neonEndpointSettings{
				AutoscalingLimitMinCU: 0.25, // Scale to zero
				AutoscalingLimitMaxCU: 2,    // Max 2 compute units
				SuspendTimeoutSeconds: 300,  // Suspend after 5 min idle
			},
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal neon request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, neonAPIBase+"/projects", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create neon request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("neon api call: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read neon response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("neon api error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var result neonCreateProjectResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal neon response: %w", err)
	}

	td := &TenantDatabase{
		ProjectID: result.Project.ID,
		Region:    result.Project.RegionID,
	}

	if len(result.ConnectionURIs) > 0 {
		td.ConnectionURI = result.ConnectionURIs[0].ConnectionURI
	}
	if len(result.Endpoints) > 0 {
		td.Host = result.Endpoints[0].Host
	}
	if len(result.Databases) > 0 {
		td.DBName = result.Databases[0].Name
	}
	if len(result.Roles) > 0 {
		td.RoleName = result.Roles[0].Name
	}

	return td, nil
}

// DeprovisionDatabase deletes a Neon project, removing all data.
func (p *NeonProvisioner) DeprovisionDatabase(ctx context.Context, projectID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, neonAPIBase+"/projects/"+projectID, nil)
	if err != nil {
		return fmt.Errorf("create neon delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("neon delete api call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("neon delete error (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetConnectionURI retrieves the connection URI for a Neon project.
func (p *NeonProvisioner) GetConnectionURI(ctx context.Context, projectID string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, neonAPIBase+"/projects/"+projectID+"/connection_uri", nil)
	if err != nil {
		return "", fmt.Errorf("create neon connection_uri request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("neon connection_uri api call: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("neon connection_uri error (status %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		URI string `json:"uri"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("unmarshal connection_uri response: %w", err)
	}

	return result.URI, nil
}
