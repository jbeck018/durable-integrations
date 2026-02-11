package cdk

import (
	"fmt"
	"sync"
)

// registry is the global connector registry.
var registry = &connectorRegistry{
	sources:       make(map[string]Source),
	destinations:  make(map[string]Destination),
	bidirectional: make(map[string]Bidirectional),
	meta:          make(map[string]ConnectorMeta),
}

type connectorRegistry struct {
	mu            sync.RWMutex
	sources       map[string]Source
	destinations  map[string]Destination
	bidirectional map[string]Bidirectional
	meta          map[string]ConnectorMeta
}

// RegisterSource registers a source connector by name.
func RegisterSource(name string, meta ConnectorMeta, s Source) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	meta.Type = "source"
	registry.sources[name] = s
	registry.meta[name] = meta
}

// RegisterDestination registers a destination connector by name.
func RegisterDestination(name string, meta ConnectorMeta, d Destination) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	meta.Type = "destination"
	registry.destinations[name] = d
	registry.meta[name] = meta
}

// RegisterBidirectional registers a bidirectional connector by name.
func RegisterBidirectional(name string, meta ConnectorMeta, b Bidirectional) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	meta.Type = "bidirectional"
	registry.bidirectional[name] = b
	registry.sources[name] = b
	registry.destinations[name] = b
	registry.meta[name] = meta
}

// GetSource returns a registered source connector.
func GetSource(name string) (Source, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	s, ok := registry.sources[name]
	if !ok {
		return nil, fmt.Errorf("source connector %q not registered", name)
	}
	return s, nil
}

// GetDestination returns a registered destination connector.
func GetDestination(name string) (Destination, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	d, ok := registry.destinations[name]
	if !ok {
		return nil, fmt.Errorf("destination connector %q not registered", name)
	}
	return d, nil
}

// GetBidirectional returns a registered bidirectional connector.
func GetBidirectional(name string) (Bidirectional, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	b, ok := registry.bidirectional[name]
	if !ok {
		return nil, fmt.Errorf("bidirectional connector %q not registered", name)
	}
	return b, nil
}

// ListConnectors returns metadata for all registered connectors.
func ListConnectors() []ConnectorMeta {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	result := make([]ConnectorMeta, 0, len(registry.meta))
	for _, m := range registry.meta {
		result = append(result, m)
	}
	return result
}

// GetMeta returns the metadata for a connector by name.
func GetMeta(name string) (ConnectorMeta, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	m, ok := registry.meta[name]
	if !ok {
		return ConnectorMeta{}, fmt.Errorf("connector %q not registered", name)
	}
	return m, nil
}
