// Package common provides shared HTTP client, OAuth, and pagination utilities
// for all FlowForge REST API connectors. Connectors import this package to avoid
// duplicating transport, authentication, and pagination logic.
package common

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Paginator defines a strategy for iterating through paginated API responses.
// Implementations inspect each HTTP response and return the URL for the next
// page, or indicate that no more pages remain.
type Paginator interface {
	// NextPage inspects the response body (already decoded) and/or headers to
	// determine the next page URL. hasMore is false when all pages are consumed.
	NextPage(resp *http.Response, body map[string]interface{}) (nextURL string, hasMore bool)
}

// OffsetPaginator implements offset/limit or page-number-based pagination.
// It increments a page counter on each call and stops when the returned
// result count is less than the page size.
type OffsetPaginator struct {
	// BaseURL is the endpoint without pagination query parameters.
	BaseURL string
	// PageParam is the query parameter name for the page number (e.g. "page" or "offset").
	PageParam string
	// LimitParam is the query parameter name for page size (e.g. "limit" or "per_page").
	LimitParam string
	// PageSize is the number of items per page.
	PageSize int
	// ResultsField is the JSON key that holds the array of results.
	// If empty, the root is assumed to be the array.
	ResultsField string
	// UseOffset when true treats PageParam as an absolute offset rather than
	// a 1-based page number.
	UseOffset bool

	currentPage   int
	currentOffset int
	started       bool
}

// NextPage returns the URL for the next page of results.
func (p *OffsetPaginator) NextPage(resp *http.Response, body map[string]interface{}) (string, bool) {
	if !p.started {
		p.started = true
		p.currentPage = 1
		p.currentOffset = 0
		return p.buildURL(), true
	}

	count := p.countResults(body)
	if count < p.PageSize {
		return "", false
	}

	if p.UseOffset {
		p.currentOffset += p.PageSize
	} else {
		p.currentPage++
	}
	return p.buildURL(), true
}

func (p *OffsetPaginator) buildURL() string {
	u, err := url.Parse(p.BaseURL)
	if err != nil {
		return p.BaseURL
	}
	q := u.Query()
	if p.UseOffset {
		q.Set(p.PageParam, strconv.Itoa(p.currentOffset))
	} else {
		q.Set(p.PageParam, strconv.Itoa(p.currentPage))
	}
	q.Set(p.LimitParam, strconv.Itoa(p.PageSize))
	u.RawQuery = q.Encode()
	return u.String()
}

func (p *OffsetPaginator) countResults(body map[string]interface{}) int {
	if p.ResultsField == "" {
		return 0
	}
	val, ok := body[p.ResultsField]
	if !ok {
		return 0
	}
	arr, ok := val.([]interface{})
	if !ok {
		return 0
	}
	return len(arr)
}

// CursorPaginator implements cursor/token-based pagination where each response
// contains a token pointing to the next page.
type CursorPaginator struct {
	// BaseURL is the endpoint without cursor parameters.
	BaseURL string
	// CursorParam is the query parameter name for the cursor (e.g. "after", "cursor", "page_token").
	CursorParam string
	// CursorField is the dot-separated JSON path to the next cursor value in the response.
	// Example: "paging.next.after" or "nextPageToken".
	CursorField string
	// LimitParam is the query parameter name for page size. Empty means no limit param.
	LimitParam string
	// PageSize is the number of items per page. Zero means use API default.
	PageSize int
	// HasMoreField is an optional JSON path to a boolean indicating more pages exist.
	// If empty, pagination stops when CursorField is absent or empty.
	HasMoreField string

	started bool
}

// NextPage extracts the cursor from the response and builds the next URL.
func (p *CursorPaginator) NextPage(resp *http.Response, body map[string]interface{}) (string, bool) {
	if !p.started {
		p.started = true
		return p.buildURL(""), true
	}

	cursor := extractNestedString(body, p.CursorField)
	if cursor == "" {
		if p.HasMoreField != "" {
			hasMore := extractNestedBool(body, p.HasMoreField)
			if !hasMore {
				return "", false
			}
		}
		return "", false
	}

	if p.HasMoreField != "" {
		hasMore := extractNestedBool(body, p.HasMoreField)
		if !hasMore {
			return "", false
		}
	}

	return p.buildURL(cursor), true
}

func (p *CursorPaginator) buildURL(cursor string) string {
	u, err := url.Parse(p.BaseURL)
	if err != nil {
		return p.BaseURL
	}
	q := u.Query()
	if cursor != "" {
		q.Set(p.CursorParam, cursor)
	}
	if p.LimitParam != "" && p.PageSize > 0 {
		q.Set(p.LimitParam, strconv.Itoa(p.PageSize))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// LinkHeaderPaginator follows RFC 5988 Link headers (rel="next") that many
// APIs use (e.g. GitHub, some REST APIs).
type LinkHeaderPaginator struct {
	// BaseURL is the starting endpoint.
	BaseURL string
	started bool
}

var linkRelNextRe = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

// NextPage extracts the next URL from the Link header.
func (p *LinkHeaderPaginator) NextPage(resp *http.Response, body map[string]interface{}) (string, bool) {
	if !p.started {
		p.started = true
		return p.BaseURL, true
	}
	if resp == nil {
		return "", false
	}
	linkHeader := resp.Header.Get("Link")
	if linkHeader == "" {
		return "", false
	}
	matches := linkRelNextRe.FindStringSubmatch(linkHeader)
	if len(matches) < 2 {
		return "", false
	}
	return matches[1], true
}

// extractNestedString walks a dot-separated path in a map and returns the
// string value at that path, or "" if not found.
func extractNestedString(m map[string]interface{}, path string) string {
	parts := strings.Split(path, ".")
	current := interface{}(m)
	for _, part := range parts {
		cm, ok := current.(map[string]interface{})
		if !ok {
			return ""
		}
		current, ok = cm[part]
		if !ok {
			return ""
		}
	}
	switch v := current.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// extractNestedBool walks a dot-separated path and returns the boolean value.
func extractNestedBool(m map[string]interface{}, path string) bool {
	parts := strings.Split(path, ".")
	current := interface{}(m)
	for _, part := range parts {
		cm, ok := current.(map[string]interface{})
		if !ok {
			return false
		}
		current, ok = cm[part]
		if !ok {
			return false
		}
	}
	b, ok := current.(bool)
	if !ok {
		return false
	}
	return b
}
