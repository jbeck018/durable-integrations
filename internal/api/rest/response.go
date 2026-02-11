// Package rest implements the FlowForge REST API server, handlers, and response utilities.
package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// envelope wraps all successful API responses.
type envelope struct {
	Data interface{} `json:"data"`
}

// errorResponse wraps all error API responses.
type errorResponse struct {
	Error errorBody `json:"error"`
}

// errorBody contains the structured error detail.
type errorBody struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
}

// paginatedResponse wraps list results with pagination metadata.
type paginatedResponse struct {
	Data       interface{}    `json:"data"`
	Pagination paginationMeta `json:"pagination"`
}

// paginationMeta provides pagination details.
type paginationMeta struct {
	Total    int `json:"total"`
	Page     int `json:"page"`
	PageSize int `json:"page_size"`
	Pages    int `json:"pages"`
}

// jsonPool is a reusable encoder pool for efficient serialization.
// We use a direct write approach with json.NewEncoder for streaming.

// JSON writes a JSON response with the given status code and payload.
// It streams directly to the ResponseWriter for efficiency.
func JSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(envelope{Data: data})
}

// Error writes a structured JSON error response.
func Error(w http.ResponseWriter, status int, message string, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(errorResponse{
		Error: errorBody{
			Code:    code,
			Message: message,
		},
	})
}

// ErrorWithDetails writes a structured JSON error response with additional detail.
func ErrorWithDetails(w http.ResponseWriter, status int, message string, code string, details interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(errorResponse{
		Error: errorBody{
			Code:    code,
			Message: message,
			Details: details,
		},
	})
}

// Paginated writes a paginated JSON response.
func Paginated(w http.ResponseWriter, items interface{}, total, page, pageSize int) {
	pages := 0
	if pageSize > 0 {
		pages = (total + pageSize - 1) / pageSize
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(paginatedResponse{
		Data: items,
		Pagination: paginationMeta{
			Total:    total,
			Page:     page,
			PageSize: pageSize,
			Pages:    pages,
		},
	})
}

// Created writes a 201 Created response with a Location header.
func Created(w http.ResponseWriter, data interface{}, location string) {
	if location != "" {
		w.Header().Set("Location", location)
	}
	JSON(w, http.StatusCreated, data)
}

// NoContent writes a 204 No Content response.
func NoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

// DecodeJSON reads and unmarshals the request body into v.
// Returns an error message suitable for API output if decoding fails.
func DecodeJSON(r *http.Request, v interface{}) error {
	if r.Body == nil {
		return fmt.Errorf("request body is empty")
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

// PaginationFromRequest extracts page and page_size query params with defaults.
func PaginationFromRequest(r *http.Request) (page, pageSize int) {
	page = queryInt(r, "page", 1)
	pageSize = queryInt(r, "page_size", 20)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}

// queryInt extracts an integer query parameter with a fallback default.
func queryInt(r *http.Request, key string, fallback int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}
