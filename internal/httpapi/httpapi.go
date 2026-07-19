// Package httpapi holds the wire formats shared by the worker's HTTP server
// and the manager's HTTP client. It belongs to neither package, so it lives
// on its own and both import it.
package httpapi

import (
	"encoding/json"
	"net/http"
)

// ErrorResponse is the JSON body every failing endpoint returns.
type ErrorResponse struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// WriteJSON sets the content type, writes the status, and encodes body.
func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// WriteError sets the HTTP status and writes an ErrorResponse whose Code is
// that same status, so the two can never disagree.
func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, ErrorResponse{Message: msg, Code: status})
}
