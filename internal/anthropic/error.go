package anthropic

import (
	"encoding/json"
	"net/http"
)

// anthropicErrorBody is the Anthropic wire error shape:
// {"type":"error","error":{"type":"...","message":"..."}}
type anthropicErrorBody struct {
	Type  string `json:"type"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// errorTypeFor maps a status code to the Anthropic error type
func errorTypeFor(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid_request_error"
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusForbidden:
		return "permission_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	default:
		return "api_error"
	}
}

// writeError writes an Anthropic-format error response; the message falls back
// to a generic one when empty
func writeError(w http.ResponseWriter, status int, message string) {
	if message == "" {
		message = "upstream error"
	}
	var body anthropicErrorBody
	body.Type = "error"
	body.Error.Type = errorTypeFor(status)
	body.Error.Message = message
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}
