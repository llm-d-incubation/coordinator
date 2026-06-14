package httplog

import (
	"net/http"
	"testing"
)

func TestRedactedHeaders_LowercasesAndFlattensHTTPHeader(t *testing.T) {
	h := http.Header{
		"X-Request-Id": {"abc-123"},
		"Epp-Phase":    {"decode"},
	}

	out := RedactedHeaders(h)

	if got := out["x-request-id"]; got != "abc-123" {
		t.Errorf("x-request-id = %q, want %q", got, "abc-123")
	}
	if got := out["epp-phase"]; got != "decode" {
		t.Errorf("epp-phase = %q, want %q", got, "decode")
	}
	if _, ok := out["X-Request-Id"]; ok {
		t.Errorf("canonical key must not be present; keys are lowercased")
	}
}

func TestRedactedHeaders_LowercasesStringMap(t *testing.T) {
	out := RedactedHeaders(map[string]string{
		"x-request-id": "abc-123",
		"EPP-Phase":    "encode",
	})

	if got := out["x-request-id"]; got != "abc-123" {
		t.Errorf("x-request-id = %q, want %q", got, "abc-123")
	}
	if got := out["epp-phase"]; got != "encode" {
		t.Errorf("epp-phase = %q, want %q", got, "encode")
	}
}

func TestRedactedHeaders_RedactsSensitiveRegardlessOfInputCase(t *testing.T) {
	out := RedactedHeaders(http.Header{
		"Authorization": {"Bearer secret"},
		"X-Api-Key":     {"key"},
		"Accept":        {"*/*"},
	})

	if got := out["authorization"]; got != redactedValue {
		t.Errorf("authorization = %q, want %q", got, redactedValue)
	}
	if got := out["x-api-key"]; got != redactedValue {
		t.Errorf("x-api-key = %q, want %q", got, redactedValue)
	}
	if got := out["accept"]; got != "*/*" {
		t.Errorf("accept = %q, want %q", got, "*/*")
	}
}

func TestRedactedHeaders_EmptyValueSliceOmitted(t *testing.T) {
	out := RedactedHeaders(http.Header{"X-Empty": {}})
	if _, ok := out["x-empty"]; ok {
		t.Errorf("header with no values should be omitted, got %v", out)
	}
}
