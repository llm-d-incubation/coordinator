// Package httplog provides helpers for logging HTTP headers safely and
// consistently across services.
package httplog

import (
	"net/http"
	"strings"
)

const redactedValue = "[REDACTED]"

var sensitiveHeaders = map[string]struct{}{
	"Authorization":       {},
	"Proxy-Authorization": {},
	"Cookie":              {},
	"Set-Cookie":          {},
	"X-Api-Key":           {},
	"X-Auth-Token":        {},
}

func isSensitiveHeader(name string) bool {
	_, ok := sensitiveHeaders[http.CanonicalHeaderKey(name)]
	return ok
}

// RedactedHeaders returns a flattened copy of h with values of sensitive
// headers replaced by a redaction sentinel. It accepts either http.Header
// (map[string][]string) or map[string]string and always returns the flat
// form. Keys are normalized to lowercase so log output is consistent
// regardless of how the input was canonicalized. A multi-valued header keeps
// only its first value. A header with no value (an empty slice or an empty
// string) retains its key with an empty-string value, so both input forms
// behave the same.
func RedactedHeaders[V string | []string](h map[string]V) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		lower := strings.ToLower(k)
		if isSensitiveHeader(k) {
			out[lower] = redactedValue
			continue
		}
		switch val := any(v).(type) {
		case string:
			out[lower] = val
		case []string:
			first := ""
			if len(val) > 0 {
				first = val[0]
			}
			out[lower] = first
		}
	}
	return out
}
