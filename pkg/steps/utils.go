package steps

import "net/http"

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

// redactedHeaders returns a flattened copy of h with values of sensitive
// headers replaced by a redaction sentinel. Accepts either http.Header
// (map[string][]string) or map[string]string and always returns the
// flat form for log uniformity.
func redactedHeaders[V string | []string](h map[string]V) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if isSensitiveHeader(k) {
			out[k] = redactedValue
			continue
		}
		switch val := any(v).(type) {
		case string:
			out[k] = val
		case []string:
			if len(val) > 0 {
				out[k] = val[0]
			}
		}
	}
	return out
}

func copyBody(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
