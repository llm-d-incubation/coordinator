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

func redactedHTTPHeaders(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, v := range h {
		if isSensitiveHeader(k) {
			out[k] = []string{redactedValue}
			continue
		}
		out[k] = v
	}
	return out
}

func redactedHeaders(h map[string]string) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if isSensitiveHeader(k) {
			out[k] = redactedValue
			continue
		}
		out[k] = v
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
