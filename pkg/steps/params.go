package steps

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// Parameter key constants for step configuration maps.
const (
	ParamKVConnector = "kv_connector"
	ParamECConnector = "ec_connector"
)

const ModalityImage = "image"

// paramInt reads an integer step parameter. The config decoder may hand a number
// back as int, int64, float64, or json.Number depending on its source and YAML
// representation (a float-formatted literal such as 8192.0 decodes as float64),
// so all are accepted; a non-integral float or a non-numeric value is an error.
// ok is false when the key is absent, leaving the caller's default in place.
func paramInt(params map[string]any, key string) (value int, ok bool, err error) {
	switch v := params[key].(type) {
	case nil:
		return 0, false, nil
	case int:
		return v, true, nil
	case int64:
		return int(v), true, nil
	case float64:
		if v != math.Trunc(v) {
			return 0, false, fmt.Errorf("%s: expected integer, got %v", key, v)
		}
		return int(v), true, nil
	case json.Number:
		i, convErr := v.Int64()
		if convErr != nil {
			return 0, false, fmt.Errorf("%s: expected integer, got %q", key, v.String())
		}
		return int(i), true, nil
	default:
		return 0, false, fmt.Errorf("%s: expected number, got %T", key, v)
	}
}

// paramDuration reads a duration step parameter from a Go duration string (e.g.
// "30s"). An unparsable string is an error rather than a silent fallback, so a
// malformed value such as "30" (no unit) fails config load instead of running
// the default. ok is false when the key is absent.
func paramDuration(params map[string]any, key string) (value time.Duration, ok bool, err error) {
	switch v := params[key].(type) {
	case nil:
		return 0, false, nil
	case string:
		d, parseErr := time.ParseDuration(v)
		if parseErr != nil {
			return 0, false, fmt.Errorf("%s: %w", key, parseErr)
		}
		return d, true, nil
	default:
		return 0, false, fmt.Errorf("%s: expected duration string, got %T", key, v)
	}
}
