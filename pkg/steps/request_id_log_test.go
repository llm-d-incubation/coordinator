package steps

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr/funcr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"

	"github.com/llm-d/coordinator/pkg/config"
	"github.com/llm-d/coordinator/pkg/connectors/ec"
	"github.com/llm-d/coordinator/pkg/connectors/kv"
	"github.com/llm-d/coordinator/pkg/gateway"
	"github.com/llm-d/coordinator/pkg/pipeline"
)

// logCapture records JSON log lines emitted by a step under test.
type logCapture struct {
	mu   sync.Mutex
	logs []string
}

func (c *logCapture) record(obj string) {
	c.mu.Lock()
	c.logs = append(c.logs, obj)
	c.mu.Unlock()
}

func (c *logCapture) joined() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.logs, "\n")
}

// captureLogCtx returns a context carrying a logger that records every entry
// (up to TRACE verbosity) so a step's debug header dump can be asserted.
func captureLogCtx() (context.Context, *logCapture) {
	cap := &logCapture{}
	logger := funcr.NewJSON(cap.record, funcr.Options{Verbosity: logutil.TRACE})
	return log.IntoContext(context.Background(), logger), cap
}

// incomingHeaders mimics what net/http delivers: header names canonicalized,
// including the X-Request-Id that previously collided with the lowercase
// re-stamp and printed twice.
func incomingHeaders(requestID string) http.Header {
	return http.Header{
		"X-Request-Id":    {requestID},
		"X-Forwarded-For": {"10.0.0.1"},
	}
}

// jsonServer returns a test server that responds with body as JSON.
func jsonServer(body map[string]any) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
}

// assertRequestIDPrinted checks every logged headers object renders the
// request id as the lowercase x-request-id key with the expected value and
// never the canonical X-Request-Id. The id may appear across several log lines
// (e.g. a step dump plus the gateway client dump); the duplicate guard is
// therefore per line, matching the original bug of two keys in one map.
func assertRequestIDPrinted(t *testing.T, logs, requestID string) {
	t.Helper()
	want := `"` + reqcommon.RequestIDHeaderKey + `":"` + requestID + `"`
	if !strings.Contains(logs, want) {
		t.Errorf("expected %s in logged headers, got:\n%s", want, logs)
	}
	if strings.Contains(logs, `"X-Request-Id"`) {
		t.Errorf("logged headers must use lowercase key only, found canonical X-Request-Id:\n%s", logs)
	}
	for _, line := range strings.Split(logs, "\n") {
		if strings.Count(line, want) > 1 {
			t.Errorf("request-id key duplicated within one log line:\n%s", line)
		}
	}
}

// Each step builds a map[string]string (encode/prefill) or an http.Header
// (decode/conditional_decode) for its debug dump. This verifies all of them
// render the request id as a single lowercase x-request-id.
func TestSteps_LogLowercaseRequestID(t *testing.T) {
	cases := []struct {
		name string
		// build returns a ready step, its request context, and a cleanup func.
		build func(t *testing.T, addr string) (pipeline.Step, *pipeline.RequestContext)
		// resp is the JSON the mock upstream returns.
		resp map[string]any
	}{
		{
			name: "encode",
			resp: map[string]any{"ec_transfer_params": map[string]any{"h1": map[string]any{"peer_host": "10.0.0.1", "peer_port": 5501}}},
			build: func(t *testing.T, addr string) (pipeline.Step, *pipeline.RequestContext) {
				step, _ := NewEncodeStep(map[string]any{"use_openai_format": false})
				step.(*EncodeStep).SetGatewayClient(gateway.New(config.GatewayConfig{Address: addr}))
				return step, &pipeline.RequestContext{
					RequestID:       "req-encode",
					Model:           "test",
					OriginalHeaders: incomingHeaders("req-encode"),
					TokenIDs:        []int{1, 32000, 32000, 32000, 2345},
					MultimodalEntries: []pipeline.MultimodalEntry{
						{Index: 0, Hash: "h1", KwargsData: "dGVzdA==", Placeholder: pipeline.PlaceholderRange{Offset: 1, Length: 3}},
					},
				}
			},
		},
		{
			name: "prefill",
			resp: map[string]any{"kv_transfer_params": map[string]any{"block_id": "block-xyz", "peer_host": "10.0.0.5", "peer_port": 6001}},
			build: func(t *testing.T, addr string) (pipeline.Step, *pipeline.RequestContext) {
				step, err := NewPrefillStep(map[string]any{"use_openai_format": false, ParamECConnector: ec.NIXL})
				if err != nil {
					t.Fatal(err)
				}
				step.(*PrefillStep).SetGatewayClient(gateway.New(config.GatewayConfig{Address: addr}))
				return step, &pipeline.RequestContext{
					RequestID:       "req-prefill",
					Model:           "llama-3",
					OriginalHeaders: incomingHeaders("req-prefill"),
					TokenIDs:        []int{1, 32000, 32000, 32000, 2345},
					MultimodalEntries: []pipeline.MultimodalEntry{
						{Index: 0, Hash: "hash-a", KwargsData: "dGVuc29yLWE=", Placeholder: pipeline.PlaceholderRange{Offset: 1, Length: 3}},
					},
					ECTransferParams: []map[string]any{
						{"hash-a": map[string]any{"peer_port": 5501, "size_bytes": 1228800, "nixl_agent_metadata_b64": "bml4..."}},
					},
					KVTransferParams: make(map[string]any),
				}
			},
		},
		{
			name: "decode",
			resp: map[string]any{"choices": []map[string]any{{"message": map[string]any{"role": "assistant", "content": "ok"}}}},
			build: func(t *testing.T, addr string) (pipeline.Step, *pipeline.RequestContext) {
				step, err := NewDecodeStep(map[string]any{ParamKVConnector: kv.NIXL})
				if err != nil {
					t.Fatal(err)
				}
				step.(*DecodeStep).SetGatewayClient(gateway.New(config.GatewayConfig{Address: addr}))
				return step, &pipeline.RequestContext{
					RequestID:        "req-decode",
					OriginalPath:     testChatCompletionsPath,
					Model:            "llama-3",
					OriginalHeaders:  incomingHeaders("req-decode"),
					TokenIDs:         []int{1, 2345},
					KVTransferParams: map[string]any{"block_id": "xyz", "peer_host": "10.0.0.5", "peer_port": 7777},
					Body:             map[string]any{"model": "llama-3", "stream": false, "messages": []any{}},
					ResponseWriter:   httptest.NewRecorder(),
				}
			},
		},
		{
			name: "conditional_decode",
			resp: map[string]any{"choices": []map[string]any{{"message": map[string]any{"role": "assistant", "content": "cached"}}}},
			build: func(t *testing.T, addr string) (pipeline.Step, *pipeline.RequestContext) {
				step, err := NewConditionalDecodeStep(nil)
				if err != nil {
					t.Fatal(err)
				}
				step.(*ConditionalDecodeStep).SetGatewayClient(gateway.New(config.GatewayConfig{Address: addr}))
				return step, &pipeline.RequestContext{
					RequestID:       "req-cond",
					OriginalPath:    testChatCompletionsPath,
					OriginalHeaders: incomingHeaders("req-cond"),
					Body:            map[string]any{"model": testModelName, "stream": false, "messages": []any{}},
					TokenIDs:        []int{1, 2345},
					ResponseWriter:  httptest.NewRecorder(),
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := jsonServer(tc.resp)
			defer server.Close()

			step, reqCtx := tc.build(t, server.URL)
			ctx, logs := captureLogCtx()

			// Conditional decode signals a served response with ErrPipelineDone.
			if err := step.Execute(ctx, reqCtx); err != nil && !errors.Is(err, pipeline.ErrPipelineDone) {
				t.Fatalf("unexpected error: %v", err)
			}
			assertRequestIDPrinted(t, logs.joined(), reqCtx.RequestID)
		})
	}
}
