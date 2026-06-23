package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"

	"github.com/llm-d/coordinator/pkg/batch"
)

// ErrPipelineDone is returned by a step to signal successful early exit.
// The pipeline treats this as success and stops executing further steps.
var ErrPipelineDone = errors.New("pipeline done")

// ErrBadRequest marks a step failure as caused by invalid client input rather
// than an internal or upstream fault. Steps wrap it (with %w) when rejecting a
// malformed request so the server can answer 400 instead of 502.
var ErrBadRequest = errors.New("bad request")

// UpstreamError carries the HTTP status a step received from an upstream
// service (render, gateway). The server forwards a 4xx status to the client
// (the request was the root cause) and treats 5xx as a 502 gateway fault.
// Body holds the upstream response for server-side logging only; it is not
// sent to the client.
type UpstreamError struct {
	Step       string
	StatusCode int
	Body       string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("%s: upstream returned HTTP %d: %s", e.Step, e.StatusCode, e.Body)
}

// defaultMaxBatch bounds synchronous fan-out when unset, so one request can't
// spawn unbounded child pipeline runs. Async bulk work belongs in batch-gateway.
const defaultMaxBatch = 256

// Pipeline orchestrates the sequential execution of steps. Execute is the entry
// point; a batched prompt fans out one run per prompt.
type Pipeline struct {
	steps  []Step
	limits batch.Limits
}

// New creates a pipeline from an ordered list of steps. limits bound batch
// fan-out; an unset MaxBatch/MaxConcurrency falls back to safe defaults.
func New(steps []Step, limits batch.Limits) *Pipeline {
	if limits.MaxConcurrency <= 0 {
		limits.MaxConcurrency = 1
	}
	if limits.MaxBatch <= 0 {
		limits.MaxBatch = defaultMaxBatch
	}
	return &Pipeline{steps: steps, limits: limits}
}

// Execute runs the request through the steps. A batched prompt fans out one run
// per prompt and merges the results; each child re-enters Execute with a single
// prompt and falls through to the steps. Any step error aborts immediately.
func (p *Pipeline) Execute(ctx context.Context, reqCtx *RequestContext) error {
	if prompt, ok := reqCtx.Body["prompt"]; ok {
		units, err := batch.Split(prompt, optInt(reqCtx.Body["n"], 1),
			streamOptionBool(reqCtx.Body, "continuous_usage_stats"), p.limits)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrBadRequest, err)
		}
		if len(units) > 1 {
			prompts, _ := prompt.([]any)
			return p.executeBatch(ctx, reqCtx, units, prompts)
		}
	}

	logger := log.FromContext(ctx)

	for _, step := range p.steps {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("pipeline cancelled: %w", err)
		}
		logger.V(logutil.TRACE).Info("step starting", "step", step.Name())
		if err := step.Execute(ctx, reqCtx); err != nil {
			if errors.Is(err, ErrPipelineDone) {
				return nil
			}
			return fmt.Errorf("step %q failed: %w", step.Name(), err)
		}
		logger.V(logutil.TRACE).Info("step complete", "step", step.Name())
	}
	return nil
}

func (p *Pipeline) executeBatch(ctx context.Context, parent *RequestContext,
	units []batch.PromptUnit, prompts []any) error {
	logger := log.FromContext(ctx)
	logger.Info("batch request", "prompts", len(units), "stream", parent.Stream)
	if parent.Stream {
		return p.streamBatch(ctx, parent, units, prompts, streamOptionBool(parent.Body, "include_usage"))
	}
	return p.aggregateBatch(ctx, parent, units, prompts)
}

func (p *Pipeline) streamBatch(ctx context.Context, parent *RequestContext,
	units []batch.PromptUnit, prompts []any, includeUsage bool) error {
	w := parent.ResponseWriter
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, _ := w.(http.Flusher)
	flush := func() {
		if flusher != nil {
			flusher.Flush()
		}
	}

	open := func(cctx context.Context, u batch.PromptUnit) (batch.ChunkSource, error) {
		body, err := childBody(parent.OriginalBody, prompts[u.Index], true)
		if err != nil {
			return nil, err
		}
		pr, pw := io.Pipe()
		cw := &pipeWriter{w: pw}
		child := childContext(parent, body, true, cw, u.Index)
		go func() {
			err := p.Execute(cctx, child)
			if err == nil && cw.status >= http.StatusBadRequest {
				err = fmt.Errorf("prompt %d: upstream status %d", u.Index, cw.status)
			}
			_ = pw.CloseWithError(err)
		}()
		return batch.NewReaderSource(pr, pr), nil
	}

	// Multiplex resolves post-header failures in-band and signals external cancel
	// via ctx; only a pre-header failure returns an error for the caller to map.
	if err := batch.Multiplex(ctx, w, flush, units, p.limits.MaxConcurrency, includeUsage, open); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func (p *Pipeline) aggregateBatch(ctx context.Context, parent *RequestContext,
	units []batch.PromptUnit, prompts []any) error {
	bodies, err := batch.Collect(ctx, units, p.limits.MaxConcurrency, func(cctx context.Context, u batch.PromptUnit) ([]byte, error) {
		body, err := childBody(parent.OriginalBody, prompts[u.Index], false)
		if err != nil {
			return nil, err
		}
		cw := &bufferWriter{}
		child := childContext(parent, body, false, cw, u.Index)
		if err := p.Execute(cctx, child); err != nil {
			return nil, err
		}
		if cw.status >= http.StatusBadRequest {
			return nil, fmt.Errorf("prompt %d: upstream status %d", u.Index, cw.status)
		}
		return cw.buf.Bytes(), nil
	})
	if err != nil {
		return err
	}
	out, err := batch.Aggregate(bodies, units, parent.RequestID, true)
	if err != nil {
		return err
	}
	w := parent.ResponseWriter
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(out); err != nil {
		log.FromContext(ctx).Error(err, "writing batch response")
	}
	return nil
}

// childBody clones the request body for one prompt (fresh map per child, since
// steps mutate Body); streaming children request usage so the mux can aggregate.
func childBody(originalBody []byte, prompt any, stream bool) (map[string]any, error) {
	var body map[string]any
	if err := json.Unmarshal(originalBody, &body); err != nil {
		return nil, fmt.Errorf("batch: cloning body: %w", err)
	}
	if body == nil {
		return nil, fmt.Errorf("batch: request body is not a JSON object")
	}
	body["prompt"] = prompt
	body["stream"] = stream
	if stream {
		opts, _ := body["stream_options"].(map[string]any)
		if opts == nil {
			opts = map[string]any{}
		}
		opts["include_usage"] = true
		body["stream_options"] = opts
	}
	return body, nil
}

func childContext(parent *RequestContext, body map[string]any, stream bool,
	w http.ResponseWriter, idx int) *RequestContext {
	model, _ := body["model"].(string)
	rawBody, _ := json.Marshal(body)
	return &RequestContext{
		RequestID:        fmt.Sprintf("%s-%d", parent.RequestID, idx),
		OriginalPath:     parent.OriginalPath,
		OriginalHeaders:  parent.OriginalHeaders,
		OriginalBody:     rawBody,
		Body:             body,
		Model:            model,
		Stream:           stream,
		KVTransferParams: make(map[string]any),
		ResponseWriter:   w,
		StartTime:        time.Now(),
	}
}

// optInt reads a JSON number field (default if absent/wrong type).
func optInt(v any, def int) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return def
}

// streamOptionBool reads a bool from the request's stream_options object.
func streamOptionBool(body map[string]any, key string) bool {
	opts, ok := body["stream_options"].(map[string]any)
	if !ok {
		return false
	}
	b, _ := opts[key].(bool)
	return b
}

// pipeWriter adapts a child's ResponseWriter to an io.Pipe: body flows to the
// reader, status is captured (so a failed child surfaces), headers discarded.
type pipeWriter struct {
	w      io.Writer
	header http.Header
	status int
}

func (p *pipeWriter) Header() http.Header {
	if p.header == nil {
		p.header = http.Header{}
	}
	return p.header
}
func (p *pipeWriter) Write(b []byte) (int, error) { return p.w.Write(b) }
func (p *pipeWriter) WriteHeader(status int)      { p.status = status }
func (p *pipeWriter) Flush()                      {}

// bufferWriter captures a non-streaming child's full response body and status.
type bufferWriter struct {
	buf    bytes.Buffer
	header http.Header
	status int
}

func (b *bufferWriter) Header() http.Header {
	if b.header == nil {
		b.header = http.Header{}
	}
	return b.header
}
func (b *bufferWriter) Write(p []byte) (int, error) { return b.buf.Write(p) }
func (b *bufferWriter) WriteHeader(status int)      { b.status = status }
