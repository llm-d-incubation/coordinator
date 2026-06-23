package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"

	"github.com/llm-d/coordinator/pkg/pipeline"
)

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	s.handleInference(w, r)
}

func (s *Server) handleCompletions(w http.ResponseWriter, r *http.Request) {
	s.handleInference(w, r)
}

const maxRequestBodySize = 64 << 20 // 64 MB

func (s *Server) handleInference(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodySize+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	if len(body) > maxRequestBodySize {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	stream, _ := parsed["stream"].(bool)
	model, _ := parsed["model"].(string)

	requestID := r.Header.Get(reqcommon.RequestIDHeaderKey)
	if requestID == "" {
		requestID = uuid.New().String()
	}

	reqCtx := &pipeline.RequestContext{
		RequestID:        requestID,
		OriginalPath:     r.URL.Path,
		OriginalHeaders:  r.Header.Clone(),
		OriginalBody:     body,
		Body:             parsed,
		Model:            model,
		Stream:           stream,
		KVTransferParams: make(map[string]any),
		ResponseWriter:   w,
		StartTime:        time.Now(),
	}

	logger := ctrl.Log.WithName("handler").WithValues(reqcommon.RequestIDHeaderKey, reqCtx.RequestID)
	ctx := log.IntoContext(r.Context(), logger)

	logger.V(logutil.DEFAULT).Info("received request", "path", r.URL.Path, "model", model, "stream", stream)

	if err := s.pipeline.Execute(ctx, reqCtx); err != nil {
		logger.Error(err, "request execution failed")
		status, msg := classifyPipelineError(err, reqCtx.RequestID)
		http.Error(w, msg, status)
	}
}

// classifyPipelineError maps a pipeline error to a client-facing status and
// message. Invalid client input is 400. An upstream 4xx is forwarded with its
// status (the request was the root cause); any other upstream status, and every
// other error, is a 502 gateway fault. Upstream response bodies stay in the
// server log (logged by the caller) and are never sent to the client.
func classifyPipelineError(err error, requestID string) (int, string) {
	if errors.Is(err, pipeline.ErrBadRequest) {
		return http.StatusBadRequest, fmt.Sprintf("bad request (request_id: %s)", requestID)
	}
	var upstream *pipeline.UpstreamError
	if errors.As(err, &upstream) && upstream.StatusCode >= 400 && upstream.StatusCode < 500 {
		return upstream.StatusCode, fmt.Sprintf("%s rejected the request: HTTP %d (request_id: %s)", upstream.Step, upstream.StatusCode, requestID)
	}
	return http.StatusBadGateway, fmt.Sprintf("internal error (request_id: %s)", requestID)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
