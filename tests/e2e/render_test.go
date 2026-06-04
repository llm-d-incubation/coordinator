//go:build e2e

// Package e2e contains end-to-end tests that exercise the RenderStep against a
// real rendering service. These tests are skipped during normal `go test ./...`
// runs (build tag `e2e`).
//
// To run:
//
//	go test -tags=e2e ./tests/e2e/...
//
// Configuration via environment variables:
//
//	RENDER_E2E_URL    base URL of the rendering service (default http://localhost:8000)
//	RENDER_E2E_MODEL  model name to send in the request body (default Qwen/Qwen3-VL-2B-Instruct)
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/llm-d/coordinator/pkg/gateway"
	"github.com/llm-d/coordinator/pkg/pipeline"
	"github.com/llm-d/coordinator/pkg/steps"
)

const (
	defaultRenderURL = "http://localhost:8000"
	defaultModel     = "Qwen/Qwen3-VL-2B-Instruct"

	// 1x1 transparent PNG, used as a minimal valid image payload.
	pixelPNG = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)

func renderURL() string {
	if v := os.Getenv("RENDER_E2E_URL"); v != "" {
		return v
	}
	return defaultRenderURL
}

func modelName() string {
	if v := os.Getenv("RENDER_E2E_MODEL"); v != "" {
		return v
	}
	return defaultModel
}

func newRenderStep(t *testing.T, address string) *steps.RenderStep {
	t.Helper()
	step, err := steps.NewRenderStep(map[string]any{"timeout": "30s"})
	if err != nil {
		t.Fatalf("NewRenderStep: %v", err)
	}
	rs := step.(*steps.RenderStep)
	rs.SetServiceAddress(address)
	return rs
}

func TestE2E_ChatCompletions_SimpleMessage(t *testing.T) {
	rs := newRenderStep(t, renderURL())

	reqCtx := &pipeline.RequestContext{
		RequestID:    "e2e-chat-simple",
		OriginalPath: gateway.PathChatCompletions,
		Model:        modelName(),
		Body: map[string]any{
			"model": modelName(),
			"messages": []any{
				map[string]any{"role": "user", "content": "Say hello."},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := rs.Execute(ctx, reqCtx); err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if len(reqCtx.TokenIDs) == 0 {
		t.Fatal("expected non-empty TokenIDs")
	}
}

func TestE2E_ChatCompletions_TwoImages(t *testing.T) {
	rs := newRenderStep(t, renderURL())

	reqCtx := &pipeline.RequestContext{
		RequestID:    "e2e-chat-two-images",
		OriginalPath: gateway.PathChatCompletions,
		Model:        modelName(),
		Body: map[string]any{
			"model": modelName(),
			"messages": []any{
				map[string]any{
					"role": "user",
					"content": []any{
						map[string]any{"type": "text", "text": "What's in these images?"},
						map[string]any{"type": "image_url", "image_url": map[string]any{"url": pixelPNG}},
						map[string]any{"type": "image_url", "image_url": map[string]any{"url": pixelPNG}},
					},
				},
			},
		},
		MultimodalEntries: []pipeline.MultimodalEntry{
			{Index: 0},
			{Index: 1},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := rs.Execute(ctx, reqCtx); err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if len(reqCtx.TokenIDs) == 0 {
		t.Fatal("expected non-empty TokenIDs")
	}
	for i, entry := range reqCtx.MultimodalEntries {
		if entry.Hash == "" {
			t.Errorf("entry %d: Hash not populated", i)
		}
		if entry.Placeholder.Length == 0 {
			t.Errorf("entry %d: Placeholder.Length is 0", i)
		}
		if entry.KwargsData == "" {
			t.Errorf("entry %d: KwargsData not populated", i)
		}
	}
}

func TestE2E_Completions_TextPrompt(t *testing.T) {
	rs := newRenderStep(t, renderURL())

	reqCtx := &pipeline.RequestContext{
		RequestID:    "e2e-completions-text",
		OriginalPath: gateway.PathCompletions,
		Model:        modelName(),
		Body: map[string]any{
			"model":  modelName(),
			"prompt": "hello world",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := rs.Execute(ctx, reqCtx); err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if len(reqCtx.TokenIDs) == 0 {
		t.Fatal("expected non-empty TokenIDs")
	}
	if _, ok := reqCtx.Body["prompt"].([]int); !ok {
		t.Fatalf("expected Body[\"prompt\"] to be []int after render, got %T", reqCtx.Body["prompt"])
	}
}

func TestE2E_Completions_TokenArray(t *testing.T) {
	// This case short-circuits inside RenderStep without calling the upstream
	// service, so it works even if the rendering service is unreachable.
	rs := newRenderStep(t, "http://127.0.0.1:1") // deliberately unreachable

	tokens := []any{float64(1), float64(2345), float64(6789)}
	reqCtx := &pipeline.RequestContext{
		RequestID:    "e2e-completions-token-array",
		OriginalPath: gateway.PathCompletions,
		Model:        modelName(),
		Body: map[string]any{
			"model":  modelName(),
			"prompt": tokens,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rs.Execute(ctx, reqCtx); err != nil {
		t.Fatalf("render failed: %v", err)
	}
	if got, want := reqCtx.TokenIDs, []int{1, 2345, 6789}; !equalInts(got, want) {
		t.Fatalf("TokenIDs = %v, want %v", got, want)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
