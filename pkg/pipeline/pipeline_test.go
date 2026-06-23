package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/llm-d/coordinator/pkg/batch"
)

type mockStep struct {
	name string
	fn   func(ctx context.Context, rc *RequestContext) error
}

func (m *mockStep) Name() string { return m.name }
func (m *mockStep) Execute(ctx context.Context, rc *RequestContext) error {
	return m.fn(ctx, rc)
}

func TestPipeline_ExecutesStepsInOrder(t *testing.T) {
	var order []string
	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			order = append(order, "a")
			return nil
		}},
		&mockStep{name: "b", fn: func(_ context.Context, _ *RequestContext) error {
			order = append(order, "b")
			return nil
		}},
		&mockStep{name: "c", fn: func(_ context.Context, _ *RequestContext) error {
			order = append(order, "c")
			return nil
		}},
	}

	p := New(steps, batch.Limits{})
	err := p.Execute(context.Background(), &RequestContext{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("unexpected execution order: %v", order)
	}
}

func TestPipeline_AbortsOnError(t *testing.T) {
	executed := map[string]bool{}
	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			executed["a"] = true
			return errors.New("step a failed")
		}},
		&mockStep{name: "b", fn: func(_ context.Context, _ *RequestContext) error {
			executed["b"] = true
			return nil
		}},
	}

	p := New(steps, batch.Limits{})
	err := p.Execute(context.Background(), &RequestContext{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !executed["a"] {
		t.Fatal("step a should have executed")
	}
	if executed["b"] {
		t.Fatal("step b should NOT have executed")
	}
}

func TestPipeline_StopsOnErrPipelineDone(t *testing.T) {
	executed := map[string]bool{}
	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			executed["a"] = true
			return ErrPipelineDone
		}},
		&mockStep{name: "b", fn: func(_ context.Context, _ *RequestContext) error {
			executed["b"] = true
			return nil
		}},
	}

	p := New(steps, batch.Limits{})
	err := p.Execute(context.Background(), &RequestContext{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !executed["a"] {
		t.Fatal("step a should have executed")
	}
	if executed["b"] {
		t.Fatal("step b should NOT have executed after ErrPipelineDone")
	}
}

func TestPipeline_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	steps := []Step{
		&mockStep{name: "a", fn: func(_ context.Context, _ *RequestContext) error {
			t.Fatal("should not execute")
			return nil
		}},
	}

	p := New(steps, batch.Limits{})
	err := p.Execute(ctx, &RequestContext{})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
}

// echoStep writes a one-choice response echoing the prompt as text, in the mode
// requested. It stands in for the render/prefill/decode steps.
type echoStep struct{}

func (echoStep) Name() string { return "echo" }

func (echoStep) Execute(_ context.Context, rc *RequestContext) error {
	w := rc.ResponseWriter
	text, _ := rc.Body["prompt"].(string)
	if rc.Stream {
		fmt.Fprintf(w, "data: {\"choices\":[{\"index\":0,\"text\":%q,\"finish_reason\":\"stop\"}]}\n\n", text)
		io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
		return nil
	}
	fmt.Fprintf(w, "{\"id\":\"cmpl\",\"object\":\"text_completion\",\"model\":\"m\","+
		"\"choices\":[{\"index\":0,\"text\":%q,\"finish_reason\":\"stop\"}],"+
		"\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}", text)
	return nil
}

// runCompletions executes body through a pipeline of just echoStep, returning the
// recorded response and Execute's error.
func runCompletions(conc int, body string) (*httptest.ResponseRecorder, error) {
	p := New([]Step{echoStep{}}, batch.Limits{MaxConcurrency: conc})
	var parsed map[string]any
	_ = json.Unmarshal([]byte(body), &parsed)
	stream, _ := parsed["stream"].(bool)
	model, _ := parsed["model"].(string)
	rec := httptest.NewRecorder()
	rc := &RequestContext{
		RequestID:        "test-id",
		OriginalPath:     "/v1/completions",
		OriginalBody:     []byte(body),
		Body:             parsed,
		Model:            model,
		Stream:           stream,
		KVTransferParams: map[string]any{},
		ResponseWriter:   rec,
	}
	return rec, p.Execute(context.Background(), rc)
}

func TestExecute_BatchNonStreamMergesChoices(t *testing.T) {
	rec, err := runCompletions(4, `{"model":"m","prompt":["a","b","c"]}`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var got struct {
		ID      string `json:"id"`
		Choices []struct {
			Index int    `json:"index"`
			Text  string `json:"text"`
		} `json:"choices"`
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v body=%s", err, rec.Body.String())
	}
	if got.ID != "test-id" {
		t.Fatalf("merged id = %q, want the parent id", got.ID)
	}
	if len(got.Choices) != 3 {
		t.Fatalf("want 3 choices, got %d", len(got.Choices))
	}
	want := []string{"a", "b", "c"}
	for i, c := range got.Choices {
		if c.Index != i || c.Text != want[i] {
			t.Fatalf("choice %d = {%d,%q}, want {%d,%q}", i, c.Index, c.Text, i, want[i])
		}
	}
	if got.Usage.PromptTokens != 3 || got.Usage.TotalTokens != 6 {
		t.Fatalf("usage not summed: %+v", got.Usage)
	}
}

func TestExecute_BatchStreamMergesIndices(t *testing.T) {
	rec, err := runCompletions(4, `{"model":"m","prompt":["a","b"],"stream":true}`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{`"index":0`, `"index":1`, `"text":"a"`, `"text":"b"`, "data: [DONE]"} {
		if !strings.Contains(body, want) {
			t.Fatalf("stream missing %q:\n%s", want, body)
		}
	}
	if n := strings.Count(body, "[DONE]"); n != 1 {
		t.Fatalf("want 1 [DONE], got %d", n)
	}
}

func TestExecute_SingleStringPromptNotBatched(t *testing.T) {
	rec, err := runCompletions(4, `{"model":"m","prompt":"hi"}`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(rec.Body.String(), `"text":"hi"`) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

func TestExecute_BatchRejectsNWithBatch(t *testing.T) {
	_, err := runCompletions(4, `{"model":"m","prompt":["a","b"],"n":2}`)
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("want ErrBadRequest for n>1 with batched prompt, got %v", err)
	}
}
