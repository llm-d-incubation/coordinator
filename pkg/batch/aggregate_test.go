package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestAggregate(t *testing.T) {
	bodies := [][]byte{
		[]byte(`{"id":"cmpl-1","object":"text_completion","model":"m","choices":[{"index":0,"text":"A","finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`),
		[]byte(`{"id":"cmpl-1","object":"text_completion","model":"m","choices":[{"index":0,"text":"B","finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":4,"total_tokens":5}}`),
	}
	units := []PromptUnit{{Index: 0}, {Index: 1}}

	out, err := Aggregate(bodies, units, "cmpl-parent", true)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}

	var got struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Choices []struct {
			Index int    `json:"index"`
			Text  string `json:"text"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	if got.ID != "cmpl-parent" {
		t.Fatalf("merged id = %q, want the parent id (not a child id)", got.ID)
	}
	if got.Object != "text_completion" {
		t.Fatalf("envelope not preserved from first child: %+v", got)
	}
	if len(got.Choices) != 2 {
		t.Fatalf("want 2 choices, got %d", len(got.Choices))
	}
	for i, c := range got.Choices {
		if c.Index != i {
			t.Fatalf("choice %d reindexed to %d, want %d", i, c.Index, i)
		}
	}
	if got.Choices[0].Text != "A" || got.Choices[1].Text != "B" {
		t.Fatalf("choice text/order wrong: %+v", got.Choices)
	}
	if got.Usage.PromptTokens != 3 || got.Usage.CompletionTokens != 7 || got.Usage.TotalTokens != 10 {
		t.Fatalf("usage not summed: %+v", got.Usage)
	}
}

func TestAggregate_OmitUsage(t *testing.T) {
	bodies := [][]byte{[]byte(`{"choices":[{"index":0,"text":"A"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)}
	out, err := Aggregate(bodies, []PromptUnit{{Index: 0}}, "cmpl-parent", false)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m["usage"]; ok {
		t.Fatalf("usage should be omitted when includeUsage=false")
	}
}

func TestAggregate_LengthMismatch(t *testing.T) {
	_, err := Aggregate([][]byte{[]byte(`{}`)}, []PromptUnit{{Index: 0}, {Index: 1}}, "id", true)
	if err == nil {
		t.Fatal("want error on bodies/units length mismatch")
	}
}

func TestAggregate_NonObjectBody(t *testing.T) {
	// A JSON null body unmarshals to a nil map; Aggregate must error, not panic.
	_, err := Aggregate([][]byte{[]byte("null")}, []PromptUnit{{Index: 0}}, "id", true)
	if err == nil {
		t.Fatal("want error for a non-object (null) child body")
	}
}

func TestCollect(t *testing.T) {
	units := []PromptUnit{{Index: 0}, {Index: 1}, {Index: 2}}
	bodies, err := Collect(context.Background(), units, 2, func(_ context.Context, u PromptUnit) ([]byte, error) {
		return fmt.Appendf(nil, "body-%d", u.Index), nil
	})
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	// Bodies must come back in unit order regardless of completion order.
	for i := range units {
		if got, want := string(bodies[i]), fmt.Sprintf("body-%d", i); got != want {
			t.Fatalf("bodies[%d] = %q, want %q", i, got, want)
		}
	}
}

func TestCollect_FirstError(t *testing.T) {
	units := []PromptUnit{{Index: 0}, {Index: 1}, {Index: 2}}
	_, err := Collect(context.Background(), units, 2, func(_ context.Context, u PromptUnit) ([]byte, error) {
		if u.Index == 1 {
			return nil, errors.New("boom")
		}
		return []byte("ok"), nil
	})
	if err == nil {
		t.Fatal("want the first child error surfaced")
	}
}
