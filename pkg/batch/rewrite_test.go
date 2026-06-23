package batch

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRewriteIndex(t *testing.T) {
	tests := []struct {
		name string
		in   string
		idx  int
		want int // choices[0].index after rewrite (-1 = no choices)
	}{
		{"child 0 no-op", `{"choices":[{"index":0,"text":"hi"}]}`, 0, 0},
		{"child 3", `{"choices":[{"index":0,"text":"hi"}]}`, 3, 3},
		{"multi-digit target", `{"choices":[{"index":0,"text":"x"}]}`, 42, 42},
		{"spaced", `{"choices":[{"index": 0,"text":"x"}]}`, 7, 7},
		{"no index field", `{"choices":[],"usage":{}}`, 5, -1},
		{"index literal in text untouched", `{"choices":[{"index":0,"text":"\"index\":99"}]}`, 5, 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := RewriteIndex([]byte(tc.in), tc.idx)
			if got := firstIndex(t, out); got != tc.want {
				t.Fatalf("index = %d, want %d (out=%s)", got, tc.want, out)
			}
		})
	}
}

// TestRewriteIndexPreservesUnknownFields guards rollout-safety: the surgical
// rewrite passes unknown fields through; the typed round-trip drops them.
func TestRewriteIndexPreservesUnknownFields(t *testing.T) {
	in := []byte(`{"id":"c1","choices":[{"index":0,"text":"hi","new_field":"keep"}],"x_vllm":1}`)
	tests := []struct {
		name  string
		fn    func([]byte, int) []byte
		field string
		want  bool
	}{
		{"surgical keeps top-level", RewriteIndex, "x_vllm", true},
		{"surgical keeps choice field", RewriteIndex, "new_field", true},
		{"typed drops top-level", rewriteIndexTyped, "x_vllm", false},
		{"typed drops choice field", rewriteIndexTyped, "new_field", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if out := tc.fn(in, 2); containsField(out, tc.field) != tc.want {
				t.Fatalf("field %q present = %v, want %v (out=%s)", tc.field, !tc.want, tc.want, out)
			}
		})
	}
}

// TestRewriteVariantsAgree: every variant sets the same choices[0].index.
func TestRewriteVariantsAgree(t *testing.T) {
	in := []byte(`{"choices":[{"index":0,"text":"hi","finish_reason":null}]}`)
	variants := []struct {
		name string
		fn   func([]byte, int) []byte
	}{
		{"surgical", RewriteIndex},
		{"map", rewriteIndexMap},
		{"typed", rewriteIndexTyped},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			for _, idx := range []int{0, 1, 9, 123} {
				if got := firstIndex(t, v.fn(in, idx)); got != idx {
					t.Fatalf("index = %d, want %d", got, idx)
				}
			}
		})
	}
}

// TestRewritePreservesTerminalFields: only index changes; finish_reason and
// stop_reason (int/string/null per vLLM) survive byte-identically.
func TestRewritePreservesTerminalFields(t *testing.T) {
	tests := []struct{ name, in string }{
		{"stop", `{"choices":[{"index":0,"text":"","finish_reason":"stop","stop_reason":null}]}`},
		{"length", `{"choices":[{"index":0,"text":"","finish_reason":"length"}]}`},
		{"content_filter", `{"choices":[{"index":0,"text":"","finish_reason":"content_filter"}]}`},
		{"stop_reason int token id", `{"choices":[{"index":0,"text":"","finish_reason":"stop","stop_reason":50256}]}`},
		{"stop_reason string", `{"choices":[{"index":0,"text":"","finish_reason":"stop","stop_reason":"</s>"}]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := string(RewriteIndex([]byte(tc.in), 3))
			if got := firstIndex(t, []byte(out)); got != 3 {
				t.Fatalf("index = %d, want 3", got)
			}
			cut := func(s string) string { return s[strings.Index(s, `"text"`):] }
			if cut(out) != cut(tc.in) {
				t.Fatalf("tail changed:\n got %s\nwant %s", cut(out), cut(tc.in))
			}
		})
	}
}

func firstIndex(t *testing.T, data []byte) int {
	t.Helper()
	var c struct {
		Choices []struct {
			Index int `json:"index"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	if len(c.Choices) == 0 {
		return -1
	}
	return c.Choices[0].Index
}

func containsField(data []byte, field string) bool {
	var m map[string]any
	if json.Unmarshal(data, &m) != nil {
		return false
	}
	if _, ok := m[field]; ok {
		return true
	}
	if choices, ok := m["choices"].([]any); ok {
		for _, c := range choices {
			if cm, ok := c.(map[string]any); ok {
				if _, ok := cm[field]; ok {
					return true
				}
			}
		}
	}
	return false
}

// BenchmarkRewriteIndex compares the surgical byte rewrite (hot path) against
// the map round-trip (anti-pattern) and the typed round-trip. -benchmem is the
// gating metric: allocs/op drives GC pressure at N prompts x T tokens.
func BenchmarkRewriteIndex(b *testing.B) {
	variants := []struct {
		name string
		fn   func([]byte, int) []byte
	}{
		{"surgical", RewriteIndex},
		{"map", rewriteIndexMap},
		{"typed", rewriteIndexTyped},
	}
	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = v.fn(benchChunk, 3)
			}
		})
	}
	// The real hot path: rewrite into a reused (pooled) buffer.
	b.Run("into_pooled", func(b *testing.B) {
		dst := make([]byte, 0, 512)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = RewriteIndexInto(dst[:0], benchChunk, 3)
		}
	})
}
