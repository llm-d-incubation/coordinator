package batch

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// indexKey's leading quote anchors to a field named `index` (not `reindex` etc).
var indexKey = []byte(`"index":`)

// RewriteIndex overwrites the first `"index":<int>` with idx. Assumes n==1; no
// JSON parse, so unknown fields pass through.
func RewriteIndex(data []byte, idx int) []byte {
	i := bytes.Index(data, indexKey)
	if i < 0 {
		return data
	}
	j := i + len(indexKey)
	for j < len(data) && data[j] == ' ' {
		j++
	}
	k := j
	for k < len(data) && data[k] >= '0' && data[k] <= '9' {
		k++
	}
	if k == j {
		return data
	}
	if idx == 0 && k == j+1 && data[j] == '0' {
		return data // child-0 no-op: avoid alloc
	}
	out := make([]byte, 0, len(data)+4)
	out = append(out, data[:j]...)
	out = strconv.AppendInt(out, int64(idx), 10)
	return append(out, data[k:]...)
}

// RewriteIndexInto appends the rewritten chunk to dst (pooled; pass dst[:0]). It
// always copies, so the result outlives the source's next read.
func RewriteIndexInto(dst, data []byte, idx int) []byte {
	i := bytes.Index(data, indexKey)
	if i < 0 {
		return append(dst, data...)
	}
	j := i + len(indexKey)
	for j < len(data) && data[j] == ' ' {
		j++
	}
	k := j
	for k < len(data) && data[k] >= '0' && data[k] <= '9' {
		k++
	}
	if k == j {
		return append(dst, data...)
	}
	dst = append(dst, data[:j]...)
	dst = strconv.AppendInt(dst, int64(idx), 10)
	return append(dst, data[k:]...)
}

// rewriteIndexMap is the unmarshal/mutate/marshal foil, kept for benchmarking.
func rewriteIndexMap(data []byte, idx int) []byte {
	var m map[string]any
	if json.Unmarshal(data, &m) != nil {
		return data
	}
	if choices, ok := m["choices"].([]any); ok {
		for _, c := range choices {
			if cm, ok := c.(map[string]any); ok {
				cm["index"] = idx
			}
		}
	}
	out, _ := json.Marshal(m)
	return out
}

// rewriteIndexTyped round-trips through a typed struct (benchmark foil; also
// shows the hazard that unknown fields are dropped on re-marshal).
func rewriteIndexTyped(data []byte, idx int) []byte {
	type choice struct {
		Index        int     `json:"index"`
		Text         string  `json:"text"`
		FinishReason *string `json:"finish_reason"`
	}
	var c struct {
		ID      string   `json:"id"`
		Object  string   `json:"object"`
		Created int64    `json:"created"`
		Model   string   `json:"model"`
		Choices []choice `json:"choices"`
	}
	if json.Unmarshal(data, &c) != nil {
		return data
	}
	for i := range c.Choices {
		c.Choices[i].Index = idx
	}
	out, _ := json.Marshal(c)
	return out
}
