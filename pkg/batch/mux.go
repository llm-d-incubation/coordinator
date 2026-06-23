package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
)

// usageCounts is the OpenAI token-usage triple, shared by the streaming usage
// chunk and the non-streaming aggregate.
type usageCounts struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Chunk is one SSE `data:` payload (JSON, no framing). Data is valid only until
// the next Next/Close, so the mux copies before handing it to the writer.
type Chunk struct{ Data []byte }

// ChunkSource yields one child's chunks in order; Next returns io.EOF at end.
type ChunkSource interface {
	Next() (Chunk, error)
	Close() error
}

// OpenFunc runs one child's prefill+decode and returns its stream (network here;
// fakes in tests). ctx is honored for cancellation, never stored.
type OpenFunc func(ctx context.Context, u PromptUnit) (ChunkSource, error)

var (
	usageHint = []byte(`"usage"`) // fast-path: skip the parse unless present
	sseData   = []byte("data: ")
	sseEnd    = []byte("\n\n")
	sseDone   = []byte("[DONE]")

	bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}
)

func putBuf(bp *[]byte) { *bp = (*bp)[:0]; bufPool.Put(bp) }

// usageAccumulator sums usage counts across children; safe for concurrent adds.
type usageAccumulator struct {
	mu sync.Mutex
	c  usageCounts
}

func (a *usageAccumulator) add(u usageCounts) {
	a.mu.Lock()
	a.c.PromptTokens += u.PromptTokens
	a.c.CompletionTokens += u.CompletionTokens
	a.c.TotalTokens += u.TotalTokens
	a.mu.Unlock()
}

func (a *usageAccumulator) total() usageCounts {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.c
}

// muxer holds the shared state for one Multiplex fan-out: producers (streamChild)
// send rewritten chunks over merged; the consumer (pump) writes them out.
type muxer struct {
	// merged is unbuffered: producers block until pump drains (backpressure).
	merged chan *[]byte
	// sem bounds the number of concurrent children.
	sem   chan struct{}
	open  OpenFunc
	usage usageAccumulator
	fail  func(error)
}

// streamChild opens one child's stream and forwards its chunks until EOF, error,
// or cancellation. Usage-only chunks are accumulated rather than forwarded.
func (m *muxer) streamChild(ctx context.Context, u PromptUnit) {
	select {
	case m.sem <- struct{}{}:
		defer func() { <-m.sem }()
	case <-ctx.Done():
		return
	}

	src, err := m.open(ctx, u)
	if err != nil {
		m.fail(err)
		return
	}
	defer src.Close()

	for ctx.Err() == nil {
		ch, err := src.Next()
		switch {
		case err == io.EOF:
			return
		case err != nil:
			m.fail(err)
			return
		}
		if usage, ok := parseUsage(ch.Data); ok {
			m.usage.add(usage)
			continue
		}
		if !m.forward(ctx, ch.Data, u.Index) {
			return
		}
	}
}

// forward rewrites the chunk to its global choice index and sends it to the
// consumer. Returns false if the context was cancelled before the send.
func (m *muxer) forward(ctx context.Context, data []byte, idx int) bool {
	bp := bufPool.Get().(*[]byte)
	*bp = RewriteIndexInto((*bp)[:0], data, idx)
	select {
	case m.merged <- bp:
		return true
	case <-ctx.Done():
		putBuf(bp)
		return false
	}
}

// pump writes merged chunks to w until the channel closes, ctx is cancelled, or
// a write fails. Reports whether anything was written. The deferred drain is the
// producer join: merged closes only after wg.Wait(), so it blocks until every
// producer has returned, making Multiplex's later firstErr/usage reads race-free.
func (m *muxer) pump(ctx context.Context, w io.Writer, flush func()) bool {
	defer m.drain()

	var wrote bool
	for {
		select {
		case bp, ok := <-m.merged:
			if !ok {
				return wrote
			}
			err := writeSSE(w, *bp, flush)
			putBuf(bp)
			if err != nil {
				m.fail(err)
				return wrote
			}
			wrote = true
		case <-ctx.Done():
			return wrote
		}
	}
}

// drain receives until merged is closed, recycling each buffer. See pump for why
// this doubles as the producer join.
func (m *muxer) drain() {
	for bp := range m.merged {
		putBuf(bp)
	}
}

// Multiplex fans out units (bounded by maxConcurrency), merges their SSE streams into w
// with global choice indices, and emits one terminal [DONE]. ctx cancel or any
// child error cancels the rest; a post-header failure becomes an SSE error event.
func Multiplex(ctx context.Context, w io.Writer, flush func(), units []PromptUnit, maxConcurrency int, includeUsage bool, open OpenFunc) error {
	// The tail tells an external cancel from our own fail() via firstErr == nil:
	// fail() sets firstErr before cancelling, so a cancelled ctx with no firstErr
	// is the caller's.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var once sync.Once

	m := &muxer{
		merged: make(chan *[]byte),
		sem:    make(chan struct{}, maxConcurrency),
		open:   open,
	}
	m.fail = func(e error) { once.Do(func() { firstErr = e; cancel() }) }

	var wg sync.WaitGroup
	for _, u := range units {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.streamChild(ctx, u)
		}()
	}
	go func() { wg.Wait(); close(m.merged) }()

	wrote := m.pump(ctx, w, flush)

	switch {
	case firstErr == nil && ctx.Err() != nil:
		return ctx.Err()
	case firstErr != nil && !wrote:
		return firstErr // pre-header: caller sets the status code
	case firstErr != nil:
		if b, err := errorPayload(firstErr); err == nil {
			_ = writeSSE(w, b, flush) // post-header: SSE error event
		}
		_ = writeSSE(w, sseDone, flush)
		return nil
	}
	if includeUsage {
		if b, err := usagePayload(m.usage.total()); err == nil {
			_ = writeSSE(w, b, flush)
		}
	}
	_ = writeSSE(w, sseDone, flush)
	return nil
}

func writeSSE(w io.Writer, payload []byte, flush func()) error {
	if _, err := w.Write(sseData); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if _, err := w.Write(sseEnd); err != nil {
		return err
	}
	if flush != nil {
		flush()
	}
	return nil
}

// parseUsage reports a usage-only chunk (empty choices, usage present) and its
// counts. The usageHint fast-path keeps ordinary token chunks off the parse.
func parseUsage(data []byte) (usageCounts, bool) {
	if !bytes.Contains(data, usageHint) {
		return usageCounts{}, false
	}
	var u struct {
		Choices []json.RawMessage `json:"choices"`
		Usage   *usageCounts      `json:"usage"`
	}
	if json.Unmarshal(data, &u) != nil || len(u.Choices) != 0 || u.Usage == nil {
		return usageCounts{}, false
	}
	return *u.Usage, true
}

// usageEvent is the terminal OpenAI usage event: an always-empty choices array
// alongside the aggregated token totals.
type usageEvent struct {
	Object  string      `json:"object"`
	Choices []struct{}  `json:"choices"`
	Usage   usageCounts `json:"usage"`
}

// usagePayload marshals the usage event.
func usagePayload(u usageCounts) ([]byte, error) {
	return json.Marshal(usageEvent{Object: "text_completion", Choices: []struct{}{}, Usage: u})
}

// errorEvent is the SSE error event written on a post-header failure.
type errorEvent struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// errorPayload marshals the error event.
func errorPayload(err error) ([]byte, error) {
	return json.Marshal(errorEvent{Error: errorDetail{Message: err.Error(), Type: "coordinator_error"}})
}
