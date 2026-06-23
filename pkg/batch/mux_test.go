package batch

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// sliceSource replays preset chunks; optional err after them; counts closes.
type sliceSource struct {
	chunks [][]byte
	i      int
	err    error
	closes *atomic.Int32
}

func (s *sliceSource) Next() (Chunk, error) {
	if s.i < len(s.chunks) {
		c := Chunk{Data: s.chunks[s.i]}
		s.i++
		return c, nil
	}
	if s.err != nil {
		return Chunk{}, s.err
	}
	return Chunk{}, io.EOF
}
func (s *sliceSource) Close() error {
	if s.closes != nil {
		s.closes.Add(1)
	}
	return nil
}

func tokenChunk(localIdx int, text string) []byte {
	return fmt.Appendf(nil, `{"id":"c","object":"text_completion","choices":[{"index":%d,"text":%q,"finish_reason":null}]}`, localIdx, text)
}

func usageChunk(p, c int) []byte {
	return fmt.Appendf(nil, `{"choices":[],"usage":{"prompt_tokens":%d,"completion_tokens":%d,"total_tokens":%d}}`, p, c, p+c)
}

func TestMultiplex(t *testing.T) {
	tests := []struct {
		name         string
		units        int
		includeUsage bool
		open         OpenFunc
		wantIdx      []int
		wantUsageSum [3]int
		wantErr      bool
	}{
		{
			name:  "two children remap to global indices",
			units: 2,
			open: func(context.Context, PromptUnit) (ChunkSource, error) {
				return &sliceSource{chunks: [][]byte{tokenChunk(0, "a"), tokenChunk(0, "b")}}, nil
			},
			wantIdx: []int{0, 1},
		},
		{
			name:         "usage aggregated across children",
			units:        2,
			includeUsage: true,
			open: func(context.Context, PromptUnit) (ChunkSource, error) {
				return &sliceSource{chunks: [][]byte{tokenChunk(0, "x"), usageChunk(10, 5)}}, nil
			},
			wantIdx:      []int{0, 1},
			wantUsageSum: [3]int{20, 10, 30},
		},
		{
			name:         "spaced usage chunk still detected",
			units:        1,
			includeUsage: true,
			open: func(context.Context, PromptUnit) (ChunkSource, error) {
				return &sliceSource{chunks: [][]byte{
					tokenChunk(0, "x"),
					[]byte(`{"choices": [], "usage": {"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`),
				}}, nil
			},
			wantIdx:      []int{0},
			wantUsageSum: [3]int{7, 3, 10},
		},
		{
			name:  "token text mentioning usage is not dropped",
			units: 1,
			open: func(context.Context, PromptUnit) (ChunkSource, error) {
				return &sliceSource{chunks: [][]byte{[]byte(`{"choices":[{"index":0,"text":"usage"}]}`)}}, nil
			},
			wantIdx: []int{0},
		},
		{
			name:  "single child passthrough",
			units: 1,
			open: func(context.Context, PromptUnit) (ChunkSource, error) {
				return &sliceSource{chunks: [][]byte{tokenChunk(0, "solo")}}, nil
			},
			wantIdx: []int{0},
		},
		{
			name:  "child open error pre-header surfaces",
			units: 1,
			open: func(context.Context, PromptUnit) (ChunkSource, error) {
				return nil, errors.New("boom")
			},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			units := make([]PromptUnit, tc.units)
			for i := range units {
				units[i] = PromptUnit{Index: i}
			}
			var buf bytes.Buffer
			err := Multiplex(t.Context(), &buf, nil, units, 8, tc.includeUsage, tc.open)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := buf.String()
			if n := strings.Count(out, "data: [DONE]"); n != 1 {
				t.Fatalf("[DONE] count = %d, want 1:\n%s", n, out)
			}
			for _, idx := range tc.wantIdx {
				if !strings.Contains(out, fmt.Sprintf(`"index":%d`, idx)) {
					t.Fatalf("missing global index %d:\n%s", idx, out)
				}
			}
			if tc.includeUsage {
				want := fmt.Sprintf(`"prompt_tokens":%d,"completion_tokens":%d,"total_tokens":%d`,
					tc.wantUsageSum[0], tc.wantUsageSum[1], tc.wantUsageSum[2])
				if !strings.Contains(out, want) {
					t.Fatalf("usage aggregate %q missing:\n%s", want, out)
				}
			}
		})
	}
}

// TestMultiplexCancelClosesOpenedChildren: when one child fails and cancels the
// rest, every opened source is closed. A sibling cancelled at the semaphore gate
// before opening is never opened, so the invariant is opens == closes.
func TestMultiplexCancelClosesOpenedChildren(t *testing.T) {
	tests := []struct {
		name    string
		units   int
		failIdx int
	}{
		{"first of three fails", 3, 0},
		{"middle of three fails", 3, 1},
		{"last of three fails", 3, 2},
		{"single fails", 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var opens, closes atomic.Int32
			open := func(_ context.Context, u PromptUnit) (ChunkSource, error) {
				opens.Add(1)
				s := &sliceSource{chunks: [][]byte{tokenChunk(0, "x")}, closes: &closes}
				if u.Index == tc.failIdx {
					s.err = errors.New("fail")
				}
				return s, nil
			}
			units := make([]PromptUnit, tc.units)
			for i := range units {
				units[i] = PromptUnit{Index: i}
			}
			var buf bytes.Buffer
			_ = Multiplex(t.Context(), &buf, nil, units, 8, false, open)
			if opens.Load() == 0 {
				t.Fatal("no sources opened")
			}
			if opens.Load() != closes.Load() {
				t.Fatalf("opens=%d closes=%d: every opened source must be closed", opens.Load(), closes.Load())
			}
		})
	}
}

// decodeBackend streams SSE like a vLLM decode pod: one token chunk, an optional
// gate (to prove incremental flush / exercise cancellation), a second token
// chunk, an optional usage chunk, then [DONE]. The gate never closes in the
// cancel test, so a started+gated handler can only finish via cancellation.
type decodeBackend struct {
	includeUsage bool
	gate         chan struct{}
	started      atomic.Int32
	finished     atomic.Int32
}

func (d *decodeBackend) handler(w http.ResponseWriter, r *http.Request) {
	d.started.Add(1)
	defer d.finished.Add(1)
	w.Header().Set("Content-Type", "text/event-stream")
	f, _ := w.(http.Flusher)
	send := func(b []byte) {
		fmt.Fprintf(w, "data: %s\n\n", b)
		f.Flush()
	}
	send(tokenChunk(0, "a"))
	if d.gate != nil {
		select {
		case <-d.gate:
		case <-r.Context().Done():
			return
		}
	}
	send(tokenChunk(0, "b"))
	if d.includeUsage {
		send(usageChunk(10, 5))
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	f.Flush()
}

// coordinatorServer runs Multiplex against backendURL with n children.
func coordinatorServer(backendURL string, n int, includeUsage bool) *httptest.Server {
	units := make([]PromptUnit, n)
	for i := range units {
		units[i] = PromptUnit{Index: i}
	}
	open := func(ctx context.Context, u PromptUnit) (ChunkSource, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, backendURL+"?i="+strconv.Itoa(u.Index), nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("backend status %d", resp.StatusCode)
		}
		return newHTTPChunkSource(resp), nil
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		_ = Multiplex(r.Context(), w, flush, units, 8, includeUsage, open)
	}))
}

func readEvents(t *testing.T, r io.Reader) []string {
	t.Helper()
	var out []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if line := sc.Text(); strings.HasPrefix(line, "data: ") {
			out = append(out, strings.TrimPrefix(line, "data: "))
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

// TestMultiplexHTTPStreaming validates the merge over real HTTP: every child's
// chunks arrive with their global index, exactly one [DONE], usage aggregated.
func TestMultiplexHTTPStreaming(t *testing.T) {
	const n = 3
	be := &decodeBackend{includeUsage: true}
	backend := httptest.NewServer(http.HandlerFunc(be.handler))
	defer backend.Close()
	coord := coordinatorServer(backend.URL, n, true)
	defer coord.Close()

	resp, err := http.Get(coord.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var done int
	idxSeen := map[int]int{}
	for _, e := range readEvents(t, resp.Body) {
		switch {
		case e == "[DONE]":
			done++
		case strings.Contains(e, `"usage"`):
			if !strings.Contains(e, `"prompt_tokens":30,"completion_tokens":15,"total_tokens":45`) {
				t.Fatalf("usage aggregate wrong: %s", e)
			}
		default:
			idxSeen[firstIndex(t, []byte(e))]++
		}
	}
	if done != 1 {
		t.Fatalf("[DONE] count = %d, want 1", done)
	}
	for i := range n {
		if idxSeen[i] != 2 { // two token chunks per child
			t.Fatalf("global index %d seen %d times, want 2", i, idxSeen[i])
		}
	}
}

// TestMultiplexHTTPIncrementalFlush proves chunks flush before the upstream
// completes: the backend holds chunk 2 behind a gate; the client receives chunk
// 1 first, then we release.
func TestMultiplexHTTPIncrementalFlush(t *testing.T) {
	be := &decodeBackend{gate: make(chan struct{})}
	backend := httptest.NewServer(http.HandlerFunc(be.handler))
	defer backend.Close()
	coord := coordinatorServer(backend.URL, 1, false)
	defer coord.Close()

	resp, err := http.Get(coord.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	br := bufio.NewReader(resp.Body)
	first, err := readOneEvent(br) // must arrive while backend is gated
	if err != nil {
		t.Fatalf("first event: %v", err)
	}
	if !strings.Contains(first, `"index":0`) {
		t.Fatalf("unexpected first event: %s", first)
	}
	close(be.gate) // release the rest
	rest := readEvents(t, br)
	if !slicesContains(rest, "[DONE]") {
		t.Fatalf("missing [DONE] after release: %v", rest)
	}
}

// TestMultiplexHTTPClientDisconnect proves a client disconnect cancels all child
// upstreams (the gated handlers can only return via cancellation) without leak.
func TestMultiplexHTTPClientDisconnect(t *testing.T) {
	const n = 3
	be := &decodeBackend{gate: make(chan struct{})} // never released
	backend := httptest.NewServer(http.HandlerFunc(be.handler))
	defer backend.Close()
	coord := coordinatorServer(backend.URL, n, false)
	defer coord.Close()

	base := stableGoroutines()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, coord.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readOneEvent(bufio.NewReader(resp.Body)); err != nil {
		t.Fatalf("first event: %v", err)
	}
	cancel() // client disconnects mid-stream
	resp.Body.Close()

	eventually(t, 3*time.Second, "all started upstreams must return after disconnect", func() bool {
		s := be.started.Load()
		return s >= 1 && be.finished.Load() == s
	})
	eventually(t, 3*time.Second, fmt.Sprintf("goroutines should settle near baseline %d", base), func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= base+2 // tolerance for httptest keep-alives
	})
}

func readOneEvent(br *bufio.Reader) (string, error) {
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return "", err
		}
		if s := strings.TrimRight(line, "\n"); strings.HasPrefix(s, "data: ") {
			return strings.TrimPrefix(s, "data: "), nil
		}
	}
}

func slicesContains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func stableGoroutines() int {
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	return runtime.NumGoroutine()
}

func eventually(t *testing.T, within time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// benchChunk is a realistic single-token completion chunk with extra/unknown
// fields, shared by the rewrite and mux benchmarks.
var benchChunk = []byte(`{"id":"cmpl-abc123","object":"text_completion","created":1717000000,"model":"meta-llama/Llama-3.1-8B","choices":[{"index":0,"text":" the","logprobs":null,"finish_reason":null,"stop_reason":null}],"x_vllm_meta":{"engine":"v1"}}`)

// BenchmarkMuxFanIn sweeps N to expose per-chunk allocation and fan-in cost.
// Each child replays a fixed number of chunks from memory (no network).
func BenchmarkMuxFanIn(b *testing.B) {
	const chunksPerChild = 64
	for _, n := range []int{1, 2, 8, 32} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			units := make([]PromptUnit, n)
			for i := range units {
				units[i] = PromptUnit{Index: i}
			}
			open := func(_ context.Context, u PromptUnit) (ChunkSource, error) {
				chunks := make([][]byte, chunksPerChild)
				for i := range chunks {
					chunks[i] = benchChunk
				}
				return &sliceSource{chunks: chunks}, nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = Multiplex(context.Background(), io.Discard, nil, units, 32, false, open)
			}
		})
	}
}

// BenchmarkN1Passthrough is the hard regression gate: N=1 must stay cheap.
func BenchmarkN1Passthrough(b *testing.B) {
	const chunks = 64
	units := []PromptUnit{{Index: 0}}
	open := func(_ context.Context, u PromptUnit) (ChunkSource, error) {
		cs := make([][]byte, chunks)
		for i := range cs {
			cs[i] = benchChunk
		}
		return &sliceSource{chunks: cs}, nil
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Multiplex(context.Background(), io.Discard, nil, units, 1, false, open)
	}
}
