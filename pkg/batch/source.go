package batch

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
)

const (
	// scanBufInitial is the SSE line scanner's starting buffer size.
	scanBufInitial = 64 * 1024
	// scanBufMax bounds a single SSE data line (large multimodal chunks).
	scanBufMax = 4 * 1024 * 1024
)

// readerSource scans an SSE stream: each `data:` line is one chunk; `data: [DONE]`
// and EOF terminate. The returned slice is valid only until the next Next/Close.
// Assumes single-line data payloads (vLLM chunks are compact).
type readerSource struct {
	closer io.Closer
	sc     *bufio.Scanner
}

// NewReaderSource adapts an SSE byte stream to a ChunkSource. closer may be nil.
func NewReaderSource(r io.Reader, closer io.Closer) ChunkSource {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, scanBufInitial), scanBufMax)
	return &readerSource{closer: closer, sc: sc}
}

func (s *readerSource) Next() (Chunk, error) {
	for s.sc.Scan() {
		line := s.sc.Bytes()
		if !bytes.HasPrefix(line, sseData) {
			continue
		}
		payload := line[len(sseData):]
		if bytes.Equal(payload, sseDone) {
			return Chunk{}, io.EOF
		}
		return Chunk{Data: payload}, nil
	}
	if err := s.sc.Err(); err != nil {
		return Chunk{}, err
	}
	return Chunk{}, io.EOF
}

func (s *readerSource) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

// newHTTPChunkSource reads an SSE response body chunk-by-chunk as a ChunkSource.
func newHTTPChunkSource(resp *http.Response) ChunkSource {
	return NewReaderSource(resp.Body, resp.Body)
}
