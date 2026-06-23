package batch

import (
	"io"
	"strings"
	"testing"
)

func TestReaderSource(t *testing.T) {
	// Skips blank and comment lines, yields each data payload, stops at [DONE].
	stream := "data: {\"index\":0,\"text\":\"a\"}\n\n" +
		": keep-alive\n" +
		"data: {\"index\":0,\"text\":\"b\"}\n\n" +
		"data: [DONE]\n\n" +
		"data: {\"index\":0,\"text\":\"ignored after done\"}\n\n"

	src := NewReaderSource(strings.NewReader(stream), nil)
	var got []string
	for {
		ch, err := src.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, string(ch.Data))
	}
	if len(got) != 2 {
		t.Fatalf("want 2 chunks, got %d: %v", len(got), got)
	}
	if got[0] != `{"index":0,"text":"a"}` || got[1] != `{"index":0,"text":"b"}` {
		t.Fatalf("unexpected chunks: %v", got)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestReaderSource_EOFWithoutDone(t *testing.T) {
	src := NewReaderSource(strings.NewReader("data: {\"index\":0}\n\n"), nil)
	if _, err := src.Next(); err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if _, err := src.Next(); err != io.EOF {
		t.Fatalf("want io.EOF at stream end, got %v", err)
	}
}
