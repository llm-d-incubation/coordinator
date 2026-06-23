package batch

import (
	"errors"
	"fmt"
	"testing"
)

func TestSplit(t *testing.T) {
	lim := Limits{MaxBatch: 4, MaxConcurrency: 8}
	tests := []struct {
		name    string
		prompt  any
		n       int
		cont    bool
		wantLen int
		wantErr error
	}{
		{"string single", "hello", 1, false, 1, nil},
		{"token array single", []any{float64(1), float64(2), float64(3)}, 1, false, 1, nil},
		{"string batch", []any{"a", "b", "c"}, 1, false, 3, nil},
		{"token batch", []any{[]any{float64(1)}, []any{float64(2)}}, 1, false, 2, nil},
		{"single token-array batch", []any{[]any{float64(1), float64(2)}}, 1, false, 1, nil},
		{"empty array", []any{}, 1, false, 1, nil},
		{"mixed string+number", []any{"a", float64(1)}, 1, false, 0, ErrBadPrompt},
		{"mixed string+bool", []any{"a", true}, 1, false, 0, ErrBadPrompt},
		{"nested 3-deep", []any{[]any{[]any{float64(1)}}}, 1, false, 0, ErrBadPrompt},
		{"n>1 with batch", []any{"a", "b"}, 2, false, 0, ErrNWithBatch},
		{"over cap", []any{"a", "b", "c", "d", "e"}, 1, false, 0, ErrBatchTooLarge},
		{"continuous usage", []any{"a", "b"}, 1, true, 0, ErrContUsage},
		{"bad type", 42, 1, false, 0, ErrBadPrompt},
		{"bad element", []any{true, false}, 1, false, 0, ErrBadPrompt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			units, err := Split(tc.prompt, tc.n, tc.cont, lim)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(units) != tc.wantLen {
				t.Fatalf("len = %d, want %d", len(units), tc.wantLen)
			}
			for i, u := range units {
				if u.Index != i {
					t.Fatalf("unit %d has Index %d", i, u.Index)
				}
			}
		})
	}
}

// BenchmarkSplit covers the [][]int token-batch parse path (toIntSlice).
func BenchmarkSplit(b *testing.B) {
	lim := Limits{MaxBatch: 1024, MaxConcurrency: 32}
	for _, sz := range []int{8, 128, 1024} {
		b.Run(fmt.Sprintf("prompts=%d", sz), func(b *testing.B) {
			prompt := make([]any, sz)
			for i := range prompt {
				prompt[i] = []any{float64(1), float64(2), float64(3), float64(4)}
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Split(prompt, 1, false, lim); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
