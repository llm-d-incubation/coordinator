// Package batch fans out a multi-prompt /v1/completions request into per-prompt
// sub-requests and merges the streamed results into one response.
package batch

import (
	"errors"
	"fmt"
	"math"
)

// Limits bound fan-out cost. Both must be > 0; the caller supplies them.
type Limits struct {
	// MaxBatch is the hard cap on prompts per request.
	MaxBatch int
	// MaxConcurrency is the max number of concurrent in-flight children.
	MaxConcurrency int
}

// PromptUnit is one prompt of a batch with its global position.
type PromptUnit struct {
	Index    int
	TokenIDs []int
}

var (
	ErrBatchTooLarge = errors.New("batch: too many prompts")
	ErrNWithBatch    = errors.New("batch: n>1 unsupported with batched prompts") // index remap assumes n==1
	ErrContUsage     = errors.New("batch: continuous_usage_stats unsupported with batched prompts")
	ErrBadPrompt     = errors.New("batch: prompt must be string, []int, []string, or [][]int")
)

// Split returns one PromptUnit per prompt. []int/[][]int carry tokens; []string
// is tokenized upstream (nil TokenIDs). n>1 and continuous_usage are rejected:
// both break the index-only remap.
func Split(prompt any, n int, continuousUsage bool, lim Limits) ([]PromptUnit, error) {
	switch p := prompt.(type) {
	case string:
		return []PromptUnit{{Index: 0}}, nil // single prompt, not a batch
	case []any:
		if len(p) == 0 {
			return []PromptUnit{{Index: 0, TokenIDs: []int{}}}, nil
		}
		switch p[0].(type) {
		case float64, int: // []int -> one prompt of token IDs
			toks, err := toIntSlice(p)
			if err != nil {
				return nil, err
			}
			return []PromptUnit{{Index: 0, TokenIDs: toks}}, nil
		case string: // []string -> N prompts, tokens resolved by render
			if err := checkBatch(len(p), n, continuousUsage, lim); err != nil {
				return nil, err
			}
			units := make([]PromptUnit, len(p))
			for i, e := range p {
				if _, ok := e.(string); !ok { // reject mixed arrays, not just p[0]
					return nil, fmt.Errorf("%w: element %d", ErrBadPrompt, i)
				}
				units[i] = PromptUnit{Index: i}
			}
			return units, nil
		case []any: // [][]int -> N prompts of token IDs
			if err := checkBatch(len(p), n, continuousUsage, lim); err != nil {
				return nil, err
			}
			units := make([]PromptUnit, len(p))
			for i, e := range p {
				sub, ok := e.([]any)
				if !ok {
					return nil, fmt.Errorf("%w: element %d", ErrBadPrompt, i)
				}
				toks, err := toIntSlice(sub)
				if err != nil {
					return nil, err
				}
				units[i] = PromptUnit{Index: i, TokenIDs: toks}
			}
			return units, nil
		default:
			return nil, ErrBadPrompt
		}
	default:
		return nil, ErrBadPrompt
	}
}

func checkBatch(count, n int, continuousUsage bool, lim Limits) error {
	if continuousUsage {
		return ErrContUsage
	}
	if n > 1 {
		return ErrNWithBatch
	}
	if lim.MaxBatch > 0 && count > lim.MaxBatch {
		return fmt.Errorf("%w: %d > %d", ErrBatchTooLarge, count, lim.MaxBatch)
	}
	return nil
}

// toIntSlice converts a JSON number array to []int (one allocation, validated).
func toIntSlice(values []any) ([]int, error) {
	out := make([]int, 0, len(values))
	for _, v := range values {
		switch n := v.(type) {
		case float64:
			if n < 0 || n != math.Trunc(n) {
				return nil, fmt.Errorf("%w: token %v not a non-negative integer", ErrBadPrompt, v)
			}
			out = append(out, int(n))
		case int:
			out = append(out, n)
		default:
			return nil, fmt.Errorf("%w: token type %T", ErrBadPrompt, v)
		}
	}
	return out, nil
}
