package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// BodyFunc runs one child to completion and returns its full, non-streamed body.
type BodyFunc func(ctx context.Context, u PromptUnit) ([]byte, error)

// Collect runs each unit through open (bounded by maxConcurrency) and returns the bodies
// in unit order; the first error cancels the rest. Each goroutine writes a
// distinct bodies[i], so the slice needs no lock.
func Collect(ctx context.Context, units []PromptUnit, maxConcurrency int, open BodyFunc) ([][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bodies := make([][]byte, len(units))
	sem := make(chan struct{}, maxConcurrency)

	var firstErr error
	var once sync.Once
	fail := func(e error) { once.Do(func() { firstErr = e; cancel() }) }

	var wg sync.WaitGroup
	for i, u := range units {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			body, err := open(ctx, u)
			if err != nil {
				fail(err)
				return
			}
			bodies[i] = body
		}()
	}
	wg.Wait()
	return bodies, firstErr
}

// Aggregate merges per-prompt non-streamed responses into one: choices reindexed
// to global position and concatenated, usage summed, the response id set to the
// parent's, the rest of the envelope from the first child. bodies[i] is the
// response for units[i].
func Aggregate(bodies [][]byte, units []PromptUnit, id string, includeUsage bool) ([]byte, error) {
	if len(bodies) != len(units) {
		return nil, fmt.Errorf("batch: %d bodies for %d units", len(bodies), len(units))
	}
	if len(bodies) == 0 {
		return nil, fmt.Errorf("batch: no responses to aggregate")
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(bodies[0], &envelope); err != nil {
		return nil, fmt.Errorf("batch: parsing child 0: %w", err)
	}
	if envelope == nil {
		return nil, fmt.Errorf("batch: child 0 response is not a JSON object")
	}

	merged := make([]json.RawMessage, 0, len(bodies))
	var pT, cT, tT int
	for i, body := range bodies {
		var child struct {
			Choices []json.RawMessage `json:"choices"`
			Usage   *usageCounts      `json:"usage"`
		}
		if err := json.Unmarshal(body, &child); err != nil {
			return nil, fmt.Errorf("batch: parsing child %d: %w", i, err)
		}
		for _, ch := range child.Choices {
			merged = append(merged, RewriteIndex(ch, units[i].Index))
		}
		if child.Usage != nil {
			pT += child.Usage.PromptTokens
			cT += child.Usage.CompletionTokens
			tT += child.Usage.TotalTokens
		}
	}

	choicesJSON, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("batch: marshaling choices: %w", err)
	}
	envelope["choices"] = choicesJSON
	// One id for the whole response, not the arbitrary child-0 id.
	idJSON, err := json.Marshal(id)
	if err != nil {
		return nil, fmt.Errorf("batch: marshaling id: %w", err)
	}
	envelope["id"] = idJSON
	if includeUsage {
		usageJSON, err := json.Marshal(usageCounts{pT, cT, tT})
		if err != nil {
			return nil, fmt.Errorf("batch: marshaling usage: %w", err)
		}
		envelope["usage"] = usageJSON
	} else {
		delete(envelope, "usage") // drop the usage inherited from the first child
	}
	return json.Marshal(envelope)
}
