package ec

import (
	"reflect"
	"testing"

	"github.com/llm-d/coordinator/pkg/pipeline"
)

func TestBuild_UnknownReturnsError(t *testing.T) {
	if _, err := Build("does-not-exist"); err == nil {
		t.Fatal("expected error for unknown ec_connector")
	}
}

func TestBuild_EmptyReturnsDefault(t *testing.T) {
	c, err := Build("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Name() != DefaultECConnectorName {
		t.Fatalf("default = %q, want %q", c.Name(), DefaultECConnectorName)
	}
}

func TestBuild_NamedConnectors(t *testing.T) {
	for _, name := range []string{NIXL, SharedStorage} {
		t.Run(name, func(t *testing.T) {
			c, err := Build(name)
			if err != nil {
				t.Fatalf("Build(%q): %v", name, err)
			}
			if c.Name() != name {
				t.Fatalf("Name() = %q, want %q", c.Name(), name)
			}
		})
	}
}

// TestNIXL_MergeAndPrepare verifies that nixl appends each per-image encode
// response in order and emits a flat map keyed by mm_hash on the prefill
// request: {hash1: {...}, hash2: {...}}.
func TestNIXL_MergeAndPrepare(t *testing.T) {
	c, err := Build(NIXL)
	if err != nil {
		t.Fatal(err)
	}

	reqCtx := &pipeline.RequestContext{}

	if got := c.PreparePrefillECParams(reqCtx); got != nil {
		t.Fatalf("expected nil ec_transfer_params before encodes, got %v", got)
	}

	resp1 := map[string]any{"hash-a": map[string]any{"peer_port": 5501, "size_bytes": 1228800, "nixl_agent_metadata_b64": "bml4..."}}
	resp2 := map[string]any{"hash-b": map[string]any{"peer_port": 5502, "size_bytes": 1228800, "nixl_agent_metadata_b64": "QWdlbnQ..."}}

	c.MergeEncodeResponse(reqCtx, resp1)
	c.MergeEncodeResponse(reqCtx, resp2)

	if len(reqCtx.ECTransferParams) != 2 {
		t.Fatalf("expected 2 entries in ECTransferParams, got %d", len(reqCtx.ECTransferParams))
	}

	want := map[string]any{
		"hash-a": map[string]any{"peer_port": 5501, "size_bytes": 1228800, "nixl_agent_metadata_b64": "bml4..."},
		"hash-b": map[string]any{"peer_port": 5502, "size_bytes": 1228800, "nixl_agent_metadata_b64": "QWdlbnQ..."},
	}
	if got := c.PreparePrefillECParams(reqCtx); !reflect.DeepEqual(got, want) {
		t.Errorf("prefill ec_transfer_params:\n got=%v\nwant=%v", got, want)
	}
}

// TestNIXL_MergeAndPrepare_DuplicateHashes verifies that when the same
// mm_hash appears in multiple encode responses (e.g., the request
// references the same image twice), the later entry wins on the prefill
// request. The connector logs a warning but does not error.
func TestNIXL_MergeAndPrepare_DuplicateHashes(t *testing.T) {
	c, err := Build(NIXL)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx := &pipeline.RequestContext{}

	c.MergeEncodeResponse(reqCtx, map[string]any{"hash-a": map[string]any{"peer_port": 5501}})
	c.MergeEncodeResponse(reqCtx, map[string]any{"hash-a": map[string]any{"peer_port": 5599}})

	got := c.PreparePrefillECParams(reqCtx)
	if len(got) != 1 {
		t.Fatalf("expected 1 entry after dedup, got %d: %v", len(got), got)
	}
	entry, ok := got["hash-a"].(map[string]any)
	if !ok {
		t.Fatalf("expected hash-a entry, got %v", got)
	}
	if entry["peer_port"] != 5599 {
		t.Fatalf("expected last-write-wins peer_port=5599, got %v", entry["peer_port"])
	}
}

// TestNIXL_SingleEncodeResponse verifies a single encode response produces a
// one-entry flat map.
func TestNIXL_SingleEncodeResponse(t *testing.T) {
	c, err := Build(NIXL)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx := &pipeline.RequestContext{}
	c.MergeEncodeResponse(reqCtx, map[string]any{"hash-x": map[string]any{"peer_port": 9000}})

	got := c.PreparePrefillECParams(reqCtx)
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d: %v", len(got), got)
	}
	if _, ok := got["hash-x"]; !ok {
		t.Errorf("expected key %q in result: %v", "hash-x", got)
	}
}

// TestNIXL_MultiHashSingleResponse verifies that an encode response carrying
// multiple mm_hashes in one map is fully expanded into the flat result.
func TestNIXL_MultiHashSingleResponse(t *testing.T) {
	c, err := Build(NIXL)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx := &pipeline.RequestContext{}

	c.MergeEncodeResponse(reqCtx, map[string]any{
		"hash-1": map[string]any{"peer_port": 5501},
		"hash-2": map[string]any{"peer_port": 5502},
		"hash-3": map[string]any{"peer_port": 5503},
	})

	got := c.PreparePrefillECParams(reqCtx)
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d: %v", len(got), got)
	}
	for _, want := range []string{"hash-1", "hash-2", "hash-3"} {
		if _, ok := got[want]; !ok {
			t.Errorf("missing key %q in result: %v", want, got)
		}
	}
}

// TestNIXL_PreparePrefillECParams_Aliases documents the pass-through aliasing
// contract: the inner metadata maps in the returned result share pointers
// with reqCtx.ECTransferParams, so mutating a returned value also mutates
// the request-context state. Callers must not mutate either side after this
// call. This test guards against an accidental deep-copy that would silently
// change the contract on which the prefill body serialization depends.
func TestNIXL_PreparePrefillECParams_Aliases(t *testing.T) {
	c, err := Build(NIXL)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx := &pipeline.RequestContext{}
	c.MergeEncodeResponse(reqCtx, map[string]any{"hash-a": map[string]any{"peer_port": 5501}})

	got := c.PreparePrefillECParams(reqCtx)
	inner, ok := got["hash-a"].(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any for hash-a, got %T", got["hash-a"])
	}
	inner["peer_port"] = 9999

	stored, ok := reqCtx.ECTransferParams[0]["hash-a"].(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any in ECTransferParams[0][hash-a], got %T", reqCtx.ECTransferParams[0]["hash-a"])
	}
	if stored["peer_port"] != 9999 {
		t.Errorf("aliasing contract broken: returned map mutation did not propagate to reqCtx.ECTransferParams; got peer_port=%v, want 9999", stored["peer_port"])
	}
}

// TestNIXL_PreparePrefillECParams_Idempotent verifies that calling
// PreparePrefillECParams twice returns the same result and does not mutate
// reqCtx.ECTransferParams.
func TestNIXL_PreparePrefillECParams_Idempotent(t *testing.T) {
	c, err := Build(NIXL)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx := &pipeline.RequestContext{}
	c.MergeEncodeResponse(reqCtx, map[string]any{"hash-a": map[string]any{"peer_port": 5501}})

	before := len(reqCtx.ECTransferParams)
	first := c.PreparePrefillECParams(reqCtx)
	second := c.PreparePrefillECParams(reqCtx)

	if len(reqCtx.ECTransferParams) != before {
		t.Errorf("PreparePrefillECParams mutated ECTransferParams: len went from %d to %d", before, len(reqCtx.ECTransferParams))
	}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("PreparePrefillECParams not idempotent:\n first=%v\nsecond=%v", first, second)
	}
	// Each call must return a fresh outer map; otherwise the function would be
	// caching state and the aliasing contract would extend across calls.
	if reflect.ValueOf(first).Pointer() == reflect.ValueOf(second).Pointer() {
		t.Errorf("PreparePrefillECParams returned the same outer map on repeat call; expected a fresh map per call")
	}
}

// TestNIXL_MergeIgnoresEmpty verifies that an empty encode response is not
// appended to the ordered list.
func TestNIXL_MergeIgnoresEmpty(t *testing.T) {
	c, err := Build(NIXL)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx := &pipeline.RequestContext{}
	c.MergeEncodeResponse(reqCtx, nil)
	c.MergeEncodeResponse(reqCtx, map[string]any{})
	if len(reqCtx.ECTransferParams) != 0 {
		t.Fatalf("expected empty ECTransferParams, got %v", reqCtx.ECTransferParams)
	}
	if got := c.PreparePrefillECParams(reqCtx); got != nil {
		t.Fatalf("expected nil ec_transfer_params, got %v", got)
	}
}

// TestSharedStorage_NoWireFields verifies that the shared_storage EC
// connector emits nothing on the prefill request and does not mutate
// ECTransferParams on encode response.
func TestSharedStorage_NoWireFields(t *testing.T) {
	c, err := Build(SharedStorage)
	if err != nil {
		t.Fatal(err)
	}
	reqCtx := &pipeline.RequestContext{}

	c.MergeEncodeResponse(reqCtx, map[string]any{"hash-x": map[string]any{"peer_host": "10.0.0.9"}})
	if len(reqCtx.ECTransferParams) != 0 {
		t.Errorf("shared_storage should not populate ECTransferParams, got %v", reqCtx.ECTransferParams)
	}
	if got := c.PreparePrefillECParams(reqCtx); got != nil {
		t.Errorf("shared_storage should emit no ec_transfer_params, got %v", got)
	}
}
