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
	for _, name := range []string{NIXLv2, SharedStorage} {
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
	c, err := Build(NIXLv2)
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
	c, err := Build(NIXLv2)
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

// TestNIXL_MergeIgnoresEmpty verifies that an empty encode response is not
// appended to the ordered list.
func TestNIXL_MergeIgnoresEmpty(t *testing.T) {
	c, err := Build(NIXLv2)
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
