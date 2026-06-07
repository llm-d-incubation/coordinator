package ec

import (
	"github.com/llm-d/coordinator/pkg/pipeline"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

// nixlEC is the NIXL EC connector: each encoder response carries an
// ec_transfer_params object keyed by the encoded image's mm_hash. The
// coordinator merges them into a single flat map and forwards it on the
// prefill request: {"hash1": {...}, "hash2": {...}}.
type nixlEC struct{}

func (nixlEC) Name() string { return NIXL }

func (nixlEC) MergeEncodeResponse(reqCtx *pipeline.RequestContext, encResp map[string]any) {
	if len(encResp) == 0 {
		logger.V(logutil.DEBUG).Info("encoder returned no ec_transfer_params; no nixl descriptor will be forwarded for this image",
			"requestID", reqCtx.RequestID)
		return
	}
	reqCtx.ECTransferParams = append(reqCtx.ECTransferParams, encResp)
	logger.V(logutil.TRACE).Info("merged encode response", "total", len(reqCtx.ECTransferParams))
}

func (nixlEC) PreparePrefillECParams(reqCtx *pipeline.RequestContext) map[string]any {
	if len(reqCtx.ECTransferParams) == 0 {
		return nil
	}
	params := make(map[string]any, len(reqCtx.ECTransferParams))
	var dups []string
	for _, entry := range reqCtx.ECTransferParams {
		for k, v := range entry {
			if _, exists := params[k]; exists {
				dups = append(dups, k)
			}
			// v aliases the inner metadata map in reqCtx.ECTransferParams[i];
			// the returned params shares pointers in both directions. Callers
			// must not mutate either side after this call returns.
			params[k] = v
		}
	}
	if len(dups) > 0 {
		logger.Info("warning: duplicate ec_transfer_params keys across encoder responses; last-write-wins",
			"keys", dups, "requestID", reqCtx.RequestID)
	}
	logger.V(logutil.TRACE).Info("preparing prefill ec params", "entries", len(params))
	return params
}
