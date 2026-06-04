package ec

import (
	"github.com/llm-d/coordinator/pkg/pipeline"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
)

// nixlV2 is the NIXL EC connector: each encoder response carries an
// ec_transfer_params object keyed by the encoded image's mm_hash. The
// coordinator merges them into a single flat map and forwards it on the
// prefill request: {"hash1": {...}, "hash2": {...}}.
type nixlV2 struct{}

func (nixlV2) Name() string { return NIXLv2 }

func (nixlV2) MergeEncodeResponse(reqCtx *pipeline.RequestContext, encResp map[string]any) {
	if len(encResp) == 0 {
		return
	}
	reqCtx.ECTransferParams = append(reqCtx.ECTransferParams, encResp)
	logger.V(logutil.TRACE).Info("merged encode response", "total", len(reqCtx.ECTransferParams))
}

func (nixlV2) PreparePrefillECParams(reqCtx *pipeline.RequestContext) map[string]any {
	if len(reqCtx.ECTransferParams) == 0 {
		return nil
	}
	params := make(map[string]any, len(reqCtx.ECTransferParams))
	for i, entry := range reqCtx.ECTransferParams {
		for k, v := range entry {
			if _, exists := params[k]; exists {
				logger.Info("warning: duplicate ec_transfer_params key across encoder responses; overwriting",
					"item", i, "key", k, "requestID", reqCtx.RequestID)
			}
			params[k] = v
		}
	}
	logger.V(logutil.TRACE).Info("preparing prefill ec params", "entries", len(params))
	return params
}
