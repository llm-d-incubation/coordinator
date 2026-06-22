# Development Guide
*(TBD)*

## Running Tests

### Unit Tests

Coverage and race detection are always enabled.

```bash
make test-unit          # run all unit tests
```

### Coordinator End-to-End Tests

```bash
make test-e2e-coordinator
```

Runs the Ginkgo suite under `test/e2e/coordinator/` against the full
disaggregated pipeline topology: one InferencePool per phase (encode, prefill,
decode), each with its own EPP, a standalone Envoy in front of all three, and
the coordinator deployed as a pod. Creates a Kind cluster named
`e2e-coordinator-tests`, applies all resources, sends a
`POST /v1/chat/completions` request to the coordinator, and deletes the cluster
on completion. No Istio, no Gateway/HTTPRoute CRDs.

**Keeping the cluster on failure**

Set `E2E_KEEP_CLUSTER_ON_FAILURE=true` to preserve the cluster when any test
fails:

```bash
E2E_KEEP_CLUSTER_ON_FAILURE=true make test-e2e-coordinator
```

Export the kubeconfig after a preserved failure:

```bash
kind export kubeconfig --name e2e-coordinator-tests
```

Re-run the suite against an existing cluster without re-deploying:

```bash
K8S_CONTEXT=kind-e2e-coordinator-tests go test -v ./test/e2e/coordinator/...
```

**Environment variables**

| Variable | Default | Description |
|---|---|---|
| `COORDINATOR_IMAGE` | _(required)_ | Coordinator image loaded into the Kind cluster |
| `EPP_IMAGE` | `ghcr.io/llm-d/llm-d-router-endpoint-picker:dev` | EPP image |
| `VLLM_IMAGE` | `ghcr.io/llm-d/llm-d-inference-sim:v0.9.2` | vLLM simulator image |
| `VLLM_RENDER_IMAGE` | `vllm/vllm-openai-cpu:v0.21.0` | Render sidecar image |
| `MODEL_NAME` | `Qwen/Qwen3-VL-2B-Instruct` | Model name sent in requests and sidecar args |
| `NAMESPACE` | `default` | Namespace to deploy test resources into |
| `K8S_CONTEXT` | _(empty)_ | Use an existing cluster context instead of creating a Kind cluster |
| `E2E_KEEP_CLUSTER_ON_FAILURE` | `false` | Preserve the Kind cluster when the suite fails |
| `E2E_PRINT_COORDINATOR_LOGS` | `false` | Print coordinator pod logs after every test, not just on failure |

## Submitting Changes

Before opening a PR, run:

```bash
make presubmit
```

This runs the same lint, vet, and test checks as the CI pipeline. Fixing failures locally

