#!/bin/bash

# This shell script deploys a kind cluster with an Istio-based Gateway API
# implementation fully configured. It deploys the vllm simulator, which it
# exposes with a Gateway -> HTTPRoute -> InferencePool. The Gateway is
# configured with the a filter for the ext_proc endpoint picker.

set -eo pipefail

# ------------------------------------------------------------------------------
# Variables
# ------------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Coordinator dev cluster name
: "${CLUSTER_NAME:=llm-d-coordinator-dev}"

# Host port mapped to the Gateway NodePort (baked in at cluster creation time)
# Override: KIND_GATEWAY_HOST_PORT=<port> make env-dev-kind
# To change after creation: make clean-env-dev-kind first, then recreate.
: "${KIND_GATEWAY_HOST_PORT:=30080}"

# Image registry + all image references come from versions.mk (via `make`).
# When running this script directly, you must source versions.mk or export
# IMAGE_REGISTRY, VLLM_IMAGE, EPP_IMAGE, SIDECAR_IMAGE, UDS_TOKENIZER_IMAGE
# yourself. Fail fast if they're missing.
: "${IMAGE_REGISTRY:?not set — run via 'make env-dev-kind' or source versions.mk}"
: "${VLLM_IMAGE:?not set — run via 'make env-dev-kind' or source versions.mk}"
: "${EPP_IMAGE:?not set — run via 'make env-dev-kind' or source versions.mk}"

# Disaggregation topology — selects the pool architecture to deploy.
# Valid values: pd (Prefill/Decode, default), epd (Encode/Prefill/Decode)
DISAGG_TOPOLOGY="${DISAGG_TOPOLOGY:-pd}"
case "${DISAGG_TOPOLOGY}" in
  pd|epd) ;;
  *) echo "Error: DISAGG_TOPOLOGY='${DISAGG_TOPOLOGY}' is not valid. Use 'pd' or 'epd'." >&2; exit 1 ;;
esac
export DISAGG_TOPOLOGY

# Default model: multimodal for EPD (encoder only makes sense with vision/audio models)
if [ "${DISAGG_TOPOLOGY}" == "epd" ]; then
  export MODEL_NAME="${MODEL_NAME:-Qwen/Qwen3-VL-2B-Instruct}"
else
  export MODEL_NAME="${MODEL_NAME:-TinyLlama/TinyLlama-1.1B-Chat-v1.0}"
fi
export MODEL_FAMILY="${MODEL_NAME%%/*}"
export MODEL_ID="${MODEL_NAME##*/}"
export MODEL_NAME_SAFE=$(echo "${MODEL_ID}" | tr '[:upper:]' '[:lower:]' | tr ' /_.' '-')

# Pool names (one per role)
export ENCODE_POOL_NAME="${ENCODE_POOL_NAME:-${MODEL_NAME_SAFE}-encode-pool}"
export PREFILL_POOL_NAME="${PREFILL_POOL_NAME:-${MODEL_NAME_SAFE}-prefill-pool}"
export DECODE_POOL_NAME="${DECODE_POOL_NAME:-${MODEL_NAME_SAFE}-decode-pool}"

# EPP names (one per pool)
export EPP_NAME_E="${EPP_NAME_E:-${MODEL_NAME_SAFE}-encode-endpoint-picker}"
export EPP_NAME_P="${EPP_NAME_P:-${MODEL_NAME_SAFE}-prefill-endpoint-picker}"
export EPP_NAME_D="${EPP_NAME_D:-${MODEL_NAME_SAFE}-decode-endpoint-picker}"

: "${SIDECAR_IMAGE:?not set — run via 'make env-dev-kind' or source versions.mk}"
: "${UDS_TOKENIZER_IMAGE:?not set — run via 'make env-dev-kind' or source versions.mk}"

export VLLM_REPLICA_COUNT_E="${VLLM_REPLICA_COUNT_E:-1}"
export VLLM_REPLICA_COUNT_P="${VLLM_REPLICA_COUNT_P:-1}"
export VLLM_REPLICA_COUNT_D="${VLLM_REPLICA_COUNT_D:-1}"
export VLLM_DATA_PARALLEL_SIZE="${VLLM_DATA_PARALLEL_SIZE:-1}"
export VLLM_SIM_MODE="${VLLM_SIM_MODE:-echo}"
export DECODE_ROLE="${DECODE_ROLE:-decode}"
export NAMESPACE="${NAMESPACE:-default}"
export METRICS_ENDPOINT_AUTH="${METRICS_ENDPOINT_AUTH:-false}"
export HF_TOKEN="${HF_TOKEN:-}"
export KV_CACHE_ENABLED="${KV_CACHE_ENABLED:-false}"
export VLLM_EXTRA_ARGS_E="${VLLM_EXTRA_ARGS_E:-}"
export VLLM_EXTRA_ARGS_P="${VLLM_EXTRA_ARGS_P:-}"
export VLLM_EXTRA_ARGS_D="${VLLM_EXTRA_ARGS_D:-}"

# KV connector for P disaggregation; EC connector for E disaggregation
export CONNECTOR_TYPE="${CONNECTOR_TYPE:-kv-nixl}"
export KV_CONNECTOR_TYPE="${KV_CONNECTOR_TYPE:-kv-nixl}"
export EC_CONNECTOR_TYPE="${EC_CONNECTOR_TYPE:-ec-example}"

# Per-pool EPP configs (single-profile each, since each EPP serves one pool)
export ENCODE_EPP_CONFIG="${ENCODE_EPP_CONFIG:-deploy/config/sim-encode-epp-config.yaml}"
export PREFILL_EPP_CONFIG="${PREFILL_EPP_CONFIG:-deploy/config/sim-prefill-epp-config.yaml}"
export DECODE_EPP_CONFIG="${DECODE_EPP_CONFIG:-deploy/config/sim-decode-epp-config.yaml}"

# ------------------------------------------------------------------------------
# Setup & Requirement Checks
# ------------------------------------------------------------------------------

if [ -z "${CONTAINER_RUNTIME}" ]; then
  if command -v docker &> /dev/null; then
    CONTAINER_RUNTIME="docker"
  elif command -v podman &> /dev/null; then
    CONTAINER_RUNTIME="podman"
  else
    echo "Neither docker nor podman could be found in PATH" >&2
    exit 1
  fi
fi

set -u

for cmd in kind kubectl ${CONTAINER_RUNTIME}; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "Error: $cmd is not installed or not in the PATH."
        exit 1
    fi
done

# TARGET_PORTS is substituted into the `targetPorts: ${TARGET_PORTS}` field in
# inference-pools.yaml. Items are indented with 2 spaces.
NEW_LINE=$'\n'
TARGET_PORTS="${NEW_LINE}  - number: 8000"
for ((i = 1; i < VLLM_DATA_PARALLEL_SIZE; ++i)); do
    EXTRA_PORT=$((8000 + i))
    TARGET_PORTS="${TARGET_PORTS}${NEW_LINE}  - number: ${EXTRA_PORT}"
done
export TARGET_PORTS

# ------------------------------------------------------------------------------
# Cluster Deployment
# ------------------------------------------------------------------------------

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Cluster '${CLUSTER_NAME}' already exists, re-using"
else
    kind create cluster --name "${CLUSTER_NAME}" --config - << EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  image: kindest/node:v1.31.2
  extraPortMappings:
  - containerPort: 30080
    hostPort: ${KIND_GATEWAY_HOST_PORT}
    protocol: TCP
EOF
fi

KUBE_CONTEXT="kind-${CLUSTER_NAME}"
kubectl config set-context ${KUBE_CONTEXT} --namespace=default

set -x

# Hotfix for https://github.com/kubernetes-sigs/kind/issues/3880
CONTAINER_NAME="${CLUSTER_NAME}-control-plane"
${CONTAINER_RUNTIME} exec ${CONTAINER_NAME} /bin/bash -c "sysctl net.ipv4.conf.all.arp_ignore=0"

kubectl --context ${KUBE_CONTEXT} -n kube-system wait --for=condition=Ready --all pods --timeout=300s

echo "Waiting for local-path-storage pods to be created..."
deadline=$(( $(date +%s) + 120 ))
until kubectl --context ${KUBE_CONTEXT} -n local-path-storage get pods -o name 2>/dev/null | grep -q pod/; do
  if (( $(date +%s) >= deadline )); then
    echo "ERROR: local-path-storage pods did not appear within 120s" >&2
    exit 1
  fi
  sleep 2
done
kubectl --context ${KUBE_CONTEXT} -n local-path-storage wait --for=condition=Ready --all pods --timeout=300s

# ------------------------------------------------------------------------------
# Load Container Images
# ------------------------------------------------------------------------------

LINUX_ARCH="$(uname -m)"
case "${LINUX_ARCH}" in
    x86_64) LINUX_ARCH="amd64" ;;
    aarch64|arm64) LINUX_ARCH="arm64" ;;
esac

PLATFORM_ARGS=()
SAVE_ARGS=()
if [ "${CONTAINER_RUNTIME}" == "docker" ]; then
    PLATFORM_ARGS=("--platform" "linux/${LINUX_ARCH}")
elif [ "${CONTAINER_RUNTIME}" == "podman" ]; then
    SAVE_ARGS=("--format=docker-archive")
fi

pull_image() {
    local image="$1"
    if [ "${CONTAINER_RUNTIME}" == "docker" ]; then
        if ! "${CONTAINER_RUNTIME}" pull ${PLATFORM_ARGS[@]+"${PLATFORM_ARGS[@]}"} "${image}" 2>/dev/null; then
            if "${CONTAINER_RUNTIME}" image inspect "${image}" > /dev/null 2>&1; then
                echo "Note: ${image} not found in registry, using local build."
            else
                echo "Error: failed to pull ${image} and no local image found." >&2
                return 1
            fi
        fi
    elif ! "${CONTAINER_RUNTIME}" image inspect "${image}" > /dev/null 2>&1; then
        echo "Image ${image} not found locally, pulling..."
        "${CONTAINER_RUNTIME}" pull ${PLATFORM_ARGS[@]+"${PLATFORM_ARGS[@]}"} "${image}"
    fi
}

load_image() {
    local image="$1"
    echo "Loading ${image} into kind cluster..."
    if [ "${CONTAINER_RUNTIME}" == "docker" ]; then
        docker save "${image}" | \
            docker exec --privileged -i "${CLUSTER_NAME}-control-plane" \
            ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs -
    else
        "${CONTAINER_RUNTIME}" save ${SAVE_ARGS[@]+"${SAVE_ARGS[@]}"} "${image}" | kind --name "${CLUSTER_NAME}" load image-archive /dev/stdin
    fi
}

for IMAGE in "${VLLM_IMAGE}" "${EPP_IMAGE}" "${SIDECAR_IMAGE}" "${UDS_TOKENIZER_IMAGE}"; do
    pull_image "${IMAGE}"
    load_image "${IMAGE}"
done

# ------------------------------------------------------------------------------
# CRD Deployment (Gateway API + GIE + Istio)
# ------------------------------------------------------------------------------

apply_crds() {
    local kustomize_extra_flags="$1"
    local kustomize_dir="$2"
    local attempt max_attempts=3
    for attempt in $(seq 1 ${max_attempts}); do
        if kubectl kustomize ${kustomize_extra_flags} "${kustomize_dir}" \
               | kubectl --context ${KUBE_CONTEXT} apply --server-side --force-conflicts -f -; then
            return 0
        fi
        if [ "${attempt}" -lt "${max_attempts}" ]; then
            echo "CRD apply failed (attempt ${attempt}/${max_attempts}), retrying in 5s..." >&2
            sleep 5
        fi
    done
    echo "Error: CRD apply failed after ${max_attempts} attempts: ${kustomize_dir}" >&2
    return 1
}

apply_crds ""               deploy/components/crds-gateway-api
apply_crds ""               deploy/components/crds-gie
apply_crds "--enable-helm"  deploy/components/crds-istio

# ------------------------------------------------------------------------------
# Development Environment
# ------------------------------------------------------------------------------

TEMP_FILE=$(mktemp)
trap "rm -f \"${TEMP_FILE}\"" EXIT

# Create one configmap per EPP from its source config.
create_epp_configmap() {
    local name="$1" src="$2"
    kubectl --context ${KUBE_CONTEXT} delete configmap "${name}" --ignore-not-found
    envsubst '$MODEL_NAME' < "${src}" > "${TEMP_FILE}"
    kubectl --context ${KUBE_CONTEXT} create configmap "${name}" --from-file=epp-config.yaml="${TEMP_FILE}"
}
create_epp_configmap epp-config-p "${PREFILL_EPP_CONFIG}"
create_epp_configmap epp-config-d "${DECODE_EPP_CONFIG}"
if [ "${DISAGG_TOPOLOGY}" == "epd" ]; then
  create_epp_configmap epp-config-e "${ENCODE_EPP_CONFIG}"
fi

# Render the epp-role kustomize template once per role with
# role-specific EPP_NAME / POOL_NAME / EPP_CONFIG_MAP, then apply.
render_epp_role() {
    local epp_name="$1" pool_name="$2" configmap="$3" role_path="$4"
    # export so envsubst (a separate process downstream in the pipe) sees them
    export EPP_NAME="${epp_name}" POOL_NAME="${pool_name}" EPP_CONFIG_MAP="${configmap}" ROLE_PATH="${role_path}"
    kubectl kustomize deploy/components/inference-gateway/epp-role \
        | envsubst '${EPP_NAME} ${POOL_NAME} ${EPP_CONFIG_MAP} ${ROLE_PATH} ${EPP_IMAGE} ${UDS_TOKENIZER_IMAGE} ${NAMESPACE} ${METRICS_ENDPOINT_AUTH} ${TARGET_PORTS}' \
        | kubectl --context ${KUBE_CONTEXT} apply -f -
}
if [ "${DISAGG_TOPOLOGY}" == "epd" ]; then
  render_epp_role "${EPP_NAME_E}" "${ENCODE_POOL_NAME}" epp-config-e /encode
fi
render_epp_role "${EPP_NAME_P}" "${PREFILL_POOL_NAME}" epp-config-p /prefill
render_epp_role "${EPP_NAME_D}" "${DECODE_POOL_NAME}" epp-config-d /decode

# Deploy Istio base + Gateway (role-independent; no envsubst needed).
kubectl kustomize --enable-helm deploy/environments/dev/base-kind-istio \
  | kubectl --context ${KUBE_CONTEXT} apply -f -

# Select kustomize environment dir based on disaggregation topology
if [ "${DISAGG_TOPOLOGY}" == "epd" ]; then
  KUSTOMIZE_ENV_DIR="deploy/environments/dev/e-p-d"
else
  KUSTOMIZE_ENV_DIR="deploy/environments/dev/p-d"
fi

# Deploy vLLM components for the selected topology
kubectl kustomize --enable-helm "${KUSTOMIZE_ENV_DIR}" \
  | envsubst '${ENCODE_POOL_NAME} ${PREFILL_POOL_NAME} ${DECODE_POOL_NAME} ${MODEL_NAME} ${MODEL_NAME_SAFE} \
  ${EPP_IMAGE} ${VLLM_IMAGE} \
  ${SIDECAR_IMAGE} ${UDS_TOKENIZER_IMAGE} ${TARGET_PORTS} ${NAMESPACE} \
  ${VLLM_REPLICA_COUNT_E} ${VLLM_REPLICA_COUNT_P} ${VLLM_REPLICA_COUNT_D} ${VLLM_DATA_PARALLEL_SIZE} \
  ${KV_CONNECTOR_TYPE} ${EC_CONNECTOR_TYPE} ${CONNECTOR_TYPE} ${KV_CACHE_ENABLED} ${HF_TOKEN} ${VLLM_SIM_MODE} \
  ${DECODE_ROLE} ${VLLM_EXTRA_ARGS_E} ${VLLM_EXTRA_ARGS_P} ${VLLM_EXTRA_ARGS_D}' \
  | awk '
    # Split quoted list items containing multiple --flags into one item per flag.
    # envsubst can produce "- \"--flag1 --flag2\"" when VLLM_EXTRA_ARGS_* holds
    # a multi-flag string; vLLM requires each flag as its own list entry.
    /^[[:space:]]*-[[:space:]]+".*"[[:space:]]*$/ {
      match($0, /^[[:space:]]*/); indent = substr($0, 1, RLENGTH)
      content = $0
      sub(/^[[:space:]]*-[[:space:]]+"/, "", content)
      sub(/"[[:space:]]*$/, "", content)
      if (content == "") { next }
      if (substr(content, 1, 2) == "--") {
        n = split(content, flags, " --")
        for (i = 1; i <= n; i++) {
          flag = flags[i]
          if (i > 1) flag = "--" flag
          if (flag != "") print indent "- \"" flag "\""
        }
        next
      }
    }
    { print }
  ' \
  | kubectl --context ${KUBE_CONTEXT} apply -f -

# ------------------------------------------------------------------------------
# Check & Verify
# ------------------------------------------------------------------------------

kubectl --context ${KUBE_CONTEXT} -n llm-d-istio-system wait --for=condition=available --timeout=600s deployment --all
kubectl --context ${KUBE_CONTEXT} -n default wait --for=condition=available --timeout=600s deployment --all
kubectl --context ${KUBE_CONTEXT} wait gateway/inference-gateway --for=condition=Programmed --timeout=600s

ENCODE_BANNER=""
if [ "${DISAGG_TOPOLOGY}" == "epd" ]; then
  ENCODE_BANNER="
Test encode path:
  curl -s http://localhost:${KIND_GATEWAY_HOST_PORT}/encode/v1/completions \\
    -H 'Content-Type: application/json' \\
    -d '{\"model\":\"${MODEL_NAME}\",\"prompt\":\"hi\",\"max_tokens\":5}' | jq
"
fi

cat <<EOF
-----------------------------------------
Deployment completed! (topology: ${DISAGG_TOPOLOGY})

* Kind Cluster Name: ${CLUSTER_NAME}
* Kubectl Context: ${KUBE_CONTEXT}
* Gateway: http://localhost:${KIND_GATEWAY_HOST_PORT}
${ENCODE_BANNER}
Test prefill path:
  curl -s http://localhost:${KIND_GATEWAY_HOST_PORT}/prefill/v1/completions \\
    -H 'Content-Type: application/json' \\
    -d '{"model":"${MODEL_NAME}","prompt":"hi","max_tokens":5}' | jq

Test decode path:
  curl -s http://localhost:${KIND_GATEWAY_HOST_PORT}/decode/v1/completions \\
    -H 'Content-Type: application/json' \\
    -d '{"model":"${MODEL_NAME}","prompt":"hi","max_tokens":5}' | jq

-----------------------------------------
EOF
