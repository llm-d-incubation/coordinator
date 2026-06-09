#!/bin/bash

# Use the CONTAINER_RUNTIME from the environment, or default to docker if it's not set.
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-docker}"
echo "Using container tool: ${CONTAINER_RUNTIME}"

# Full image references default to versions.mk values, overridden by the
# environment the Makefile exports.
export COORDINATOR_IMAGE="${COORDINATOR_IMAGE:-ghcr.io/llm-d/llm-d-coordinator:dev}"
export EPP_IMAGE="${EPP_IMAGE:-ghcr.io/llm-d/llm-d-router-endpoint-picker:dev}"
export VLLM_IMAGE="${VLLM_IMAGE:-ghcr.io/llm-d/llm-d-inference-sim:v0.9.2}"
export VLLM_RENDER_IMAGE="${VLLM_RENDER_IMAGE:-vllm/vllm-openai-cpu:v0.21.0}"

TARGETOS="${TARGETOS:-linux}"
TARGETARCH="${TARGETARCH:-$(go env GOARCH)}"

# --- Helper Function to Ensure Image Availability ---
# This function checks for a local image first, then falls back to the registry.
# Locally built images (coordinator, EPP) are reused, not re-pulled.
ensure_image() {
  local image_name="$1"
  echo "Checking for image: ${image_name}"

  if [ -n "$(${CONTAINER_RUNTIME} images -q "${image_name}")" ]; then
    echo " -> Found local image. Proceeding."
  elif ${CONTAINER_RUNTIME} manifest inspect "${image_name}" > /dev/null 2>&1; then
    echo " -> Image found on registry. Pulling..."
    if ! ${CONTAINER_RUNTIME} pull --platform ${TARGETOS}/${TARGETARCH} "${image_name}"; then
        echo "    ❌ ERROR: Failed to pull image '${image_name}'."
        exit 1
    fi
    echo "    ✅ Successfully pulled image."
  else
      echo "    ❌ ERROR: Image '${image_name}' is not available locally and could not be found on the registry."
      exit 1
  fi
}

# --- Print Final Images and Pull Dependencies ---
echo "--- Using the following images ---"
echo "Coordinator Image:   ${COORDINATOR_IMAGE}"
echo "EPP Image:           ${EPP_IMAGE}"
echo "Simulator Image:     ${VLLM_IMAGE}"
echo "vLLM Render Image:   ${VLLM_RENDER_IMAGE}"
echo "----------------------------------------------------"

echo "Pulling dependencies..."
ensure_image "${COORDINATOR_IMAGE}"
ensure_image "${EPP_IMAGE}"
ensure_image "${VLLM_IMAGE}"
ensure_image "${VLLM_RENDER_IMAGE}"
echo "Successfully pulled dependencies"
