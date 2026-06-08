#!/bin/bash

# Clone llm-d-router at the tip of main and build EPP image.
set -eo pipefail

REPO_URL="https://github.com/llm-d/llm-d-router.git"
BRANCH="main"

: "${EPP_IMAGE:?not set — run via 'make image-build-epp' or export EPP_IMAGE}"
: "${CONTAINER_RUNTIME:?not set — run via 'make image-build-epp'}"


CLONE_DIR="$(mktemp -d)"
trap 'rm -rf "${CLONE_DIR}"' EXIT

git -C "${CLONE_DIR}" init -q
git -C "${CLONE_DIR}" remote add origin "${REPO_URL}"
git -C "${CLONE_DIR}" fetch --depth=1 --no-tags origin "${BRANCH}"
git -C "${CLONE_DIR}" -c advice.detachedHead=false checkout FETCH_HEAD

make -C "${CLONE_DIR}" image-build-epp \
    EPP_IMAGE="${EPP_IMAGE}" \
    CONTAINER_RUNTIME="${CONTAINER_RUNTIME}"

echo "Built ${EPP_IMAGE}"
