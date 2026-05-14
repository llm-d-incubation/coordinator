# Centralized image registry + version tags for the dev environment.
# Override any of these via `make VAR=... env-dev-kind` or
# `export VAR=... && make env-dev-kind`.

IMAGE_REGISTRY       ?= ghcr.io/llm-d

# Image tags
COORDINATOR_TAG      ?= dev
VLLM_SIMULATOR_TAG   ?= v0.8.2
EPP_TAG              ?= dev
SIDECAR_TAG          ?= v0.8.0
UDS_TOKENIZER_TAG    ?= v0.8.0

# Full image references (derived; override only if you need a non-standard repo)
COORDINATOR_IMAGE    ?= $(IMAGE_REGISTRY)/llm-d-coordinator:$(COORDINATOR_TAG)
VLLM_IMAGE           ?= $(IMAGE_REGISTRY)/llm-d-inference-sim:$(VLLM_SIMULATOR_TAG)
EPP_IMAGE            ?= $(IMAGE_REGISTRY)/llm-d-inference-scheduler:$(EPP_TAG)
SIDECAR_IMAGE        ?= $(IMAGE_REGISTRY)/llm-d-routing-sidecar:$(SIDECAR_TAG)
UDS_TOKENIZER_IMAGE  ?= $(IMAGE_REGISTRY)/llm-d-uds-tokenizer:$(UDS_TOKENIZER_TAG)
