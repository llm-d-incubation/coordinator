# Coordination Service - High Level Design

**Date:** 2026-05-05

---

## 1. Introduction

### 1.1 Purpose

This document describes the high-level design of the Coordination Service for LLM inference serving. While the Coordinator is primarily targeted at disaggregated inference scenarios, it provides functionality beyond disaggregation -- including request pre-processing, e.g. Tokenization, and access external caching service.

Disaggregation itself is not restricted to the Encode/Prefill/Decode (E/P/D) pattern. As model architectures evolve -- particularly omni-models that unify text, vision, audio, and code generation -- new disaggregation phases will emerge: Talker, Code2Wav, ImageGen, DiT (Diffusion Transformer), and others not yet defined. The Coordinator is designed around an extensible phase graph rather than a fixed E/P/D sequence.

The Coordinator is a standalone, stateless service responsible for orchestrating multi-phase inference pipelines across specialized worker pools.

### 1.2 Problem Statement

Current orchestration approaches have two key limitations:

1. **Worker selection is done in advance, not at execution time.** Existing implementations choose the Prefill or Decode worker when the request is first planned, before earlier phases complete. By the time execution reaches that phase, cluster state has changed -- new requests have arrived, workers have become idle or saturated. Selecting the optimal worker just-in-time (JIT), at the moment each phase executes, yields better utilization and lower tail latency.

2. **Adding new processing steps requires deep integration changes.** As model architectures evolve (omni-models, new disaggregation phases, custom pre/post-processing), the orchestration layer must accommodate new pipeline steps without rewriting core logic. A pluggable, extensible pipeline is needed so that new phases (Talker, ImageGen, DiT, etc.) and new pre-processing stages (tokenization, content caching, guardrails) can be added with minimal effort.


### 1.3 Scope

This design covers:
- Coordinator architecture and responsibilities
- Communication patterns with EPPs (Endpoint Picker/Proxy), Gateways, and Model Servers
- Pipeline orchestration and decider logic
- InferencePool topology
- Protocol and API considerations
- Suggested implementation framework (Pingora) justification and architecture mapping

---

## 2. Architecture Overview

### 2.1 System Context

```
┌────────────┐         ┌──────────────────┐         ┌──────────────────────────────────-┐
│            │         │                  │         │         Envoy Gateway             │
│   Client   │ ──────> │   Coordinator    │ ──────> │                                   │
│            │ <────── │    Service       │ <────── │  ┌─────────────────────────────┐  │
│            │         │  (Stateless,     │         │  │ HTTPRoute(/encode)          │  │
└────────────┘         │   Horizontally   │         │  │   ext_proc ──> EPP-E        │  │
                       │   Scalable)      │         │  │     ──> Encode vLLM Pods    │  │
                       │                  │         │  ├─────────────────────────────┤  │
                       └──────────────────┘         │  │ HTTPRoute(/prefill)         │  │
                                                    │  │   ext_proc ──> EPP-P        │  │
                                                    │  │     ──> Prefill vLLM Pods   │  │
                                                    │  ├─────────────────────────────┤  │
                                                    │  │ HTTPRoute(/decode)          │  │
                                                    │  │   ext_proc ──> EPP-D        │  │
                                                    │  │     ──> Decode vLLM Pods    │  │
                                                    │  └─────────────────────────────┘  │
                                                    └─────────────────────────────────-─┘
```

The Coordinator communicates directly with helper services (tokenizer, content download/caching, state resolution) during pre-processing. For inference phases, all requests to vLLM workers are scheduled through the Envoy Gateway -- the Coordinator never calls EPPs or vLLM pods directly. Envoy owns endpoint selection (via EPP ext_proc) and routing to the appropriate vLLM pods.

### 2.2 Key Design Principles

> **Coordinator sees the "forest" (pool-level saturation, request context, pipeline state). EPP sees the "trees" (per-pod KV cache, prefix hits, queue depth). Neither needs to query the other's state.**

---

## 3. Coordinator Responsibilities

### 3.1 Request Pipeline Orchestration

The Coordinator implements a plugin-based workflow pipeline that handles:

| Stage | Responsibility |
|-------|---------------|
| Request hydration | Convert stateful inputs (responses API, messages API) into stateless requests for model servers |
| Asset prefetching | Prefetch images and multimodal content (delegates to an external service) |
| Tokenization | Input tokenization and pre-processing (delegates to an external service)|
| Phase sequencing | Determine optimal execution path (E->P->D) with deferred, JIT scheduling. Pipeline is extensible to accommodate future phases (see Section 4.3). |
| State management | Carry a request processing state |

#### Pre-processing and External Service Calls

During the pre-processing stage, the Coordinator may call external services to prepare the request before phase execution begins. These calls are independent of the Gateway/EPP path and happen before any inference phase is initiated:

- **Tokenization service** - The Coordinator can delegate tokenization to a dedicated service rather than performing it in-process. This allows sharing a tokenizer across Coordinator replicas without bundling model-specific tokenizer weights into the Coordinator image.
- **Multimedia content download service** - For requests containing image URLs, audio URLs, or other media references, the Coordinator calls a content download/caching service to fetch and prepare the binary assets. This avoids blocking the pipeline on external network fetches and enables caching across requests.
- **Stateful response API resolution** - For clients using the stateful responses API (where prior conversation turns are referenced by ID rather than sent in full), the Coordinator calls a state/session service to resolve message IDs into their full content, producing a self-contained stateless request that model servers can process.

These pre-processing service calls are made directly by the Coordinator (not through the Gateway). They are internal platform services, not inference endpoints managed by EPPs, so they do not require ext_proc-based endpoint selection or Gateway-mediated routing.

### 3.2 Aggregated Pool-Level Signals

The Coordinator consumes coarse-grained, pool-level signals:
- Pool saturation levels
- Aggregate capacity
- Queue depth per pool

These inform high-level decisions: whether to disaggregate, whether to shed load, autoscaling hints.

### 3.3 Explicit Non-Responsibilities

The Coordinator must **not** own:
- Per-pod KV cache contents
- Prefix cache hit rates
- Per-endpoint queue depths
- Model server implementation-specific state (e.g., vLLM vs. SGLang differences)

All per-endpoint state is exclusively owned by the EPP.

---

## 4. Pipeline Flow

### 4.1 Request Processing Pipeline

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Coordinator Pipeline                               │
│                                                                          │
│  ┌──────────┐   ┌────────────┐   ┌──────────────────────────────────┐   │
│  │  Entry   │-->│   Pre-     │-->│         Phase Execution           │   │
│  │  Point   │   │ Processing │   │                                  │   │
│  └──────────┘   └────────────┘   │  1. Try Conditional Decode       │   │
│                                   │     ├── Success? Done.           │   │
│  Pre-Processing:                  │     └── Fail ↓                   │   │
│  - Request Hydration              │  2. Try Conditional Prefill      │   │
│  - MM Downloads                   │     ├── Success? Decode          │   │
│  - Tokenization                   │     └── Fail ↓                   │   │
│                                   │  3. Full E/P/D Disaggregation    │   │
│                                   │     Encode(s) -> Prefill -> Decode│   │
│                                   └──────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Detailed Flow

1. **Entry** - Request arrives at Coordinator
2. **Pre-processing** - Download multimodal assets, tokenize input, hydrate stateful references

3. **Conditional Decode** - Coordinator sends request to Gateway with path `/decode`:
   - Gateway invokes EPP-D via ext_proc; EPP-D inspects per-pod state (KV cache coverage, prefix hits, load)
   - **Success (200)**: EPP-D finds a pod that can handle the request without disaggregation. Routes to that pod, streams response back. **Done.**
   - **Conditional failure**: EPP-D determines additional phases are needed. Signals via a specific response code/header (e.g., `x-epp-needs-prefill`). Coordinator proceeds to step 4.
   - **Generic error** (5xx, timeout, resource exhausted): The request cannot be served. Coordinator aborts processing and returns an error to the client. (See Section 6.2 for error classification.)

4. **Conditional Prefill** - Coordinator sends request to Gateway with path `/prefill`:
   - Gateway invokes EPP-P via ext_proc; EPP-P inspects per-pod state
   - **Success (200)**: EPP-P finds a pod that can handle prefill+decode locally. Routes to that pod, response includes KV metadata. Coordinator proceeds to Decode (step 7).
   - **Conditional failure**: EPP-P determines that encoding is required first. Signals via response header (e.g., `x-epp-needs-encode`). Coordinator proceeds to step 5.
   - **Generic error** (5xx, timeout, resource exhausted): Coordinator aborts and returns error to client.

5. **Encode** (parallel for multiple multimodal entries) - If request contains multimodal content:
   - Coordinator sends one encode request per multimodal entry to Gateway with path `/encode`
   - If multiple entries exist, requests are sent **in parallel**
   - Coordinator **waits until ALL encode responses return** before proceeding
   - Extracts cross-phase metadata from each response
   - **Generic error** on any encode request: Coordinator aborts the entire request and returns error to client.

6. **Prefill** - Coordinator sends request to Gateway with path `/prefill` and accumulated metadata:
   - Gateway invokes EPP-P via ext_proc; EPP-P selects the optimal prefill vLLM pod
   - Gateway routes to the selected pod and returns response to Coordinator
   - Coordinator extracts KV cache transfer metadata from response headers
   - **Generic error**: Coordinator aborts and returns error to client.

7. **Decode** - Coordinator sends request to Gateway with path `/decode` and accumulated metadata:
   - Gateway invokes EPP-D via ext_proc; EPP-D selects the optimal decode vLLM pod
   - Gateway routes to the selected decode vLLM pod
   - **Generic error**: Coordinator aborts and returns error to client.

8. **Post-processing** - Optional transformations on response
9. **Return** - Stream final response back to client

**Summary of the cascading fallback:**
```
Pre-processing -> Conditional Decode (try aggregated)
                      |
                      ├── Success -> stream response (done)
                      ├── Conditional failure (needs more phases) ↓
                      └── Generic error -> abort, return error to client
                              |
                              v
                  Conditional Prefill (try P+D on one pod)
                      |
                      ├── Success -> Decode -> stream response
                      ├── Conditional failure (needs encode) ↓
                      └── Generic error -> abort, return error to client
                              |
                              v
                  Encode (parallel) -> Prefill -> Decode -> stream response
                  (generic error at any stage -> abort)
```

### 4.3 Future Disaggregation Steps

The current E/P/D pipeline reflects today's disaggregation model for text and multimodal inference. Future workloads -- particularly omni-models that unify vision, audio, and language generation -- may require additional disaggregation steps. Examples include:

- **Talker** - A dedicated speech synthesis/dialogue phase for real-time conversational audio generation in omni-models
- **ImageGen** - An image generation phase for models that produce visual outputs alongside text (e.g., interleaved text-and-image generation)
- **DiT (Diffusion Transformer)** - A diffusion-based generation phase for high-quality image or video synthesis, requiring iterative denoising steps on specialized hardware
- **Cross-modal attention** - A dedicated phase for fusing representations across modalities before decoding
- **Disaggregated attention** - Separating attention computation from the rest of the decode phase for memory-bandwidth optimization
- **Speculative decoding** - A draft/verify pipeline where a smaller model generates candidates and a larger model validates

The Coordinator's pipeline architecture must not hard-code the E/P/D sequence. The phase graph should be configurable and extensible so that new phases can be added without changes to the Coordinator core -- only new path registrations in the Gateway and new InferencePool/EPP deployments.

---

## 5. Decider Modes

The Coordinator uses the **cascading conditional** execution model described in Section 4.2: try Conditional Decode first, then Conditional Prefill, then full E/P/D. The decision to disaggregate is made by the EPPs at runtime (JIT), not planned in advance by the Coordinator.

The Coordinator's role in this decision is limited to:
- Initiating the cascade (always starting with Conditional Decode)
- Interpreting EPP response signals (`x-epp-needs-prefill`, `x-epp-needs-encode`) to determine the next step
- Optionally skipping the conditional steps entirely based on configuration or pool-level signals

### 5.1 Configuration Modes

| Mode | Behavior | When to use |
|------|----------|-------------|
| `conditional` (default) | Run the full cascading fallback (Conditional Decode -> Conditional Prefill -> E/P/D). EPPs decide at each step. | Production: optimal utilization via JIT decisions. |
| `always-disaggregate` | Skip conditional steps. Go directly to full E/P/D. | Testing, debugging, or when disaggregation is known to be required (e.g., very long prompts). |
| `pool-aware` | Skip conditional steps if pool-level signals indicate disaggregation is necessary (e.g., decode pool saturated). Otherwise run the cascade. | Optimization: avoid wasted conditional round-trips when the answer is predictable from pool state. |

---

## 6. Communication Architecture

### 6.1 Inference Phase Communication via Envoy

```
┌─────────────┐                   ┌────────────────────────────────┐         ┌───────────────┐
│ Coordinator │ ── /encode ────> │        Envoy Gateway            │ ──────> │ Encode Worker │
│             │ ── /prefill ───> │  ext_proc(EPP) selects endpoint │ ──────> │ Prefill Worker│
│             │ ── /decode ────> │  then routes to model server    │ ──────> │ Decode Worker │
│             │ <─── response ── │  EPP sets response headers      │ <────── │               │
└─────────────┘                   └────────────────────────────────┘         └───────────────┘
```

**Flow per phase:**
1. Coordinator sends request to Envoy with phase-specific path (`/encode`, `/prefill`, `/decode`)
2. Envoy's HTTPRoute matches the path to the corresponding InferencePool
3. Envoy invokes the EPP via ext_proc; EPP selects the optimal endpoint
4. Envoy routes the request to the selected model server
5. Response flows back through Envoy to the Coordinator
6. EPP can set response headers (via ext_proc) to pass metadata back to the Coordinator

**Rationale:**
- Full alignment with the Kubernetes Gateway API Inference Extension (GAIE)
- Envoy handles TLS termination, health checks, retries, and observability for all traffic
- EPP ext_proc integration is the standard GAIE pattern -- no custom protocol needed
- The Coordinator remains decoupled from per-endpoint state and model server specifics
- Fail-open behavior: if EPP is unavailable, Envoy can be configured to route using default load balancing

**Tradeoffs accepted:**
- Higher latency per phase (ext_proc round-trip per phase call)
- Single endpoint selection per ext_proc call (no multi-endpoint fan-out from EPP)
- Consult-planner pattern requires EPP to signal disaggregation via response headers/status codes through Envoy
- Cross-phase metadata must be passed via HTTP headers (EPP sets them in ext_proc response)

### 6.2 Response Classification

The Coordinator must distinguish between conditional failures (control flow signals) and generic errors (actual failures) based on the Gateway response:

| Response | Category | Coordinator Behavior |
|----------|----------|---------------------|
| 200 OK | Success | Phase completed. Stream response or proceed to next phase. |
| 2xx + `x-epp-needs-prefill` | Conditional failure | Phase cannot complete without prefill. Proceed to Prefill step. |
| 2xx + `x-epp-needs-encode` | Conditional failure | Phase cannot complete without encoding. Proceed to Encode step. |
| 5xx (Internal Server Error) | Generic error | Abort request, return error to client. |
| 408 / 504 (Timeout) | Generic error | Abort request, return error to client. |
| 429 (Resource Exhausted) | Generic error | Abort request, return error to client (or apply backpressure). |
| Network failure | Generic error | Abort request, return error to client. |

Conditional failures use a success status code with a signal header to distinguish them from actual errors. This ensures that generic HTTP error handling (Envoy retries, monitoring alerts) does not trigger on normal pipeline control flow.

**Fail-over strategies (future consideration):**
- Retry on a different pod (if the error is transient and the EPP can select an alternative)
- Retry with a different execution path (e.g., skip conditional steps and go straight to full disaggregation)
- Degrade gracefully (e.g., if encode fails for one multimodal entry, proceed with partial results if the model supports it)
- Circuit-breaker at the pool level (if a pool is consistently failing, shed load or route to an aggregated fallback)

### 6.3 Metadata Exchange via Headers

Since the Coordinator communicates with EPPs only indirectly (through Envoy), all cross-phase state is exchanged via HTTP headers:

| Direction | Header | Purpose |
|-----------|--------|---------|
| Coordinator -> EPP (request) | `x-request-id` | Correlate phases of the same request |
| EPP -> Coordinator (response) | `x-epp-needs-prefill` | Conditional signal: prefill required before decode can proceed |
| EPP -> Coordinator (response) | `x-epp-needs-encode` | Conditional signal: encoding required before prefill can proceed |


### 6.4 Protocol Support

- **Coordinator to Gateway**: HTTP/2 with persistent connections (connection pooling)
- **Gateway to EPP**: ext_proc (gRPC-based Envoy external processing protocol)
- **Gateway to Model Servers**: HTTP or gRPC depending on model server implementation
- **Client to Coordinator**: HTTP with SSE for streaming inference responses
- **Streaming**: Final decode phase response streams SSE events through Coordinator back to client

### 6.5 Client-Facing API

The Coordinator exposes OpenAI-compatible APIs as its client-facing interface.

**Initial support:**

| API | Endpoint | Specification |
|-----|----------|---------------|
| Completions | `POST /v1/completions` | [OpenAI Completions API](https://platform.openai.com/docs/api-reference/completions) |
| Chat Completions | `POST /v1/chat/completions` | [OpenAI Chat Completions API](https://platform.openai.com/docs/api-reference/chat) |
| Models | `GET /v1/models` | [OpenAI Models API](https://platform.openai.com/docs/api-reference/models) |

Both APIs support streaming (`"stream": true`) via Server-Sent Events (SSE).

**Planned (future):**

| API | Endpoint | Specification |
|-----|----------|---------------|
| Responses | `POST /v1/responses` | [OpenAI Responses API](https://platform.openai.com/docs/api-reference/responses) |

The Responses API introduces stateful, multi-turn conversations where prior turns are referenced by ID. Support for this API requires the stateful response resolution pre-processing step (see Section 3.1) and will be added after the initial Completions/Chat Completions implementation is stable.

---

## 7. InferencePool Topology


| Criteria | Separate Pools (Recommended) | Unified Pool |
|----------|------------------------------|--------------|
| Auto-scaling | Each pool scales independently on phase-specific metrics | Requires custom per-phase HPA logic |
| Flow control | Each EPP manages saturation independently | Single EPP must partition by phase |
| Metrics | Clean per-phase latency, queue depth, saturation | Requires phase labels; dashboards more complex |
| Hardware | Natural heterogeneity (smaller GPUs for encode, large for decode) | Possible but less explicit |
| Operational cost | Higher (3 pool deployments, 3 EPPs) | Lower (single config) |
| Cross-phase optimization | Requires metadata passing or EPP coordination | EPP can make cross-phase decisions |

For smaller-scale or demo deployments, a single EPP with no disaggregation (and potentially no Coordinator) remains the simplest option.

---

## 8. Envoy Path Routing

The Gateway routes requests to different backend services based on path:

| Path | Target |
|------|--------|
| `/v1/chat/completions` | Coordinator (or direct to pool if no disaggregation) |
| `/v1/responses` | Coordinator |
| `/v1/embeddings` | Coordinator or dedicated embedding service |
| `/v1/images/generations` | Coordinator |
| `/v1/audio/transcriptions` | Coordinator |
| `/v1/models` | Model registry |
| `/v1/files` | File storage service |
| `/encode` | Internal: Encode pool (via Coordinator pipeline) |
| `/prefill` | Internal: Prefill pool (via Coordinator pipeline) |
| `/decode` | Internal: Decode pool (via Coordinator pipeline) |

Note: Using separate internal paths (e.g., `/encode`, `/prefill`) couples Envoy configuration to Coordinator pipelines. An alternative is to move phase coordination entirely into the EPP, narrowing the Coordinator to a next-request trigger with phase metadata passed in headers.

---

## 9. Non-Functional Requirements

### 9.1 Performance
- The Coordinator is on the critical path for every inference request
- Target: sub-millisecond overhead for pipeline orchestration logic (excluding network time to Gateway)
- Each phase incurs one round-trip to the Gateway (which includes ext_proc to EPP + routing to vLLM pod)

### 9.2 Deployment Flexibility

The Coordinator is a stateless binary with no runtime dependency on Kubernetes or any specific orchestrator. It can be deployed in any environment:

| Environment | Mechanism |
|---|---|
| Bare metal / VM | Single binary managed by systemd or equivalent |
| Docker containers | Minimal container image |
| Kubernetes | Deployment or DaemonSet, scaled via HPA or KEDA |
| Other orchestrators | ECS, Nomad, HashiCorp or any container scheduler |
| Cloud autoscaling groups | EC2 ASG, GCP MIG, Azure VMSS |

The only hard requirement is network connectivity to the Envoy Gateway. No sidecar, service mesh, or cluster-specific API is needed at runtime.

### 9.3 High Availability
- Stateless design enables horizontal scaling
- Multiple Coordinator replicas behind a load balancer or Kubernetes Service
- No single point of failure; any replica can handle any request

### 9.4 Scalability
- Horizontally scalable (add replicas to handle more concurrent requests)
- No shared mutable state between Coordinator instances
- Per-request state is ephemeral and carried within the request context
- Autoscaling can be driven by any metric source (CPU, memory, request rate, queue depth) regardless of orchestrator

### 9.5 Resilience
- EPP failure handling: Envoy can be configured with fail-open behavior (route using default load balancing if EPP ext_proc is unavailable)
- Gateway failure: standard load balancer or Kubernetes Service failover
- vLLM pod failure: handled by Envoy health checks and retries (transparent to Coordinator)

---

## 10. Suggested Implementation Framework: Pingora

### 10.1 What is Pingora?

Pingora is an open-source Rust framework created by Cloudflare for building fast, reliable, and programmable network services. It was open-sourced in February 2024 under the Apache 2.0 license. At Cloudflare, Pingora handles over 1 trillion requests per day, replacing their previous Nginx-based infrastructure.

**Repository:** github.com/cloudflare/pingora

### 10.2 Why Pingora

#### vs. Building from Scratch (Hyper + Tower + Axum)

| Concern | Pingora | From Scratch |
|---------|---------|--------------|
| Connection pooling | Built-in, battle-tested at trillions of requests/day | Must implement: connection lifecycle, keepalive, HTTP/2 multiplexing |
| Health checking | Built-in with configurable strategies | Must implement: active/passive checks, circuit breaking |
| Retry logic | Built-in with customizable policies | Must implement: retry budgets, backoff, idempotency awareness |
| Load balancing | Built-in (round-robin, consistent hashing, weighted, random) | Must implement or integrate a library |
| TLS/mTLS | Built-in with OpenSSL/BoringSSL, hot-reloadable certs | Must integrate rustls or openssl crate, manage cert rotation |
| Graceful restarts | Built-in zero-downtime upgrades | Must implement: socket passing, draining, signal handling |
| Memory safety at scale | Proven at Cloudflare scale; addresses C/C++ memory bugs that motivated the rewrite | Same Rust guarantees, but unproven custom code |
| Time to production | Weeks (implement business logic on solid foundation) | Months (infrastructure + business logic) |

#### Is Pingora overkill for our scale?

Pingora is designed for Cloudflare's trillion-requests-per-day scale, but that does not mean it carries unnecessary weight at smaller scales. Key footprint considerations:

| Metric | Pingora footprint |
|--------|-------------------|
| Binary size | ~15-25 MB static binary (comparable to a typical Rust HTTP server) |
| Memory at idle | ~5-10 MB RSS (no JVM heap, no Go runtime overhead) |
| Memory per connection | ~few KB (no per-goroutine 8KB stack, no GC pressure) |
| Startup time | < 100ms (static binary, no class loading, no interpreter) |
| Dependencies pulled in | Tokio, hyper, h2, openssl bindings -- same crates you would use building from scratch |
| CPU at low traffic | Near-zero; async runtime idles when no connections active |
| Container image | ~30-50 MB (FROM scratch + static binary + CA certs) |

**Pingora is not a monolith with features you pay for whether you use them or not.** It is a library/framework -- you import the crates you need. The Coordinator would use:
- `pingora-core` (server lifecycle, graceful restart)
- `pingora-proxy` (ProxyHttp trait, upstream connection pooling)
- `pingora-http` (HTTP/1.1 and HTTP/2 handling)

Features like the built-in load balancer, cache module, or advanced rate limiting are separate crates that are not compiled in unless imported. The binary only includes what you use.

**The value proposition at our scale is not raw throughput -- it is engineering time saved.** Even at hundreds or thousands of requests per second (not trillions), the Coordinator still needs:
- HTTP/2 connection pooling to the Gateway (correctness concern, not scale)
- Streaming body passthrough without full buffering (memory concern at any scale)
- Retry/re-routing for multi-phase chaining (correctness concern)
- Graceful restarts for zero-downtime upgrades (operational concern)
- Backpressure propagation (reliability concern)

Building these correctly from scratch on Hyper + Tower takes months of engineering regardless of target throughput. Pingora provides them as tested primitives. The "overkill" concern would apply if Pingora imposed overhead or complexity we don't need -- but it doesn't. It is simply a well-built foundation that works equally well at 100 req/s and 100M req/s.

#### vs. Envoy (C++) as the Coordinator itself

| Concern | Pingora (Coordinator) + Envoy (Gateway) | Envoy alone |
|---------|------------------------------------------|-------------|
| Extensibility model | Native Rust code - full language power, compile-time safety | WASM filters (limited), Lua (slow), or C++ (unsafe, complex build) |
| Pipeline customization | Arbitrary async Rust in each phase - pre-processing, decider logic, phase chaining | ext_proc can participate but cannot own multi-phase orchestration |
| Multi-hop orchestration | Natural - state machine drives sequential calls to Envoy for each phase | Not designed for this; single request/response model |
| Memory safety | Rust's ownership system prevents use-after-free, buffer overflows | C++ - Cloudflare reported frequent memory safety CVEs as motivation for Pingora |
| Performance | Coordinator overhead is sub-ms; Envoy handles actual proxying to model servers | Excellent proxying but not suited as an orchestrator |
| Separation of concerns | Coordinator owns pipeline logic; Envoy owns proxying, TLS, health, EPP ext_proc | Conflates orchestration with proxying |

Envoy remains essential as the Gateway - it handles TLS termination, health checks, retries, ext_proc integration with EPPs, and final routing to model servers. The Coordinator (built on Pingora) sits upstream of Envoy and drives the multi-phase pipeline by making sequential calls through the Gateway.

#### vs. Go-based Proxies (e.g., Traefik, Caddy, custom)

| Concern | Pingora (Rust) | Go proxy |
|---------|----------------|----------|
| Latency | No GC pauses; predictable sub-millisecond overhead | GC pauses at p99/p999 under high allocation pressure |
| Memory | ~70% less memory per connection (Cloudflare benchmarks) | Higher per-goroutine overhead; GC heap pressure |
| Throughput | Better CPU utilization via zero-copy, no runtime overhead | Good but bounded by runtime scheduling overhead |
| Concurrency | Tokio async runtime - millions of concurrent connections | Goroutines scale well but with higher memory per connection |
| Safety | Compile-time memory safety | Runtime panics possible; race detector helps but doesn't catch all |

For a service on the critical path of every inference request, Rust's predictable latency and lower resource usage directly translate to cost savings (fewer Coordinator replicas needed) and better tail latencies.

#### Why Pingora and not just an application framework (e.g., Axum)?

The Coordinator looks like a web service, but its core loop is **proxy-shaped**:

1. Accept client connection (keep it open)
2. Make one or more upstream calls (to Envoy)
3. Stream the final upstream response back on the original client connection

This is fundamentally different from a typical request-handler that computes a response locally. Pingora provides:
- **Connection lifecycle management** between the client-facing side and the Envoy-facing side
- **Retry/re-routing mechanism** that naturally maps to phase chaining (same client connection, different upstream calls)
- **Streaming body passthrough** without buffering the entire response in memory
- **Backpressure propagation** between client and upstream

With Axum or a similar framework, you would need to manually manage the client connection while making multiple reqwest/hyper calls to Envoy, handle streaming correctly, implement backpressure, and manage connection pooling. Pingora provides all of this as its core abstraction.

### 10.3 Pipeline Extensibility

Pingora's trait-based architecture makes it straightforward to extend the Coordinator pipeline. Here is what each type of extension requires:

#### Adding a new disaggregation phase (e.g., audio encoding for omni-models)

Requires only three changes -- no framework modifications:

1. Add a new `Phase` enum variant
2. Add the path mapping in `upstream_request_filter` (e.g., `Phase::AudioEncode => "/audio-encode"`)
3. Add the transition in the phase graph (`next_phase()`)

On the infrastructure side: register a new HTTPRoute + InferencePool/EPP in the Gateway. The new phase works end-to-end.

#### Adding a new pre-processing or post-processing plugin

Implement the `Preprocessor` trait (a single async method), then register it at startup. Examples: guardrails, content filtering, RAG retrieval, custom tokenizers.

#### Adding a new decider mode

Implement the single-method `Decider` trait. No other changes needed.

#### Making the phase graph configurable (per-model pipelines)

Pingora does not provide a dynamic phase graph out of the box. The `next_phase()` function is Rust code. To support different pipelines for different models (e.g., text-only skips Encode, omni-model adds AudioEncode), build a small abstraction on top:

```rust
pub struct ConfigurablePipelineGraph {
    transitions: HashMap<Phase, Phase>,  // loaded from YAML config
}
```

This is straightforward application code on top of Pingora -- not fighting the framework.

#### Constraints to be aware of

| Concern | Impact |
|---------|--------|
| Pingora's phase ordering is fixed | All custom logic must live within the existing hooks (request_filter, upstream_peer, upstream_request_filter, response_filter, response_body_filter, logging). You cannot inject new framework-level phases. |
| Single upstream per hop | Each retry/phase sends to one upstream (the Gateway). Parallel fan-out (e.g., encoding multiple images across workers simultaneously) requires spawning a separate HTTP client within `request_filter`, outside the proxy trait's linear flow. |
| Intermediate response buffering | When chaining phases, intermediate response bodies must be consumed and stored in the request context. For large encode outputs this means memory pressure proportional to the payload size. |
| Pipeline changes require recompilation | Pingora supports graceful restarts (zero-downtime binary swap), but logic changes require a new build. There is no hot-reload of pipeline configuration without redeployment. |

#### Summary

| Extension type | Effort | Framework changes needed |
|---------------|--------|--------------------------|
| New disaggregation phase | Trivial | None -- enum variant + path + graph edge |
| New pre/post-processing plugin | Trivial | None -- implement trait, register at startup |
| New decider mode | Trivial | None -- implement trait |
| Configurable phase graph (per-model) | Moderate | Small abstraction layer on top |
| Parallel fan-out to multiple workers | Moderate | Internal HTTP client alongside proxy trait |
| Fundamentally non-linear pipelines | Harder | The proxy trait assumes sequential upstream hops |

The Coordinator's pipeline is fundamentally sequential (E -> P -> D through the Gateway), which aligns well with Pingora's linear proxy model. The few cases that don't fit (parallel fan-out) are handled cleanly with internal HTTP clients without conflicting with the framework.

---

### 10.4 Strategic Considerations

**Operational Track Record:**
- Handles 1+ trillion requests/day at Cloudflare
- Replaced Nginx due to memory safety concerns and extensibility limitations
- 160% improvement in CPU efficiency reported by Cloudflare vs. their prior C-based proxy
- Active development with regular releases

**Community and Ecosystem:**
- Apache 2.0 license (permissive, enterprise-friendly)
- Active GitHub community (15k+ stars)
- Used by ISRG/Let's Encrypt for their River proxy project
- Cloudflare maintains it as critical infrastructure (strong incentive to continue development)

**Risk Mitigation:**

| Risk | Mitigation |
|------|-----------|
| Cloudflare abandons project | Apache 2.0 license allows forking; codebase is well-documented |
| Learning curve (Rust) | Team can onboard incrementally; Pingora abstracts many low-level concerns |
| Missing feature | Extensible via standard Rust traits; can contribute upstream |
| Performance regression | Comprehensive benchmarking built into CI; same framework handles Cloudflare's production load |

### 10.5 Praxis: LLM-Optimized Proxy Built on Pingora

**Repository:** [github.com/praxis-proxy/praxis](https://github.com/praxis-proxy/praxis)

Praxis is a high-performance, security-first proxy framework built on top of Pingora, specifically designed for AI inference and cloud-native workloads. It adds a higher-level abstraction layer that is directly relevant to the Coordinator's needs:

**Key features for the Coordinator:**

| Feature | Relevance |
|---------|-----------|
| Filter pipeline architecture | All behavior is a composable filter (routing, load balancing, model selection). Maps directly to the Coordinator's plugin-based pipeline. |
| StreamBuffer (peek-then-stream) | Inspects incoming request body chunks (e.g., first few KB for model routing) while deferring upstream forwarding. Enables content-based phase decisions without full buffering. |
| Body-based routing (`json_body_field`) | Extracts fields from JSON request bodies and promotes them to headers. Enables routing by model name, request type, or other payload fields without custom parsing code. |
| Native Rust extensions | Implement `HttpFilter` trait, register with one macro, reference in YAML config. Same compilation model as Pingora but with less boilerplate. |
| Built-in health checks, circuit breaker, rate limiting | Production-ready traffic management primitives out of the box. |
| Security-first defaults | `#![deny(unsafe_code)]`, Rustls (no OpenSSL), SSRF protection, guardrails filter for content scanning. |

**How Praxis relates to Pingora:**

Praxis uses Pingora as its protocol adapter layer (HTTP and TCP) but adds:
- Declarative YAML-based filter chain configuration
- Named, reusable filter chains composable per listener
- Conditional filter execution (`when`/`unless` gates on paths, methods, headers, status codes)
- Built-in AI inference primitives (model routing from request body, streaming payload processing)

**Recommendation:** Consider building the Coordinator on Praxis rather than raw Pingora. Praxis provides the filter pipeline abstraction that the Coordinator needs (pre-processing filters, decider filter, phase routing filter) without requiring the team to build this composition layer from scratch on Pingora's lower-level `ProxyHttp` trait. The Coordinator's pipeline stages map naturally to Praxis filter chains, and the StreamBuffer pattern is directly applicable to inspecting inference requests for routing decisions.

---

## 11. Architecture Mapping: Coordinator on Pingora

### 11.1 Communication Flow

```
┌────────────┐       ┌──────────────────────────────────────────────┐       ┌──────────┐
│            │       │              Envoy Gateway                    │       │  Model   │
│Coordinator │──────>│  HTTPRoute(/encode) ──> ext_proc(EPP-E) ─────│──────>│  Server  │
│  (Pingora) │       │  HTTPRoute(/prefill) ──> ext_proc(EPP-P) ────│──────>│ (Worker) │
│            │<──────│  HTTPRoute(/decode)  ──> ext_proc(EPP-D) ────│──────>│          │
│            │       │                                              │       │          │
└────────────┘       └──────────────────────────────────────────────┘       └──────────┘
     ^                                                                           │
     │                         Response (with headers set by EPP)                │
     └───────────────────────────────────────────────────────────────────────────┘
```

### 11.2 Pingora's Phase Model

Pingora processes requests through a series of **phases**, each represented by a trait method you override:

```
Client Request
     │
     v
┌─────────────────────────────────────────────────────────────┐
│  request_filter(&mut req)                                    │
│  - Inspect/modify the incoming request                       │
│  - Can short-circuit (return response directly)              │
│                                                              │
│  upstream_peer(&req) -> Box<HttpPeer>                        │
│  - Select which upstream to connect to                       │
│  - For the Coordinator, this ALWAYS points to the Envoy GW  │
│                                                              │
│  upstream_request_filter(&mut req, upstream)                  │
│  - Modify request before sending to Envoy                    │
│  - Set path (/encode, /prefill, /decode)                     │
│  - Inject metadata headers for EPP consumption               │
│                                                              │
│  response_filter(&mut resp, upstream_resp)                    │
│  - Inspect Envoy response (which includes EPP-set headers)   │
│  - Decide whether to chain to next phase                     │
│                                                              │
│  response_body_filter(&mut body, end_of_stream)              │
│  - Process response body chunks (streaming)                  │
│                                                              │
│  logging(&req, &resp)                                        │
│  - Emit metrics, traces, logs                                │
└─────────────────────────────────────────────────────────────┘
     │
     v
Client Response
```

### 11.3 Mapping Coordinator Pipeline to Pingora Phases

```rust
pub struct CoordinatorProxy {
    decider: Arc<dyn Decider>,
    gateway_addr: SocketAddr,  // Envoy Gateway address
    pool_signals: Arc<PoolSignalCache>,
    preprocessors: Vec<Arc<dyn Preprocessor>>,
}

#[async_trait]
impl ProxyHttp for CoordinatorProxy {

    type CTX = RequestContext;

    fn new_ctx(&self) -> Self::CTX {
        RequestContext::new()
    }

    /// Phase 1: Pre-processing pipeline
    /// Runs ONCE when the client request arrives.
    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<bool> {
        // 1. Request hydration (stateful -> stateless conversion)
        self.hydrate_request(session, ctx).await?;

        // 2. Multimodal asset download (parallel fetch)
        if ctx.has_multimodal_content() {
            self.prefetch_assets(session, ctx).await?;
        }

        // 3. Tokenization
        self.tokenize(session, ctx).await?;

        // 4. Run decider to determine execution mode
        let mode = self.decider.decide(ctx, &self.pool_signals).await;
        ctx.set_execution_mode(mode);

        // 5. Determine first phase based on execution mode
        match mode {
            ExecutionMode::Conditional | ExecutionMode::PoolAware => {
                // Start with Conditional Decode; EPP signals will drive the cascade
                ctx.set_phase(Phase::Decode);
            }
            ExecutionMode::AlwaysDisaggregate => {
                if ctx.has_multimodal_content() {
                    ctx.set_phase(Phase::Encode);
                } else {
                    ctx.set_phase(Phase::Prefill);
                }
            }
        }

        Ok(false)
    }

    /// Phase 2: Upstream is ALWAYS the Envoy Gateway
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        Ok(Box::new(HttpPeer::new(self.gateway_addr, true, "gateway.local")))
    }

    /// Phase 3: Set the path and headers to tell Envoy which phase this is.
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Set path for Envoy's HTTPRoute matching
        let phase_path = match ctx.current_phase() {
            Phase::Encode => "/encode",
            Phase::Prefill => "/prefill",
            Phase::Decode => "/decode",
        };
        upstream_request.set_uri(phase_path.try_into()?);

        // Inject metadata headers that the EPP can read via ext_proc
        upstream_request.insert_header("x-coord-request-id", &ctx.request_id)?;
        upstream_request.insert_header("x-coord-phase", ctx.current_phase().as_str())?;
        upstream_request.insert_header(
            "x-coord-token-count",
            &ctx.token_count.to_string(),
        )?;

        // Pass cross-phase metadata from prior phases
        if let Some(kv_meta) = &ctx.kv_transfer_metadata {
            upstream_request.insert_header(
                "x-coord-kv-transfer",
                &serde_json::to_string(kv_meta)?,
            )?;
        }
        if let Some(prefill_ep) = &ctx.prefill_endpoint_used {
            upstream_request.insert_header(
                "x-coord-prefill-endpoint",
                prefill_ep,
            )?;
        }

        Ok(())
    }

    /// Phase 4: Handle the response from Envoy.
    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Extract EPP-set metadata from response headers
        if let Some(ep) = upstream_response.headers.get("x-epp-selected-endpoint") {
            ctx.set_last_selected_endpoint(ep.to_str()?.to_string());
        }
        if let Some(kv) = upstream_response.headers.get("x-epp-kv-transfer-params") {
            ctx.kv_transfer_metadata = Some(serde_json::from_str(kv.to_str()?)?);
        }

        // Handle conditional failure signals
        if let Some(_) = upstream_response.headers.get("x-epp-needs-encode") {
            ctx.advance_phase(Phase::Encode);
            ctx.set_needs_retry(true);
            return Ok(());
        }
        if let Some(_) = upstream_response.headers.get("x-epp-needs-prefill") {
            ctx.advance_phase(Phase::Prefill);
            ctx.set_needs_retry(true);
            return Ok(());
        }

        // For normal phase completion, check if there is a next phase
        match ctx.next_phase() {
            Some(next_phase) => {
                ctx.record_phase_completion();
                ctx.advance_phase(next_phase);
                ctx.set_needs_retry(true);
                Ok(())
            }
            None => {
                ctx.set_phase(Phase::Done);
                Ok(())
            }
        }
    }

    /// Phase 5: Stream response body back to client (final phase only)
    fn response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        if ctx.current_phase() != Phase::Done {
            // Intermediate phase - consume body, store if needed for next phase
            if let Some(b) = body.take() {
                ctx.store_phase_response_body(b);
            }
            Ok(None)
        } else {
            // Final phase - stream response body to client as-is
            if ctx.is_streaming() {
                Ok(None)
            } else if end_of_stream {
                self.postprocess_response(body, ctx);
                Ok(None)
            } else {
                Ok(None)
            }
        }
    }

    /// Phase 6: Logging and metrics
    async fn logging(
        &self,
        _session: &mut Session,
        _error: Option<&pingora::Error>,
        ctx: &mut Self::CTX,
    ) {
        metrics::histogram!(
            "coordinator_request_duration_ms",
            ctx.elapsed_ms(),
            "phase_count" => ctx.phases_executed().to_string(),
            "mode" => ctx.execution_mode().as_str(),
            "disaggregated" => ctx.was_disaggregated().to_string(),
        );
        ctx.finish_trace_span();
    }
}
```

### 11.4 Multi-Phase Chaining via Envoy

The Coordinator chains phases by making sequential HTTP calls to the same Envoy Gateway, varying the path each time. Envoy handles everything downstream (EPP consultation, endpoint selection, routing to model server).

```
Request arrives at Coordinator
    │
    ├─[Decider: always-disaggregate, has multimodal]
    │   │
    │   ├── POST /encode  ──> Envoy ──> ext_proc(EPP-E) ──> Encode Worker
    │   │   Response: 200 + x-epp-kv-transfer-params header
    │   │
    │   ├── POST /prefill ──> Envoy ──> ext_proc(EPP-P) ──> Prefill Worker
    │   │   Request includes: x-coord-kv-transfer header from prior phase
    │   │   Response: 200 + x-epp-selected-endpoint header
    │   │
    │   └── POST /decode  ──> Envoy ──> ext_proc(EPP-D) ──> Decode Worker
    │       Request includes: x-coord-prefill-endpoint header
    │       Response: 200 + SSE stream (final response to client)
    │
    ├─[Mode: conditional (cascading fallback)]
    │   │
    │   ├── POST /decode  ──> Envoy ──> ext_proc(EPP-D) ──> Decode Worker
    │   │   EPP-D inspects per-pod state:
    │   │     - If can handle locally: routes to worker, returns 200 (done)
    │   │     - If needs prefill: returns 200 + x-epp-needs-prefill
    │   │
    │   ├── POST /prefill ──> Envoy ──> ext_proc(EPP-P) ──> Prefill Worker
    │   │     - If can handle P+D: routes to worker, returns 200 + KV metadata -> Decode
    │   │     - If needs encode: returns 200 + x-epp-needs-encode
    │   │
    │   └── [If encode needed] ──> Encode ──> Prefill ──> Decode (as above)
    │
    └── Response streamed back to client
```

### 11.5 Envoy Gateway Configuration

The Envoy Gateway is configured with HTTPRoutes per phase, each with an ext_proc filter pointing to the phase-specific EPP:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: encode-route
spec:
  parentRefs:
    - name: inference-gateway
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /encode
      backendRefs:
        - name: encode-pool
      filters:
        - type: ExtensionRef
          extensionRef:
            group: inference.networking.x-k8s.io
            kind: InferencePool
            name: encode-pool  # triggers ext_proc to EPP-E
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: prefill-route
spec:
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /prefill
      backendRefs:
        - name: prefill-pool
      filters:
        - type: ExtensionRef
          extensionRef:
            group: inference.networking.x-k8s.io
            kind: InferencePool
            name: prefill-pool  # triggers ext_proc to EPP-P
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: decode-route
spec:
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /decode
      backendRefs:
        - name: decode-pool
      filters:
        - type: ExtensionRef
          extensionRef:
            group: inference.networking.x-k8s.io
            kind: InferencePool
            name: decode-pool  # triggers ext_proc to EPP-D
```

### 11.6 Supporting Components

#### Decider Plugin Interface

```rust
#[async_trait]
pub trait Decider: Send + Sync {
    async fn decide(
        &self,
        ctx: &RequestContext,
        pool_signals: &PoolSignalCache,
    ) -> ExecutionMode;
}

pub struct ConditionalDecider;

#[async_trait]
impl Decider for ConditionalDecider {
    async fn decide(&self, _ctx: &RequestContext, _signals: &PoolSignalCache) -> ExecutionMode {
        ExecutionMode::Conditional
    }
}

pub struct AlwaysDisaggregateDecider;

#[async_trait]
impl Decider for AlwaysDisaggregateDecider {
    async fn decide(&self, _ctx: &RequestContext, _signals: &PoolSignalCache) -> ExecutionMode {
        ExecutionMode::AlwaysDisaggregate
    }
}

pub struct PoolAwareDecider {
    saturation_threshold: f64,
}

#[async_trait]
impl Decider for PoolAwareDecider {
    async fn decide(&self, _ctx: &RequestContext, signals: &PoolSignalCache) -> ExecutionMode {
        if signals.decode_saturation() > self.saturation_threshold {
            ExecutionMode::AlwaysDisaggregate
        } else {
            ExecutionMode::Conditional
        }
    }
}
```

#### Pool Signal Cache

```rust
pub struct PoolSignalCache {
    signals: RwLock<HashMap<PoolId, PoolSignals>>,
}

pub struct PoolSignals {
    pub saturation: f64,
    pub capacity: u32,
    pub queue_depth: u32,
    pub last_updated: Instant,
}

impl PoolSignalCache {
    pub async fn refresh_loop(self: Arc<Self>, interval: Duration) {
        loop {
            if let Ok(signals) = self.fetch_signals().await {
                let mut cache = self.signals.write().await;
                for (pool_id, signal) in signals {
                    cache.insert(pool_id, signal);
                }
            }
            tokio::time::sleep(interval).await;
        }
    }

    pub fn should_disaggregate(&self) -> bool {
        let signals = self.signals.read().unwrap();
        signals.get(&PoolId::Decode)
            .map(|s| s.saturation > 0.8)
            .unwrap_or(false)
    }
}
```

### 11.7 Request Context (Per-Request State Machine)

```rust
pub struct RequestContext {
    pub request_id: String,

    // Pipeline state machine
    execution_mode: ExecutionMode,
    current_phase: Phase,
    phases_completed: Vec<PhaseResult>,

    // Pre-processing results
    pub token_count: usize,
    pub multimodal_assets: Vec<Asset>,
    pub hydrated_prompt: Option<String>,

    // Cross-phase metadata (extracted from Envoy response headers)
    pub kv_transfer_metadata: Option<KvTransferParams>,
    pub encode_embeddings: Option<Vec<u8>>,
    pub prefill_endpoint_used: Option<String>,
    last_selected_endpoint: Option<String>,

    // Intermediate phase response bodies
    phase_response_body: Option<Bytes>,

    // Observability
    start_time: Instant,
    trace_context: TraceContext,
}

impl RequestContext {
    pub fn next_phase(&self) -> Option<Phase> {
        match self.current_phase {
            Phase::Encode => Some(Phase::Prefill),
            Phase::Prefill => Some(Phase::Decode),
            Phase::Decode => None,
            Phase::Done => None,
        }
    }

    pub fn was_disaggregated(&self) -> bool {
        self.phases_completed.len() > 1
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Phase {
    Encode,
    Prefill,
    Decode,
    Done,
}

#[derive(Clone, Debug)]
pub enum ExecutionMode {
    Conditional,
    AlwaysDisaggregate,
    PoolAware,
}
```

### 11.8 Binary Structure

```rust
use pingora::prelude::*;

fn main() {
    let mut server = Server::new(Some(Opt::default())).unwrap();
    server.bootstrap();

    let config = CoordinatorConfig::load("coordinator.yaml").unwrap();

    let pool_signals = Arc::new(PoolSignalCache::new(&config));
    let decider = build_decider(&config);

    let signals_clone = pool_signals.clone();
    tokio::spawn(async move {
        signals_clone.refresh_loop(Duration::from_secs(5)).await;
    });

    let coordinator = CoordinatorProxy {
        decider,
        gateway_addr: config.envoy_gateway_addr,
        pool_signals,
        preprocessors: build_preprocessors(&config),
    };

    let mut proxy_service = http_proxy_service(
        &server.configuration,
        coordinator,
    );
    proxy_service.add_tcp("0.0.0.0:8080");

    let metrics_service = metrics_server(&server.configuration);

    server.add_service(proxy_service);
    server.add_service(metrics_service);
    server.run_forever();
}
```

---

## 12. Open Questions

1. **ext_proc response semantics** - What headers/status codes should the EPP set (via ext_proc) to communicate metadata back to the Coordinator through Envoy? How does the consult-planner signal disaggregation (HTTP status code vs. custom header)?

2. **Aggregated signals delivery** - How does the Coordinator get pool-level saturation and capacity signals? Options: Prometheus scraping, EPP-exposed status endpoint, or K8s resource status. (Note: this is the only potential direct communication path outside the Gateway.)

3. **Partial disaggregation** - Should the system support EP/D (encode+prefill together, decode separate) or other partial splits? The solution should scale from demos to full production with minimal special-casing.

4. **Pre-processing scope** - Beyond tokenization and image prefetching, should the Coordinator handle guardrails, content filtering, or RAG retrieval? (Plugin architecture enables this but scope needs agreement.)

5. **State passing format** - Cross-phase metadata is passed via HTTP headers (set by EPP in ext_proc response, read by Coordinator from Gateway response). What are the size limits for headers? Should large payloads (e.g., embedding vectors) use request/response body fields instead?

6. **k8s Gateway API alignment** - Is full Gateway API / GW API Inference Extension (GAIE) conformance critical or nice-to-have?

7. **Cloud-provider managed gateways** - How do managed gateways (e.g., GKE Gateway, ALB) affect the architecture and available features (ext_proc support, direct routing)?

---

## 13. Glossary

| Term | Definition |
|------|-----------|
| **Coordinator** | Stateless service that orchestrates multi-phase inference pipelines |
| **EPP** | Endpoint Picker/Proxy - scrapes per-pod metrics and makes endpoint-level scheduling decisions |
| **E/P/D** | Encode/Prefill/Decode - the three phases of disaggregated inference |
| **InferencePool** | A Kubernetes resource representing a set of model server pods serving a specific phase |
| **GAIE** | Gateway API Inference Extension - Kubernetes API for inference-aware routing |
| **ext_proc** | Envoy external processing filter - allows an external service to participate in request/response processing |
| **KV Cache** | Key-Value cache used by transformer models during decoding |
| **Consult-planner** | Decider mode where the Decode EPP determines if disaggregation is needed |
| **JIT scheduling** | Just-In-Time endpoint selection at each phase (deferred, not planned upfront) |
| **Pingora** | Open-source Rust framework by Cloudflare for building programmable network services |
| **Praxis** | High-performance proxy framework built on Pingora, optimized for AI inference and cloud-native workloads (github.com/praxis-proxy/praxis) |
