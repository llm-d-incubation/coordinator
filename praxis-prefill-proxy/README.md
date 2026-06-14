# praxis-prefill-proxy

A standalone Rust proxy for disaggregated AI inference. Incoming requests are first sent to a **prefill** node (to warm the KV cache), then forwarded to a **decode** node (to generate tokens).

## How It Works

Every request passes through two filters:

1. **noop_log** — logs the method and path, passes through
2. **prefill_decode** — sends a sub-request to `/prefill{path}` using the `Host` header from the original request; on success rewrites the path to `/decode{path}` and forwards

```
Client  →  proxy :8080
              │
              ├─ sub-request →  backend :8081/prefill/api/generate  (KV cache warm)
              │                 ← 200 OK
              │
              └─ forward    →  backend :8081/decode/api/generate    (token generation)
                               ← response → Client
```

The backend must serve both `/prefill/...` and `/decode/...` paths. The proxy listener and backend address are configured in `praxis.yaml`.

## Requirements

- Rust toolchain (edition 2024, any recent stable)
- Python 3 (only for the smoke-test backend)

## Build

```bash
cargo build --release
```

The binary is placed at `target/release/praxis-prefill-proxy`.

## Running

### 1. Start the backend

The backend must handle both `/prefill/...` and `/decode/...` requests. For a quick smoke test, use the included Python server:

```bash
python3 test_server.py
# Backend listening on 127.0.0.1:8081
```

For production, point `praxis.yaml`'s `endpoints` at your actual inference server.

### 2. Start the proxy

```bash
cargo run --release
# or: ./target/release/praxis-prefill-proxy
```

The proxy reads `praxis.yaml` from the current working directory and listens on `127.0.0.1:8080`.

### 3. Send a request

The `Host` header controls where the prefill sub-request is sent. Set it to the backend's address:

```bash
curl -H "Host: 127.0.0.1:8081" http://127.0.0.1:8080/api/generate
```

The proxy will:
1. Log `noop: received request method=GET path=/api/generate`
2. Send `GET http://127.0.0.1:8081/prefill/api/generate`
3. On 200, forward the request to `127.0.0.1:8081/decode/api/generate`
4. Return the decode response to the client

Query parameters are preserved: `/api/generate?stream=true` becomes `/prefill/api/generate?stream=true` and `/decode/api/generate?stream=true`.

## Configuration

Edit `praxis.yaml` to change the listener address or backend endpoints:

```yaml
listeners:
  - name: main
    address: "127.0.0.1:8080"   # proxy listens here
    filter_chains: [main]

filter_chains:
  - name: main
    filters:
      - filter: noop_log
      - filter: prefill_decode
      - filter: router
        routes:
          - path_prefix: "/"
            cluster: backend
      - filter: load_balancer
        clusters:
          - name: backend
            endpoints:
              - "127.0.0.1:8081"   # your inference backend
```

## Error Behavior

| Situation | Client receives |
|-----------|----------------|
| `Host` header missing | `400 Bad Request` |
| Prefill endpoint unreachable | `502 Bad Gateway` |
| Prefill returns non-2xx | Same status code (e.g. `503`) |
| Prefill returns 2xx | Decode response |

## Tests

```bash
cargo test
```

Six tests run:
- `noop_log::from_config_succeeds` — filter constructs without error
- `noop_log::on_request_returns_continue` — filter passes requests through
- `prefill_decode::from_config_succeeds` — filter constructs without error
- `prefill_decode::missing_host_header_returns_400` — missing Host header is rejected
- `proxy_integration::prefill_then_decode_happy_path` — full pipeline with echo backend
- `proxy_integration::prefill_failure_propagates_status` — 503 from prefill reaches client
