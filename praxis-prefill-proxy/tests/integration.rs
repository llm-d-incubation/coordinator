#![allow(clippy::unwrap_used, reason = "tests")]

mod helpers {
    use std::{collections::HashMap, time::Instant};
    use praxis_filter::{BodyMode, HttpFilterContext, Request};

    pub fn make_request(method: http::Method, path: &str) -> Request {
        Request {
            method,
            uri: path.parse().unwrap(),
            headers: http::HeaderMap::new(),
        }
    }

    pub fn make_ctx(req: &Request) -> HttpFilterContext<'_> {
        HttpFilterContext {
            body_done_indices: Vec::new(),
            branch_iterations: HashMap::new(),
            client_addr: None,
            executed_filter_indices: Vec::new(),
            cluster: None,
            extra_request_headers: Vec::new(),
            filter_metadata: HashMap::new(),
            filter_results: HashMap::new(),
            health_registry: None,
            kv_stores: None,
            request: req,
            request_body_bytes: 0,
            request_body_mode: BodyMode::Stream,
            request_start: Instant::now(),
            response_body_bytes: 0,
            response_body_mode: BodyMode::Stream,
            response_header: None,
            response_headers_modified: false,
            selected_endpoint_index: None,
            rewritten_path: None,
            upstream: None,
        }
    }
}

mod noop_log {
    use praxis_filter::FilterAction;
    use praxis_prefill_proxy::filters::noop_log::NoopLogFilter;
    use super::helpers::{make_ctx, make_request};

    #[tokio::test]
    async fn from_config_succeeds() {
        let result = NoopLogFilter::from_config(&serde_yaml::Value::Null);
        assert!(result.is_ok(), "from_config should succeed with null config");
    }

    #[tokio::test]
    async fn on_request_returns_continue() {
        let filter = NoopLogFilter::from_config(&serde_yaml::Value::Null).unwrap();
        let req = make_request(http::Method::GET, "/api/test");
        let mut ctx = make_ctx(&req);
        let action = filter.on_request(&mut ctx).await.unwrap();
        assert!(matches!(action, FilterAction::Continue));
    }
}

mod prefill_decode {
    use praxis_filter::FilterAction;
    use praxis_prefill_proxy::filters::prefill_decode::PrefillDecodeFilter;
    use super::helpers::{make_ctx, make_request};

    #[tokio::test]
    async fn from_config_succeeds() {
        let result = PrefillDecodeFilter::from_config(&serde_yaml::Value::Null);
        assert!(result.is_ok(), "from_config should succeed with null config");
    }

    #[tokio::test]
    async fn missing_host_header_returns_400() {
        // No Host header in request headers → filter must reject with 400.
        let filter = PrefillDecodeFilter::from_config(&serde_yaml::Value::Null).unwrap();
        let req = make_request(http::Method::POST, "/api/generate");
        // req.headers is empty — no Host header
        let mut ctx = make_ctx(&req);

        let action = filter.on_request(&mut ctx).await.unwrap();

        assert!(
            matches!(action, FilterAction::Reject(ref r) if r.status == 400),
            "expected Reject(400), got {action:?}",
        );
    }

    // The happy-path (host present, prefill returns 2xx → FilterAction::Continue with
    // ctx.rewritten_path = "/decode{original_path}") requires a running HTTP server and
    // is covered by proxy_integration::prefill_then_decode_happy_path (Task 5).
}

mod proxy_integration {
    use std::sync::Arc;

    use praxis_core::config::Config;
    use praxis_filter::{FilterFactory, FilterRegistry};
    use praxis_test_utils::{
        free_port, http_get, start_proxy_with_registry, start_uri_echo_backend,
    };
    use praxis_prefill_proxy::filters::{
        noop_log::NoopLogFilter, prefill_decode::PrefillDecodeFilter,
    };

    fn build_registry() -> FilterRegistry {
        let mut registry = FilterRegistry::with_builtins();
        registry
            .register(
                "noop_log",
                FilterFactory::Http(Arc::new(NoopLogFilter::from_config)),
            )
            .unwrap();
        registry
            .register(
                "prefill_decode",
                FilterFactory::Http(Arc::new(PrefillDecodeFilter::from_config)),
            )
            .unwrap();
        registry
    }

    fn proxy_yaml(proxy_port: u16, backend_port: u16) -> String {
        format!(
            r#"
listeners:
  - name: main
    address: "127.0.0.1:{proxy_port}"
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
              - "127.0.0.1:{backend_port}"
"#
        )
    }

    #[test]
    fn prefill_then_decode_happy_path() {
        // Backend echoes the request URI path as the response body.
        // It will receive:
        //   1) GET /prefill/api/test  (from PrefillDecodeFilter sub-request)
        //   2) GET /decode/api/test   (from proxy pipeline after rewrite)
        // The client sees the response from request #2.
        let backend_port = start_uri_echo_backend();
        let proxy_port = free_port();

        let config = Config::from_yaml(&proxy_yaml(proxy_port, backend_port)).unwrap();
        let registry = build_registry();
        let _proxy = start_proxy_with_registry(&config, &registry);

        // Client sends Host: 127.0.0.1:{backend_port} so the filter's
        // prefill sub-request reaches the same backend.
        let host = format!("127.0.0.1:{backend_port}");
        let (status, body) = http_get(
            &format!("127.0.0.1:{proxy_port}"),
            "/api/test",
            Some(&host),
        );

        assert_eq!(status, 200, "proxy should return 200");
        assert_eq!(body, "/decode/api/test", "response body should be the decode path");
    }

    #[test]
    fn prefill_failure_propagates_status() {
        // Backend always returns 503. The PrefillDecodeFilter sees 503
        // from the prefill sub-request and rejects with 503 before
        // the decode path is ever attempted.
        use praxis_test_utils::Backend;
        let backend_port = Backend::status(503, "service unavailable").start();
        let proxy_port = free_port();

        let config = Config::from_yaml(&proxy_yaml(proxy_port, backend_port)).unwrap();
        let registry = build_registry();
        let _proxy = start_proxy_with_registry(&config, &registry);

        let host = format!("127.0.0.1:{backend_port}");
        let (status, _body) = http_get(
            &format!("127.0.0.1:{proxy_port}"),
            "/api/test",
            Some(&host),
        );

        assert_eq!(status, 503, "proxy should propagate the 503 from the prefill endpoint");
    }
}
