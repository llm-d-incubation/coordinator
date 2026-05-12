use praxis_filter::register_filters;
use praxis_prefill_proxy::filters;

register_filters! {
    http "noop_log"       => filters::noop_log::NoopLogFilter::from_config,
    http "prefill_decode" => filters::prefill_decode::PrefillDecodeFilter::from_config,
}

fn main() {
    let registry = custom_registry();
    let config = praxis::load_config(None).unwrap_or_else(|e| praxis::fatal(&e));
    praxis::init_tracing(&config).unwrap_or_else(|e| praxis::fatal(&e));
    praxis::run_server_with_registry(config, registry, None)
}
