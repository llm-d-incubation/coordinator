use async_trait::async_trait;
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext};

pub struct NoopLogFilter;

impl NoopLogFilter {
    pub fn from_config(
        _config: &serde_yaml::Value,
    ) -> Result<Box<dyn HttpFilter>, FilterError> {
        Ok(Box::new(Self))
    }
}

#[async_trait]
impl HttpFilter for NoopLogFilter {
    fn name(&self) -> &'static str {
        "noop_log"
    }

    async fn on_request(
        &self,
        ctx: &mut HttpFilterContext<'_>,
    ) -> Result<FilterAction, FilterError> {
        tracing::info!(
            method = %ctx.request.method,
            path = ctx.request.uri.path(),
            "noop: received request",
        );
        Ok(FilterAction::Continue)
    }
}
