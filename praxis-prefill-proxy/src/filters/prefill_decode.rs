use async_trait::async_trait;
use reqwest::Client;
use praxis_filter::{FilterAction, HttpFilter, FilterError, HttpFilterContext, Rejection};

pub struct PrefillDecodeFilter {
    client: Client,
}

impl PrefillDecodeFilter {
    pub fn from_config(
        _config: &serde_yaml::Value,
    ) -> Result<Box<dyn HttpFilter>, FilterError> {
        let client = Client::builder().build()?;
        Ok(Box::new(Self { client }))
    }
}

#[async_trait]
impl HttpFilter for PrefillDecodeFilter {
    fn name(&self) -> &'static str {
        "prefill_decode"
    }

    async fn on_request(
        &self,
        ctx: &mut HttpFilterContext<'_>,
    ) -> Result<FilterAction, FilterError> {
        let Some(host) = ctx
            .request
            .headers
            .get("host")
            .and_then(|v| v.to_str().ok())
        else {
            tracing::warn!("prefill: rejecting request, missing or non-UTF-8 Host header");
            return Ok(FilterAction::Reject(Rejection::status(400)));
        };
        let host = host.to_owned();

        let path_and_query = ctx.request.uri
            .path_and_query()
            .map(|pq| pq.as_str().to_owned())
            .unwrap_or_else(|| ctx.request.uri.path().to_owned());
        let prefill_url = format!("http://{host}/prefill{path_and_query}");

        tracing::info!(url = %prefill_url, "prefill: sending sub-request");

        let method = reqwest::Method::from_bytes(ctx.request.method.as_str().as_bytes())?;
        let mut req_builder = self.client.request(method, &prefill_url);

        for (name, value) in &ctx.request.headers {
            if name.as_str().eq_ignore_ascii_case("host") {
                continue;
            }
            req_builder = req_builder.header(name.as_str(), value.as_bytes());
        }

        let response = req_builder.send().await?;
        let status = response.status();
        tracing::info!(status = %status, "prefill: sub-request complete");

        if !status.is_success() {
            return Ok(FilterAction::Reject(Rejection::status(status.as_u16())));
        }

        ctx.rewritten_path = Some(format!("/decode{path_and_query}"));
        Ok(FilterAction::Continue)
    }
}
