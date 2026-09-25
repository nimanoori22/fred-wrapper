# fred-wrapper

`fred-wrapper` is an async Rust client for the St. Louis Fed FRED API and its
point-in-time ALFRED data. It always requests JSON, supplies your API key, and
handles rate limiting, retries, and backoff for you.

```rust,no_run
use fred_wrapper::{FredClient, FredResilienceConfig, Observations, Frequency, Units};

# async fn example() -> Result<(), fred_wrapper::FredError> {
let client = FredClient::new(
    std::env::var("FRED_API_KEY").unwrap(),
    FredResilienceConfig::default(),
)?;

let mut request = Observations::new("UNRATE");
request.frequency = Some(Frequency::Annual);
request.units = Some(Units::PercentChangeFromYearAgo);

let values = client.series().observations(request).await?;
# Ok(()) }
```

## Design

`FredClient` owns the HTTP configuration, authentication, and resilience
policy. It's cheap to `Clone`: every clone shares the same rate limiter and
concurrency limit, so you can hand clones to as many concurrent tasks as you
like without exceeding FRED's request budget.

Resource accessors (`series`, `category`, `release`, `source`, `tags`, and
`geofred`) keep the endpoint surface grouped like the upstream documentation.
Core series calls are strongly typed; `raw` is available on every accessor for
the less common documented endpoints and deserializes directly into a
caller-supplied response type. This keeps uncommon FRED response shapes usable
without forcing JSON values on every caller.

`Observation::value` is `Option<f64>`: FRED's `"."` missing-value marker maps
to `None`. ALFRED windows are supplied through `Realtime`, and observation
vintages use `Observations::vintage_dates`.

## Resilience

Requests go through an [`Arm`](https://github.com/nimanoori22/rustopus) that
handles rate limiting, bounded concurrency, and retries with backoff.
`FredResilienceConfig` is the FRED-specific set of knobs:

```rust
use fred_wrapper::FredResilienceConfig;
use std::time::Duration;

let resilience = FredResilienceConfig {
    requests_per_second: 2,
    max_retries: 3,
    initial_backoff: Duration::from_millis(500),
    max_backoff: Duration::from_secs(10),
    max_concurrent_requests: 4,
};
```

`FredResilienceConfig::default()` is tuned to FRED's documented limits, so
`FredClient::new(api_key, FredResilienceConfig::default())` is a reasonable
starting point.

Retry behavior is FRED-specific and not just "retry everything":

| Response                                     | Behavior                                                  |
|-----------------------------------------------|-------------------------------------------------------------|
| Timeout / connect / body-read errors          | Retried with backoff                                        |
| `429` (rate limited)                          | Retried, honoring `Retry-After` when FRED sends one          |
| `500`–`504`                                   | One shallow retry, then fails and logs at `error` level     |
| `400`, `401`, `403`, `404`                    | Fails immediately, no retry, logs at `error` level          |

The reasoning: rate limits and network blips are transient and worth retrying;
a client-side error like a bad `series_id` or an invalid key isn't going to
succeed on attempt two, so retrying it only wastes your rate-limit budget and
delays surfacing the real problem. A `5xx` gets a single shallow retry since
servers occasionally have a bad moment independent of what you sent, but a
request that keeps failing is more likely hitting something structurally
wrong (an unusual date range, an edge-of-range vintage) than a transient
outage, so it doesn't consume your full retry budget.

If every retry is exhausted, or the request hits FRED's documented
"no vintage data before this window" case, that's handled without treating it
as a hard failure (see `observations_windowed` below).

### Bringing your own `reqwest::Client`

`FredClient::new` and `FredClient::with_urls` build a default
`reqwest::Client`, which respects proxy environment variables
(`HTTPS_PROXY`, `ALL_PROXY`, etc.). If you need a client configured
differently — no proxy, custom timeouts, a preconfigured proxy — use
`FredClient::with_client`:

```rust,no_run
use fred_wrapper::{FredClient, FredResilienceConfig, FRED_BASE_URL, GEOFRED_BASE_URL};

# fn example() -> Result<(), fred_wrapper::FredError> {
let http = reqwest::Client::builder().no_proxy().build().unwrap();

let client = FredClient::with_client(
    http,
    std::env::var("FRED_API_KEY").unwrap(),
    FRED_BASE_URL,
    GEOFRED_BASE_URL,
    FredResilienceConfig::default(),
)?;
# Ok(()) }
```

## Fetching large date ranges

FRED caps how many vintage dates a single ALFRED request can cover.
`SeriesApi::observations_windowed` splits a wide `Realtime` window into
consecutive sub-windows and streams one page per sub-window, so you can pull a
series' full history without hitting that limit:

```rust,no_run
use fred_wrapper::{FredClient, FredResilienceConfig, Observations, Realtime};
use futures::TryStreamExt;
use chrono::NaiveDate;

# async fn example() -> Result<(), fred_wrapper::FredError> {
let client = FredClient::new(
    std::env::var("FRED_API_KEY").unwrap(),
    FredResilienceConfig::default(),
)?;

let mut params = Observations::new("CPIAUCSL");
params.realtime = Realtime {
    start: NaiveDate::from_ymd_opt(1776, 7, 4),
    end: Some(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()),
};

let series = client.series();
let pages: Vec<Vec<_>> = series
    .observations_windowed(params, 5) // 5-year sub-windows
    .try_collect()
    .await?;

let all: Vec<_> = pages.into_iter().flatten().collect();
# Ok(()) }
```

Sub-windows are fetched concurrently (bounded by the arm's concurrency limit),
so this is significantly faster than paging by hand.

## Example

Run the example with `FRED_API_KEY` set:

```sh
cargo run --example unrate_yoy
```

For a multi-series example using `observations_windowed` and structured
logging, see `examples/async_indicator_fetch.rs`:

```sh
RUST_LOG=fred_wrapper=debug cargo run --example async_indicator_fetch
```