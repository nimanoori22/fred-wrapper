# fred-wrapper

`fred-wrapper` is an async Rust client for the St. Louis Fed FRED API and its
point-in-time ALFRED data. It always requests JSON and supplies your API key.

```rust,no_run
use fred_wrapper::{FredClient, Observations, Frequency, Units};

# async fn example() -> Result<(), fred_wrapper::FredError> {
let client = FredClient::new(std::env::var("FRED_API_KEY")?)?;
let mut request = Observations::new("UNRATE");
request.frequency = Some(Frequency::Annual);
request.units = Some(Units::PercentChangeFromYearAgo);
let values = client.series().observations(request).await?;
# Ok(()) }
```

## Design

`FredClient` owns the HTTP configuration and authentication. Resource
accessors (`series`, `category`, `release`, `source`, `tags`, and `geofred`)
keep the endpoint surface grouped like the upstream documentation. Core series
calls are strongly typed; `raw` is available on every accessor for the less
common documented endpoints and deserializes directly into a caller-supplied
response type. This keeps uncommon FRED response shapes usable without forcing
JSON values on every caller.

`Observation::value` is `Option<f64>`: FRED's `"."` missing-value marker maps
to `None`. ALFRED windows are supplied through `Realtime`, and observation
vintages use `Observations::vintage_dates`.

Run the example with `FRED_API_KEY` set:

```sh
cargo run --example unrate_yoy
```
