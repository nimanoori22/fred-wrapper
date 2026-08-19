# fred-rs — Plan

A Rust wrapper for the St. Louis Fed FRED API (and its point-in-time
counterpart, ALFRED), covering every documented endpoint with strongly-typed
request parameters and response structs.

Standalone crate, decoupled from the macro-FX platform so it can be reused
and potentially published independently. Depends on the shared
`api-resilience-crate` for retry/rate-limiting rather than implementing its
own — see §6.

## 1. Project Initialization & Dependencies

- **HTTP client:** `reqwest` (async).
- **Serialization:** `serde` + `serde_json` for mapping API responses to
  Rust structs.
- **Date/time:** `chrono`, for the `YYYY-MM-DD` strings used in
  `realtime_start`, `realtime_end`, `observation_start`, `observation_end`,
  and `vintage_dates`.
- **Query strings:** `serde_qs` or manual URL construction — needed for
  parameters like semicolon-delimited tag lists that don't map cleanly to
  simple `serde_urlencoded` output.
- **Resilience:** `api-resilience-crate` (internal, shared across all API
  wrapper crates) — see §6.

## 2. Core Infrastructure

### Client struct & authentication

- `FredClient` struct holding the 32-character alphanumeric `api_key`.
- Base URL constant: `https://api.stlouisfed.org/fred/` (GeoFRED endpoints
  live under a separate `geofred/` path — see §5).
- Every request always appends `file_type=json` for consistent parsing into
  Rust structs (FRED also supports XML; we never use it).

### Error handling

- Custom `FredError` enum, mapping:
  - HTTP status codes FRED returns: `400` (bad request), `404` (not
    found), `423` (locked — API key issue), `429` (rate limited), `500`
    (server error).
  - FRED's own error response body shape: `{"error_code": ..., "error_message": ...}`,
    parsed into a `FredApiError { code: u16, message: String }` variant
    distinct from transport-level errors.
- `429` handling is delegated to `api-resilience-crate` (see §6) rather than
  handled ad hoc in this crate.

## 3. Data Models (serde structs)

- **`Category`** — `id`, `name`, `parent_id`.
- **`Series`** — `id`, `title`, `observation_start`, `observation_end`,
  `frequency`, `units`, `seasonal_adjustment`, `last_updated`, `popularity`.
- **`Observation`** — `date`, `value`, `realtime_start`, `realtime_end`.
  Note: `value` needs a custom deserializer — FRED returns `"."` (a literal
  dot) for missing observations rather than `null` or omitting the field,
  so this must deserialize into `Option<f64>` with a `"."` → `None` mapping,
  not a plain `f64`.
- **`Release`**, **`Source`**, **`Tag`** — corresponding metadata structs
  per §5.

## 4. Parameter Enums

Fixed-set string parameters get real Rust enums (with `serde(rename_all)`
or explicit `#[serde(rename = "...")]` matching FRED's exact casing) rather
than raw strings, so invalid values are caught at compile time:

- **`Frequency`** — `D`, `W`, `BW`, `M`, `Q`, `SA`, `A` (and their
  end-of-period variants where FRED distinguishes them).
- **`AggregationMethod`** — `Avg`, `Sum`, `Eop`.
- **`Units`** (transformation) — `Lin`, `Chg`, `Ch1`, `Pch`, `Pc1`, `Pca`,
  `Cch`, `Cca`, `Log`.
- **`Seasonality`** — `SA`, `NSA`, `SSA`, `SAAR`, `NSAAR`.
- **`SortOrder`** — `Asc`, `Desc`.
- **`OutputType`** — `1`–`4` (real-time observations, real-time vintage,
  vintage-all, initial-release-only — confirm exact FRED semantics per
  value against docs during implementation).
- **`SearchType`** — `FullText`, `SeriesId`.

## 5. Endpoint Implementation

Organized into modules mirroring FRED's own documentation hierarchy —
one module per resource type, each returning the corresponding struct(s)
from §3.

### `fred::category`
- `fred/category` — metadata for a single category.
  *Params:* `category_id` (optional, default `0`). *Returns:* `id`, `name`, `parent_id`.
- `fred/category/children` — child categories of a parent.
  *Params:* `category_id`, `realtime_start`, `realtime_end`.
- `fred/category/related` — categories related via one-way, non-parental links.
  *Params:* `category_id` (required), `realtime_start`, `realtime_end`.
- `fred/category/series` — series within a category.
  *Params:* `category_id` (required), `filter_variable`, `filter_value`,
  `tag_names`, `exclude_tag_names`, `order_by`, `sort_order`, `limit`, `offset`.
- `fred/category/tags` — tags associated with series in a category.
  *Params:* `category_id`, `tag_names`, `tag_group_id`, `search_text`, + standard filters.
- `fred/category/related_tags` — related tags within a category.
  *Params:* `category_id`, `tag_names` (required), `exclude_tag_names`,
  `tag_group_id`, `search_text`.

### `fred::release`
- `fred/releases` — all data releases. *Params:* standard pagination + real-time filters.
- `fred/releases/dates` — release dates across all releases.
  *Params:* `include_release_dates_with_no_data`.
- `fred/release` — metadata for one release.
- `fred/release/dates` — dates for one release.
- `fred/release/series` — series for a release.
  *Params:* `release_id`, plus the same category-style series filtering.
- `fred/release/sources` — agencies for a release.
  *Params:* `release_id`, `realtime_start`, `realtime_end`.
- `fred/release/tags` — tags for a release.
- `fred/release/related_tags` — related tags within a release.
- `fred/release/tables` — hierarchical release table trees.
  *Params:* `release_id`, `element_id`, `include_observation_values`, `observation_date`.

### `fred::series` (the core module)
- `fred/series` — metadata for a series.
  *Params:* `series_id`, `realtime_start`, `realtime_end`.
- `fred/series/categories` — categories a series is assigned to.
- `fred/series/observations` — **the data endpoint**.
  *Params:* `series_id`, `observation_start`, `observation_end`, `units`
  (transformation), `frequency` (aggregation), `aggregation_method`,
  `output_type` (1–4), `vintage_dates`.
- `fred/series/release` — the release a series belongs to.
- `fred/series/search` — keyword search.
  *Params:* `search_text`, `search_type` (`full_text` | `series_id`), + extensive filtering.
- `fred/series/search/tags` — tags found in a search result.
- `fred/series/search/related_tags` — related tags for a series search.
- `fred/series/tags` — tags for a specific series.
- `fred/series/updates` — series updated in the last two weeks.
  *Params:* `filter_value` (`macro` | `regional` | `all`), `start_time`, `end_time`.
- `fred/series/vintagedates` — revision history dates.

### `fred::source`
- `fred/sources` — all sources.
- `fred/source` — metadata for one source.
- `fred/source/releases` — releases from a source.

### `fred::tags`
- `fred/tags` — fetch/search all tags. *Params:* `tag_names`, `tag_group_id`, `search_text`.
- `fred/related_tags` — tags related to other tags.
- `fred/tags/series` — series matching specific tags.

### `fred::geofred` (Maps API — separate module, separate base path)
- `geofred/regional/data` — cross-section of regional data for a series group.
  *Params:* `series_group`, `region_type` (`state`, `county`, etc.), `date`,
  `units`, `transformation`, `frequency`, `aggregation_method`.
- `geofred/series/data` — regional data for a specific series + release date.
- `geofred/series/group` — metadata (min/max date, group ID) for a GeoFRED-enabled series.
- `geofred/shapes/file` — GeoJSON shape files.
  *Params:* `shape` (`bea`, `msa`, `frb`, `state`, `country`, etc.).

## 6. Resilience — retry & rate limiting

FRED enforces standard API rate limits and returns `429` on excess.
Per the platform's shared-infrastructure decision, `fred-rs` does **not**
implement its own retry/backoff/rate-limit logic — it depends on
`api-resilience-crate` and implements whatever trait that crate defines for
per-request retry policy and rate-limit backoff. This keeps `fred-rs`
focused purely on request-shape/response-shape correctness, and means any
improvement to retry behavior benefits every wrapper crate at once, not
just this one.

## 7. Advanced Features

- **Real-time periods (ALFRED mode):** `realtime_start` / `realtime_end`
  supported across every applicable endpoint, to enable point-in-time data
  retrieval — this is the whole reason ALFRED is used over plain FRED in
  the macro-FX platform (avoiding look-ahead bias from data revisions).
- **Vintage dates:** support passing a comma-separated `vintage_dates` list
  to `fred/series/observations`.
- **Output type:** support `output_type` 1–4 on `fred/series/observations`
  for the different real-time/vintage observation modes.

## 8. Documentation & Testing

- **Integration tests** per major endpoint module, run against a real test
  API key (gate behind an env var so `cargo test` doesn't fail for
  contributors without one).
- **Unit tests** for the `Observation.value` `"."` → `None` deserialization
  edge case specifically, since it's the one place FRED's JSON shape
  doesn't map naturally onto a typed field.
- **Example (`examples/unrate_yoy.rs`):** fetch `UNRATE` observations,
  aggregate to annual frequency, and transform to "Percent Change from
  Year Ago" — demonstrates `frequency` + `units` transformation together
  in one realistic call.

## Suggested build order

1. `FredClient` + base URL + `file_type=json` + `FredError` (§2) — nothing
   else works without this.
2. `Observation` struct with the `"."` deserializer (§3) — highest-risk
   parsing edge case, worth nailing early with a unit test.
3. `fred::series` module (§5) — the module the macro-FX platform actually
   needs first (`fred/series`, `fred/series/observations`).
4. Wire in `api-resilience-crate` (§6) once the basic request path works
   end-to-end without it, so retry/backoff is validated against a real
   working client rather than mocked from day one.
5. Remaining modules (`category`, `release`, `source`, `tags`, `geofred`)
   in roughly that priority order — `geofred` last, since it's the least
   likely to be needed by the macro-FX platform's current signal layers.
6. Integration tests + the `unrate_yoy` example, once enough of the
   surface exists to make them meaningful.