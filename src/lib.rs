//! An asynchronous, strongly typed client for the [FRED] and ALFRED APIs.
//!
//! Every request adds `file_type=json` and authenticates with the API key
//! supplied to [`FredClient::new`]. Build endpoint-specific parameters with
//! the types in [`params`], then call the corresponding resource accessor.
//!
//! [FRED]: https://fred.stlouisfed.org/docs/api/fred/

use std::collections::BTreeMap;

use chrono::NaiveDate;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use url::Url;

/// The default FRED API base URL.
pub const FRED_BASE_URL: &str = "https://api.stlouisfed.org/fred/";
/// The GeoFRED API base URL.
pub const GEOFRED_BASE_URL: &str = "https://api.stlouisfed.org/geofred/";

/// An asynchronous FRED/ALFRED API client.
#[derive(Clone, Debug)]
pub struct FredClient {
    http: Client,
    api_key: String,
    fred_base: Url,
    geofred_base: Url,
}

impl FredClient {
    /// Creates a client with the production FRED and GeoFRED endpoints.
    ///
    /// The API key is checked for FRED's 32-character alphanumeric format.
    pub fn new(api_key: impl Into<String>) -> Result<Self, FredError> {
        let api_key = api_key.into();
        if api_key.len() != 32 || !api_key.bytes().all(|c| c.is_ascii_alphanumeric()) {
            return Err(FredError::InvalidApiKey);
        }
        Self::with_urls(api_key, FRED_BASE_URL, GEOFRED_BASE_URL)
    }

    /// Creates a client against custom bases. Useful for tests and proxies.
    pub fn with_urls(
        api_key: impl Into<String>,
        fred_base: &str,
        geofred_base: &str,
    ) -> Result<Self, FredError> {
        Ok(Self {
            http: Client::new(),
            api_key: api_key.into(),
            fred_base: Url::parse(fred_base)?,
            geofred_base: Url::parse(geofred_base)?,
        })
    }

    /// Access FRED category endpoints.
    pub fn category(&self) -> CategoryApi<'_> {
        CategoryApi(self)
    }
    /// Access FRED release endpoints.
    pub fn release(&self) -> ReleaseApi<'_> {
        ReleaseApi(self)
    }
    /// Access FRED series endpoints.
    pub fn series(&self) -> SeriesApi<'_> {
        SeriesApi(self)
    }
    /// Access FRED source endpoints.
    pub fn source(&self) -> SourceApi<'_> {
        SourceApi(self)
    }
    /// Access FRED tag endpoints.
    pub fn tags(&self) -> TagsApi<'_> {
        TagsApi(self)
    }
    /// Access GeoFRED map endpoints.
    pub fn geofred(&self) -> GeoFredApi<'_> {
        GeoFredApi(self)
    }

    async fn get<T: DeserializeOwned>(
        &self,
        geo: bool,
        endpoint: &str,
        query: Query,
    ) -> Result<T, FredError> {
        let mut url = (if geo {
            &self.geofred_base
        } else {
            &self.fred_base
        })
        .join(endpoint)?;
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("api_key", &self.api_key);
        pairs.append_pair("file_type", "json");
        for (key, value) in query.0 {
            pairs.append_pair(&key, &value);
        }
        drop(pairs);
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(FredError::Transport)?;
        let status = response.status();
        let body = response.text().await.map_err(FredError::Transport)?;
        if !status.is_success() {
            if let Ok(error) = serde_json::from_str::<FredApiError>(&body) {
                return Err(FredError::Api(error));
            }
            return Err(FredError::Http { status, body });
        }
        serde_json::from_str(&body).map_err(FredError::Decode)
    }

    async fn collection<T: DeserializeOwned>(
        &self,
        geo: bool,
        endpoint: &str,
        query: Query,
        field: &str,
    ) -> Result<Vec<T>, FredError> {
        self.get::<List<T>>(geo, endpoint, query)
            .await?
            .into_items(field)
    }
}

/// Errors returned by this crate.
#[derive(Debug, Error)]
pub enum FredError {
    #[error("the FRED API key must contain exactly 32 alphanumeric characters")]
    InvalidApiKey,
    #[error("invalid API URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("HTTP transport failed: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("FRED API error: {0:?}")]
    Api(FredApiError),
    #[error("FRED returned HTTP {status}: {body}")]
    Http { status: StatusCode, body: String },
    #[error("could not decode FRED response: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("FRED returned no {resource} records for a request that requires one")]
    EmptyResponse { resource: &'static str },
}

/// Error body returned by FRED for invalid API requests.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct FredApiError {
    pub error_code: u16,
    pub error_message: String,
}
impl FredApiError {
    pub fn code(&self) -> u16 {
        self.error_code
    }
    pub fn message(&self) -> &str {
        &self.error_message
    }
}

/// Common data models returned by FRED.
pub mod models {
    use super::*;
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct Category {
        pub id: u64,
        pub name: String,
        pub parent_id: Option<u64>,
    }
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct Series {
        pub id: String,
        pub title: String,
        pub observation_start: Option<NaiveDate>,
        pub observation_end: Option<NaiveDate>,
        pub frequency: String,
        pub units: String,
        pub seasonal_adjustment: String,
        pub last_updated: String,
        pub popularity: u32,
    }
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct Observation {
        pub realtime_start: NaiveDate,
        pub realtime_end: NaiveDate,
        pub date: NaiveDate,
        #[serde(deserialize_with = "missing_value")]
        pub value: Option<f64>,
    }
    fn missing_value<'de, D>(d: D) -> Result<Option<f64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = Option::<String>::deserialize(d)?;
        match v.as_deref() {
            None | Some(".") => Ok(None),
            Some(v) => v.parse().map(Some).map_err(serde::de::Error::custom),
        }
    }
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct Release {
        pub id: u64,
        pub name: String,
        pub press_release: Option<bool>,
        pub link: Option<String>,
    }
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct Source {
        pub id: u64,
        pub name: String,
        pub link: Option<String>,
    }
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct Tag {
        pub name: String,
        pub group_id: String,
        pub notes: Option<String>,
        pub created: Option<String>,
        pub popularity: Option<u32>,
        pub series_count: Option<u64>,
    }
    #[derive(Clone, Debug, Deserialize, Serialize)]
    pub struct ReleaseDate {
        pub release_id: u64,
        pub release_name: Option<String>,
        pub date: NaiveDate,
    }
}
pub use models::*;

/// Typed request parameters and fixed-set FRED values.
pub mod params {
    use super::*;
    macro_rules! string_enum { ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum $name { $($variant),+ }
        impl $name { pub fn as_str(self) -> &'static str { match self { $(Self::$variant => $value),+ } } }
    }; }
    
    string_enum!(Frequency { 
        Daily=>"d", 
        Weekly=>"w", 
        Biweekly=>"bw", 
        Monthly=>"m", 
        Quarterly=>"q", 
        Semiannual=>"sa", 
        Annual=>"a", 
        WeeklyEndingFriday=>"wef",
        WeeklyEndingThursday=>"weth",
        WeeklyEndingWednesday=>"wew",
        WeeklyEndingTuesday=>"wetu",
        WeeklyEndingMonday=>"wem",
        WeeklyEndingSunday=>"wesu",
        WeeklyEndingSaturday=>"wesa",
        BiweeklyEndingWednesday=>"bwew",
        BiweeklyEndingMonday=>"bwem"
    });

    string_enum!(AggregationMethod { Average=>"avg", Sum=>"sum", EndOfPeriod=>"eop" });
    
    string_enum!(Units { 
        Levels=>"lin", 
        Change=>"chg", 
        ChangeFromYearAgo=>"ch1", 
        PercentChange=>"pch", 
        PercentChangeFromYearAgo=>"pc1", 
        CompoundedAnnualRate=>"pca", 
        ContinuouslyCompoundedChange=>"cch", 
        ContinuouslyCompoundedAnnualRate=>"cca", 
        NaturalLog=>"log" 
    });

    string_enum!(Seasonality { SeasonallyAdjusted=>"sa", NotSeasonallyAdjusted=>"nsa", SeasonallyAdjustedAnnualRate=>"saar", NotSeasonallyAdjustedAnnualRate=>"nsaar" });
    
    string_enum!(SortOrder { Ascending=>"asc", Descending=>"desc" });
    
    string_enum!(OutputType {
        ObservationsByRealtimePeriod=>"1",
        ObservationsByVintageDateAll=>"2",
        ObservationsByVintageDateNewAndRevised=>"3",
        InitialReleaseOnly=>"4"
    });
    
    string_enum!(SearchType { FullText=>"full_text", SeriesId=>"series_id" });
    
    /// Parameters shared by most list endpoints.
    #[derive(Clone, Debug, Default)]
    pub struct Pagination {
        pub limit: Option<u32>,
        pub offset: Option<u32>,
        pub sort_order: Option<SortOrder>,
    }
    /// Common real-time and pagination parameters for collection endpoints.
    #[derive(Clone, Debug, Default)]
    pub struct ListOptions {
        pub realtime: Realtime,
        pub pagination: Pagination,
    }
    /// ALFRED real-time window.
    #[derive(Clone, Debug, Default)]
    pub struct Realtime {
        pub start: Option<NaiveDate>,
        pub end: Option<NaiveDate>,
    }
    /// Parameters for [`SeriesApi::observations`](crate::SeriesApi::observations).
    #[derive(Clone, Debug)]
    pub struct Observations {
        pub series_id: String,
        pub realtime: Realtime,
        pub observation_start: Option<NaiveDate>,
        pub observation_end: Option<NaiveDate>,
        pub units: Option<Units>,
        pub frequency: Option<Frequency>,
        pub aggregation_method: Option<AggregationMethod>,
        pub output_type: Option<OutputType>,
        pub vintage_dates: Vec<NaiveDate>,
        pub limit: Option<u32>,
        pub offset: Option<u32>,
        pub sort_order: Option<SortOrder>,
    }
    impl Observations {
        pub fn new(series_id: impl Into<String>) -> Self {
            Self {
                series_id: series_id.into(),
                realtime: Realtime::default(),
                observation_start: None,
                observation_end: None,
                units: None,
                frequency: None,
                aggregation_method: None,
                output_type: None,
                vintage_dates: vec![],
                limit: None,
                offset: None,
                sort_order: None,
            }
        }
    }
    /// Parameters for series search and tag-search endpoints.
    #[derive(Clone, Debug)]
    pub struct Search {
        pub search_text: String,
        pub search_type: Option<SearchType>,
        pub realtime: Realtime,
        pub pagination: Pagination,
    }
    impl Search {
        pub fn new(search_text: impl Into<String>) -> Self {
            Self {
                search_text: search_text.into(),
                search_type: None,
                realtime: Realtime::default(),
                pagination: Pagination::default(),
            }
        }
    }
}
pub use params::*;

#[derive(Default)]
struct Query(BTreeMap<String, String>);
impl Query {
    fn put(&mut self, k: &str, v: impl ToString) {
        self.0.insert(k.into(), v.to_string());
    }
    fn date(&mut self, k: &str, v: Option<NaiveDate>) {
        if let Some(v) = v {
            self.put(k, v.format("%F"));
        }
    }
}
fn realtime(q: &mut Query, value: &Realtime) {
    q.date("realtime_start", value.start);
    q.date("realtime_end", value.end);
}
fn pagination(q: &mut Query, value: &Pagination) {
    if let Some(v) = value.limit {
        q.put("limit", v)
    }
    if let Some(v) = value.offset {
        q.put("offset", v)
    }
    if let Some(v) = value.sort_order {
        q.put("sort_order", v.as_str())
    }
}
fn list_options(q: &mut Query, value: &ListOptions) {
    realtime(q, &value.realtime);
    pagination(q, &value.pagination);
}

macro_rules! api {
    ($name:ident) => {
        pub struct $name<'a>(&'a FredClient);
    };
}
api!(CategoryApi);
api!(ReleaseApi);
api!(SeriesApi);
api!(SourceApi);
api!(TagsApi);
api!(GeoFredApi);

/// Response envelopes preserve FRED's pagination metadata.
#[derive(Debug, Deserialize)]
pub struct List<T> {
    pub count: Option<u64>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
    #[serde(flatten)]
    pub items: BTreeMap<String, serde_json::Value>,
    #[serde(skip)]
    _marker: std::marker::PhantomData<T>,
}
/// A response containing one FRED collection. Use [`List::into_items`] to deserialize it.
impl<T: DeserializeOwned> List<T> {
    pub fn into_items(self, name: &str) -> Result<Vec<T>, FredError> {
        let value = self
            .items
            .get(name)
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Array(vec![]));
        serde_json::from_value(value).map_err(FredError::Decode)
    }
}

impl<'a> SeriesApi<'a> {
    /// Fetches metadata for one series.
    pub async fn get(&self, series_id: &str, window: Realtime) -> Result<Series, FredError> {
        let mut q = Query::default();
        q.put("series_id", series_id);
        realtime(&mut q, &window);
        #[derive(Deserialize)]
        struct R {
            seriess: Vec<Series>,
        }
        self.0
            .get::<R>(false, "series", q)
            .await?
            .seriess
            .into_iter()
            .next()
            .ok_or(FredError::EmptyResponse { resource: "series" })
    }
    /// Fetches observations, including ALFRED vintage data when requested.
    pub async fn observations(&self, p: Observations) -> Result<Vec<Observation>, FredError> {
        let mut q = Query::default();
        q.put("series_id", p.series_id);
        realtime(&mut q, &p.realtime);
        q.date("observation_start", p.observation_start);
        q.date("observation_end", p.observation_end);
        if let Some(v) = p.units {
            q.put("units", v.as_str())
        }
        if let Some(v) = p.frequency {
            q.put("frequency", v.as_str())
        }
        if let Some(v) = p.aggregation_method {
            q.put("aggregation_method", v.as_str())
        }
        if let Some(v) = p.output_type {
            q.put("output_type", v.as_str())
        }
        if !p.vintage_dates.is_empty() {
            q.put(
                "vintage_dates",
                p.vintage_dates
                    .iter()
                    .map(|d| d.format("%F").to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        }
        pagination(
            &mut q,
            &Pagination {
                limit: p.limit,
                offset: p.offset,
                sort_order: p.sort_order,
            },
        );
        #[derive(Deserialize)]
        struct R {
            observations: Vec<Observation>,
        }
        Ok(self
            .0
            .get::<R>(false, "series/observations", q)
            .await?
            .observations)
    }
    /// Searches the FRED series catalogue.
    pub async fn search(&self, p: Search) -> Result<Vec<Series>, FredError> {
        let mut q = Query::default();
        q.put("search_text", p.search_text);
        if let Some(v) = p.search_type {
            q.put("search_type", v.as_str())
        }
        realtime(&mut q, &p.realtime);
        pagination(&mut q, &p.pagination);
        #[derive(Deserialize)]
        struct R {
            seriess: Vec<Series>,
        }
        Ok(self.0.get::<R>(false, "series/search", q).await?.seriess)
    }
    pub async fn categories(&self, series_id: &str, window: Realtime) -> Result<Vec<Category>, FredError> {
        let mut q = Query::default(); q.put("series_id", series_id); realtime(&mut q, &window);
        self.0.collection(false, "series/categories", q, "categories").await
    }
    pub async fn release(&self, series_id: &str, window: Realtime) -> Result<Release, FredError> {
        let mut q = Query::default(); q.put("series_id", series_id); realtime(&mut q, &window);
        self.0.collection(false, "series/release", q, "releases").await?.into_iter().next()
            .ok_or(FredError::EmptyResponse { resource: "release" })
    }
    pub async fn tags(&self, series_id: &str, window: Realtime) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("series_id", series_id); realtime(&mut q, &window);
        self.0.collection(false, "series/tags", q, "tags").await
    }
    pub async fn search_tags(&self, p: Search) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("search_text", p.search_text);
        if let Some(v) = p.search_type { q.put("search_type", v.as_str()) }
        realtime(&mut q, &p.realtime); pagination(&mut q, &p.pagination);
        self.0.collection(false, "series/search/tags", q, "tags").await
    }
    pub async fn search_related_tags(&self, p: Search, tag_names: &str) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("search_text", p.search_text); q.put("tag_names", tag_names);
        if let Some(v) = p.search_type { q.put("search_type", v.as_str()) }
        realtime(&mut q, &p.realtime); pagination(&mut q, &p.pagination);
        self.0.collection(false, "series/search/related_tags", q, "tags").await
    }
    /// Fetches recently updated series. `filter_value` is FRED's `macro`, `regional`, or `all`.
    pub async fn updates(&self, filter_value: Option<&str>, start_time: Option<&str>, end_time: Option<&str>, options: ListOptions) -> Result<Vec<Series>, FredError> {
        let mut q = Query::default();
        if let Some(v) = filter_value { q.put("filter_value", v); }
        if let Some(v) = start_time { q.put("start_time", v); }
        if let Some(v) = end_time { q.put("end_time", v); }
        list_options(&mut q, &options);
        self.0.collection(false, "series/updates", q, "seriess").await
    }
    pub async fn vintage_dates(&self, series_id: &str, window: Realtime) -> Result<Vec<NaiveDate>, FredError> {
        let mut q = Query::default(); q.put("series_id", series_id); realtime(&mut q, &window);
        #[derive(Deserialize)] struct R { vintage_dates: Vec<NaiveDate> }
        Ok(self
            .0
            .get::<R>(false, "series/vintagedates", q)
            .await?
            .vintage_dates)
    }

    /// Calls a remaining series endpoint with typed common query arguments.
    pub async fn raw<T: DeserializeOwned>(
        &self,
        suffix: &str,
        query: BTreeMap<String, String>,
    ) -> Result<T, FredError> {
        self.0
            .get(false, &format!("series/{suffix}"), Query(query))
            .await
    }
}

impl<'a> CategoryApi<'a> {
    pub async fn get(&self, id: Option<u64>) -> Result<Category, FredError> {
        let mut q = Query::default();
        if let Some(id) = id {
            q.put("category_id", id)
        }
        #[derive(Deserialize)]
        struct R {
            categories: Vec<Category>,
        }
        self.0
            .get::<R>(false, "category", q)
            .await?
            .categories
            .into_iter()
            .next()
            .ok_or(FredError::EmptyResponse {
                resource: "category",
            })
    }
    /// Fetches child categories of `category_id`.
    pub async fn children(&self, category_id: u64, window: Realtime) -> Result<Vec<Category>, FredError> {
        let mut q = Query::default();
        q.put("category_id", category_id);
        realtime(&mut q, &window);
        self.0.collection(false, "category/children", q, "categories").await
    }
    /// Fetches categories related to `category_id`.
    pub async fn related(&self, category_id: u64, window: Realtime) -> Result<Vec<Category>, FredError> {
        let mut q = Query::default();
        q.put("category_id", category_id);
        realtime(&mut q, &window);
        self.0.collection(false, "category/related", q, "categories").await
    }
    /// Fetches series assigned to a category. Use [`raw`](Self::raw) for advanced tag filters.
    pub async fn series(&self, category_id: u64, options: ListOptions) -> Result<Vec<Series>, FredError> {
        let mut q = Query::default(); q.put("category_id", category_id); list_options(&mut q, &options);
        self.0.collection(false, "category/series", q, "seriess").await
    }
    pub async fn tags(&self, category_id: u64, options: ListOptions) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("category_id", category_id); list_options(&mut q, &options);
        self.0.collection(false, "category/tags", q, "tags").await
    }
    pub async fn related_tags(&self, category_id: u64, tag_names: &str, options: ListOptions) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("category_id", category_id); q.put("tag_names", tag_names); list_options(&mut q, &options);
        self.0.collection(false, "category/related_tags", q, "tags").await
    }
    pub async fn raw<T: DeserializeOwned>(
        &self,
        path: &str,
        query: BTreeMap<String, String>,
    ) -> Result<T, FredError> {
        self.0
            .get(false, &format!("category/{path}"), Query(query))
            .await
    }
}
impl<'a> ReleaseApi<'a> {
    /// Fetches all releases.
    pub async fn all(&self, options: ListOptions) -> Result<Vec<Release>, FredError> {
        let mut q = Query::default(); list_options(&mut q, &options);
        self.0.collection(false, "releases", q, "releases").await
    }
    pub async fn get(&self, release_id: u64, window: Realtime) -> Result<Release, FredError> {
        let mut q = Query::default(); q.put("release_id", release_id); realtime(&mut q, &window);
        self.0.collection(false, "release", q, "releases").await?.into_iter().next()
            .ok_or(FredError::EmptyResponse { resource: "release" })
    }
    pub async fn dates(&self, release_id: u64, options: ListOptions) -> Result<Vec<ReleaseDate>, FredError> {
        let mut q = Query::default(); q.put("release_id", release_id); list_options(&mut q, &options);
        self.0.collection(false, "release/dates", q, "release_dates").await
    }
    /// Fetches release dates across all releases.
    pub async fn all_dates(&self, include_without_data: bool, options: ListOptions) -> Result<Vec<ReleaseDate>, FredError> {
        let mut q = Query::default(); q.put("include_release_dates_with_no_data", include_without_data); list_options(&mut q, &options);
        self.0.collection(false, "releases/dates", q, "release_dates").await
    }
    pub async fn series(&self, release_id: u64, options: ListOptions) -> Result<Vec<Series>, FredError> {
        let mut q = Query::default(); q.put("release_id", release_id); list_options(&mut q, &options);
        self.0.collection(false, "release/series", q, "seriess").await
    }
    pub async fn sources(&self, release_id: u64, window: Realtime) -> Result<Vec<Source>, FredError> {
        let mut q = Query::default(); q.put("release_id", release_id); realtime(&mut q, &window);
        self.0.collection(false, "release/sources", q, "sources").await
    }
    pub async fn tags(&self, release_id: u64, options: ListOptions) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("release_id", release_id); list_options(&mut q, &options);
        self.0.collection(false, "release/tags", q, "tags").await
    }
    pub async fn related_tags(&self, release_id: u64, tag_names: &str, options: ListOptions) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("release_id", release_id); q.put("tag_names", tag_names); list_options(&mut q, &options);
        self.0.collection(false, "release/related_tags", q, "tags").await
    }
    /// Fetches the hierarchical release table response. Its tree is represented as JSON because
    /// FRED's table elements are recursively shaped.
    pub async fn tables(&self, release_id: u64, element_id: Option<u64>, include_observation_values: bool, observation_date: Option<NaiveDate>) -> Result<serde_json::Value, FredError> {
        let mut q = Query::default(); q.put("release_id", release_id);
        if let Some(id) = element_id { q.put("element_id", id); }
        q.put("include_observation_values", include_observation_values);
        q.date("observation_date", observation_date);
        self.0.get(false, "release/tables", q).await
    }
    pub async fn raw<T: DeserializeOwned>(
        &self,
        path: &str,
        query: BTreeMap<String, String>,
    ) -> Result<T, FredError> {
        self.0.get(false, path, Query(query)).await
    }
}
impl<'a> SourceApi<'a> {
    pub async fn all(&self, options: ListOptions) -> Result<Vec<Source>, FredError> {
        let mut q = Query::default(); list_options(&mut q, &options);
        self.0.collection(false, "sources", q, "sources").await
    }
    pub async fn get(&self, source_id: u64, window: Realtime) -> Result<Source, FredError> {
        let mut q = Query::default(); q.put("source_id", source_id); realtime(&mut q, &window);
        self.0.collection(false, "source", q, "sources").await?.into_iter().next()
            .ok_or(FredError::EmptyResponse { resource: "source" })
    }
    pub async fn releases(&self, source_id: u64, options: ListOptions) -> Result<Vec<Release>, FredError> {
        let mut q = Query::default(); q.put("source_id", source_id); list_options(&mut q, &options);
        self.0.collection(false, "source/releases", q, "releases").await
    }
    pub async fn raw<T: DeserializeOwned>(
        &self,
        path: &str,
        query: BTreeMap<String, String>,
    ) -> Result<T, FredError> {
        self.0.get(false, path, Query(query)).await
    }
}
impl<'a> TagsApi<'a> {
    pub async fn get(&self, options: ListOptions) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); list_options(&mut q, &options);
        self.0.collection(false, "tags", q, "tags").await
    }
    pub async fn related(&self, tag_names: &str, options: ListOptions) -> Result<Vec<Tag>, FredError> {
        let mut q = Query::default(); q.put("tag_names", tag_names); list_options(&mut q, &options);
        self.0.collection(false, "related_tags", q, "tags").await
    }
    pub async fn series(&self, tag_names: &str, options: ListOptions) -> Result<Vec<Series>, FredError> {
        let mut q = Query::default(); q.put("tag_names", tag_names); list_options(&mut q, &options);
        self.0.collection(false, "tags/series", q, "seriess").await
    }
    pub async fn raw<T: DeserializeOwned>(
        &self,
        path: &str,
        query: BTreeMap<String, String>,
    ) -> Result<T, FredError> {
        self.0.get(false, path, Query(query)).await
    }
}
impl<'a> GeoFredApi<'a> {
    pub async fn raw<T: DeserializeOwned>(
        &self,
        path: &str,
        query: BTreeMap<String, String>,
    ) -> Result<T, FredError> {
        self.0.get(true, path, Query(query)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_observation_is_none() {
        let o:Observation=serde_json::from_str(r#"{"realtime_start":"2020-01-01","realtime_end":"2020-01-01","date":"2020-01-01","value":"."}"#).unwrap();
        assert_eq!(o.value, None)
    }
    #[test]
    fn numeric_observation_is_parsed() {
        let o:Observation=serde_json::from_str(r#"{"realtime_start":"2020-01-01","realtime_end":"2020-01-01","date":"2020-01-01","value":"12.5"}"#).unwrap();
        assert_eq!(o.value, Some(12.5))
    }
}
