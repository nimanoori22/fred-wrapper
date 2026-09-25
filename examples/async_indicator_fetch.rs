use fred_wrapper::{
    FredClient, FredError, FredResilienceConfig, Observation, Observations, Realtime,
};
use futures::TryStreamExt;
use futures::future::join_all;

#[tokio::main]
async fn main() -> Result<(), FredError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let api_key = keyring::Entry::new("macro-economics", "FRED_API_KEY")?.get_password()?;

    let config = FredResilienceConfig {
        requests_per_second: 2,
        max_retries: 3,
        ..Default::default()
    };

    // Cheap to clone: every clone shares the same rustopus Arm, so the rate
    // limit and concurrency cap apply across all tasks below.
    let client = FredClient::new(api_key, config)?;

    let indicators = [
        "CPIAUCSL", // CPI
        "UNRATE",   // Unemployment rate
        //"FEDFUNDS",
        //"DGS10",
        //"INDPRO",
    ];

    let tasks = indicators.into_iter().map(|series_id| {
        let client = client.clone();

        tokio::spawn(async move {
            println!("[{series_id}] starting");

            let mut params = Observations::new(series_id);
            params.realtime = Realtime {
                start: Some(chrono::NaiveDate::from_ymd_opt(2010, 1, 1).unwrap()),
                end: Some(chrono::Local::now().date_naive()),
            };

            match fetch_all(&client, params, 5).await {
                Ok(observations) => {
                    println!(
                        "[{series_id}] finished: {} observations",
                        observations.len()
                    );
                    Ok::<_, FredError>((series_id, observations))
                }
                Err(error) => {
                    eprintln!("[{series_id}] failed: {error}");
                    Err(error)
                }
            }
        })
    });

    let results = join_all(tasks).await;

    println!("\n========== SUMMARY ==========");

    for result in results {
        match result {
            Ok(Ok((series_id, observations))) => print_summary(series_id, &observations),
            Ok(Err(error)) => eprintln!("indicator failed: {error}"),
            Err(join_error) => eprintln!("task panicked: {join_error}"),
        }
    }

    Ok(())
}

/// Replaces `FredQueue::observations_windowed`: drains the windowed stream
/// into one flat Vec, stopping at the first failed window.
async fn fetch_all(
    client: &FredClient,
    params: Observations,
    window_years: u32,
) -> Result<Vec<Observation>, FredError> {
    // `observations_windowed` borrows the SeriesApi, so bind it to a local.
    let series = client.series();
    let pages: Vec<Vec<Observation>> = series
        .observations_windowed(params, window_years)
        .try_collect()
        .await?;
    Ok(pages.into_iter().flatten().collect())
}

fn print_summary(series_id: &str, observations: &[Observation]) {
    println!("{series_id}: {} observations", observations.len());

    if let Some(first) = observations.first() {
        println!(
            "  first: date={}, value={:?}, vintage={}",
            first.date, first.value, first.realtime_start,
        );
    }

    if let Some(last) = observations.last() {
        println!(
            "  last:  date={}, value={:?}, vintage={}",
            last.date, last.value, last.realtime_start,
        );
    }
}