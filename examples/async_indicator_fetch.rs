use fred_wrapper::queue::FredQueue;
use fred_wrapper::{
    FredClient, FredError, FredResilienceConfig, Observation, Observations, Realtime,
};
use futures::future::join_all;

#[tokio::main]
async fn main() -> Result<(), FredError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // However you obtain your key in your real application.
    let api_key = keyring::Entry::new("macro-economics", "FRED_API_KEY")?.get_password()?;

    let config = FredResilienceConfig {
        requests_per_second: 2,
        max_retries: 3,
        ..Default::default()
    };

    let client = FredClient::new(api_key, config)?;

    // One background worker owns the FredClient.
    //
    // The worker processes requests from the queue one at a time.
    let (queue, _events) = FredQueue::spawn(client, 32);

    let indicators = [
        "CPIAUCSL", // CPI
        "UNRATE",   // Unemployment rate
        //"FEDFUNDS", // Federal funds rate
        //"DGS10",    // 10-year Treasury yield
        //"INDPRO",   // Industrial production
    ];

    let tasks = indicators.into_iter().map(|series_id| {
        let queue = queue.clone();

        tokio::spawn(async move {
            println!("[{series_id}] starting");

            let mut params = Observations::new(series_id);

            // params.realtime = Realtime {
            //     start: Some(NaiveDate::from_ymd_opt(1776, 7, 4).unwrap()),
            //     end: Some(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()),
            // };

            params.realtime = Realtime {
                start: Some(chrono::NaiveDate::from_ymd_opt(2010, 1, 1).unwrap()),
                end: Some(chrono::Local::now().date_naive()),
            };

            match queue.observations_windowed(params, 5).await {
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

    // Wait for all indicator tasks.
    let results = join_all(tasks).await;

    println!("\n========== SUMMARY ==========");

    for result in results {
        match result {
            Ok(Ok((series_id, observations))) => {
                print_summary(series_id, &observations);
            }

            Ok(Err(error)) => {
                eprintln!("indicator failed: {error}");
            }

            Err(join_error) => {
                eprintln!("task panicked: {join_error}");
            }
        }
    }

    Ok(())
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
