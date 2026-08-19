use chrono::NaiveDate;
use fred_wrapper::{FredClient, Observation, params::{Observations, Realtime}};

async fn pull_full_history

(client: &FredClient, series_id: &str) -> Result<Vec<Observation>, fred_wrapper::FredError> 
    
    {
    let mut params = Observations::new(series_id);
    params.realtime = Realtime {
        start: NaiveDate::from_ymd_opt(1776, 7, 4),
        end: Some(chrono::Local::now().date_naive()),
    };
    // output_type left None -> default (type 1): 
    // full revision history in the window
    let obs = client.series().observations(params).await?;
    // obs now contains every (date, value, realtime_start, realtime_end) 
    //row ever recorded

    // persist_rows(series_id, obs).await
    Ok(obs)
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = keyring::Entry::new("macro-economics", "FRED_API_KEY")?.get_password()?;
    let client = FredClient::new(api_key)?;
    let observations = pull_full_history(&client, "UMCSENT").await?;
    // Do something with the observations, e.g., print them or save them to a file
    println!("{:?}", observations);
    Ok(())
}
