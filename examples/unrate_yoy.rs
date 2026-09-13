//! Fetch UNRATE at annual frequency and transform it to year-over-year change.
use fred_wrapper::{FredClient, Frequency, Observations, Units};
use keyring::Entry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = Entry::new("macro-economics", "FRED_API_KEY")?.get_password()?;
    println!("Using FRED API key: {}", api_key);
    let client = FredClient::new(api_key)?;
    
    let request = Observations::new("UMCSENT");
    // request.frequency = Some(Frequency::Annual);
    // request.units = Some(Units::PercentChangeFromYearAgo);
    
    for observation in client.series().observations(request).await? {
        println!("{}: {:?}", observation.date, observation.value);
    }
    Ok(())
}
