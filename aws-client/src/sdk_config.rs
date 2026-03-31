use std::time::Duration;

use aws_config::{SdkConfig, retry::RetryConfig};

pub async fn load_config(profile: Option<String>) -> SdkConfig {
    let loader = aws_config::from_env().retry_config(
        RetryConfig::standard()
            .with_initial_backoff(Duration::from_millis(50))
            .with_max_backoff(Duration::from_secs(60))
            .with_max_attempts(100),
    );

    match profile {
        Some(profile) => loader.profile_name(profile),
        None => loader,
    }
    .load()
    .await
}
