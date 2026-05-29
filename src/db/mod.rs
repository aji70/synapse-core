use crate::config::Config;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::Arc;
use std::time::Duration;

pub mod audit;
pub mod cron;
pub mod models;
pub mod partition;
pub mod pool_manager;
pub mod queries;
pub mod session;
pub mod slow_query;

/// Build the database pool and eagerly establish `min_connections` without
/// running extra queries during warm-up.
pub async fn create_pool(config: &Config) -> Result<PgPool, sqlx::Error> {
    let pool = build_pool(
        &config.database_url,
        config.db_min_connections,
        config.db_max_connections,
        config.db_idle_timeout_secs,
        config.db_statement_timeout_ms,
    )
    .await?;
    warm_up(&pool, config.db_min_connections).await?;
    Ok(pool)
}

pub async fn create_long_running_pool(config: &Config) -> Result<PgPool, sqlx::Error> {
    let pool = build_pool(
        &config.database_url,
        config.db_min_connections,
        config.db_max_connections,
        config.db_idle_timeout_secs,
        config.db_long_running_statement_timeout_ms,
    )
    .await?;
    warm_up(&pool, config.db_min_connections).await?;
    Ok(pool)
}

async fn build_pool(
    url: &str,
    min: u32,
    max: u32,
    idle_timeout_secs: u64,
    statement_timeout_ms: u64,
) -> Result<PgPool, sqlx::Error> {
    let statement_timeout_sql = Arc::<str>::from(format!("SET statement_timeout = {statement_timeout_ms}"));

    PgPoolOptions::new()
        .min_connections(min)
        .max_connections(max)
        .idle_timeout(Duration::from_secs(idle_timeout_secs))
        .after_connect({
            let statement_timeout_sql = Arc::clone(&statement_timeout_sql);
            move |conn, _meta| {
                let statement_timeout_sql = Arc::clone(&statement_timeout_sql);
                Box::pin(async move {
                    sqlx::query(statement_timeout_sql.as_ref())
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            }
        })
        .connect(url)
        .await
}

async fn warm_up(pool: &PgPool, min_connections: u32) -> Result<(), sqlx::Error> {
    for _ in 0..min_connections {
        let _conn = pool.acquire().await?;
    }
    Ok(())
}
