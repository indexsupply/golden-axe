#[cfg(feature = "testhelper")]
pub mod test {
    use deadpool_postgres::Pool;
    use postgresql_embedded::{PostgreSQL, Settings, Version};
    use tokio_postgres::NoTls;

    pub async fn pg(schema: &str) -> (PostgreSQL, Pool) {
        let pg_settings = Settings {
            version: Version::new(17, Some(2), Some(0)),
            ..Default::default()
        };
        let mut db = PostgreSQL::new(pg_settings);
        db.setup().await.expect("setting up pg");
        db.start().await.expect("starting pg");
        db.create_database("ga-test")
            .await
            .expect("creating test db");
        let mut pool_config = deadpool_postgres::Config::new();
        pool_config.url = Some(db.settings().url("ga-test"));
        let pool = pool_config
            .create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
            .expect("creating pool");
        let pg = pool
            .get()
            .await
            .expect("unable to get test client from test pool");
        pg.batch_execute(schema).await.expect("resetting schema");
        (db, pool)
    }
}
