#[cfg(test)]
mod pl_pgsql_test {
    use alloy::{hex, primitives::U256};
    use postgresql_embedded::{PostgreSQL, Settings, Version};
    use tokio_postgres::{Client, NoTls};

    static SCHEMA: &str = include_str!("./schema.sql");

    async fn test_pg() -> (PostgreSQL, Client) {
        let pg_settings = Settings {
            version: Version::new(16, Some(2), Some(3)),
            ..Default::default()
        };
        let mut db = PostgreSQL::new(pg_settings);
        db.setup().await.expect("setting up pg");
        db.start().await.expect("starting pg");
        db.create_database("dozer-test")
            .await
            .expect("creating test db");
        let (client, connection) = tokio_postgres::connect(&db.settings().url("dozer-test"), NoTls)
            .await
            .expect("unable to start test database");
        tokio::spawn(connection);
        client
            .batch_execute(SCHEMA)
            .await
            .expect("resetting schema");
        (db, client)
    }

    #[tokio::test]
    async fn test_abi_uint_array() {
        let (_pg_server, pg) = test_pg().await;
        let data = hex!(
            r#"
            0000000000000000000000000000000000000000000000000000000000000020
            0000000000000000000000000000000000000000000000000000000000000005
            0000000000000000000000000000000000000000000000000000000000000001
            0000000000000000000000000000000000000000000000000000000000000002
            0000000000000000000000000000000000000000000000000000000000000003
            0000000000000000000000000000000000000000000000000000000000000004
            0000000000000000000000000000000000000000000000000000000000000005
            "#
        );
        let row = pg
            .query_one("select abi_uint_array(abi_dynamic($1, 0))", &[&data])
            .await
            .expect("issue with query");
        let res: Vec<U256> = row.get(0);
        assert_eq!(
            vec![
                U256::from(1),
                U256::from(2),
                U256::from(3),
                U256::from(4),
                U256::from(5)
            ],
            res
        )
    }
}
