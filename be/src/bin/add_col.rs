use clap::Parser;
use deadpool_postgres::Client;

async fn tables(pg: &Client, prefix: &str) -> Result<Vec<String>, tokio_postgres::Error> {
    let rows = pg
        .query(
            "select relname from pg_class
             where relkind = 'r'
               and relname like $1 || '%'
             order by relname",
            &[&prefix],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

async fn alter_table(table_name: &str, new_column: &str) -> String {
    format!("ALTER TABLE {table_name} ADD COLUMN {new_column};")
}

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(
        long = "pg-be",
        env = "PG_URL",
        default_value = "postgres://localhost/be"
    )]
    pg_url: String,

    #[arg(short = 't')]
    table_name: String,

    #[arg(short = 'c')]
    column_desc: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let pool = shared::pg::new_pool(&args.pg_url, 2).expect("pg_be pool");
    let pg = pool.get().await.expect("backend pool");
    let tables = tables(&pg, &args.table_name).await.unwrap();
    for table in &tables {
        println!("{}", alter_table(table, &args.column_desc).await);
    }
}
