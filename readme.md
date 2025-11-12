# Golden Axe

`be` Hosts the api at: api.indexsupply.net

`fe` Hosts the web site at: www.indexsupply.net

`fe`'s database stores plan information and user queries and is r/w by `be` for account rate limiting and analytics.

## Local Setup

1. Dependencies

[Install rust](https://www.rust-lang.org/tools/install)

Mac
```
brew install icu4c pkg-config openssl@3
export PKG_CONFIG_PATH="/opt/homebrew/opt/icu4c/lib/pkgconfig"
```

Linux
```
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > \
  /etc/apt/sources.list.d/pgdg.list'
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
sudo apt update -y
sudo apt install -y build-essential pkg-config libssl-dev postgresql-server-dev-17
```

3. Build

```
cargo install --locked cargo-pgrx --version="0.15.0"
cargo pgrx init --pg17 download
cargo build
cargo test
```

If you get an error indicating that pg_golden_axe is not installed:

```
cargo pgrx install \
  -p pg_golden_axe \
  -c /tmp/golden-axe-pg-test/install/postgresql-17.2.0-aarch64-apple-darwin/bin/pg_config
```


4. Clone and test

```
git clone git@github.com:indexsupply/golden-axe.git
cd golden-axe
cargo test
```

5. Create databases

```
createdb be
psql be -f src/sql/schema.sql
psql be -f src/sql/indexes.sql
psql be -f src/sql/roles.sql

createdb fe
```

6. Start frontend `fe`

```
cargo run -p fe
```

7. Start backend `be`

The backend depends on the account_limits view and the config table provided by the frontend. The backend will not attempt to update the frontend's schema and therefore must initially be ran after `fe`.

```
cargo run -p be
```
