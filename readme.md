# Golden Axe

`be` Hosts the api at: api.indexsupply.net

`fe` Hosts the web site at: www.indexsupply.net

`fe`'s database stores plan information and user queries and is r/w by `be` for account rate limiting and analytics.

## Local Setup

1. Dependencies

[Install rust](https://www.rust-lang.org/tools/install)

Mac
```
brew install postgresql@18
brew services start postgresql@18
echo 'export PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH"' >> ~/.zshrc

brew install icu4c pkg-config openssl@3
export PKG_CONFIG_PATH="/opt/homebrew/opt/icu4c/lib/pkgconfig"
```

Linux
```
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --dearmor --yes --batch --no-tty \
    -o /etc/apt/keyrings/postgresql.gpg
sudo chmod 0644 /etc/apt/keyrings/postgresql.gpg
echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
        | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
sudo apt-get update -y
sudo apt-get install -y build-essential pkg-config libssl-dev postgresql-server-dev-18 postgresql-18 postgresql-client-18
```

3. Install Postgres Extension

```
cargo install --locked cargo-pgrx --version="0.16.1"
cargo pgrx init --pg18 pg_config
cargo pgrx install -p pg_golden_axe
```

4. Test

```
createdb golden_axe_test
cargo test
```

5. Run

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
