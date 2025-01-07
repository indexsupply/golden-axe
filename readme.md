# Golden Axe

`be` Hosts the api at: api.indexsupply.net

`fe` Hosts the web site at: www.indexsupply.net

`fe`'s database stores plan information and user queries and is r/w by `be` for account rate limiting and analytics.

## Local Setup

1. [Install rust](https://www.rust-lang.org/tools/install)
2. Install Postgres

```
brew install postgresql@17
```

3. Clone and test

```
git clone git@github.com:indexsupply/golden-axe.git
cd golden-axe
cargo test
```

4. Create databases

```
createdb be
createdb fe
```

5. Start frontend `fe`

```
cargo run -p fe
```

6. Start backend `be`

The backend depends on the account_limits view and the config table provided by the frontend. The backend will not attempt to update the frontend's schema and therefore must initially be ran after `fe`.

```
cargo run -p be
```
