## Mac Development

```
brew install git icu4c pkg-config
export PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c/lib/pkgconfig
cargo pgrx init --pg18 download

cargo test -p be #expect failures
cargo pgrx install -p pg_golden_axe -c /tmp/golden-axe-pg-test/install/postgresql-18.1.0-aarch64-apple-darwin/bin/pg_config
```


## Server Deployment

```
sudo apt-get install clang build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache pkg-config
```
