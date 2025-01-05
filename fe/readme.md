# Golden Axe Front End

Hosts the web site at: www.indexsupply.net

It's database stores plan information and is read by `ga` for account rate limiting.

## Local Setup

1. [Install rust](https://www.rust-lang.org/tools/install)
2. Install Postgres

    brew install postgresql@17

3. Create database

    createdb ga

4. Start `gafe`

    cargo run

