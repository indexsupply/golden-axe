use std::{
    collections::HashMap,
    io::{self, Write},
};

use clap::Parser;
use deadpool_postgres::Pool;
use eyre::Result;
use fe::{postmark, stripe};

#[derive(Parser)]
struct Args {
    #[arg(long, env = "PG_URL_GAFE", default_value = "postgres://localhost/fe")]
    pg_url: String,
    #[arg(long, env = "STRIPE_KEY")]
    stripe_key: Option<String>,
    #[arg(long, env = "POSTMARK_KEY")]
    postmark_key: Option<String>,
    #[clap(short = 'c', action = clap::ArgAction::SetTrue)]
    charge: bool,
    #[arg(long, default_value = "2024")]
    year: u16,
    #[arg(long, default_value = "12")]
    month: u8,
}

fn wait_for_yes() -> bool {
    println!("charge?");
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    input.trim().to_lowercase() == "y"
}

const INVOICE_HEADER: &str = "From:\nIndex Supply, Co.\n1095 Hilltop Dr.\nRedding, CA 96003\n\n";

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let pool = shared::pg::new_pool(&args.pg_url, 1).expect("unable to create pg pool");
    let stripe_client = stripe::Client::new(args.stripe_key);
    let postmark_client = postmark::Client::new(args.postmark_key);
    let customer_charges = query(&pool, args.year, args.month).await.expect("query");
    for (customer, charges) in customer_charges {
        let invoice_id = format!("Invoice Id: {}{}{}\n", customer.id, args.year, args.month);
        let invoice_date = format!("Invoice Date: {}-{}-{}\n", args.year, args.month + 1, 1);
        let mut line_items = Vec::new();
        let mut amount: i64 = 0;
        for charge in charges {
            amount += charge.amount;
            line_items.push(format!(
                "{} Plan {}/{} to {}/{} ${}.",
                charge.plan,
                args.month,
                charge.from,
                args.month,
                charge.to,
                charge.amount as f64 / 100.0,
            ));
        }
        println!("\n\n-----------\n{}\n", customer.owner_email);
        let description = format!(
            "{}{}\n{}{}Indexing Services:\n\n{}\n\nTotal: ${}\n\nThank you for your business!",
            invoice_id,
            invoice_date,
            INVOICE_HEADER,
            customer
                .extras
                .map(|e| format!("{}\n\n", e))
                .unwrap_or_default(),
            line_items.join("\n"),
            amount as f64 / 100.0,
        );
        println!("{}", description);
        println!("-----------\n");
        if args.charge && wait_for_yes() {
            stripe_client
                .charge_customer(customer.stripe_id, description.clone(), amount)
                .await
                .map_err(|e| println!("charging customer {} {}", customer.owner_email, e))
                .ok();
            postmark_client
                .send(
                    "notifications@indexsupply.net",
                    &customer.owner_email,
                    &format!("Invoice {}/{}", args.year, args.month),
                    &description,
                )
                .await
                .map_err(|e| println!("sending email {} {}", customer.owner_email, e))
                .ok();
        } else {
            println!("skipping")
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Customer {
    id: i64,
    owner_email: String,
    stripe_id: String,
    extras: Option<String>,
}

#[derive(Debug)]
struct Charge {
    plan: String,
    from: u8,
    to: u8,
    amount: i64,
}

type Charges = HashMap<Customer, Vec<Charge>>;

async fn query(pool: &Pool, year: u16, month: u8) -> Result<Charges, shared::Error> {
    let res = pool
        .get()
        .await?
        .query(
            "
            with collapsed as (
                select
                    id,
                    owner_email,
                    name,
                    amount,
                    stripe_customer,
                    created_at,
                    lead(created_at) over (
                        partition by owner_email
                        order by created_at
                    ) as stopped_at
                from plan_changes
                where daimo_tx is null and stripe_customer is not null
            ), with_days as (
                select
                    id,
                    owner_email,
                    name,
                    amount,
                    stripe_customer,
                    extract(day from greatest(
                            created_at,
                            date_trunc('month', make_date($1, $2, 1))
                    ))::int4 as started_at,
                    extract(day from coalesce(
                            stopped_at,
                            date_trunc('month', make_date($1, $2, 1)) + '1 month'::interval - '1 day'::interval
                    ))::int4 as stopped_at,
                    extract(day from date_trunc('month', make_date($1, $2, 1)) + '1 month'::interval - '1 day'::interval) as num_days
                from collapsed
                where stopped_at is null
                or stopped_at > date_trunc('month', make_date($1, $2, 1))
            )
            select
                id,
                with_days.owner_email,
                stripe_customer,
                extras,
                INITCAP(name) as name,
                started_at,
                stopped_at,
                (round(amount * ((stopped_at - started_at) + 1) / num_days, 2))::int8 as amount
            from with_days
            left join invoice_extras
            on invoice_extras.owner_email = with_days.owner_email
            where exists (
                select 1
                from api_keys
                where owner_email = with_days.owner_email
            )
            order by started_at asc
            ",
            &[&(year as i32), &(month as i32)],
        )
        .await?;
    let mut charges = Charges::new();
    for row in res {
        charges
            .entry(Customer {
                id: row.get("id"),
                owner_email: row.get("owner_email"),
                stripe_id: row.get::<&str, String>("stripe_customer"),
                extras: row.get("extras"),
            })
            .or_default()
            .push(Charge {
                plan: row.get("name"),
                from: row.get::<&str, i32>("started_at") as u8,
                to: row.get::<&str, i32>("stopped_at") as u8,
                amount: row.get("amount"),
            });
    }
    Ok(charges)
}
