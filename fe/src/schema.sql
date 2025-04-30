create table if not exists wl_api_keys(
    provision_key text not null,
    org text not null,
    name text,
    secret text not null,
    origins text[] default '{}',
    hard_limit bool default true,
    created_at timestamptz default now(),
    deleted_at timestamptz
);
create unique index if not exists unique_api_keys
on wl_api_keys(secret);

create table if not exists login_links(
    email text not null,
    secret bytea not null,
    created_at timestamptz default now(),
    completed_at timestamptz,
    created_by inet,
    completed_by inet,
    invalidated_at timestamptz
);
create unique index if not exists unique_login_links
on login_links(email)
where invalidated_at is null;

create table if not exists invoice_extras (
    owner_email text not null,
    extras text not null
);

create table if not exists plan_changes (
    id bigserial unique,
    owner_email text not null,
    name text not null,
    amount int8,
    daimo_id text,
    daimo_tx text,
    stripe_session text,
    stripe_customer text,
    rate int default 10,
    timeout int default 10,
    connections int default 10,
    queries int default 3000000,
    hard_limit bool default true,
    created_at timestamptz default now()
);

create table if not exists plan_options (
    name text primary key,
    owner_email text,
    features text[] not null default '{}',
    rate int default 10,
    timeout int default 10,
    connections int default 10,
    queries int default 100000,
    daimo_amount int8 not null,
    stripe_amount int8 not null
);

insert into plan_options (name, owner_email, rate, timeout, connections, queries, features, daimo_amount, stripe_amount) values
('Indie', null, 5, 5, 10, 3000000, '{"5 queries \/ second \/ connection", "5 second query timeout", "10 active connections", "3M queries per month", "Hard limit", "No overage","Best Effort Support"}', 40000, 5000),
('Pro', null, 10, 10, 10000, 15000000, '{"10 queries \/ second \/ connection", "10 second query timeout", "Unlimited connections", "15M queries per month", "Configurable limit", "$5 per additional 1M queries", "Same Day Support"}', 280000, 25000),
('Dedicated', null, 10, 10, 10000, 500000, '{"Custom Chains", "Custom Performance", "On-call Support"}', 2200000, 200000),
('Ryan''s Special', 'r@32k.io', 10, 60, 1000, 500000, '{"10 requests per second", "60 second query timeout", "Unlimited connections"}', 100, 100)
on conflict (name) do update set
  rate = excluded.rate,
  timeout = excluded.timeout,
  connections = excluded.connections,
  queries = excluded.queries,
  features = excluded.features;

create table if not exists api_keys (
    owner_email text not null,
    secret text not null,
    ip_connections int,
    origins text[] not null default '{}',
    created_at timestamptz default now(),
    deleted_at timestamptz
);

create table if not exists provision_keys(
    secret text not null,
    created_at timestamptz default now(),
    deleted_at timestamptz
);

create index if not exists api_keys_owner_email
on api_keys(owner_email);

create table if not exists collabs(
    owner_email text not null,
    email text not null,
    created_at timestamptz default now() not null,
    disabled_at timestamptz
);

drop view if exists account_limits;
create view account_limits as
    with current_plans as (
        select distinct on (owner_email) owner_email, rate, timeout, connections, queries
        from plan_changes
        where (daimo_tx is not null or stripe_customer is not null)
        order by owner_email, created_at desc
    )
    select
        current_plans.owner_email,
        secret,
        timeout,
        rate,
        connections,
        queries,
        least(ip_connections, connections) as ip_connections,
        origins
    from api_keys
    inner join current_plans on current_plans.owner_email = api_keys.owner_email
    where api_keys.deleted_at is null
    union all
    select org, secret, 10, 10, 1000, 500000, 1000, coalesce(origins, '{}')
    from wl_api_keys
    where deleted_at is null;

create table if not exists user_queries(
    chain bigint,
    api_key text,
    events text[],
    user_query text,
    rewritten_query text,
    generated_query text,
    latency int,
    status int2,
    created_at timestamptz default now(),
    ip text
);

create table if not exists daily_user_queries (
    owner_email text not null,
    day date not null,
    n int8 not null,
    updated_at timestamptz not null default now(),
    primary key (owner_email, day)
);

create table if not exists config (
    enabled bool default true,
    name text,
    chain int8 primary key,
    url text not null,
    start_block int8,
    batch_size int2 not null default 2000,
    concurrency int2 not null default 10,
    popular bool default false,
    provision_key text
);

insert into
    config(enabled, chain, name, url)
    values
        (false, 1,          'Main',                 'https://eth.merkle.io'),
        (false, 100,        'Gnosis',               'https://rpc.gnosischain.com/'),
        (false, 137,        'Polygon',              'https://polygon-rpc.com'),
        (false, 414,        'Fly',                  'https://rpc-flynet-u82lq6zgmf.t.conduit.xyz'),
        (false, 998,        'Hyperliquid Testnet',  'https://rpc.hyperliquid-testnet.xyz/evm'),
        (false, 999,        'Hyperliquid',          'https://rpc.hyperliquid.xyz/evm'),
        (false, 1514,       'Story',                'https://mainnet.storyrpc.io'),
        (false, 1996,       'Sanko',                'https://mainnet.sanko.xyz'),
        (false, 8453,       'Base',                 'https://mainnet.base.org'),
        (false, 10143,      'Monad Testnet',        'https://testnet-rpc.monad.xyz'),
        (false, 42026,      'Donatuz',              'https://rpc.donatuz.com'),
        (false, 42161,      'Arbitrum One',         'https://arb1.arbitrum.io/rpc'),
        (false, 80002,      'Polygon Amoy',         'https://rpc-amoy.polygon.technology'),
        (false, 80094,      'Berachain',            'https://rpc.berachain.com'),
        (false, 84532,      'Base Sepolia',         'https://sepolia.base.org'),
        (false, 911867,     'Odyssey',              'https://odyssey.ithaca.xyz'),
        (false, 984122,     'Forma',                'https://rpc.forma.art'),
        (false, 984123,     'Forma Testnet',        'https://rpc.sketchpad-1.forma.art'),
        (true,  7777777,    'Zora',                 'https://rpc.zora.energy'),
        (false, 10058112,   'Spotlight Sepolia',    'https://spotlight-sepolia.g.alchemy.com/v2/RBovy_2RtzmHz-3xpxIbzSArz0v_-oc9'),
        (false, 52085143,   'Ble Testnet',          'https://rpc-ethena-testnet-0.t.conduit.xyz')
    on conflict(chain)
    do nothing;
