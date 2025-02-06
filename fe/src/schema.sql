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

create table if not exists accounts(
    owner_email text not null,
    stripe_id text not null,
    primary key (owner_email)
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
    chains bigint[] not null default '{}',
    rate int default 10,
    timeout int default 10,
    created_at timestamptz default now()
);

create table if not exists api_keys (
    owner_email text not null,
    secret text not null,
    origins text[] not null default '{}',
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
        select distinct on (owner_email) owner_email, chains, rate, timeout
        from plan_changes
        order by owner_email, created_at desc
    )
    select
        secret,
        timeout,
        rate,
        origins,
        chains
    from api_keys
    left join current_plans on current_plans.owner_email = api_keys.owner_email
    where api_keys.deleted_at is null;

create unlogged table if not exists user_queries(
    chain bigint,
    api_key text,
    events text[],
    user_query text,
    rewritten_query text,
    generated_query text,
    latency int,
    status int2,
    created_at timestamptz default now()
);

create table if not exists config (
    enabled bool default true,
    chain int8 primary key,
    url text not null,
    start_block int8,
    batch_size int2 not null default 2000,
    concurrency int2 not null default 10,
    conduit_id text unique
);

insert into
    config(enabled, chain, url)
    values
        (false, 1, 'https://multi-omniscient-mound.quiknode.pro/fdedc14dec34659ffbb65528ec174998087d0df7'),
        (false, 100, 'https://sly-fluent-shadow.xdai.quiknode.pro/efa31e398dd8294c4ffb394e62b95750299cd918'),
        (false, 480, 'https://smart-winter-sun.worldchain-mainnet.quiknode.pro/f9891920fd207eb0143303f53bd71ebf5a4ea66a'),
        (false, 4801, 'https://maximum-damp-replica.worldchain-sepolia.quiknode.pro/558c716ed53af313e8c9db1e176334ea3f5b588e'),
        (false, 8453, 'https://capable-thrumming-film.base-mainnet.quiknode.pro/56b49c04b3b9ad2e6162c946d89854181338f420'),
        (false, 42026, 'https://rpc.donatuz.com'),
        (false, 42161, 'https://hardworking-shy-arrow.arbitrum-mainnet.quiknode.pro/aa8d2cbd1f0a856b1ea64a66dfa0da3c9b704ca4'),
        (false, 84532, 'https://special-divine-pond.base-sepolia.quiknode.pro/14a6b6521b135c48a9e71884c14b8beb984d6f93'),
        (false, 80002, 'https://tiniest-sparkling-dawn.matic-amoy.quiknode.pro/db261d98a880460e6c5a1a5de39fddc189817bec'),
        (false, 911867, 'https://odyssey.ithaca.xyz'),
        (false, 984122, 'https://rpc.forma.art'),
        (true,  7777777, 'https://rpc.zora.energy'),
        (false, 10058112, 'https://spotlight-sepolia.g.alchemy.com/v2/RBovy_2RtzmHz-3xpxIbzSArz0v_-oc9'),
        (false, 52085143, 'https://rpc-ethena-testnet-0.t.conduit.xyz')
    on conflict(chain)
    do nothing;

create table if not exists plans(name text, amount numeric, primary key (name));
insert into plans(name, amount) values ('pro', 100), ('indie', 20) on conflict(name) do nothing;
