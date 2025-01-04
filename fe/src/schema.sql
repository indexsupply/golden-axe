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
    owner_email text not null,
    name text not null,
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
