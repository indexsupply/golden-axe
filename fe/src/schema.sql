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
    name text,
    stripe_id text not null,
    primary key (owner_email)
);

create table if not exists plan_changes (
    owner_email text not null,
    name text not null,
    created_at timestamptz default now()
);

create table if not exists collabs(
    owner_email text not null,
    email text not null,
    created_at timestamptz default now() not null,
    disabled_at timestamptz
);
