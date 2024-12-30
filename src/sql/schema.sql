create table if not exists config (
    enabled bool default true,
    chain int8 primary key,
    url text not null,
    batch_size int2 not null default 2000,
    concurrency int2 not null default 10
);

-- for testing rate limiting
-- in production GAFE_PG_URL should be set
-- and this view will be provided by GAFE PG.
drop view if exists account_limits;
create view account_limits as
    select
        'face'                                  as secret,
        10                                      as timeout,
        10                                      as rate,
        '{" foo.com", " www.foo.com "}'::text[] as origins,
        '{7777777}'::bigint[]                   as chains;

drop view if exists webhooks;
create view webhooks as
    select
        'http://127.0.0.1:8000'                 as destination_url,
        0                                       as block_height,
        'face'                                  as api_key,
        7777777                                 as chain,
        '{"Foo(uint a)"}'::text[]               as event_signatures,
        'select a from foo'                     as query;

-- for testing. in production ga instances should write to
-- gafe's database.
 create unlogged table if not exists user_queries(
    chain bigint,
    api_key text,
    events text[],
    user_query text,
    rewritten_query text,
    generated_query text,
    latency int,
    created_at timestamptz default now()
);

create table if not exists blocks(
    chain int8 not null,
    num int8,
    hash bytea,
    primary key (chain, num)
);

create table if not exists logs (
    chain int8,
    block_num int8,
    log_idx int4,
    tx_hash bytea,
    address bytea,
    topics bytea[],
    data bytea
) partition by list(chain);

create table if not exists logs_1           partition of logs for values in (1);
create table if not exists logs_100         partition of logs for values in (100);
create table if not exists logs_480         partition of logs for values in (480);
create table if not exists logs_4801        partition of logs for values in (4801);
create table if not exists logs_8453        partition of logs for values in (8453);
create table if not exists logs_42026       partition of logs for values in (42026);
create table if not exists logs_42161       partition of logs for values in (42161);
create table if not exists logs_84532       partition of logs for values in (84532);
create table if not exists logs_80002       partition of logs for values in (80002);
create table if not exists logs_911867      partition of logs for values in (911867);
create table if not exists logs_984122      partition of logs for values in (984122);
create table if not exists logs_7777777     partition of logs for values in (7777777);
create table if not exists logs_10058112    partition of logs for values in (10058112);
create table if not exists logs_52085143    partition of logs for values in (52085143);



insert into
    config(enabled, chain, url)
    values
        (false, 1, 'https://multi-omniscient-mound.quiknode.pro/fdedc14dec34659ffbb65528ec174998087d0df7'),
        (false, 100, 'https://sly-fluent-shadow.xdai.quiknode.pro/efa31e398dd8294c4ffb394e62b95750299cd918'),
        (false, 480, 'https://smart-winter-sun.worldchain-mainnet.quiknode.pro/f9891920fd207eb0143303f53bd71ebf5a4ea66a'),
        (false, 4801, 'https://maximum-damp-replica.worldchain-sepolia.quiknode.pro/558c716ed53af313e8c9db1e176334ea3f5b588e'),
        (false, 8453, 'https://base-mainnet.g.alchemy.com/v2/vURcmBOw_PHumnoIUEqNh2XY0jvH3_Fe'),
        (false, 42026, 'https://rpc.donatuz.com'),
        (false, 42161, 'https://arb-mainnet.g.alchemy.com/v2/Oq2TNKri4h_vXFtJQkujWI_MmPQoHYeo'),
        (false, 84532, 'https://special-divine-pond.base-sepolia.quiknode.pro/14a6b6521b135c48a9e71884c14b8beb984d6f93'),
        (false, 80002, 'https://tiniest-sparkling-dawn.matic-amoy.quiknode.pro/db261d98a880460e6c5a1a5de39fddc189817bec'),
        (false, 911867, 'https://odyssey.ithaca.xyz'),
        (false, 984122, 'https://rpc.forma.art'),
        (true,  7777777, 'https://rpc.zora.energy'),
        (false, 10058112, 'https://spotlight-sepolia.g.alchemy.com/v2/RBovy_2RtzmHz-3xpxIbzSArz0v_-oc9'),
        (false, 52085143, 'https://rpc-ethena-testnet-0.t.conduit.xyz')
    on conflict(chain)
    do nothing;

create or replace function b2i(data bytea) returns int4 as $$
declare
	n int4 = 0;
	m int2 = length(data);
	b bytea;
begin
	if length(data) >= 4 then
		m := 4;
	end if;

	for i in 0..(m-1) loop
		n := (n << 8) | get_byte(data, i);
	end loop;

	return n;
end;
$$ language plpgsql immutable strict;

create or replace function abi_uint(input bytea) returns numeric as $$
declare
    n numeric := 0;
begin
    if length(input) > 32 then
        raise exception 'abi_uint: input exceeds maximum length of 32 bytes';
    end if;
    for i in 1..length(input) loop
        n := n * 256 + get_byte(input, i - 1);
    end loop;
    return n;
end;
$$ language plpgsql immutable strict;

create or replace function abi_uint_array(input bytea) returns numeric[] as $$
declare
	length int;
    result numeric[] = array[]::numeric[];
begin
	length := b2i(substring(input from 29 for 4));
    for i in 0..(length - 1) loop
        result := array_append(result, abi_uint(substring(input from (1 + 32) + (i * 32) for 32)));
    end loop;
    return result;
end;
$$ language plpgsql immutable strict;

create or replace function abi_int(b bytea) returns numeric as $$
declare
    n numeric := 0;
    len int;
    is_neg bool;
begin
    len := length(b);
    if len > 32 then
        raise exception 'input exceeds maximum length of 32 bytes';
    end if;

    is_neg := (get_byte(b, 0) & 128) > 0;
    if is_neg then
        for i in 1..len loop
            n := n * 256 + (~get_byte(b, i - 1) & 255);
        end loop;
        n := (n + 1) * -1;
    else
        for i in 1..length(b) loop
            n := n * 256 + get_byte(b, i - 1);
        end loop;
    end if;
    return n;
end;
$$ language plpgsql strict immutable parallel safe cost 1;

create or replace function abi_int_array(input bytea) returns numeric[] as $$
declare
	length int;
    result numeric[] = array[]::numeric[];
begin
	length := b2i(substring(input from 29 for 4));
    for i in 0..(length - 1) loop
        result := array_append(result, abi_int(substring(input from (1 + 32) + (i * 32) for 32)));
    end loop;
    return result;
end;
$$ language plpgsql immutable strict parallel safe cost 1;

create or replace function abi_dynamic(input bytea, pos int) returns bytea as $$
declare
	offset_data bytea;
	offset_pos int;
begin
	offset_data := substring(input from pos+1 for 32);
	if length(offset_data) != 32 then
		raise exception 'expected length(offset_data) == 32. got: %', length(offset_data);
	end if;

	offset_pos := b2i(substring(offset_data from 29 for 4));
	if offset_pos = 0::int then
		return ''::bytea;
	end if;

	return substring(input from offset_pos+1);
end;
$$ language plpgsql immutable strict;

create or replace function abi_address(input bytea) returns bytea as $$
begin
	return substring(input from 13 for 20);
end;
$$ language plpgsql immutable strict;


create or replace function abi_fixed_bytes(input bytea, pos int, n int) returns bytea as $$
begin
	return substring(input from pos+1 for n);
end;
$$ language plpgsql immutable strict;

create or replace function abi_bool(input bytea)
returns bool as $$
begin
    return get_byte(input, length(input) - 1) = 1;
end;
$$ language plpgsql immutable strict;

create or replace function abi_bytes(input bytea)
returns bytea as $$
declare
    length int;
begin
	length := b2i(substring(input from 29 for 4));
	return substring(input from (1 + 32) for length);
end;
$$ language plpgsql immutable strict;

create or replace function abi_fixed_bytes_array(input bytea, size int)
returns bytea[] as $$
declare
    length int;
    result bytea[] = array[]::bytea[];
begin
	length := b2i(substring(input from 29 for 4));
    for i in 0..(length - 1) loop
        result := array_append(result, substring(input from (1 + 32) + (i * size) for size));
    end loop;
    return result;
end;
$$ language plpgsql;

create or replace function h2s(input bytea) returns text
    language sql immutable
    returns null on null input
    return convert_from(rtrim(input, '\x00'), 'UTF8');

create or replace function abi_string(input bytea) returns text
    language sql immutable
    returns null on null input
    return convert_from(rtrim(input, '\x00'), 'UTF8');
