create extension if not exists pg_golden_axe;

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

create table if not exists txs (
    -- Fixed-width columns first, in descending order of size
    chain int8 NOT NULL,
    block_num int8 NOT NULL,
    gas int8 NOT NULL,
    gas_price int8 NOT NULL,
    idx int4 NOT NULL,
    type int2,

    -- Variable-width columns next
    hash bytea NOT NULL,
    "from" bytea NOT NULL,
    "to" bytea NOT NULL,
    input bytea,
    value numeric
) partition by list(chain);

create table if not exists txs_c1
partition of txs
for values in (1)
partition by range (block_num);

create table if not exists txs_c1_b22
partition of txs_c1
for values from (22000000) to (24000000);
alter table txs_c1_b22 set (toast_tuple_target = 128);

create table if not exists txs_c8453
partition of txs
for values in (8453)
partition by range (block_num);

create table if not exists txs_c8453_b28
partition of txs_c8453
for values from (28000000) to (30000000);
alter table txs_c8453_b28 set (toast_tuple_target = 128);

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

create or replace function h2s(input bytea) returns text
    language sql immutable
    returns null on null input
    return convert_from(rtrim(input, '\x00'), 'UTF8');

create or replace function abi_string(input bytea) returns text
    language sql immutable
    returns null on null input
    return convert_from(rtrim(input, '\x00'), 'UTF8');
