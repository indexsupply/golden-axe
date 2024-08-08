create table if not exists blocks(
	chain_id bigint,
	num numeric,
	topic bytea,
	hash bytea,
	primary key (num, topic)
);

create unlogged table if not exists logs (
	chain_id bigint,
	block_num numeric,
	tx_hash bytea,
	log_idx int4,
	address bytea,
	topics bytea[],
	data bytea
);

create index if not exists logs_block_num_idx on logs (block_num desc);
create index if not exists logs_address_topics_idx on logs(address, (topics[1]));
create extension if not exists btree_gin;
create index if not exists logs_block_num_address_topics_idx on logs using gin (block_num, address, topics);

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
