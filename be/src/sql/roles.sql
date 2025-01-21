-- schema.sql is an idempotent 'migration' file
-- unfortunately, roles a messy. no way to
-- create-if-not-exists. no way to drop cascade.

-- these settings are designed to be run
-- on the database's initial setup.
create role uapi with login password 'XXX' noinherit;

revoke all on all tables in schema public from uapi;
revoke execute on all functions in schema public from uapi;

grant select on logs TO uapi;
grant select on blocks TO uapi;

alter role uapi set statement_timeout = '30s';
alter role uapi set work_mem = '1GB';
alter role uapi set temp_file_limit = '1GB';
alter role uapi connection limit 64;
