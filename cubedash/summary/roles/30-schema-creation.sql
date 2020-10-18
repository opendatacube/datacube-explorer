
--
-- Create the Explorer schema, ready for explorer_owner to run `--init`.
--
--

begin;

-- Give them a schema, which the Explorer Owner can populate.
create schema if not exists cubedash authorization explorer_owner;
grant create on schema cubedash to explorer_owner;

commit;
