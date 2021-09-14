
--
-- Create the three Explorer roles.
--
-- This can be run on an existing host if adding the roles for the first time.
--
begin;

----- Explorer Viewer -----
-- A read-only user of datacube and explorer
-- (Suitable for Web interface, cli commands)
create user explorer_viewer inherit in role agdc_user;
comment on role explorer_viewer is 'Explorer read-only viewer';
grant usage on schema cubedash to explorer_viewer;
grant select on all tables in schema cubedash to explorer_viewer;

----- Explorer Generator -----
-- Suitable for generating and updating summaries
-- (ie. Running `cubedash-gen`)
create user explorer_generator inherit in role explorer_viewer;
comment on role explorer_generator is 'Explorer data summariser (for running cubedash-gen)';

----- Explorer Owner -----
-- For creating and updating the schema.
-- (ie. Running `cubedash-gen --init`)
create user explorer_owner inherit in role explorer_generator;
comment on role explorer_owner is 'Explorer schema creator and updater';

commit;
