--
-- Grant all permissions on the three Explorer roles.
--
-- This is designed to be re-runnable, to add anything missing.
--

begin;

----- Explorer Viewer -----
-- A read-only user of datacube and explorer
-- (Suitable for Web interface, cli commands)
grant usage on schema cubedash to explorer_viewer;
grant select on all tables in schema cubedash to explorer_viewer;


----- Explorer Generator -----
-- Suitable for generating and updating summaries
-- (ie. Running `cubedash-gen`)

grant insert, update, delete on all tables in schema cubedash to explorer_generator;

-- Must be owner of materialised views to refresh them.
alter materialized view cubedash.mv_dataset_spatial_quality owner to explorer_generator;
alter materialized view cubedash.mv_spatial_ref_sys owner to explorer_generator;
grant usage on sequence cubedash.product_id_seq to explorer_generator;


----- Explorer Owner -----
-- For creating and updating the schema.
-- (ie. Running `cubedash-gen --init`)

grant all privileges on all tables in schema cubedash to explorer_owner;

-- Double-check that tables are owned by them (they should be if it created them)
alter table cubedash.dataset_spatial owner to explorer_owner;
alter table cubedash.product owner to explorer_owner;
alter table cubedash.region owner to explorer_owner;
alter table cubedash.time_overview owner to explorer_owner;
alter sequence cubedash.product_id_seq owner to explorer_owner;

alter schema cubedash owner to explorer_owner;

commit;
