
--
-- This is a stand-alone copy of the built-in datacube agdc_user role.
--
-- It's built-in to datacube but optional:
-- https://github.com/opendatacube/datacube-core/blob/1353bde7e2cc0bb095b53a60351fd5c301d1b3c4/datacube/drivers/postgres/_core.py#L118-L120
--
-- You do not need to run this file if you already use datacube's
-- default roles.
--
-- Explorer is a datacube user, and so its roles extend agdc_user.
--
begin;

create role agdc_user nologin inherit;
comment on role agdc_user is 'Default read-only datacube user role';
grant usage on schema agdc to agdc_user;
grant select on all tables in schema agdc to agdc_user;
grant execute on function agdc.common_timestamp(TEXT) to agdc_user;

commit;
