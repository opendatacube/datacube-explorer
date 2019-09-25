#!/usr/bin/env bash

set -eu
set -x
umask 0022

# Explorer region
export TZ="Australia/Sydney"

export PGHOST="$1"
export PGUSER="$2"
export DBNAME="$3"
export PGPORT=5432

## dataset_type_ref id for the desired product is obtained from `SELECT id,name FROM agdc.dataset_type;` psql command
id_1="$4"
id_2="$5"
id_3="$6"
id_4="$7"
test_product="$8"

# Remove any previous pgdump files before processing cubedash job
rm -rf /data/${DBNAME}/*-datacube.pgdump

# Optional first argument is day to load (eg. "yesterday")
dump_id="$(date +%Y%m%d)"

psql_args="-h ${PGHOST} -p ${PGPORT} -U ${PGUSER}"
dump_file="/data/${DBNAME}/${dump_id}-datacube.pgdump"

archive_dir="/data/${DBNAME}/archive"
summary_dir="${archive_dir}/${dump_id}"
dbname="${DBNAME}_${dump_id}"

log_file="${summary_dir}/restore-$(date +'%dT%H%M').log"
python=/opt/conda/bin/python

echo "======================="
echo "Loading dump: ${dump_id}"
echo "      dbname: ${dbname}"
echo "        args: ${psql_args}"
echo "         log: ${log_file}"
echo "======================="
echo " in 5, 4, 3..."
sleep 5

mkdir -p "${summary_dir}"
exec > "${log_file}"
exec 2>&1

function log_info {
    printf '### %s\n' "$@"
}

function finish {
    log_info "Exiting $(date)"
}
trap finish EXIT

log_info "Starting restore $(date)"
# Print local lowercase variables into log
log_info "Vars:"
(set -o posix; set) | grep -e '^[a-z_]\+=' | sed 's/^/    /'

if psql -lqtA | grep -q "^$dbname|";
then
    log_info "DB exists"
else
    if [[ ! -e "${dump_file}" ]];
    then
        # Create a dump of rds database
        log_info "Create a copy of rds database"

        out_prefix="/data/${DBNAME}/${DBNAME}-$(date +%Y%m%d)"

        set -x
        pg_dump ${psql_args} ${DBNAME} -n agdc -T 'agdc.dv_*' -F c -f "${out_prefix}-datacube-partial.pgdump"
        mv -v "${out_prefix}-datacube-partial.pgdump" "${dump_file}"
    fi

    # Record date/time of DB backup, cubedash will show it as last update time
    date -r "${dump_file}" > "${summary_dir}/generated.txt"

    createdb "$dbname"

    # TODO: the dump has "create extension" statements which will fail (but that's ok here)
    log_info "Restoring"
    # "no data for failed tables": when postgis extension fails to (re)initialise, don't populate its data
    # owner, privileges and tablespace are all NCI-specific.
    pg_restore -v --no-owner --no-privileges --no-tablespaces --no-data-for-failed-tables -d "${dbname}" -j 4 "${dump_file}" || true

    # Hygiene
    log_info "Vacuuming"
    psql "${dbname}" -c "vacuum analyze;"
fi

## Collect query stats on the new DB and remove the dump file
psql "${dbname}" -c "create extension if not exists pg_stat_statements;"

[[ -e "${dump_file}" ]] && rm -v "${dump_file}"

## Summary generation
## get list of products
psql "${dbname}" -X -c 'copy (select name from agdc.dataset_type order by name asc) to stdout' > "${summary_dir}/all-products.txt"

echo "
[datacube]
db_database: ${dbname}
db_hostname: ${PGHOST}
db_port:     5432
db_username: ${PGUSER}
" > datacube_${DBNAME}.conf

log_info "Summary gen"

log_info "Drop Schema and update the center_dt for Sentinel NRT products"
## example ids for ows database in eks prod stack
##   id ¦   name
##   ---|----------------
##   26 ¦ s2a_l1c_aws_pds
##   27 ¦ s2b_l1c_aws_pds
##   28 ¦ s2a_nrt_granule
##   29 ¦ s2b_nrt_granule
echo "drop schema if exists cubedash cascade;
  UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_1} AND d.id = s.id;
  UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_2} AND d.id = s.id;
  UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_3} AND d.id = s.id;
  UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_4} AND d.id = s.id;" |
  "${psql_args}" -d "${dbname}"

$python -m cubedash.generate -C datacube_${DBNAME}.conf --all || true

log_info "Clustering $(date)"
psql "${dbname}" -X -c 'cluster cubedash.dataset_spatial using "dataset_spatial_dataset_type_ref_center_time_idx";'
psql "${dbname}" -X -c 'create index tix_region_center ON cubedash.dataset_spatial (dataset_type_ref, region_code text_pattern_ops, center_time);'
log_info "Done $(date)"

log_info "Testing a summary"
if ! $python -m cubedash.summary.show -C datacube_${DBNAME}.conf "${test_product};"
then
    log_info "Summary gen seems to have failed"
    exit 1
fi

log_info "All Done $(date) ${summary_dir}"
log_info "Cubedash Database (${dbname}) updated on $(date)"

## Publish cubedash database update to SNS topic
AWS_PROFILE='default'
export AWS_PROFILE="${AWS_PROFILE}"
TOPIC_ARN=$(/opt/conda/bin/aws sns list-topics | grep "cubedash" | cut -f4 -d'"')

log_info "Publish new updated db (${dbname}) to AWS SNS topic"
/opt/conda/bin/aws sns publish --topic-arn "${TOPIC_ARN}" --message "${dbname}"

## Clean old databases
log_info "Cleaning up old DBs"
old_databases=$(psql -X -t -d template1 -c "select datname from pg_database where datname similar to '${DBNAME}_\d{8}' and ((now() - split_part(datname, '_', 2)::date) > interval '3 days');")

sleep 120
for database in ${old_databases};
do
    log_info "Dropping ${database}";
    dropdb "${database}";
done;

log_info "All Done $(date) ${summary_dir}"
