#!/bin/bash
########################################################################################################################
#  Bash script to run within a docker container
#  Use AWS S3 bucket for storing pgdump file and once pg_restore is completed, delete the pgdump file from S3 bucket
########################################################################################################################
set -eu
umask 0022

# Explorer region
export TZ="Australia/Sydney"

export PGHOST="$1"
export PGUSER="$2"
export DBNAME="$3"
export PGPASSWORD="$4"
export PGPORT=5432
export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"  # Values read from k8s secrets
export AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION"  # Values read from k8s secrets
export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"  # Values read from k8s secrets
export AWS_DEFAULT_OUTPUT='json'

set -x

## dataset_type_ref id for the desired product is obtained from `SELECT id,name FROM agdc.dataset_type;` psql command
id_1="$5"
id_2="$6"
id_3="$7"
id_4="$8"
test_product="$9"

dump_id="$(date +%Y%m%d)"

psql_args="-h ${PGHOST} -p ${PGPORT} -U ${PGUSER}"

# Create a dump of rds database
dump_file="${DBNAME}-$(date +%Y%m%d)".pgdump
dbname="${DBNAME}_${dump_id}"
s3_dump_file=s3://dea-dev-bucket/pgdump/"${dump_file}"

echo "======================="
echo "Loading dump: ${dump_id}"
echo "      dbname: ${dbname}"
echo "        args: ${psql_args}"
echo "======================="
echo " in 5, 4, 3..."
sleep 5

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
    log_info "Create a copy of rds database and save that in AWS S3 bucket"
    pg_dump ${psql_args} ${DBNAME} -n agdc -T 'agdc.dv_*' -Fc -Z9 -v | \
    aws s3 cp - "${s3_dump_file}"

    createdb "$dbname"

    # TODO: the dump has "create extension" statements which will fail (but that's ok here)
    log_info "Restoring pgdump from AWS S3 bucket"
    # "no data for failed tables": when postgis extension fails to (re)initialise, don't populate its data
    # owner, privileges and tablespace are all NCI-specific.
    aws s3 cp "${s3_dump_file}" - | pg_restore -v --no-owner --no-privileges --no-tablespaces \
     --no-data-for-failed-tables -d "${dbname}"

    # Hygiene
    log_info "Vacuuming"
    psql "${dbname}" -c "vacuum analyze;"
fi

## Collect query stats on the new DB and remove the dump file
psql "${dbname}" -c "create extension if not exists pg_stat_statements;"

## Remove the dump file from S3 bucket
aws s3 rm "${s3_dump_file}"

## Summary generation
## get list of products
psql "${dbname}" -X -c 'copy (select name from agdc.dataset_type order by name asc) to stdout'

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
  psql ${psql_args} -d ${dbname}

$python3 -m cubedash.generate -C datacube_${DBNAME}.conf --all || true

log_info "Clustering $(date)"
psql "${dbname}" -X -c 'cluster cubedash.dataset_spatial using "dataset_spatial_dataset_type_ref_center_time_idx";'
psql "${dbname}" -X -c 'create index tix_region_center ON cubedash.dataset_spatial (dataset_type_ref, region_code text_pattern_ops, center_time);'
log_info "Done $(date)"

log_info "Testing a summary"
if ! $python3 -m cubedash.summary.show -C datacube_${DBNAME}.conf "${test_product}";
then
    log_info "Summary gen seems to have failed"
    exit 1
fi

log_info "Cubedash Database (${dbname}) updated on $(date)"

## Publish cubedash database update to SNS topic
TOPIC_ARN=$(aws sns list-topics | grep "cubedash" | cut -f4 -d'"')

log_info "Publish new updated db (${dbname}) to AWS SNS topic"
aws sns publish --topic-arn "${TOPIC_ARN}" --message "${dbname}"

## Clean old databases
log_info "Cleaning up old DBs"
old_databases=$(psql -X -t -d template1 -c "select datname from pg_database where datname similar to '${DBNAME}_\d{8}' and ((now() - split_part(datname, '_', 2)::date) > interval '3 days');")

log_info "Old databases: ${old_databases}"

sleep 120
for database in ${old_databases};
do
    log_info "Dropping ${database}";
    dropdb "${database}";
done;

log_info "All Done $(date)"
