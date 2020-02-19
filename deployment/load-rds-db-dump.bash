#!/bin/bash
########################################################################################################################
#  Bash script to run daily explorer updates
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

set -x
test_product1="$5"
test_product2="$6"
test_product3="$7"
bit_bucket_branch="$8"

if [[ "${bit_bucket_branch}" != "eks-deafrica" ]]; then
  ## dataset_type_ref id for the desired product is obtained from `SELECT id,name FROM agdc.dataset_type;` psql command
  id_1="$9"
  id_2="${10}"
  id_3="${11}"
  id_4="${12}"
fi

psql_args="-h ${PGHOST} -p ${PGPORT} -U ${PGUSER}"

# Create a dump of rds database
dbname="${DBNAME}"
sleep 5

function log_info {
    printf '### %s\n' "$@"
}

function finish {
    log_info "Exiting $(date)"
}
trap finish EXIT

# log_info "Starting restore $(date)"
# Print local lowercase variables into log
log_info "Vars:"
(set -o posix; set) | grep -e '^[a-z_]\+=' | sed 's/^/    /'

## Collect query stats on the new DB and remove the dump file
psql "${dbname}" -c "create extension if not exists pg_stat_statements;"

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

if [[ "${bit_bucket_branch}" != "eks-deafrica" ]]; then
    log_info "Drop Schema and update the center_dt for Sentinel NRT products"
    ## example ids for ows database in eks prod stack
    ##   id ¦   name
    ##   ---|----------------
    ##   26 ¦ s2a_l1c_aws_pds
    ##   27 ¦ s2b_l1c_aws_pds
    ##   28 ¦ s2a_nrt_granule
    ##   29 ¦ s2b_nrt_granule
    echo "UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_1} AND d.id = s.id;
      UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_2} AND d.id = s.id;
      UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_3} AND d.id = s.id;
      UPDATE agdc.dataset d SET metadata = jsonb_build_object('creation_dt', s.metadata#>>'{extent, center_dt}') || s.metadata FROM agdc.dataset s WHERE d.dataset_type_ref=${id_4} AND d.id = s.id;" |
      psql ${psql_args} -d ${dbname}
fi

log_info "Drop Schema"
echo "drop schema if exists cubedash cascade;" |
psql ${psql_args} -d ${dbname}

# Generate minimal product cache to enable Healthz checks
python3 -m cubedash.generate -C datacube_${DBNAME}.conf "${test_product1}" "${test_product2}" "${test_product3}" || true

log_info "Clustering "${test_product1}" "${test_product2}" "${test_product3}" - $(date)"
psql "${dbname}" -X -c 'cluster cubedash.dataset_spatial using "dataset_spatial_dataset_type_ref_center_time_idx";'

log_info "Testing a summary"
if ! python3 -m cubedash.summary.show -C datacube_${DBNAME}.conf "${test_product1}";
then
    log_info "Summary gen seems to have failed"
    exit 1
fi

# Now generate product summaries for all the products
python3 -m cubedash.generate -C datacube_${DBNAME}.conf --all || true

log_info "Clustering $(date)"
psql "${dbname}" -X -c 'cluster cubedash.dataset_spatial using "dataset_spatial_dataset_type_ref_center_time_idx";'
psql "${dbname}" -X -c 'create index tix_region_center ON cubedash.dataset_spatial (dataset_type_ref, region_code text_pattern_ops, center_time);'
log_info "Done $(date)"

log_info "Cubedash Database (${dbname}) updated on $(date)"
log_info "All Done $(date)"
