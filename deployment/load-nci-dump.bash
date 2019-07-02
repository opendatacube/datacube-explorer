#!/usr/bin/env bash

set -eu
umask 0022

# NCI dates are in AEST
export TZ="Australia/Sydney"

# Optional first argument is day to load (eg. "yesterday")
dump_id="$(date "-d${1:-today}" +%Y%m%d)"
psql_args="-h db-dev-eks-datacube-default.cxhoeczwhtar.ap-southeast-2.rds.amazonaws.com -U superuser"
dump_file="/data/nci/105-${dump_id}-datacube.pgdump"
app_dir="/var/www/dea-dashboard"

archive_dir="archive"
summary_dir="${archive_dir}/${dump_id}"
dbname="nci_${dump_id}"

log_file="${summary_dir}/restore-$(date +'%dT%H%M').log"
python=/opt/conda/bin/python

echo "======================="
echo "Loading dump: ${dump_id}"
echo "      dbname: ${dbname}"
echo "         app: ${app_dir}"
echo "        args: ${psql_args}"
echo "         log: ${app_dir}/${log_file}"
echo "======================="
echo " in 5, 4, 3..."
sleep 5

cd "${app_dir}"
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

if psql ${psql_args} -lqtA | grep -q "^$dbname|";
then 
    log_info "DB exists"
else
    if [[ ! -e "${dump_file}" ]];
    then
        # Fetch new one
        log_info "Downloading backup from NCI. If there's no credentials, you'll have to do this manually and rerun:"
        # Our public key is whitelisted in lpgs to scp the latest backup (only)
        # '-p' to preserve time the backup was taken: we refer to it below
        set -x
        scp -p "lpgs@r-dm.nci.org.au:/g/data/v10/agdc/backup/archive/105-${dump_id}-datacube.pgdump" "${dump_file}"
        set +x
    fi

    # Record date/time of DB backup, cubedash will show it as last update time
    date -r "${dump_file}" > "${summary_dir}/generated.txt"

    createdb ${psql_args} "$dbname"

    # TODO: the dump has "create extension" statements which will fail (but that's ok here)
    log_info "Restoring"
    # "no data for failed tables": when postgis extension fails to (re)initialise, don't populate its data
    # owner, privileges and tablespace are all NCI-specific.
    pg_restore -v --no-owner --no-privileges --no-tablespaces --no-data-for-failed-tables ${psql_args} -d "${dbname}" -j 4 "${dump_file}" || true

    # Hygiene
    log_info "Vacuuming"
    psql ${psql_args} "${dbname}" -c "vacuum analyze;"
fi

# Collect query stats on the new DB
psql ${psql_args} "${dbname}" -c "create extension if not exists pg_stat_statements;"

[[ -e "${dump_file}" ]] && rm -v "${dump_file}"

## Summary generation

# get list of products
psql ${psql_args} "${dbname}" -X -c 'copy (select name from agdc.dataset_type order by name asc) to stdout' > "${summary_dir}/all-products.txt"

# Will load `datacube.conf` from current directory. Cubedash will use this directory too.
echo "
[datacube]
db_database: ${dbname}
" > new-dump.conf

log_info "Summary gen"

$python -m cubedash.generate -C /etc/datacube.conf -C new-dump.conf -v --all -j 4 || true

echo "Clustering $(date)"
psql ${psql_args} "${dbname}" -X -c 'cluster cubedash.dataset_spatial using "dataset_spatial_dataset_type_ref_center_time_idx";'
psql ${psql_args} "${dbname}" -X -c 'create index tix_region_center ON cubedash.dataset_spatial (dataset_type_ref, region_code text_pattern_ops, center_time);'
echo "Done $(date)"

echo "Testing a summary"
if ! $python -m cubedash.summary.show -C /etc/datacube.conf -C new-dump.conf ls8_nbar_scene;
then
    log_info "Summary gen seems to have failed"
    exit 1
fi

log_info "Restarting deadash (with updated summaries)"
mv new-dump.conf datacube.conf
sudo systemctl restart deadash
cp -v "${summary_dir}/generated.txt" "${app_dir}/generated.txt"


log_info "Warming caching"
# Not strictly necessary, but users will see the new data sooner
bash /data/warm-cache.sh


log_info "Cleaning up old DBs"

old_databases=$(psql ${psql_args} -X -t -c "select datname from pg_database where datname similar to 'nci_\d{8}' and ((now() - split_part(datname, '_', 2)::date) > interval '3 days');")

for database in ${old_databases};
do
    echo "Dropping ${database}";
    dropdb "${database}";
done;


log_info "All Done $(date) ${summary_dir}"
log_info "Cubedash Database (${dbname}) updated on $(date)"

## Publish cubedash database update to SNS topic
AWS_PROFILE='default'
export AWS_PROFILE="${AWS_PROFILE}"
TOPIC_ARN=$(aws sns list-topics | grep "cubedash" | cut -f4 -d'"')

log_info "Publish new updated db (${dbname}) on AWS SNS topic"
aws sns publish --topic-arn "${TOPIC_ARN}" --message "${dbname}"
