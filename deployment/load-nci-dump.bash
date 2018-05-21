#!/usr/bin/env bash

set -eu
umask 0022

# NCI dates are in AEST
export TZ="Australia/Sydney"

# Optional first argument is day to load (eg. "yesterday")
dump_id="$(date "-d${1:-today}" +%Y%m%d)"
psql_args="-h dea-pipeline-dev.cfeq4wxgcaui.ap-southeast-2.rds.amazonaws.com -U dea_db_admin"
dump_file="/data/nci/105-${dump_id}-datacube.pgdump"
app_dir="/var/www/dea-dashboard"

archive_dir="archive"
summary_dir="${archive_dir}/${dump_id}"
dbname="nci_${dump_id}"

log_file="${summary_dir}/restore-$(date +'%dT%H%M').log"

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
	if [ ! -e ${dump_file} ];
	then
		# Fetch new one
		log_info "Downloading backup from NCI. If there's no credentials, you'll have to do this manually and rerun:"
		# Our public key is whitelisted in lpgs to scp the latest backup (only)
		# '-p' to preserve time the backup was taken: we refer to it below
		set -x
		scp -p "lpgs@r-dm.nci.org.au:/g/data/v10/agdc/backup/archive/105-${dump_id}-datacube.pgdump" "${dump_file}"
		set +x
	fi

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

[ -e "${dump_file}" ] && rm -v "${dump_file}"

## Summary generation

# get list of products
psql ${psql_args} "${dbname}" -X -c 'copy (select name from agdc.dataset_type order by name asc) to stdout' > "${summary_dir}/all-products.txt"

# Will load `datacube.conf` from current directory. Cubedash will use this directory too.
echo "
[datacube]
db_database: ${dbname}
" > datacube.conf

log_info "Restarting deadash (with updated dataset information)"
sudo systemctl restart deadash

log_info "Summary gen"
# We randomise the product list to more evenly space large/small products between workers
<"${summary_dir}/all-products.txt" sort -R | parallel --line-buffer -m -j4 /opt/conda/bin/python -m cubedash.generate --summaries-dir "${summary_dir}" || true

# Record date/time of DB backup, cubedash will show it as last update time
date -r "${dump_file}" > "${summary_dir}/generated.txt"

if [ ! -e "${summary_dir}/timeline.json" ];
then
	log_info "Summary gen failure: no overall summary"
	exit 1
fi

log_info "Switching to new summaries"
# Switch to new summaries
ln -snf "${summary_dir}" product-summaries

log_info "Restarting deadash (with updated summaries)"
sudo systemctl restart deadash

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


log_info "Taring old summaries"
cd "${archive_dir}"
find .  -maxdepth 1 -iname '2???????' -mtime +4 -exec tar --remove-files -czf {}.tar.gz {} \;


log_info "All Done $(date) ${summary_dir}"

