#!/usr/bin/env bash

set -eu
umask 0022

echo "Starting restore $(date)"

dump_id="$(date +%Y%m%d)"
psql_args="-h dea-pipeline-dev.cfeq4wxgcaui.ap-southeast-2.rds.amazonaws.com -U dea_db_admin"
dump_file="/data/tmp/105-${dump_id}-datacube.pgdump"
app_dir="/var/www/dea-dashboard"

if [ ! -e ${dump_file} ];
then
	# Fetch new one
	echo "Downloading backup from NCI. If there's no credentials, you'll have to do this manually and rerun:"
	set -x
	scp "r-dm.nci.org.au:/g/data/v10/agdc/backup/archive/105-${dump_id}-datacube.pgdump" "${dump_file}"
	set +x
fi

cd "${app_dir}"
summary_dir="archive/${dump_id}"
dbname="nci_${dump_id}"

echo "======================="
echo "Loading dump: ${dump_id}"
echo "      dbname: ${dbname}"
echo "         app: ${app_dir}"
echo "        args: ${psql_args}"
echo "in 5..4..3..."
echo "======================="
sleep 5

createdb ${psql_args} "$dbname"

mkdir -p "${summary_dir}"

# TODO: the dump has "create extension" statements which will fail (but that's ok here)
echo "Restoring"
# "no data for failed tables": when postgis extension fails to (re)initialise, don't populate its data
# owner, privileges and tablespace are all NCI-specific.
pg_restore -v --no-owner --no-privileges --no-tablespaces --no-data-for-failed-tables ${psql_args} -d "${dbname}" -j 4 "${dump_file}" &>> "${summary_dir}/restore-${dump_id}-$(date +'%dT%H%M').log"


# Hygiene
echo "Vacuuming"
psql ${psql_args} "${dbname}" -c "vacuum analyze;"

## Summary generation

# get list of products
psql ${psql_args} "${dbname}" -c 'copy (select name from agdc.dataset_type order by name asc) to stdout' > "${summary_dir}/all-products.txt"

# Will load `datacube.conf` from current directory. Cubedash will use this directory too.
echo "
[datacube]
db_database: ${dbname}
" > datacube.conf

echo "Summary gen"
# We randomise the product list to more evenly space large/small products between workers
<"${summary_dir}/all-products.txt" sort -R | parallel --line-buffer -m -j4 /opt/conda/bin/python -m cubedash.generate --summaries-dir "${summary_dir}" &>> "${summary_dir}/gen-$(date +'%dT%H%M').log"

# Switch to new summaries
ln -snf "${summary_dir}" product-summaries

sudo systemctl restart deadash

# Not strictly necessary, but users will see the new data sooner
bash /data/warm-cache.sh

rm "${dump_file}"
echo "All Done $(date) ${summary_dir}"

