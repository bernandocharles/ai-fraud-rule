#!/bin/bash
set -e
cd `dirname $0`
. ../../global_config.py

DT_ID=$(date -d "$1" +%Y%m%d)

# RAW_PATH=/user/nobu/raw/dhis/dt_id=$DT_ID
# FULL_PATH=$(cd `dirname $0` && pwd)

kinit -kt /home/hdp-user1/hdp-user1.keytab hdp-user1@DDV.ID

# hadoop fs -rm -r -skipTrash $RAW_PATH

spark-submit \
--master yarn \
--deploy-mode cluster \
--executor-cores 2 \
--executor-memory 2g \
--num-executors 2 \
--py-files ../../global_config.py \
--queue $YARN_QUEUE_NAME \
--principal $KERBEROS_PRINCIPAL \
--keytab $KERBEROS_KEYTAB \
country_src_to_raw.py $DT_ID

# if [[ $? -gt 0 ]]
# then
#    exit 1
# fi

#add partition
#hive -e "alter table ${HIVE_SCHEMA_PREFIX}_raw.dhis add IF NOT EXISTS partition (dt_id='$DT_ID')"
