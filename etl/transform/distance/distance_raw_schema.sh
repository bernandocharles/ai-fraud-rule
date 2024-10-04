#!/bin/bash

set -e
cd `dirname $0`
. ../../global_config.py

hive -e "
CREATE SCHEMA IF NOT EXISTS ${HIVE_SCHEMA_PREFIX};

DROP TABLE IF EXISTS ${HIVE_SCHEMA_PREFIX}.timeseries;

CREATE EXTERNAL TABLE ${HIVE_SCHEMA_PREFIX}.timeseries(
	`id` bigint,
  	`account_id` string,
  	`account_bank` string,
  	`account_type` string,
  	`account_status` string,
  	`account_balance` double,
  	`account_country` string,
  	`account_lat` double,
  	`account_lon` double,
  	`transaction_id` string,
 	`transaction_amount` double,
  	`transaction_date` timestamp,
  	`transaction_type` string,
  	`transaction_type_name` string,
  	`transaction_category` string,
  	`transaction_channel` string,
  	`transaction_limit` double,
  	`terminal_id` string,
  	`terminal_network` string,
  	`dest_account_id` string,
  	`dest_bank` string,
  	`dest_account_type` string,
  	`dest_account_status` string,
  	`dest_account_balance` double,
  	`dest_account_country` string,
  	`ip` string,
  	`country` string,
  	`transaction_status` string
)
  
STORED AS parquet
LOCATION '${HDFS_PATH_PREFIX}/raw/timeseries';

MSCK REPAIR TABLE ${HIVE_SCHEMA_PREFIX}.timeseries SYNC PARTITIONS;
"
