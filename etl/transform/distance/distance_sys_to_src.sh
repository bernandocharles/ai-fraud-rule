#!/bin/bash
set -e
cd `dirname $0`

table="transfer"
mis_date=$(date -d "$1 +1 day" +%Y-%m-%d)
dt_id=$(date -d "$1" +%Y%m%d)
query="select * from record_$table where date(transaction_date)<='$dt_id' AND date(transaction_date)>=DATE_SUB('$dt_id', INTERVAL 180 DAY)"

# path=caddr/caddr_${dt_id}.csv

../../common/mysql_to_src "$query" "$dt_id" "$table"
