#!/bin/bash
set -e

input_path=$1
output_path=$2
global_config=$3

echo "Checking global config..."

cd `dirname $0`
. $global_config

echo "Run spark submit with parameter input_path=$input_path with output_path=$output_path and global_config=$global_config"

/opt/spark/bin/spark-submit \
--master $SPARK_MASTER \
--executor-cores $EXECUTOR_CORES \
--executor-memory $EXECUTOR_MEMORY \
--conf $CONF \
--py-files $global_config \
account.py $input_path $output_path
