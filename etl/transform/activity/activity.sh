#!/bin/bash
set -e

output_path=$1
global_config=$2

echo "Checking global config..."

cd `dirname $0`
. $global_config

echo "Run spark submit with parameter input_file_path=$input_file_path with output_path=$output_path and global_config=$global_config"

/opt/spark/bin/spark-submit \
--master $SPARK_MASTER \
--executor-cores $EXECUTOR_CORES \
--executor-memory $EXECUTOR_MEMORY \
--conf $CONF \
--py-files $global_config \
activity.py $output_path
