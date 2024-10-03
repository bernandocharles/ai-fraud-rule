#!/bin/bash
#sh feature.sh

input_path=$1
output_path=$2
script_path=$3
global_config=$4

# file config
sleep=30;
max_retries=5;

CURRENTDATE=`date +"%Y%m%d"`

echo "Start of AI FDS run"

echo "Running the ETL..."

find $output_path/correction/ -maxdepth 1 -type f -name "*.csv" -delete 

find $output_path/transaksi_internal/ -maxdepth 1 -type f -name "*.csv" -delete

echo "Run the ETL for correction..."

sh $script_path/correction/correction.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/correction/*.csv 2> /dev/null | wc -l) != 0 ]]; then
    echo "The run has finished!"
    break
  else
    retry_count=$(($retry_count + 1))
    if [[ $retry_count -lt $max_retries ]]; then
      echo "Retrying check file (${retry_count}/${max_retries}) in 30 second..."
      sleep $sleep
    else
      echo "max retry reached (${retry_count}/${max_retries})"
      exit 1
    fi
  fi
done

find $output_path/correction/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/correction/correction.csv"' \;


echo "Run the ETL to join all derived features..."

sh $script_path/transaksi_internal/transaksi_internal.sh $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/transaksi_internal/*.csv 2> /dev/null | wc -l) != 0 ]]; then
    echo "The run has finished!"
    break
  else
    retry_count=$(($retry_count + 1))
    if [[ $retry_count -lt $max_retries ]]; then
      echo "Retrying check file (${retry_count}/${max_retries}) in 30 second..."
      sleep $sleep
    else
      echo "max retry reached (${retry_count}/${max_retries})"
      exit 1
    fi
  fi
done

find $output_path/transaksi_internal/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/transaksi_internal/transaksi_internal.csv"' \;

echo "All ETL runs have finished. Start of AI run..."
