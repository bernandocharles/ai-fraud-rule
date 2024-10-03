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

find $output_path/timeseriesbp/ -maxdepth 1 -type f -name "*.csv" -delete 

find $output_path/timebp/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/amtratiobp/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/bill_payment/ -maxdepth 1 -type f -name "*.csv" -delete

echo "Run the ETL for timeseries..."

sh $script_path/timeseriesbp/timeseriesbp.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/timeseriesbp/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/timeseriesbp/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/timeseriesbp/timeseriesbp.csv"' \;


echo "Run the ETL for time..."

sh $script_path/timebp/timebp.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/timebp/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/timebp/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/timebp/timebp.csv"' \;


echo "Run the ETL for amount ratio..."

sh $script_path/amtratiobp/amtratiobp.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/amtratiobp/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/amtratiobp/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/amtratiobp/amtratiobp.csv"' \;


echo "Run the ETL to join all derived features..."

sh $script_path/bill_payment/bill_payment.sh $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/bill_payment/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/bill_payment/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/bill_payment/bill_payment.csv"' \;

echo "All ETL runs have finished. Start of AI run..."
