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

find $output_path/timeseriescw/ -maxdepth 1 -type f -name "*.csv" -delete 

find $output_path/inoutcw/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/timecw/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/velocitycw/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/tarik_tunai/ -maxdepth 1 -type f -name "*.csv" -delete

echo "Run the ETL for timeseries..."

sh $script_path/timeseriescw/timeseriescw.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/timeseriescw/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/timeseriescw/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/timeseriescw/timeseriescw.csv"' \;


echo "Run the ETL for time..."

sh $script_path/timecw/timecw.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/timecw/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/timecw/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/timecw/timecw.csv"' \;


echo "Run the ETL for inout ratio..."

sh $script_path/inoutcw/inoutcw.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/inoutcw/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/inoutcw/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/inoutcw/inoutcw.csv"' \;



echo "Run the ETL for velocity..."

sh $script_path/velocitycw/velocitycw.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/velocitycw/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/velocitycw/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/velocitycw/velocitycw.csv"' \;


echo "Run the ETL to join all derived features..."

sh $script_path/tarik_tunai/tarik_tunai.sh $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/tarik_tunai/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/tarik_tunai/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/tarik_tunai/tarik_tunai.csv"' \;

echo "All ETL runs have finished. Start of AI run..."
