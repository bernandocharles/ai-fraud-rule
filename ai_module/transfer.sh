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

find $output_path/inout/ -maxdepth 1 -type f -name "*.csv" -delete 

find $output_path/account/ -maxdepth 1 -type f -name "*.csv" -delete

#find $output_path/country/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/ibft/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/season/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/small/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/time/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/timeseries/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/velocity/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/amtratio/ -maxdepth 1 -type f -name "*.csv" -delete

find $output_path/transfer/ -maxdepth 1 -type f -name "*.csv" -delete

echo "Run the ETL for inout ratio..."

sh $script_path/inout/inout.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/inout/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/inout/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/inout/inout.csv"' \;


echo "Run the ETL for number of account..."

sh $script_path/account/account.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/account/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/account/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/account/account.csv"' \;


echo "Run the ETL for IBFT..."

sh $script_path/ibft/ibft.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/ibft/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/ibft/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/ibft/ibft.csv"' \;


echo "Run the ETL for season..."

sh $script_path/season/season.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/season/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/season/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/season/season.csv"' \;


echo "Run the ETL for small amount transaction..."

sh $script_path/small/small.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/small/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/small/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/small/small.csv"' \;

echo "Run the ETL for transaction time..."

sh $script_path/time/time.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/time/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/time/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/time/time.csv"' \;

echo "Run the ETL for timeseries transactions..."

sh $script_path/timeseries/timeseries.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/timeseries/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/timeseries/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/timeseries/timeseries.csv"' \;


echo "Run the ETL for velocity..."

sh $script_path/velocity/velocity.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/velocity/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/velocity/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/velocity/velocity.csv"' \;


echo "Run the ETL for amount ratio..."

sh $script_path/amtratio/amtratio.sh $input_path $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/amtratio/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/amtratio/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/amtratio/amtratio.csv"' \;


echo "Run the ETL to join all derived features..."

sh $script_path/transfer/transfer.sh $output_path $global_config

retry_count=0;
while [[ $retry_count -lt $max_retries ]]; do
  if [[ $(ls $output_path/transfer/*.csv 2> /dev/null | wc -l) != 0 ]]; then
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

find $output_path/transfer/ -depth -name "part-*.csv" -exec sh -c 'f="{}"; mv -- "$f" "'$output_path'/transfer/transfer.csv"' \;

echo "All ETL runs have finished. Start of AI run..."
