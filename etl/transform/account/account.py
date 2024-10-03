'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - Account").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/transfer.csv"
target_path = f"file://{output_path}/account"

print(f"checking source path in {src_path}")
print(f"checking target path in {target_path}")

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df = spark.read.csv(src_path, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")

df = df.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )

df = df.withColumn("dates", F.to_date("trx_date", 'yyyy-MM-dd hh:mm:ss'))

df.createOrReplaceTempView("trf_ts")

spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_bank as string) as account_bank,
                    cast(account_type as string) as account_type,
                    cast(account_status as string) as account_status,
                    cast(account_balance as double) as account_balance,
                    cast(account_country as string) as account_country,
                    cast(account_lat as double) as account_lat,
                    cast(account_lon as double) as account_lon,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(trx_date as timestamp) as transaction_date,
                    cast(dates as date) as dates,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_type_name as string) as transaction_type_name,
                    cast(transaction_category as string) as transaction_category,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_limit as double) as transaction_limit,
                    cast(terminal_id as string) as terminal_id,
                    cast(terminal_network as string) as terminal_network,
                    cast(dest_account_id as string) as dest_account_id,
                    cast(dest_bank as string) as dest_bank,
                    cast(dest_account_type as string) as dest_account_type,
                    cast(dest_account_status as string) as dest_account_status,
                    cast(dest_account_balance as double) as dest_account_balance,
                    cast(dest_account_country as string) as dest_account_country,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(transaction_status as string) as transaction_status
                  FROM
                    trf_ts
                  """).createOrReplaceTempView("trf")

spark.sql("""select *, DENSE_RANK () OVER (PARTITION BY account_id, dates ORDER BY dest_account_id ASC) dest_acc
		from trf
""").createOrReplaceTempView("number_dest_acc")

spark.sql("""select *, DENSE_RANK () OVER (PARTITION BY dest_account_id, dates ORDER BY account_id ASC) acc
		from trf
""").createOrReplaceTempView("number_acc")

spark.sql("""select a.*, b.dest_acc, c.acc 
from trf a
left join number_dest_acc b
ON a.id = b.id
left join number_acc c
ON a.id = c.id
""").createOrReplaceTempView("accounts") 


query = spark.sql("""SELECT DISTINCT
                    a.id,
                    a.account_id,
                    a.account_bank,
                    a.account_type,
                    a.account_status,
                    a.account_balance,
                    a.account_country,
                    a.account_lat,
                    a.account_lon,
                    a.transaction_id,
                    a.transaction_amount,
                    a.transaction_date,
                    a.transaction_type,
                    a.transaction_type_name,
                    a.transaction_category,
                    a.transaction_channel,
                    a.transaction_limit,
                    a.terminal_id,
                    a.terminal_network,
                    a.dest_account_id,
                    a.dest_bank,
                    a.dest_account_type,
                    a.dest_account_status,
                    a.dest_account_balance,
                    a.dest_account_country,
                    a.ip,
                    a.country,
                    a.transaction_status,
                    CASE WHEN b.dest_acc IS NULL THEN 0 ELSE b.dest_acc END AS dest_acc,
                    CASE WHEN c.acc IS NULL THEN 0 ELSE c.acc END AS acc  
                      from trf a
                      left join number_dest_acc b
                      ON a.id = b.id
                      left join number_acc c
                      ON a.id = c.id
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
