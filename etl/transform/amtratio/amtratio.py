'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - AmtRatio").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/transfer.csv"
target_path = f"file://{output_path}/amtratio"

print(f"checking source path in {src_path}")
print(f"checking target path in {target_path}")

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df = spark.read.csv(src_path, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")

df = df.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )

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

spark.sql("""select trf.*,
to_timestamp(transaction_date) as ts, 
cast(transaction_date as TIMESTAMP) - INTERVAL 30 days as ts30d,
cast(transaction_date as TIMESTAMP) - INTERVAL 90 days as ts90d
from trf
""").createOrReplaceTempView("data1")



spark.sql("""select account_id, avg(transaction_amount) as avgamt30d from
    (select a.id, a.account_id, last_date, ts, ts30d, ts90d, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts30d, ts90d from data1) a 
        CROSS JOIN 
        (select id, account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts30d <= b.dates) c
GROUP BY account_id
""").createOrReplaceTempView("data30d")


spark.sql("""select account_id, avg(transaction_amount) as avgamt90d from
    (select a.id, a.account_id, last_date, ts, ts30d, ts90d, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts30d, ts90d from data1) a 
        CROSS JOIN 
        (select id, account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts90d <= b.dates) c
GROUP BY account_id
""").createOrReplaceTempView("data90d")



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
                    b.avgamt30d,
                    CASE WHEN a.transaction_amount/b.avgamt30d IS NULL THEN 1 ELSE a.transaction_amount/b.avgamt30d END AS amtratio30d,
                    c.avgamt90d, 
                    CASE WHEN a.transaction_amount/c.avgamt90d IS NULL THEN 1 ELSE a.transaction_amount/c.avgamt90d END AS amtratio90d 
                      from data1 a
                      left join data30d b
                      ON a.account_id = b.account_id
                      left join data90d c
                      ON a.account_id = c.account_id 
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
