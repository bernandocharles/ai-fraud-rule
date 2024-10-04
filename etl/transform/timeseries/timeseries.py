'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - Timeseries").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/transfer.csv"
target_path = f"file://{output_path}/timeseries"

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
cast(transaction_date as TIMESTAMP) - INTERVAL 30 seconds as ts30s,
cast(transaction_date as TIMESTAMP) - INTERVAL 1 minutes as ts1m,
cast(transaction_date as TIMESTAMP) - INTERVAL 30 minutes as ts30m,
cast(transaction_date as TIMESTAMP) - INTERVAL 1 hours as ts1h
from trf 
""").createOrReplaceTempView("data1")

spark.sql("""select id, account_id, ts as ts30s, count(*) as trx30s, sum(transaction_amount) as amt30s, count(distinct dest_account_id) as dest30s from
    (select a.id, a.account_id, dest_account_id, last_date, ts, ts30s, ts1m, ts30m, ts1h, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts30s, ts1m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts30s <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("data30s")

spark.sql("""select id, account_id, ts as ts1m, count(*) as trx1m, sum(transaction_amount) as amt1m, count(distinct dest_account_id) as dest1m from
    (select a.id, a.account_id, dest_account_id, last_date, ts, ts30s, ts1m, ts30m, ts1h, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts30s, ts1m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts1m <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("data1m")

spark.sql("""select id, account_id, ts as ts30m, count(*) as trx30m, sum(transaction_amount) as amt30m, count(distinct dest_account_id) as dest30m from
    (select a.id, a.account_id, dest_account_id, last_date, ts, ts30s, ts1m, ts30m, ts1h, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts30s, ts1m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts30m <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("data30m")

spark.sql("""select id, account_id, ts as ts1h, count(*) as trx1h, sum(transaction_amount) as amt1h, count(distinct dest_account_id) as dest1h from
    (select a.id, a.account_id, dest_account_id, last_date, ts, ts30s, ts1m, ts30m, ts1h, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts30s, ts1m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts1h <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("data1h")


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
                    CASE WHEN b.trx30s IS NULL THEN 0 ELSE b.trx30s END AS trx30s,
                    CASE WHEN c.trx1m IS NULL THEN 0 ELSE c.trx1m END AS trx1m,
                    CASE WHEN c.dest1m IS NULL THEN 0 ELSE c.dest1m END AS dest1m,
                    CASE WHEN d.trx30m IS NULL THEN 0 ELSE d.trx30m END AS trx30m,
                    CASE WHEN d.dest30m IS NULL THEN 0 ELSE d.dest30m END AS dest30m,
                    CASE WHEN e.trx1h IS NULL THEN 0 ELSE e.trx1h END AS trx1h 
                      from data1 a
                      left join data30s b
                      ON a.id = b.id
                      left join data1m c
                      ON b.id = c.id 
                      left join data30m d
                      ON c.id = d.id 
                      left join data1h e
                      ON d.id = e.id
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
