'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - InOut").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/transfer.csv"
target_path = f"file://{output_path}/inout"

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

spark.sql("""select trf.*,
to_timestamp(transaction_date) as ts, 
cast(transaction_date as TIMESTAMP) - INTERVAL 5 minutes as ts5m,
cast(transaction_date as TIMESTAMP) - INTERVAL 30 minutes as ts30m,
cast(transaction_date as TIMESTAMP) - INTERVAL 1 hours as ts1h
from trf 
""").createOrReplaceTempView("data1")


spark.sql("""select id, account_id, ts as ts5m, count(*) as trx5m, sum(transaction_amount) as amtout5m from
    (select a.id, a.account_id, dest_account_id, last_date, ts, ts5m, ts30m, ts1h, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts5m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts5m <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("out5m")

spark.sql("""select id, account_id, ts as ts30m, count(*) as trx30m, sum(transaction_amount) as amtout30m from
    (select a.id, a.account_id, dest_account_id, last_date, ts, ts5m, ts30m, ts1h, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts5m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts30m <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("out30m")

spark.sql("""select id, account_id, ts as ts1h, count(*) as trx1h, sum(transaction_amount) as amtout1h from
    (select a.id, a.account_id, dest_account_id, last_date, ts, ts5m, ts30m, ts1h, dates, transaction_amount from 
        (select id, account_id, transaction_date as last_date, ts, ts5m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts1h <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("out1h")


spark.sql("""select id, account_id, ts, sum(transaction_amount) as amtin5m from
(select a.id as id, a.account_id as account_id, a.dest_account_id as dest_account_id, a.last_date, a.ts as ts, a.ts5m, a.ts30m, a.ts1h, b.id as bid, b.account_id as baccid, b.dest_account_id as bdestaccid, b.dates, b.transaction_amount from
        (select id, account_id, dest_account_id, transaction_date as last_date, ts, ts5m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.dest_account_id and a.ts >= b.dates and a.ts5m <= b.dates) c
GROUP BY id, account_id, ts""").createOrReplaceTempView("in5m")

spark.sql("""select id, account_id, ts, sum(transaction_amount) as amtin30m from
(select a.id as id, a.account_id as account_id, a.dest_account_id as dest_account_id, a.last_date, a.ts as ts, a.ts5m, a.ts30m, a.ts1h, b.id as bid, b.account_id as baccid, b.dest_account_id as bdestaccid, b.dates, b.transaction_amount from
        (select id, account_id, dest_account_id, transaction_date as last_date, ts, ts5m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.dest_account_id and a.ts >= b.dates and a.ts30m <= b.dates) c
GROUP BY id, account_id, ts""").createOrReplaceTempView("in30m")

spark.sql("""select id, account_id, ts, sum(transaction_amount) as amtin1h from
(select a.id as id, a.account_id as account_id, a.dest_account_id as dest_account_id, a.last_date, a.ts as ts, a.ts5m, a.ts30m, a.ts1h, b.id as bid, b.account_id as baccid, b.dest_account_id as bdestaccid, b.dates, b.transaction_amount from
        (select id, account_id, dest_account_id, transaction_date as last_date, ts, ts5m, ts30m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, dest_account_id, ts as dates, transaction_amount from data1) b
    ON a.account_id = b.dest_account_id and a.ts >= b.dates and a.ts1h <= b.dates) c
GROUP BY id, account_id, ts""").createOrReplaceTempView("in1h")


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
                    b.amtout5m, 
                    CASE WHEN c.amtin5m IS NULL THEN 0 ELSE c.amtin5m END AS amtin5m,
                    CASE WHEN c.amtin5m IS NULL THEN 0 ELSE b.amtout5m/c.amtin5m END AS inout5m,
                    d.amtout30m, 
                    CASE WHEN e.amtin30m IS NULL THEN 0 ELSE e.amtin30m END AS amtin30m,
                    CASE WHEN e.amtin30m IS NULL THEN 0 ELSE d.amtout30m/e.amtin30m END AS inout30m,
                    f.amtout1h, 
                    CASE WHEN g.amtin1h IS NULL THEN 0 ELSE g.amtin1h END AS amtin1h,
                    CASE WHEN g.amtin1h IS NULL THEN 0 ELSE f.amtout1h/g.amtin1h END AS inout1h
                      from trf a
                      left join out5m b
                      ON a.id = b.id
                      left join in5m c
                      ON a.id = c.id
                      left join out30m d
                      ON a.id = d.id
                      left join in30m e
                      ON a.id = e.id
                      left join out1h f
                      ON a.id = f.id
                      left join in1h g
                      ON a.id = g.id
                  """)



#query.write.mode("overwrite").parquet(target_path)
query.repartition(1).write.mode("overwrite").csv(target_path, header=True)

