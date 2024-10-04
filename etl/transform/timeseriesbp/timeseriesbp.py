'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - TimeseriesBP").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/bill_payment.csv"
target_path = f"file://{output_path}/timeseriesbp"

print(f"checking source path in {src_path}")
print(f"checking target path in {target_path}")

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df = spark.read.csv(src_path, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")

df = df.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )

df.createOrReplaceTempView("bp_ts")

spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_type as string) as account_type,
                    cast(account_status as string) as account_status,
                    cast(account_balance as double) as account_balance,
                    cast(phone_number as string) as phone_number,
                    cast(dest_phone_number as string) as dest_phone_number,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(trx_date as timestamp) as transaction_date,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_type_name as string) as transaction_type_name,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(terminal_id as string) as terminal_id,
                    cast(terminal_network as string) as terminal_network,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(provider_id as string) as provider_id,
                    cast(cust_lat as double) as cust_lat,
                    cast(cust_lon as double) as cust_lon,
                    cast(transaction_status as string) as transaction_status
                  FROM
                    bp_ts
                  where
                    transaction_type_name IN ('Top Up GoPay', 'Top Up Paytren', 'Pembayaran XL Postpaid', 
                    'Pembelian XL Paket Data', 'Pembelian Pulsa Internet Telkomsel', 'Pembelian XL Postpaid',
                    'Pembelian Indosat Prepaid', 'Pembayaran Indosat Postpaid', 'Pembelian XL Prepaid',
                    'Pembelian Pulsa Kartu AS & Simpati', 'Pembayaran Tagihan Kartu Halo', 'Top Up OVO')
                  """).createOrReplaceTempView("bp")

spark.sql("""select bp.*,
to_timestamp(transaction_date) as ts, 
cast(transaction_date as TIMESTAMP) - INTERVAL 1 minutes as ts1m,
cast(transaction_date as TIMESTAMP) - INTERVAL 10 minutes as ts10m,
cast(transaction_date as TIMESTAMP) - INTERVAL 1 hours as ts1h,
cast(transaction_date as TIMESTAMP) - INTERVAL 24 hours as ts1d
from bp
""").createOrReplaceTempView("data1")



spark.sql("""select id, dest_phone_number, ts as ts1m, count(*) as trx1m from
    (select a.id, a.dest_phone_number, last_date, ts, ts1m, ts10m, ts1h, ts1d, dates, transaction_amount from 
        (select id, dest_phone_number, transaction_date as last_date, ts, ts1m, ts10m, ts1h, ts1d from data1) a 
        CROSS JOIN 
        (select id, dest_phone_number, ts as dates, transaction_amount from data1) b
    ON a.dest_phone_number = b.dest_phone_number and a.ts >= b.dates and a.ts1m <= b.dates) c
GROUP BY id, dest_phone_number, ts
""").createOrReplaceTempView("data1m")

spark.sql("""select id, dest_phone_number, ts as ts10m, count(*) as trx10m from
    (select a.id, a.dest_phone_number, last_date, ts, ts1m, ts10m, ts1h, ts1d, dates, transaction_amount from 
        (select id, dest_phone_number, transaction_date as last_date, ts, ts1m, ts10m, ts1h, ts1d from data1) a 
        CROSS JOIN 
        (select id, dest_phone_number, ts as dates, transaction_amount from data1) b
    ON a.dest_phone_number = b.dest_phone_number and a.ts >= b.dates and a.ts10m <= b.dates) c
GROUP BY id, dest_phone_number, ts
""").createOrReplaceTempView("data10m")

spark.sql("""select id, dest_phone_number, ts as ts1h, count(*) as trx1h from
    (select a.id, a.dest_phone_number, last_date, ts, ts1m, ts10m, ts1h, ts1d, dates, transaction_amount from 
        (select id, dest_phone_number, transaction_date as last_date, ts, ts1m, ts10m, ts1h, ts1d from data1) a 
        CROSS JOIN 
        (select id, dest_phone_number, ts as dates, transaction_amount from data1) b
    ON a.dest_phone_number = b.dest_phone_number and a.ts >= b.dates and a.ts1h <= b.dates) c
GROUP BY id, dest_phone_number, ts
""").createOrReplaceTempView("data1h")

spark.sql("""select id, dest_phone_number, ts as ts1d, count(*) as trx1d from
    (select a.id, a.dest_phone_number, last_date, ts, ts1m, ts10m, ts1h, ts1d, dates, transaction_amount from 
        (select id, dest_phone_number, transaction_date as last_date, ts, ts1m, ts10m, ts1h, ts1d from data1) a 
        CROSS JOIN 
        (select id, dest_phone_number, ts as dates, transaction_amount from data1) b
    ON a.dest_phone_number = b.dest_phone_number and a.ts >= b.dates and a.ts1d <= b.dates) c
GROUP BY id, dest_phone_number, ts
""").createOrReplaceTempView("data1d")



query = spark.sql("""SELECT DISTINCT
                    a.id,
                    a.account_id,
                    a.account_type,
                    a.account_status,
                    a.account_balance,
                    a.phone_number,
                    a.dest_phone_number,
                    a.transaction_id,
                    a.transaction_amount,
                    a.transaction_date,
                    a.transaction_type,
                    a.transaction_type_name,
                    a.transaction_channel,
                    a.terminal_id,
                    a.terminal_network,
                    a.ip,
                    a.country,
                    a.provider_id,
                    a.cust_lat,
                    a.cust_lon,
                    a.transaction_status,
                    CASE WHEN b.trx1m IS NULL THEN 0 ELSE b.trx1m END AS trx1m,
                    CASE WHEN c.trx10m IS NULL THEN 0 ELSE c.trx10m END AS trx10m,
                    CASE WHEN d.trx1h IS NULL THEN 0 ELSE d.trx1h END AS trx1h,
                    CASE WHEN e.trx1d IS NULL THEN 0 ELSE e.trx1d END AS trx1d
                      from data1 a
                      left join data1m b
                      ON a.id = b.id
                      left join data10m c
                      ON a.id = c.id 
                      left join data1h d
                      ON a.id = d.id 
                      left join data1d e
                      ON a.id = e.id 
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
