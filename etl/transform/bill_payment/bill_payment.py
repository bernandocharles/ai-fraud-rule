'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - BP").getOrCreate()
output_path = sys.argv[1]
#Define the data path
src_path1 = f"file://{output_path}/timeseriesbp/*.csv"
src_path2 = f"file://{output_path}/timebp/*.csv"
src_path3 = f"file://{output_path}/amtratiobp/*.csv"

target_path = f"file://{output_path}/bill_payment"

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df1 = spark.read.csv(src_path1, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df1 = df1.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df1.createOrReplaceTempView("bp1")

df2 = spark.read.csv(src_path2, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df2 = df2.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df2.createOrReplaceTempView("bp2")

df3 = spark.read.csv(src_path3, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df3 = df3.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df3.createOrReplaceTempView("bp3")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(trx1m as bigint) as trxdest1m,
                    cast(trx10m as bigint) as trxdest10m,
                    cast(trx1h as bigint) as trxdest1h,
                    cast(trx1d as bigint) as trxdest1d
                  FROM
                    bp1
                  """).createOrReplaceTempView("bp_1")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,          
                    cast(timebp_0_5 as bigint) as timebp_0_5,
                    cast(timebp_0_3 as bigint) as timebp_0_3,
                    cast(timebp_21_0 as bigint) as timebp_21_0
                  FROM
                    bp2
                  """).createOrReplaceTempView("bp_2")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(amtratiobp30d as double) as amtratiobp30d,
                    cast(amtratiobp90d as double) as amtratiobp90d
                  FROM
                    bp3
                  """).createOrReplaceTempView("bp_3")



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
                    a.trxdest1m,
                    a.trxdest10m,
                    a.trxdest1h,
                    a.trxdest1d,
                    b.timebp_0_5,
                    b.timebp_0_3,
                    b.timebp_21_0,
                    c.amtratiobp30d,
                    c.amtratiobp90d                    
                      from bp_1 a
                      inner join bp_2 b
                      ON a.id = b.id
                      inner join bp_3 c
                      ON a.id = c.id 
                    where a.trxdest1m IS NOT NULL and 
                    a.trxdest10m IS NOT NULL and
                    a.trxdest1h IS NOT NULL and
                    a.trxdest1d IS NOT NULL and
                    b.timebp_0_5 IS NOT NULL and
                    b.timebp_0_3 IS NOT NULL and
                    b.timebp_21_0 IS NOT NULL and
                    c.amtratiobp30d IS NOT NULL and
                    c.amtratiobp90d IS NOT NULL
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
