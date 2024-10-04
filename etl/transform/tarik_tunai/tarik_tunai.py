'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - CW").getOrCreate()
output_path = sys.argv[1]

#Define the data path
src_path1 = f"file://{output_path}/timeseriescw/*.csv"
src_path2 = f"file://{output_path}/timecw/*.csv"
src_path3 = f"file://{output_path}/inoutcw/*.csv"
src_path4 = f"file://{output_path}/velocitycw/*.csv"

target_path = f"file://{output_path}/tarik_tunai"

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df1 = spark.read.csv(src_path1, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df1 = df1.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df1.createOrReplaceTempView("cw1")

df2 = spark.read.csv(src_path2, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df2 = df2.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df2.createOrReplaceTempView("cw2")

df3 = spark.read.csv(src_path3, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df3 = df3.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df3.createOrReplaceTempView("cw3")

df4 = spark.read.csv(src_path4, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df4 = df4.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df4.createOrReplaceTempView("cw4")


spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_type as string) as account_type,
                    cast(account_status as string) as account_status,
                    cast(account_balance as double) as account_balance,
                    cast(cust_lat as double) as cust_lat,
                    cast(cust_lon as double) as cust_lon,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(transaction_date as timestamp) as transaction_date,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_limit as double) as transaction_limit,                    
                    cast(terminal_id as string) as terminal_id,
                    cast(terminal_network as string) as terminal_network,                    
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(transaction_status as string) as transaction_status,
                    cast(trx1m as bigint) as trxcw1m,
                    cast(trx5m as bigint) as trxcw5m,
                    cast(trx1h as bigint) as trxcw1h,
                    cast(trx1d as bigint) as trxcw1d
                  FROM
                    cw1
                  """).createOrReplaceTempView("cw_1")


spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_type as string) as account_type,
                    cast(account_status as string) as account_status,
                    cast(account_balance as double) as account_balance,
                    cast(cust_lat as double) as cust_lat,
                    cast(cust_lon as double) as cust_lon,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(transaction_date as timestamp) as transaction_date,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_limit as double) as transaction_limit,                    
                    cast(terminal_id as string) as terminal_id,
                    cast(terminal_network as string) as terminal_network,                    
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(transaction_status as string) as transaction_status,
                    cast(time_0_5 as bigint) as timecw_0_5,
                    cast(time_0_3 as bigint) as timecw_0_3,
                    cast(time_21_0 as bigint) as timecw_21_0
                  FROM
                    cw2
                  """).createOrReplaceTempView("cw_2")


spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_type as string) as account_type,
                    cast(account_status as string) as account_status,
                    cast(account_balance as double) as account_balance,
                    cast(cust_lat as double) as cust_lat,
                    cast(cust_lon as double) as cust_lon,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(transaction_date as timestamp) as transaction_date,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_limit as double) as transaction_limit,                    
                    cast(terminal_id as string) as terminal_id,
                    cast(terminal_network as string) as terminal_network,                    
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(transaction_status as string) as transaction_status,
                    cast(inout5m as double) as inoutcw5m,
                    cast(inout30m as double) as inoutcw30m,
                    cast(inout1h as double) as inoutcw1h
                  FROM
                    cw3
                  """).createOrReplaceTempView("cw_3")


spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_type as string) as account_type,
                    cast(account_status as string) as account_status,
                    cast(account_balance as double) as account_balance,
                    cast(cust_lat as double) as cust_lat,
                    cast(cust_lon as double) as cust_lon,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(transaction_date as timestamp) as transaction_date,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_limit as double) as transaction_limit,                    
                    cast(terminal_id as string) as terminal_id,
                    cast(terminal_network as string) as terminal_network,                    
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(transaction_status as string) as transaction_status,
                    cast(velocity as double) as velocitycw
                  FROM
                    cw4
                  """).createOrReplaceTempView("cw_4")



query = spark.sql("""SELECT DISTINCT
                    a.id,
                    a.account_id,
                    a.account_type,
                    a.account_status,
                    a.account_balance,
                    a.cust_lat,
                    a.cust_lon,
                    a.transaction_id,
                    a.transaction_amount,
                    a.transaction_date,
                    a.transaction_type,
                    a.transaction_channel,
                    a.transaction_limit,                    
                    a.terminal_id,
                    a.terminal_network,                    
                    a.ip,
                    a.country,
                    a.transaction_status,
                    a.trxcw1m,
                    a.trxcw5m,
                    a.trxcw1h,
                    a.trxcw1d,
                    b.timecw_0_5,
                    b.timecw_0_3,
                    b.timecw_21_0,
                    c.inoutcw5m,
                    c.inoutcw30m,
                    c.inoutcw1h,
                    d.velocitycw
                      from cw_1 a
                      inner join cw_2 b
                      ON a.id = b.id
                      inner join cw_3 c
                      ON a.id = c.id
                      inner join cw_4 d
                      ON a.id = d.id  
                    where a.trxcw1m IS NOT NULL and
                    a.trxcw5m IS NOT NULL and
                    a.trxcw1h IS NOT NULL and
                    a.trxcw1d IS NOT NULL and 
                    b.timecw_0_5 IS NOT NULL and
                    b.timecw_0_3 IS NOT NULL and
                    b.timecw_21_0 IS NOT NULL and
                    c.inoutcw5m IS NOT NULL and
                    c.inoutcw30m IS NOT NULL and
                    c.inoutcw1h IS NOT NULL and
                    d.velocitycw IS NOT NULL
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
