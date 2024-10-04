'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - Transfer").getOrCreate()
output_path = sys.argv[1]

#Define the data path
src_path1 = f"file://{output_path}/account/*.csv"
src_path2 = f"file://{output_path}/ibft/*.csv"
src_path3 = f"file://{output_path}/inout/*.csv"
src_path4 = f"file://{output_path}/season/*.csv"
src_path5 = f"file://{output_path}/small/*.csv"
src_path6 = f"file://{output_path}/time/*.csv"
src_path7 = f"file://{output_path}/timeseries/*.csv"
src_path8 = f"file://{output_path}/velocity/*.csv"
src_path9 = f"file://{output_path}/amtratio/*.csv"

target_path = f"file://{output_path}/transfer"

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df1 = spark.read.csv(src_path1, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df1 = df1.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df1.createOrReplaceTempView("trf1")

df2 = spark.read.csv(src_path2, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df2 = df2.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df2.createOrReplaceTempView("trf2")

df3 = spark.read.csv(src_path3, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df3 = df3.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df3.createOrReplaceTempView("trf3")

df4 = spark.read.csv(src_path4, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df4 = df4.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df4.createOrReplaceTempView("trf4")

df5 = spark.read.csv(src_path5, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df5 = df5.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df5.createOrReplaceTempView("trf5")

df6 = spark.read.csv(src_path6, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df6 = df6.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df6.createOrReplaceTempView("trf6")

df7 = spark.read.csv(src_path7, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df7 = df7.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df7.createOrReplaceTempView("trf7")

df8 = spark.read.csv(src_path8, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df8 = df8.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df8.createOrReplaceTempView("trf8")

df9 = spark.read.csv(src_path9, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df9 = df9.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df9.createOrReplaceTempView("trf9")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(dest_acc as bigint) as dest_acc,
                    cast(acc as bigint) as acc
                  FROM
                    trf1
                  """).createOrReplaceTempView("trf_1")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(ibft_out as bigint) as ibft_out,
                    cast(ibft_in as bigint) as ibft_in
                  FROM
                    trf2
                  """).createOrReplaceTempView("trf_2")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(amtout5m as double) as amtout5m,
                    cast(amtin5m as double) as amtin5m,
                    cast(inout5m as double) as inout5m,
                    cast(amtout30m as double) as amtout30m,
                    cast(amtin30m as double) as amtin30m,
                    cast(inout30m as double) as inout30m,
                    cast(amtout1h as double) as amtout1h,
                    cast(amtin1h as double) as amtin1h,
                    cast(inout1h as double) as inout1h
                  FROM
                    trf3
                  """).createOrReplaceTempView("trf_3")

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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(trx_dec as bigint) as trx_dec
                  FROM
                    trf4
                  """).createOrReplaceTempView("trf_4")

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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(trx_5k as bigint) as trx_5k,
                    cast(trx_10k as bigint) as trx_10k
                  FROM
                    trf5
                  """).createOrReplaceTempView("trf_5")

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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(time_0_5 as bigint) as time_0_5,
                    cast(time_0_3 as bigint) as time_0_3,
                    cast(time_21_0 as bigint) as time_21_0
                  FROM
                    trf6
                  """).createOrReplaceTempView("trf_6")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(trx30s as bigint) as trx30s,
                    cast(trx1m as bigint) as trx1m,
                    cast(dest1m as bigint) as dest1m,
                    cast(trx30m as bigint) as trx30m,
                    cast(dest30m as bigint) as dest30m,
                    cast(trx1h as bigint) as trx1h
                  FROM
                    trf7
                  """).createOrReplaceTempView("trf_7")

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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(velocity as double) as velocity
                  FROM
                    trf8
                  """).createOrReplaceTempView("trf_8")


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
                    cast(transaction_date as timestamp) as transaction_date,
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
                    cast(transaction_status as string) as transaction_status,
                    cast(amtratio30d as double) as amtratio30d,
                    cast(amtratio90d as double) as amtratio90d
                  FROM
                    trf9
                  """).createOrReplaceTempView("trf_9")



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
                    a.dest_acc,
                    a.acc,
                    b.ibft_out,
                    b.ibft_in,
                    c.amtout5m,
                    c.amtin5m,
                    c.inout5m,
                    c.amtout30m,
                    c.amtin30m,
                    c.inout30m,
                    c.amtout1h,
                    c.amtin1h,
                    c.inout1h,
                    d.trx_dec,
                    e.trx_5k,
                    e.trx_10k,
                    f.time_0_5,
                    f.time_0_3,
                    f.time_21_0,
                    g.trx30s,
                    g.trx1m,
                    g.dest1m,
                    g.trx30m,
                    g.dest30m,
                    g.trx1h,
                    h.velocity,
                    i.amtratio30d,
                    i.amtratio90d
                      from trf_1 a
                      inner join trf_2 b
                      ON a.id = b.id
                      inner join trf_3 c
                      ON b.id = c.id 
                      inner join trf_4 d
                      ON c.id = d.id 
                      inner join trf_5 e
                      ON d.id = e.id
                      inner join trf_6 f
                      ON e.id = f.id
                      inner join trf_7 g
                      ON f.id = g.id
                      inner join trf_8 h
                      ON g.id = h.id
                      inner join trf_9 i
                      ON h.id = i.id 
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
