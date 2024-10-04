'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - TimeCW").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/tarik_tunai.csv"
target_path = f"file://{output_path}/timecw"

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

df = df.withColumn("hours", F.hour("trx_date"))

df.createOrReplaceTempView("cw_ts")

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
                    cast(trx_date as timestamp) as transaction_date,
                    cast(dates as date) as dates,
                    cast(hours as integer) as hours,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_limit as double) as transaction_limit,                    
                    cast(terminal_id as string) as terminal_id,
                    cast(terminal_network as string) as terminal_network,                    
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(transaction_status as string) as transaction_status
                  FROM
                    cw_ts                     
                  """).createOrReplaceTempView("cw")


spark.sql("""select *, RANK () OVER (PARTITION BY account_id, dates ORDER BY transaction_date ASC) time_0_5
		from cw
		where hours >= 0 AND hours <= 4
""").createOrReplaceTempView("trx_0_5")

spark.sql("""select *, RANK () OVER (PARTITION BY account_id, dates ORDER BY transaction_date ASC) time_0_3
		from cw
		where hours >= 0 AND hours <= 2
""").createOrReplaceTempView("trx_0_3")

spark.sql("""select *, RANK () OVER (PARTITION BY account_id, dates ORDER BY transaction_date ASC) time_21_0
		from cw
		where hours >= 21
""").createOrReplaceTempView("trx_21_0")


spark.sql("""select a.*, CASE WHEN b.time_0_5 IS NULL THEN 0 ELSE b.time_0_5 END AS time_0_5, 
                         CASE WHEN c.time_0_3 IS NULL THEN 0 ELSE c.time_0_3 END AS time_0_3,
                         CASE WHEN d.time_21_0 IS NULL THEN 0 ELSE d.time_21_0 END AS time_21_0
from cw a
left join trx_0_5 b
ON a.id = b.id
left join trx_0_3 c
ON a.id = c.id
left join trx_21_0 d
ON a.id = d.id
""").createOrReplaceTempView("times") 


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
                    CASE WHEN b.time_0_5 IS NULL THEN 0 ELSE b.time_0_5 END AS time_0_5, 
                    CASE WHEN c.time_0_3 IS NULL THEN 0 ELSE c.time_0_3 END AS time_0_3,
                    CASE WHEN d.time_21_0 IS NULL THEN 0 ELSE d.time_21_0 END AS time_21_0
                      from cw a 
                      left join trx_0_5 b
                      ON a.id = b.id
                      left join trx_0_3 c
                      ON a.id = c.id
                      left join trx_21_0 d
                      ON a.id = d.id
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
