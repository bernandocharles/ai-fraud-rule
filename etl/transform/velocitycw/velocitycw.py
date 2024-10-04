'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - VelocityCW").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/tarik_tunai.csv"
target_path = f"file://{output_path}/velocitycw"

print(f"checking source path in {src_path}")
print(f"checking target path in {target_path}")

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


df = spark.read.csv(src_path, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")

df = df.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )

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

spark.sql("""select cw.*, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY transaction_date ASC) rank
from cw
""").createOrReplaceTempView("data1")


spark.sql("""select a.*, b.id as prev_id, b.cust_lat as prev_lat, b.cust_lon as prev_lon, b.transaction_date as prev_date, b.rank as prev_rank, (unix_timestamp(a.transaction_date) - unix_timestamp(b.transaction_date))/3600 as hours
        from 
        (select * from data1) a 
        LEFT JOIN 
        (select id, account_id, cust_lat, cust_lon, transaction_date, rank from data1) b
    ON a.account_id = b.account_id and b.rank = a.rank - 1
""").createOrReplaceTempView("datarank")

spark.sql("""select a.*,
round((acos((sin(radians(prev_lat)) * sin(radians(cust_lat))) + ((cos(radians(prev_lat)) * cos(radians(cust_lat))) * (cos(radians(prev_lon) - radians(cust_lon))))) * (6371.0)), 4) as distance
from datarank a""").createOrReplaceTempView("datadist")

spark.sql("""select a.*, 
CASE WHEN a.distance IS NULL OR a.distance = 'NaN' THEN 0
WHEN a.distance IS NOT NULL and a.hours IS NOT NULL and a.hours != 0 THEN a.distance/a.hours
WHEN a.distance IS NOT NULL and a.hours IS NOT NULL and a.hours = 0 THEN 10000
WHEN a.hours IS NULL THEN 0
END AS velocity
from datadist a """).createOrReplaceTempView("vel")


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
                    CASE WHEN a.velocity IS NULL OR a.velocity = 'NaN' THEN 0 ELSE a.velocity END AS velocity 
                      from vel a
                  """)


repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
