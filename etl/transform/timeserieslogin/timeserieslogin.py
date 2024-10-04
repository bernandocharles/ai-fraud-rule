'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - TimeseriesLogin").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/activity.csv"
target_path = f"file://{output_path}/timeserieslogin"

print(f"checking source path in {src_path}")
print(f"checking target path in {target_path}")

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df = spark.read.csv(src_path, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")

df = df.withColumn("act_date", 
        F.when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
    )

df.createOrReplaceTempView("act_ts")

spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,                    
                    cast(phone_number as string) as phone_number,
                    cast(device_id as string) as device_id,
                    cast(device_os as string) as device_os,
                    cast(nik as string) as nik,
                    cast(activity_id as string) as activity_id,
                    cast(activity_lat as double) as activity_lat,
                    cast(activity_lon as double) as activity_lon,
                    cast(act_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status
                  FROM
                    act_ts
                  where
                    cast(activity_type_name as string) = 'Login'
                    and
                    cast(activity_channel as string) IN ('6017', 'mobile')
                  """).createOrReplaceTempView("act")

spark.sql("""select act.*,
to_timestamp(activity_date) as ts, 
cast(activity_date as TIMESTAMP) - INTERVAL 1 minutes as ts1m,
cast(activity_date as TIMESTAMP) - INTERVAL 5 minutes as ts5m,
cast(activity_date as TIMESTAMP) - INTERVAL 1 hours as ts1h
from act 
""").createOrReplaceTempView("data1")

spark.sql("""select id, account_id, ts as ts1m, count(*) as login1m from
    (select a.id, a.account_id, last_date, ts, ts1m, ts5m, ts1h, dates from 
        (select id, account_id, activity_date as last_date, ts, ts1m, ts5m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, ts as dates from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts1m <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("data1m")

spark.sql("""select id, account_id, ts as ts5m, count(*) as login5m from
    (select a.id, a.account_id, last_date, ts, ts1m, ts5m, ts1h, dates from 
        (select id, account_id, activity_date as last_date, ts, ts1m, ts5m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, ts as dates from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts5m <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("data5m")

spark.sql("""select id, account_id, ts as ts1h, count(*) as login1h from
    (select a.id, a.account_id, last_date, ts, ts1m, ts5m, ts1h, dates from 
        (select id, account_id, activity_date as last_date, ts, ts1m, ts5m, ts1h from data1) a 
        CROSS JOIN 
        (select id, account_id, ts as dates from data1) b
    ON a.account_id = b.account_id and a.ts >= b.dates and a.ts1h <= b.dates) c
GROUP BY id, account_id, ts
""").createOrReplaceTempView("data1h")


query = spark.sql("""SELECT DISTINCT
                    a.id,
                    a.account_id,                    
                    a.phone_number,
                    a.device_id,
                    a.device_os,
                    a.nik,
                    a.activity_id,
                    a.activity_lat,
                    a.activity_lon,
                    a.activity_date,
                    a.activity_type,
                    a.activity_type_name,
                    a.activity_channel,
                    a.ip,
                    a.country,
                    a.activity_status,
                    CASE WHEN b.login1m IS NULL THEN 0 ELSE b.login1m END AS login1m,
                    CASE WHEN c.login5m IS NULL THEN 0 ELSE c.login5m END AS login5m,
                    CASE WHEN d.login1h IS NULL THEN 0 ELSE d.login1h END AS login1h
                      from data1 a
                      left join data1m b
                      ON a.id = b.id
                      left join data5m c
                      ON a.id = c.id 
                      left join data1h d
                      ON a.id = d.id  
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
