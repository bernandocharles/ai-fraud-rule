'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - Activity").getOrCreate()
output_path = sys.argv[1]
#Define the data path
src_path1 = f"file://{output_path}/timeseriesact/*.csv"
src_path2 = f"file://{output_path}/timeserieslogin/*.csv"

target_path = f"file://{output_path}/activity"

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df1 = spark.read.csv(src_path1, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df2 = spark.read.csv(src_path2, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")


if df1.rdd.isEmpty() == True and df2.rdd.isEmpty() == True:
    df1.createOrReplaceTempView("act1")
    df2.createOrReplaceTempView("act2")
    
    
    query = spark.sql("""SELECT *
                      from 
                      act1
                  """)

elif df1.rdd.isEmpty() == False and df2.rdd.isEmpty() == True:
    df1 = df1.withColumn("act_date", 
        F.when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
    )
    df1.createOrReplaceTempView("act1")
    
    df2.createOrReplaceTempView("act2")

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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status
                  FROM
                    act1
                  """).createOrReplaceTempView("act")
    
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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status,
                    cast(accopen1m as bigint) as accopen1m,
                    cast(accopen10m as bigint) as accopen10m,
                    cast(accopen1h as bigint) as accopen1h,
                    cast(accopen1d as bigint) as accopen1d
                  FROM
                    act1
                  """).createOrReplaceTempView("act_1")
    
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
                    CASE WHEN b.accopen1m IS NULL THEN 0 ELSE b.accopen1m END AS accopen1m,
                    CASE WHEN b.accopen10m IS NULL THEN 0 ELSE b.accopen10m END AS accopen10m,
                    CASE WHEN b.accopen1h IS NULL THEN 0 ELSE b.accopen1h END AS accopen1h,
                    CASE WHEN b.accopen1d IS NULL THEN 0 ELSE b.accopen1d END AS accopen1d
                      from 
                      act a
                      LEFT join act_1 b
                      ON a.id = b.id
                  """)

elif df1.rdd.isEmpty() == True and df2.rdd.isEmpty() == False:

    df2 = df2.withColumn("act_date", 
            F.when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
             .when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
        )
    
    df1.createOrReplaceTempView("act1")
    
    df2.createOrReplaceTempView("act2")


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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status
                  FROM
                    act2
                  """).createOrReplaceTempView("act")
    

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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status,
                    cast(login1m as bigint) as login1m,
                    cast(login5m as bigint) as login5m,
                    cast(login1h as bigint) as login1h
                  FROM
                    act2
                  """).createOrReplaceTempView("act_2")


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
                    CASE WHEN c.login1m IS NULL THEN 0 ELSE c.login1m END AS login1m,
                    CASE WHEN c.login5m IS NULL THEN 0 ELSE c.login5m END AS login5m,
                    CASE WHEN c.login1h IS NULL THEN 0 ELSE c.login1h END AS login1h
                      from 
                      act a
                      LEFT join act_2 c
                      ON a.id = c.id 
                  """)
    
elif df1.rdd.isEmpty() == False and df2.rdd.isEmpty() == False:
    df1 = df1.withColumn("act_date", 
        F.when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
    )
    df1.createOrReplaceTempView("act1")

    df2 = df2.withColumn("act_date", 
        F.when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("activity_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("activity_date", 'yyyy-MM-dd hh:mm:ss'))
        )
    
    df2.createOrReplaceTempView("act2")


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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status
                  FROM
                    act1
                  UNION
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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status
                  FROM
                    act2
                  """).createOrReplaceTempView("act")
    
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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status,
                    cast(accopen1m as bigint) as accopen1m,
                    cast(accopen10m as bigint) as accopen10m,
                    cast(accopen1h as bigint) as accopen1h,
                    cast(accopen1d as bigint) as accopen1d
                  FROM
                    act1
                  """).createOrReplaceTempView("act_1")


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
                    cast(activity_date as timestamp) as activity_date,
                    cast(activity_type as string) as activity_type,
                    cast(activity_type_name as string) as activity_type_name,
                    cast(activity_channel as string) as activity_channel,
                    cast(ip as string) as ip,
                    cast(country as string) as country,
                    cast(activity_status as string) as activity_status,
                    cast(login1m as bigint) as login1m,
                    cast(login5m as bigint) as login5m,
                    cast(login1h as bigint) as login1h
                  FROM
                    act2
                  """).createOrReplaceTempView("act_2")


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
                    CASE WHEN b.accopen1m IS NULL THEN 0 ELSE b.accopen1m END AS accopen1m,
                    CASE WHEN b.accopen10m IS NULL THEN 0 ELSE b.accopen10m END AS accopen10m,
                    CASE WHEN b.accopen1h IS NULL THEN 0 ELSE b.accopen1h END AS accopen1h,
                    CASE WHEN b.accopen1d IS NULL THEN 0 ELSE b.accopen1d END AS accopen1d,
                    CASE WHEN c.login1m IS NULL THEN 0 ELSE c.login1m END AS login1m,
                    CASE WHEN c.login5m IS NULL THEN 0 ELSE c.login5m END AS login5m,
                    CASE WHEN c.login1h IS NULL THEN 0 ELSE c.login1h END AS login1h
                      from 
                      act a
                      LEFT join act_1 b
                      ON a.id = b.id
                      LEFT join act_2 c
                      ON a.id = c.id 
                  """)


repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
