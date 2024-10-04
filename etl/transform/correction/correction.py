'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - Correction").getOrCreate()
input_path = sys.argv[1]
output_path = sys.argv[2]

#Define the data path
src_path = f"file://{input_path}/transaksi_internal.csv"
target_path = f"file://{output_path}/correction"

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

#df = df.withColumn("hours", F.hour("trx_date"))

df.createOrReplaceTempView("int_ts")

spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_type as string) as account_type,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_id_orig as string) as transaction_id_orig,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(trx_date as timestamp) as transaction_date,
                    cast(dates as date) as dates,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_type_name as string) as transaction_type_name,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_code as string) as transaction_code,
                    cast(is_db_cr as string) as is_db_cr,
                    cast(user_id as string) as user_id,
                    cast(spv_id as string) as spv_id,
                    cast(reversal_code as string) as reversal_code,
                    cast(reversal_code_name as string) as reversal_code_name
                  FROM
                    int_ts
                  where
                    cast(reversal_code as string) IN ("1", "3")
                  """).createOrReplaceTempView("int")


spark.sql("""select *, DENSE_RANK () OVER (PARTITION BY user_id, dates ORDER BY transaction_id_orig ASC) correction
		from int
""").createOrReplaceTempView("corr")


query = spark.sql("""SELECT DISTINCT
                    a.id,
                    a.account_id,
                    a.account_type,
                    a.transaction_id,
                    a.transaction_id_orig,
                    a.transaction_amount,
                    a.transaction_date,
                    a.dates,
                    a.transaction_type,
                    a.transaction_type_name,
                    a.transaction_channel,
                    a.transaction_code,
                    a.is_db_cr,
                    a.user_id,
                    a.spv_id,
                    a.reversal_code,
                    a.reversal_code_name,
                    CASE WHEN b.correction IS NULL THEN 0 ELSE b.correction END AS correction 
                      from int a 
                      left join corr b
                      ON a.id = b.id
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)

