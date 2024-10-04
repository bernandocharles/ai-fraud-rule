'''
Created on May 11, 2023

@author: Charles
'''

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
import sys
import global_config

spark = SparkSession.builder.appName("FDS AI - Internal").getOrCreate()
output_path = sys.argv[1]
#Define the data path
src_path1 = f"file://{output_path}/correction/*.csv"

target_path = f"file://{output_path}/transaksi_internal"

# Compatibility script for spark version 2 running on spark 3
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df1 = spark.read.csv(src_path1, sep=",", header="true", inferSchema= 'true', timestampFormat="yyyy-MM-dd hh:mm:ss")
df1 = df1.withColumn("trx_date", 
        F.when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
         .when(F.col("transaction_date").rlike("\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{1,2}"), F.to_timestamp("transaction_date", 'yyyy-MM-dd hh:mm:ss'))
    )
df1.createOrReplaceTempView("int1")


spark.sql("""
                  SELECT
                    cast(id as bigint) as id,
                    cast(account_id as string) as account_id,
                    cast(account_type as string) as account_type,
                    cast(transaction_id as string) as transaction_id,
                    cast(transaction_id_orig as string) as transaction_id_orig,
                    cast(transaction_amount as double) as transaction_amount,
                    cast(transaction_date as timestamp) as transaction_date,
                    cast(transaction_type as string) as transaction_type,
                    cast(transaction_type_name as string) as transaction_type_name,
                    cast(transaction_channel as string) as transaction_channel,
                    cast(transaction_code as string) as transaction_code,
                    cast(is_db_cr as string) as is_db_cr,
                    cast(user_id as string) as user_id,
                    cast(spv_id as string) as spv_id,
                    cast(reversal_code as string) as reversal_code,
                    cast(reversal_code_name as string) as reversal_code_name,
                    cast(correction as bigint) as correction
                  FROM
                    int1
                  """).createOrReplaceTempView("int_1")


query = spark.sql("""SELECT DISTINCT
                    a.id,
                    a.account_id,
                    a.account_type,
                    a.transaction_id,
                    a.transaction_id_orig,
                    a.transaction_amount,
                    a.transaction_date,
                    a.transaction_type,
                    a.transaction_type_name,
                    a.transaction_channel,
                    a.transaction_code,
                    a.is_db_cr,
                    a.user_id,
                    a.spv_id,
                    a.reversal_code,
                    a.reversal_code_name,
                    a.correction
                      from int_1 a
                    where a.correction IS NOT NULL
                  """)

repartitionDF = query.repartition(1)
repartitionDF.write.mode("overwrite").csv(target_path, header=True)
