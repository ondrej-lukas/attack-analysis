from __future__ import division
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
import pyspark.sql.types as T


conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

data = sqlContext.read.parquet("CTU-Flows_main/Flows.parquet/_yyyymd=2017-*")

#filter flows with dstIP outside of the university and srcIP inside range 80-83 mask 22
df = data.filter(data.Proto=="tcp").filter(data.DstAddr.startswith("147.32.")).filter(~data.SrcAddr.startswith("147.32.")).select("DstAddr", "Dport", "State", "StartTime")

df = df.withColumn('Dport', df["Dport"].cast(T.IntegerType()))
df = df.withColumn('day', unix_timestamp('StartTime', 'yyyy/MM/dd').cast(T.TimestampType()))
#df = df.withColumn('timestamp', unix_timestamp('StartTime', 'yyyy/MM/dd hh:mm:ss.SSSSSS').cast(T.TimestampType()))

df = df.where(col("Dport").isNotNull())

df_open = df.withColumn("closed_port", col('State').rlike('_[^S]*(R|F|)[^S]*$')).where(~col("closed_port"))
#df_open = df.withColumn("open_port", col('State').rlike('.*_.*S.*A')).distinct()
df_total_dst_per_day = df_open.select("DstAddr", "day").distinct().groupBy("day").count().selectExpr("day", "count as ActiveHostsCount")
df_open_dst_per_day = df_open.select("DstAddr","day","Dport").distinct().groupBy("Dport","day").count().selectExpr("day","Dport","count as DstAddrWithPortCount")

#join tables
df_tmp = df_open_dst_per_day.join(df_total_dst_per_day,["day"])

#count the probability
df_probabilities = df_tmp.withColumn('ratio', col("DstAddrWithPortCount")/col("ActiveHostsCount")).filter(col('ratio') > 0)
df_probabilities.write.parquet('open_port_probabilities_daily_2017.parquet')
#res = df_probabilities.sort(col('day')).collect()
#print("*****RESULTS START*******")
#for row in res:
#       print("{}, {}, {}".format(row['day'], row['Dport'], row["ratio"]))
print("******RESULTS END********")