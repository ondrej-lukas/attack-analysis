from __future__ import division
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.types as T
import re

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

def find_port_class(d_port, state):
    if re.match( r'.*_.*S.*A', state):
        return "production"
    else:
        return "possible_HP"

def count_ratio(hosts_with_port, total_hosts):
		return hosts_with_port/total_hosts

find_port_class_udf = F.udf(find_port_class,T.StringType())
count_ratio_udf = F.udf(count_ratio,T.FloatType())

data = sqlContext.read.parquet("CTU-Flows_main/Flows.parquet/_yyyymd=2018-3-7")

#filter flows with dstIP outside of the university and srcIP inside range 80-83 mask 22
df = data.filter(data.Proto=="tcp").filter(data.DstAddr.startswith("147.32.")).filter(~data.SrcAddr.startswith("147.32.")).select("DstAddr", "Dport", "State", "StartTime", "SrcAddr")

df = df.withColumn('Dport', df["Dport"].cast(T.IntegerType()))
df = df.withColumn('day', unix_timestamp('StartTime', 'yyyy/MM/dd').cast(T.TimestampType()))
df = df.withColumn('timestamp', unix_timestamp('StartTime', 'yyyy/MM/dd hh:mm:ss.SSSSSS').cast(T.TimestampType()))

df = df.withColumn("portClass",find_port_class_udf(df["Dport"],df["State"])).select("DstAddr", "Dport", "portClass","day").filter(col("portClass")=="production").distinct().select("DstAddr", "Dport", "day")
df_total_dst_per_day = df.select("DstAddr", "day").distinct().groupBy("day").count().selectExpr("day", "count as total_address_count")
df_counts_per_port = df.groupBy("Dport", "day").count().selectExpr("day","Dport","count as DstAddrWithPortCount")
res = df_counts_per_port.join(df_total_dst_per_day,["day"])
#count the ratio
res = res.withColumn("ratio", count_ratio_udf(res['DstAddrWithPortCount'],res['total_address_count'])).select("day","Dport","ratio")
#average over days
result = res.groupBy(res.Dport).agg(F.avg('ratio').alias('average_ratio')).select("Dport","average_ratio").sort(col('average_ratio').desc()).head(100)
print("********RESULTS*************")
for row in result:
    print("{},{}".format(row["Dport"],row["average_ratio"]))
print("********RESULTS END*************")
