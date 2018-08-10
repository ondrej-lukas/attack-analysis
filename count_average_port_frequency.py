from __future__ import division
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.types as T
import re

def find_port_class(d_port, state):
	if re.match( r'.*_.*S.*A', state):
		return "production"
	else:
		return "possible_HP"

find_port_class_udf = F.udf(find_port_class,T.StringType())

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

data = sqlContext.read.parquet("CTU-Flows_main/Flows.parquet/_yyyymd=2017-*")

#filter flows with dstIP outside of the university and srcIP inside range 80-83 mask 22
df = data.filter(data.Proto=="tcp").filter(data.DstAddr.startswith("147.32.")).filter(~data.SrcAddr.startswith("147.32.")).select("DstAddr", "Dport", "State", "StartTime", "SrcAddr")

df = df.withColumn('Dport', df["Dport"].cast(T.IntegerType()))
df = df.withColumn('day', unix_timestamp('StartTime', 'yyyy/MM/dd').cast(T.TimestampType()))
df = df.withColumn('timestamp', unix_timestamp('StartTime', 'yyyy/MM/dd hh:mm:ss.SSSSSS').cast(T.TimestampType()))

df = df.withColumn("portClass",find_port_class_udf(df["Dport"],df["State"])).select("DstAddr", "Dport", "portClass","day").distinct()
res = df.filter(col("portClass")=="production").groupBy("Dport", "day").count().select("day","Dport","count")
result = res.groupBy(res.Dport).agg(F.avg('count').alias('average_count')).select("day","average_count").sort(col('average_count').desc()).head(100)
print("********RESULTS*************")
for row in RESULTS:
        print("{},{},{}".format(row["day"],row["Dport"],row["count"]))
print("********RESULTS END*************")
