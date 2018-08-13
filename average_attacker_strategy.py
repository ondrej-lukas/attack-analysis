from __future__ import division
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import pyspark.sql.types as T
import re
from pyspark.sql.window import Window
import random
import csv


conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

#filter vic jak 5 adres nebo vic jak 3 porty na jdne IP

#get data
data = sqlContext.read.parquet("CTU-Flows_main/Flows.parquet/_yyyymd=2018-3-7")

#filter flows with dstIP outside of the university and srcIP inside range 80-83 mask 22
df = data.filter(data.Proto=="tcp").filter(data.DstAddr.startswith("147.32.")).filter(~data.SrcAddr.startswith("147.32.")).select("DstAddr", "Dport", "State", "StartTime", "SrcAddr")

touchAddrLimit = 5
touchPortLimit = 3

#predelat typy IP, port

#select day from timestamp
df = df.withColumn('Dport', df["Dport"].cast(T.IntegerType()))
df = df.withColumn('day', unix_timestamp('StartTime', 'yyyy/MM/dd').cast(T.TimestampType()))
df = df.withColumn('timestamp', unix_timestamp('StartTime', 'yyyy/MM/dd hh:mm:ss.SSSSSS').cast(T.TimestampType()))

srcAddrs = df.select('SrcAddr','DstAddr','Dport','day').distinct().groupBy('SrcAddr', 'day').agg(F.countDistinct('DstAddr').alias('addrCount'), F.countDistinct('Dport').alias('portCount')).filter((col('addrCount')>=touchAddrLimit) | (col('portCount')>=touchPortLimit)).select('SrcAddr',"day")
df = df.join(srcAddrs, ['SrcAddr','day'], 'leftsemi')
#distinct SrcAddr and DSTport
res = df.select("SrcAddr", "day","Dport")distinct().groupBy("day", "Dport").count().selectExpr("day","Dport","count as connectionCount")
result = res.groupBy(df.Dport).agg(F.avg('connectionCount').alias("average_count")).select("Dport","average_count").sort(col('average_count').desc()).head(100)
print("********RESULTS*************")
for row in result:
    print("{},{}".format(row["Dport"],row["average_count"]))
print("********RESULTS END*************")