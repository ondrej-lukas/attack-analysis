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


#load strategy

probabilities = {21:0.263346, 22:0.248634,23:0.959599, 25:0.6516, 80:0.5, 81:0.161263, 443: 0.68306, 445: 0.863388, 1433: 0.371651, 2000:0.200172, 3306: 0.2762, 3389: 0.928962, 5900: 0.131671, 8000: 0.16186, 8080: 0.598594}
"""
#CZNIC STRATEGY
probabilities = {23:1, 2323:1, 22:1, 80:1, 8080:1, 8023:1, 2380:1}
"""

def find_port_class(d_port, state):
	if re.match( r'.*_.*S.*A', state):
		return "production"
	else:
		return "possible_HP"


#function which decides where to put honeypots
def place_honeypots(d_port, port_class,count,total_count,probabilities=probabilities):
	#is the flow open
	if port_class == "possible_HP" and d_port in probabilities.keys():
		# count the ratioon of free ports
		print("OK")
		r = count/total_count
		if r <= probabilities[d_port]:
			return True
		else:
			#roll the dice
			if random.uniform(0,1) < probabilities[d_port]:
				return True
	return False
#register functions for spark
place_honeypots_udf = F.udf(place_honeypots,T.BooleanType())
find_port_class_udf = F.udf(find_port_class,T.StringType())

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

#create df with openPorts
df = df.withColumn("portClass",find_port_class_udf(df["Dport"],df["State"])).select("DstAddr", "Dport", "State", "timestamp", "portClass","day", "SrcAddr").distinct()
#count production ports in each port number and total 
df_counts = df.select("DstAddr", "Dport", "day","portClass").distinct().groupBy("Dport","portClass", "day").count().join(df.select("DstAddr", "Dport", "day","portClass").distinct().groupBy("Dport","day").count().selectExpr("Dport as Dport", "day as day","count as total"),["Dport","Day"],how="right")
df =  df.join(df_counts, ["Dport","day","portClass"],how="left")

#place Honeypots
df_with_HP = df.withColumn("isHP", place_honeypots_udf(df["Dport"], df["portClass"],df["count"],df["total"])).select("DstAddr", "Dport", "State", "timestamp", "isHP", "portClass","day", "SrcAddr").distinct()

#filter out flows without honeypots
df_HP = df_with_HP.filter(df_with_HP.isHP).select("DstAddr", "Dport", "State", "timestamp", "isHP","day", "SrcAddr").orderBy("SrcAddr")

#get rid of duplicate honeypots (choose minimum timestamp)
window = Window.partitionBy('SrcAddr')
df_HP1 = df_HP.withColumn('minTime', F.min(F.col('timestamp')).over(window))
df_HP = df_HP1.filter(df_HP1.timestamp==df_HP1.minTime).select("DstAddr", "Dport",'timestamp',"day", "SrcAddr").distinct()

#get open ports (those we might save with honeypots)
df_open_ports = df_with_HP.filter(df_with_HP['portClass']=="production").select("timestamp","day","SrcAddr", "Dport", "State")

#join tables
tmp = df_HP.join(df_open_ports.selectExpr("timestamp as timestamp_PROD", "day as day", "SrcAddr as SrcAddr"),['SrcAddr','day'])
#filter out flows with sooner starttime than HP
tmp2 = tmp.filter(tmp.timestamp < tmp.timestamp_PROD)
#aggregate resutls
res = tmp2.groupBy(tmp.DstAddr,tmp.Dport,tmp.day).count()

#find out how many Honeypots we used in each day
df_HP_counts = df_HP.groupBy(df_HP.day).count().alias("HP_count")
results_per_day = res.groupBy(res.day).agg(F.sum('count').alias('saved_count')).join(df_counts,['day']).select('saved_count', "HP_count").collect()
print("Results:")
for row in results_per_day:
	print(row['count'], row['HP_count'])	