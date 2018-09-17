from __future__ import division
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit

import pyspark.sql.types as T
import re
from pyspark.sql.window import Window
import random

SrcAddrSampleRatio= 0.01
random_seed = 42

touchAddrLimit = 5
touchPortLimit = 3

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)




#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#################################################################
#EXPERIMENT SETUP:
## STRATEGY
#load strategy
probabilities_mix = {21:0.263346, 22:0.248634,23:0.959599, 25:0.6516, 80:0.5, 81:0.161263, 443: 0.68306, 445: 0.863388, 1433: 0.371651, 2000:0.200172, 3306: 0.2762, 3389: 0.928962, 5900:0.131671, 8000: 0.16186, 8080: 0.598594}

#cznic honeypots
#  22, 23 80 8080, 3128, 2323, 8123
probabilities_cznic = { 22:1, 23:1, 80:1, 8080:1, 3128:1, 2323:1, 8123:1}

#function which decides where to put honeypots
def place_honeypots_mix(d_port, count, total):
	global probabilities_mix
	#is the flow open
	if d_port in probabilities_mix.keys():
		# count the ratioon of free ports
		r = (count + 1)/total
		if r <= probabilities_mix[d_port]:
			return True
		else:
			#roll the dice
			if random.uniform(0,1) < probabilities_mix[d_port]/r:
				return True
	return False

place_honeypots_udf_mix = F.udf(place_honeypots_mix,T.BooleanType())
def place_honeypots_cznic(d_port, count, total):
	global probabilities_cznic
	#is the flow open
	if d_port in probabilities_cznic.keys():
		# count the ratioon of free ports
		r = (count + 1)/total
		if r <= probabilities_cznic[d_port]:
			return True
		else:
			#roll the dice
			if random.uniform(0,1) < probabilities_cznic[d_port]/r:
				return True
	return False

place_honeypots_udf_cznic = F.udf(place_honeypots_cznic,T.BooleanType())


#inputs = ["CTU-Flows_main/Flows.parquet/_yyyymd=2017-1-4", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-1-5", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-1-6", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-10", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-11", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-12", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-13", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-14", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-15", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-16", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-17", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-18", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-19", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-20", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-21", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-22", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-23", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-24", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-26", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-27", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-28", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-2-9", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-1", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-10", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-11", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-12", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-13", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-14", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-15", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-16", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-17", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-18", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-19", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-20", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-21", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-22", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-23", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-24", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-25", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-26", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-27", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-28", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-29", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-30", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-31", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-8", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-9"]
inputs = [ "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-15", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-16", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-17", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-18", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-19", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-20", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-21", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-22", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-23", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-24", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-25", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-26", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-27", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-28", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-29", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-30", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-31", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-8", "CTU-Flows_main/Flows.parquet/_yyyymd=2017-3-9"]
##INPUT

for input_file in inputs:
    
	print('Processing file ', input_file, ':')
	#get data
	data = sqlContext.read.parquet(input_file)

	df = data.dropDuplicates()
	
	# filter data according to the sample ratio
	#filter flows with dstIP outside of the university and srcIP inside range 80-83 mask 22
	df = data.filter(data.Proto=="tcp").filter(data.DstAddr.startswith("147.32.8")).filter(~data.SrcAddr.startswith("147.32.8")).select("DstAddr", "Dport", "State", "StartTime", "SrcAddr")
	src_samples_collect = df.select('SrcAddr').distinct().sample(False, SrcAddrSampleRatio, random_seed).collect()
	src_samples = map(lambda x: x['SrcAddr'], src_samples_collect)
	df = df.where(df.SrcAddr.isin(src_samples))
	
	
	#select day from timestamp
	df = df.withColumn('Dport', df["Dport"].cast(T.IntegerType()))
	df = df.withColumn('timestamp', unix_timestamp('StartTime', 'yyyy/MM/dd hh:mm:ss.SSSSSS').cast(T.TimestampType()))
	
	df = df
	
	# keep only those flows that come from an a_[^S,]*\(R\|F\)*[^S,]*ttacker (touched addrs or ports more or equal to the given limits)
	srcAddrs = map(lambda x: x['SrcAddr'], df.select('SrcAddr','DstAddr','Dport').distinct().groupBy('SrcAddr').agg(F.countDistinct('DstAddr').alias('addrCount'), F.countDistinct('Dport').alias('portCount')).filter((col('addrCount')>=touchAddrLimit) | (col('portCount')>=touchPortLimit)).select('SrcAddr').collect())
	df = df.where(df.SrcAddr.isin(srcAddrs))
	
	#create df with openPorts
	df = df.withColumn("possible_HP", col('State').rlike('_[^S]*(R|F|)[^S]*$'))
	
	# count number of possible honeypots for each (dstAddr, dport)
	df_port_counts = df.filter(col('possible_HP')==True).select("DstAddr", "Dport").distinct().groupBy("Dport").count()
	df =  df.join(df_port_counts, ["Dport"],how="left")
	
	total_count = df.select('DstAddr').distinct().count()
	df = df.withColumn("total_count", lit(total_count))
	
	
	#place Honeypots
	df = df.withColumn("chosenToBeHP1", place_honeypots_udf_mix(df["Dport"], df["count"], df["total_count"]))
	df = df.withColumn("isHP1", col('chosenToBeHP1') & col('possible_HP'))
	df = df.withColumn("chosenToBeHP2", place_honeypots_udf_cznic(df["Dport"], df["count"], df["total_count"]))
	df = df.withColumn("isHP2", col('chosenToBeHP2') & col('possible_HP'))
	
	
	#filter out flows without honeypots
	df_att_det_mix = df.filter(df.isHP1).groupBy('SrcAddr').agg(F.min(F.col('timestamp')).alias("detectionTime_mix"))
	df_att_det_cznic = df.filter(df.isHP2).groupBy('SrcAddr').agg(F.min(F.col('timestamp')).alias("detectionTime_cznic"))
	df = df.join(df_att_det_mix, ['SrcAddr'], how="left")
	df = df.join(df_att_det_cznic, ['SrcAddr'], how="left")
	
	
	# count saved
	try:
		saved_mix = df.filter(col('detectionTime_mix').isNotNull()).filter(~df["possible_HP"]).filter(col('timestamp')>col('detectionTime_mix')).count()
		saved_cznic = df.filter(col('detectionTime_cznic').isNotNull()).filter(~df["possible_HP"]).filter(col('timestamp')>col('detectionTime_cznic')).count()
		print('In ', input_file, ' saved_mix ', str(saved_mix), ' saved_cznic ', str(saved_cznic))
	except py4j.protocol.Py4JJavaError:
		print('oops! Something went wrong.')
	sqlContext.clearCache()

print("DONE")
