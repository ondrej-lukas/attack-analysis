from __future__ import division
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, DataFrameWriter
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
import datetime
import pyspark.sql.types as T
import re
from pyspark.sql.window import Window
import random
import sys

touchAddrLimit = 5
touchPortLimit = 3
STRATEGY_FILE = "./strategies/"+ sys.argv[2] #json file one json obejct per line
DATA_FILTER = sys.argv[1] #format YYYY-MM-DD

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

sqlContext = HiveContext(sc)


#function which decides where to put honeypots
def place_honeypot(probability, random_roll):
	return  random_roll <= probability

place_honeypot_udf = F.udf(place_honeypot,T.BooleanType())

"""
#Read strategy from the json file, convert all possible values from string and add unique ID
"""
strategies = sqlContext.read.json(STRATEGY_FILE)
#filter strategy
if len(sys.argv) > 3:
	strategies = strategies.filter(col("rationality") == float(sys.argv[3])).filter(col("num_of_hp") == float(sys.argv[4]))
#add unique name
strategies_full = strategies.withColumn("strategyID", concat(col("date"), lit("_"), col("num_of_hp"), lit("_"), col("rationality")))

#cast date to timestamp
strategies = strategies_full.withColumn('date', unix_timestamp('date', 'yyyy-MM-dd').cast(T.TimestampType()))
#unfold the structs
strategies = strategies_full.select("date","strategyID",explode("stg")).select("date","strategyID","col.port","col.prob")
#add new column with date when the strategy should be applied
strategies = strategies.withColumn('application_date',F.date_add(strategies['date'], 1)).select("application_date", "strategyID", "port", "prob")
strategies = strategies.withColumn('application_date', unix_timestamp('application_date', 'yyyy-MM-dd hh:mm:ss').cast(T.TimestampType()))

"""
Read data based on the date filter. Find attacks and discard rest of the flows. Convert all possible values from string so we speed up the computations
"""
data = sqlContext.read.parquet("CTU-Flows_main/Flows.parquet/_yyyymd={}".format(DATA_FILTER))
#data = sqlContext.read.parquet(sys.argv[1])

df = data.dropDuplicates()

#filter flows with dstIP outside of the university and srcIP inside range 80-83 mask 22
df = data.filter(data.Proto=="tcp").filter(data.DstAddr.startswith("147.32.8")).filter(~data.SrcAddr.startswith("147.32.8")).select("DstAddr", "Dport", "State", "StartTime", "SrcAddr")

#select day from timestamp and convert cast numbers to int
df = df.withColumn('Dport', df["Dport"].cast(T.IntegerType()))
df = df.withColumn('timestamp', unix_timestamp('StartTime', 'yyyy/MM/dd hh:mm:ss.SSSSSS').cast(T.TimestampType()))
#add column for day
df = df.withColumn('day', unix_timestamp('StartTime', 'yyyy/MM/dd').cast(T.TimestampType()))
df = df.filter(col('Dport').isNotNull())

#get flows from attackers
attackers = df.select('SrcAddr','DstAddr','Dport', "day").distinct().groupBy('SrcAddr',"day").agg(F.countDistinct('DstAddr').alias('addrCount'), F.countDistinct('Dport').alias('portCount')).filter((col('addrCount')>=touchAddrLimit) | (col('portCount')>=touchPortLimit)).select('SrcAddr', "day").distinct()

df = df.join(attackers,["SrcAddr", "day"], how="inner")
#find possible honeypots
df = df.withColumn("possible_HP", col('State').rlike('_[^S]*(R|F|)[^S]*$'))

"""
#count distinct IPs in the network
total_counts = df.select('DstAddr',"day").distinct().groupBy("day").count().selectExpr("day", "count as total_count")
df = df.join(total_counts, ["day"])
df_attacker_count = df.select("SrcAddr", "day","Dport").distinct().groupBy("day","Dport").count().selectExpr("day as day_1","Dport as port","count as unique_attackers")

# count number of possible honeypots for each Dport per day
df_port_counts = df.filter(col('possible_HP')==True).select("DstAddr", "Dport","day").distinct().groupBy("Dport","day").count().selectExpr("Dport as dp1", "day as day1", "count")
df_HP =  df.filter(col('possible_HP')==True).select("Dport", "DstAddr","StartTime","SrcAddr","timestamp", "possible_HP", "day","total_count")
df_HP = df_HP.join(df_port_counts, (df_HP.Dport == df_port_counts.dp1) & (df_HP.day == df_port_counts.day1))
#count open port ratio in each port to be used for placing honeypots
df_HP = df_HP.withColumn("open_port_ratio", (col('count') + 1)/col('total_count')).select("Dport", "DstAddr","StartTime","SrcAddr","timestamp","open_port_ratio", "possible_HP", "day")
"""
#roll the dice in each possible honeypot
df_HP =  df.filter(col('possible_HP')==True).select("Dport", "DstAddr","StartTime","SrcAddr","timestamp", "possible_HP", "day")
df_HP = df_HP.withColumn("random_roll", rand())

#merge each possible honeypot with all strategies which contain same Dport
tmp = df_HP.join(strategies,(df_HP.day == strategies.application_date) & (df_HP.Dport == strategies.port))

#actually assign the honeypots based od the strategy probability and the open port
withHP = tmp.withColumn("isHP",place_honeypot_udf(tmp["prob"], tmp["random_roll"]))
#find the the HP for each strategy that each attacker hits first
detectionTimes = withHP.filter("isHP").groupBy('SrcAddr', "strategyID","day").agg(F.min(F.col('timestamp')).alias("detectionTime"))

#get time of detection for every strategy and every SrcAddr
withHP = withHP.join(detectionTimes,['SrcAddr', "strategyID","day"]).filter(col("timestamp") == col("detectionTime"))

#get number of attack per day
df_attacks = df.filter(~df["possible_HP"]).select("SrcAddr","DstAddr", "Dport","day","timestamp").groupBy("day").count().selectExpr("day", "count as total_attacks")

#df_attacks.write.format('com.databricks.spark.csv').save('attack_per_day.csv')
#match each open port with the detection time of its SrcAddr
df_final = df.filter(~df["possible_HP"]).select("Dport", "DstAddr","SrcAddr","timestamp","day").join(detectionTimes.select("SrcAddr" ,"strategyID", "detectionTime","day").distinct(),["SrcAddr","day"],how="inner")
df_final = df_final.filter(col('timestamp')>col('detectionTime'))
#count open ports visited AFTER the detection of the SrcAddr
saved = df_final.groupBy("strategyID","day").count().selectExpr("strategyID", "day", "count as saved")
saved = saved.join(df_attacks, ["day"])
saved = saved.withColumn("saved_ratio",col("saved")/col("total_attacks"))

#aggregate results per strategy type (rationality, number of honeypots)
results = strategies_full.select("rationality","num_of_hp", "strategyID").join(saved,["strategyID"])
#out = results.select("day","rationality", "num_of_hp","saved_ratio","saved","total_attacks").orderBy("rationality").orderBy("num_of_hp").orderBy("day")
#out.write.format('com.databricks.spark.csv').save('output_daily.csv')

out = results.select("rationality","num_of_hp","saved_ratio").groupBy("rationality","num_of_hp").agg(F.avg('saved_ratio').alias("avg_saved_ratio"), F.stddev("saved_ratio").alias("stddev"))
#out_sum = results.select("rationality","num_of_hp","saved", "total_attacks").groupBy("rationality","num_of_hp").agg(F.sum('saved').alias("saved_sum"), F.sum("total_attacks").alias("total_attacks_sum"))
#out = out_avg.join(out_sum, ["rationality","num_of_hp"]).sort('avg_saved_ratio', ascending=False)

#store output into csv
out.write.format('com.databricks.spark.csv').save('./output/strategy_results.csv',header='true')
print("DONE")