filename=port_statistic_daily.json
#GET DATA FROM HADOOP
echo "STEP(1/3): Processing data in Hadoop"
spark-submit --master yarn --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.5.0 --executor-cores 4 --executor-memory 55G --num-executors 24 port_statistic_daily.py
hdfs dfs -getmerge port_statistic_per_day.json ludus_statistic_tmp.json
#PROCESS IT AND STORE IN JSON
echo "STEP(2/3): Storing data in JSON"
python processing.py $filename
#DELETE ALL TEMPORARY FILES
echo "STEP(3/3): Deleting temporary files"
hdfs dfs -rm -r port_statistic_per_day.json
rm ludus_statistic_tmp.json
echo "DONE - results stored in ${filename}" 
