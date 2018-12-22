# Collection of scripts used for analysing netflows in Hadoop
Used for estimating efficiency of strategy of distributing honeypots in the network.
## Game Description
TODO
## Usage:
### Retrieving logs for any task:
```bash
yarn logs -applicationId application_1524729467101_<task_ID> | less
```

#### Daily statistic

```bash
daily_statistic_ludus.sh
```

#### Strategy evaluation
Each strategy can be described by number of honeypot used in each host and level of rationality (proportion of usage of NE in the game). strategy_evaluation.py is a script which evaluate all setups of the strategy file for each day and gives the average values for each setup. 
#####Running task
```bash
spark-submit --master yarn --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.5.0 --executor-cores 4 --executor-memory 20G --num-executors 24 --driver-memory 2g  strategy_evaluation.py <DATE FILTER> <STRATEGY_FILENAME> <RATIONALITY (optional)> <NUMBE OF HP (optional)>
```

#####Retrieving results
```bash
hdfs dfs -getmerge ./output/strategy_results.csv <local_filename>
hdfs dfs -rm -r ./output/strategy_results.csv
```

#### Hitrate evalueation
TODO


#### Port evaluation
TODO
