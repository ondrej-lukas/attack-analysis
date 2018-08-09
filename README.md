# Collection of scripts used for analysing netflows in Hadoop
Used for estimating efficiency of strategy of distributing honeypots in the network.
## Game Description
TODO
## Usage:
### Running scripts:
```bash
spark-submit --master yarn --deploy-mode cluster --packages com.databricks:spark-csv_2.10:1.5.0 --executor-cores 4 --executor-memory 55G --num-executors 24 <script>
```
### Retrieving results:
```bash
yarn logs -applicationId application_1524729467101_<task_ID> | less
```
