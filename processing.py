import sys
import json
import datetime

data = {}
with open("ludus_statistic_tmp.json", "r") as f:
	for line in f:
		tmp_d = json.loads(line)
		if tmp_d["day"] not in data.keys():
			data[tmp_d["day"]] = {"attacker_strategy":{}, "production_distribution":{}}
		data[tmp_d["day"]]["attacker_strategy"][tmp_d["port"]] = tmp_d["attackProbability"]
		data[tmp_d["day"]]["production_distribution"][tmp_d["port"]] = tmp_d["openPortProbability"]
metadata = {"created":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
output_dict = {"data":data, "metadata":metadata}

with open(sys.argv[1], "w") as output:
	json.dump(output_dict,output)

