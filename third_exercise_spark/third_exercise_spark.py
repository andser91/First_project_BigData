from pyspark import SparkContext
import pyspark
from itertools import islice
import datetime

def correct_parsing(line):
	correct_line = []
	i = 0
	while i < len(line):
		if (line[i][0] == '"'):
			index = i + 1
			merged_string = line[i][1:]
			while line[index][-1] != '"':
				merged_string = merged_string + line[index]
				index = index + 1
				i = i + 1
			merged_string = merged_string + line[index][:-1]
			correct_line.append(merged_string)
		else:
			if line[i][-1] != '"':
				correct_line.append(line[i])
		i = i + 1
	ticker = correct_line[0]
	name  = correct_line[2]
	sector = correct_line[3]
	return [ticker,name,sector]


def parse_data(stringa):
	year, month, day = stringa.split("-")
	return datetime.datetime(int(year), int(month), int(day))


conf = pyspark.SparkConf().setAll([('spark.executor.memory','8g'),('spark.driver.memory','8g'),('spark.driver.maxResultSize','3g')])
sc = SparkContext("local", "PySpark Word Count Exmaple", conf=conf)

stocks = sc.textFile("historical_stocks.csv") \
    .map(lambda line: correct_parsing(line.split(",")))\
	.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)\
	.filter(lambda line: line[2]!="N/A")\
	.map(lambda line: (line[0],[line[1], line[2]]))

with_prices = sc.textFile("historical_stock_prices.csv") \
	.map(lambda line: line.split(",")) \
    .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)\
    .filter(lambda line: int(line[7][:4]) >= 2016) \
    .map(lambda line: (line[0],[float(line[2]),parse_data(line[7])]))

stocks2 = stocks.map(lambda a : (a[1][0],a[1][1]))
join_table = with_prices.join(stocks).map(lambda a : ((a[1][1][0],a[1][0][1].year),[a[1][0][0], a[1][1][1], a[1][0][1]]))
min_date = join_table.reduceByKey(lambda a,b : a if a[2]<=b[2] else b).map(lambda line : ((line[0][0], line[1][2]), line[0][1]))
max_date = join_table.reduceByKey(lambda a,b : a if a[2]>=b[2] else b).map(lambda line : ((line[0][0], line[1][2]), line[0][1]))
join_table2 = join_table.map(lambda line : ((line[0][0], line[1][2]), line[1][0]))
min_date_value = join_table2.join(min_date).map(lambda line : ( (line[0][0], line[0][1].year), line[1][0]))
max_date_value = join_table2.join(max_date).map(lambda line : ( (line[0][0], line[0][1].year), line[1][0]))
min_date_value_sum = min_date_value.reduceByKey(lambda a,b : a+b)
min_date_value_count = min_date_value.map(lambda linea: (linea[0],1)).reduceByKey(lambda a,b: a+b)
min_date_value_avg = min_date_value_sum.join(min_date_value_count).map(lambda a: (a[0],a[1][0]/a[1][1]))
max_date_value_sum = max_date_value.reduceByKey(lambda a,b : a+b)
max_date_value_count = max_date_value.map(lambda linea: (linea[0],1)).reduceByKey(lambda a,b: a+b)
max_date_value_avg = max_date_value_sum.join(max_date_value_count).map(lambda a: (a[0],a[1][0]/a[1][1]))
min_max_2016 = min_date_value_avg.join(max_date_value_avg).filter(lambda a : a[0][1] == 2016).map(lambda a : (a[0][0], (a[1][1] - a[1][0]) / a[1][0]))
min_max_2017 = min_date_value_avg.join(max_date_value_avg ).filter(lambda a : a[0][1] == 2017).map(lambda a : (a[0][0], (a[1][1] - a[1][0]) / a[1][0]))
min_max_2018 = min_date_value_avg.join(max_date_value_avg ).filter(lambda a : a[0][1] == 2018).map(lambda a : (a[0][0], (a[1][1] - a[1][0]) / a[1][0]))
percentage = min_max_2016.join(min_max_2017).join(min_max_2018).map(lambda a : (a[0], [int(a[1][0][0]*100) , int(a[1][0][1]*100), int(a[1][1]*100)]))
percentage_sector = percentage.join(stocks2).map(lambda x : ((x[1][0][0],x[1][0][1],x[1][0][2]), [x[0], x[1][1]]))
result_duplicates = (percentage_sector.join(percentage_sector)).filter(lambda line : line[1][0][0] != line[1][1][0] and line[1][0][1] != line[1][1][1])
result = result_duplicates.reduceByKey(lambda a,b: a).map(lambda a : (a[1][0][0], a[1][1][0], a[0]))

result.saveAsTextFile("output")