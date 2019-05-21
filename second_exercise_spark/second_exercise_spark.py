
from pyspark import SparkContext
import pyspark
from itertools import islice
import datetime
import os

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
	sector = correct_line[3]
	return [ticker,sector]


def parse_data(stringa):
    year, month, day = stringa.split("-")
    return datetime.datetime(int(year), int(month), int(day))

os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython3"
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
conf = pyspark.SparkConf().setAll([('spark.executor.memory','8g'),('spark.driver.memory','8g'),('spark.driver.maxResultSize','3g')])
sc = SparkContext("local", "PySpark Word Count Exmaple", conf=conf)


stocks = sc.textFile("../historical_stocks.csv") \
    .map(lambda line: correct_parsing(line.split(",")))\
	.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)\
	.filter(lambda line: line[1]!="N/A")\
	.map(lambda line: (line[0],line[1]))

with_prices = sc.textFile("../prova.csv") \
    .map(lambda line: line.split(",")) \
    .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)\
    .filter(lambda line: int(line[7][:4]) >= 2004) \
    .map(lambda line: (line[0],[float(line[2]),int(line[6]),parse_data(line[7])]))\

sum_volume_ticker_close = with_prices.map(lambda line:((line[0],line[1][2].year),[line[1][0],line[1][1],line[1][2]])) \
	.reduceByKey(lambda a,b : [a[0]+b[0], a[1]+b[1],a[2]]).map(lambda line:(line[0][0],[line[0][1],line[1][0],line[1][1],line[1][2]]))



join_table = sum_volume_ticker_close.join(stocks).map(lambda a : ((a[1][1],a[1][0][0]), [a[1][0][1], a[1][0][2]]))

join_table_total_volume_close = join_table.reduceByKey(lambda a,b : [a[0]+b[0],a[1]+b[1]])\
	.map(lambda line:(line[0],[line[1][0]/365,line[1][1]]))


ticker_year = with_prices.map(lambda line:((line[0],line[1][2].year),[line[1][0],line[1][2]]))

ticker_year_max_date = ticker_year.reduceByKey(lambda a,b : a if a[1]>=b[1] else b)
ticker_year_min_date = ticker_year.reduceByKey(lambda a,b : a if a[1]<=b[1] else b)

annual_ticker_percentage = ticker_year_max_date.join(ticker_year_min_date).map(lambda a : (a[0][0],[a[0][1],(a[1][0][0]-a[1][1][0])/a[1][1][0]]))

annual_sector_percentage = annual_ticker_percentage.join(stocks).map(lambda a:((a[1][1],a[1][0][0]),a[1][0][1]))\
	.reduceByKey(lambda a,b:a+b)


final_join = annual_sector_percentage.join(join_table_total_volume_close)\
	.map(lambda line:(line[0],[line[1][0]*100,line[1][1][0],line[1][1][1]])).sortBy(lambda a : a[0])
for i in final_join.collect():
	print (i)