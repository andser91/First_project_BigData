
from pyspark import SparkContext
import pyspark
from itertools import islice
import datetime

conf = pyspark.SparkConf().setAll([('spark.executor.memory','8g'),('spark.driver.memory','8g'),('spark.driver.maxResultSize','3g')])
sc = SparkContext("local", "PySpark Word Count Exmaple", conf=conf)

def parse_data(stringa):
    year, month, day = stringa.split("-")
    return datetime.datetime(int(year), int(month), int(day))

words = sc.textFile("prova.csv") \
    .map(lambda line: line.split(",")) \
    .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)\
    .filter(lambda line: int(line[7][:4]) >= 1998) \
    .map(lambda line: (line[0],[float(line[2]),float(line[4]),float(line[5]),int(line[6]),parse_data(line[7])]))\

lowTheMin = words.reduceByKey(lambda a,b : a if a[1]<b[1] else b).map(lambda line: (line[0],line[1][1]))
highTheMax = words.reduceByKey(lambda a,b : a if a[2]>b[2] else b).map(lambda line: (line[0],line[1][2]))
volume_totale = words.reduceByKey(lambda a,b : [a[0],a[1],a[2],a[3]+b[3],a[4]]).map(lambda line: (line[0],line[1][3]))
ticker_giorni = words.map(lambda linea: (linea[0],1)).reduceByKey(lambda a,b: a+b)
volume_medio = volume_totale.join(ticker_giorni).map(lambda a: (a[0],a[1][0]/a[1][1]))
min_max = lowTheMin.join(highTheMax).join(volume_medio).map(lambda x: (x[0],[x[1][0][0],x[1][0][1], x[1][1]]))
date_max = words.reduceByKey(lambda a,b : a if a[4]>=b[4] else b).map(lambda line: (line[0],line[1][0]))
date_min = words.reduceByKey(lambda a,b : a if a[4]<=b[4] else b).map(lambda line: (line[0],line[1][0]))
percentuale = date_min.join(date_max).map(lambda a : (a[0], (a[1][1] - a[1][0]) /a[1][0]*100))
result = percentuale.join(min_max).map(lambda a : (a[0], a[1][0], a[1][1][0], a[1][1][1], a[1][1][2])).sortBy(lambda a : a[1], ascending=False).take(10)

for i in result:
    print(i)

