#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from collections import defaultdict
import datetime
import numpy as np

dictionary = defaultdict(list)
lista = []
for line in sys.stdin:
    sector, close, volume,date,ticker = line.split('\t')
    dictionary[ticker.strip("\n"),date.strip("\n")[:4]].append([sector.strip("\n"),float(close),int(volume),date.strip("\n")])

for key, list_value in dictionary.items():
	year_key = key[1]
	data_minima = datetime.datetime(int(year_key), 12, 31)
	data_massima = datetime.datetime(int(year_key), 1, 1)
	valore_data_min = np.inf
	valore_data_max = 0
	volume_year = []
	close_year = []
	for el in list_value:
		year, month, day = el[3]  .split("-")
		data = datetime.datetime(int(year), int(month), int(day))
		if (data <= data_minima):
			data_minima = data
			valore_data_min = el[1]
		if (data >= data_massima):
			data_massima = data
			valore_data_max = el[1]
		volume_year.append(el[2])
		close_year.append(el[1])
	sector = dictionary[key][0][0]
	year = key[1]



		#settore,anno,percentuale,volume della quota
	print('%s\t%s\t%s\t%s\t%s' % (sector,year,((valore_data_max - valore_data_min)/valore_data_min),np.sum(volume_year),np.sum(close_year)))
	#print (key[0],key[1],(valore_data_max - valore_data_min)/valore_data_min)