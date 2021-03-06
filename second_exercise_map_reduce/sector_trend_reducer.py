#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import defaultdict
import sys
import numpy as np
from collections import defaultdict
import sys
import datetime

values = defaultdict(list)
for line in sys.stdin:
    sector, year, percentage, volume,close = line.split('\t')

    values[sector,year.strip("\n")].append([float(percentage),int(volume),float(close.strip("\n"))])


for key,list_value in values.items():
    volume_tot = 0
    percentage_tot = 0
    avg_close = 0
    for value in list_value:
        volume_tot = volume_tot + value[1]
        percentage_tot = percentage_tot + value[0]
        avg_close = avg_close + value[2]

    avg_close = avg_close/365
    #print('%s\t%s\t%s\t%s' % (key, volume_sum_sector, (str(percent) + "%"), round(avg_year, 4)))
    print ('%s\t%s\t%s\t%s\t%s' % (key[0],key[1],volume_tot,percentage_tot,avg_close))


#for key, list_value in values.items():
	#print (key,list_value[0])
# for key,list_value in values.items():
# 	volume_sum_sector = 0
# 	data_minima = datetime.datetime(2018, 12, 31)
# 	data_massima = datetime.datetime(2004, 1, 1)
# 	valore_data_min = np.inf
# 	valore_data_max = 0
# 	avg_year = 0
#
# 	#la percentuale di variazione annuale (differenza percentuale
# 	#arrotondata tra la quotazione di fine anno e quella di inizio anno)
#
# 	for value in list_value:
# 		volume_sum_sector = volume_sum_sector + value[1]
# 		#####
# 		year, month, day = value[2].split("-")
# 		data = datetime.datetime(int(year), int(month), int(day))
# 		if (data <= data_minima):
# 			data_minima = data
# 			valore_data_min = value[0]
# 		if (data >= data_massima):
# 			data_massima = data
# 			valore_data_max = value[0]
# 		####
# 		avg_year = avg_year+value[0]
# 	avg_year = avg_year/365
#
# 	percent = ((valore_data_max-valore_data_min)/valore_data_min)*100
# 	percent = round(percent,2)
#
# 	print('%s\t%s\t%s\t%s' % (key,volume_sum_sector,(str(percent) + "%"),round(avg_year,4)))
