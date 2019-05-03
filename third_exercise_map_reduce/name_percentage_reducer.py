from collections import defaultdict
import sys
import numpy as np
from collections import defaultdict
import sys
import datetime
import collections

values = defaultdict(list)
for line in sys.stdin:
    name, close, sector, date = line.split('\t')

    values[name,date.strip("\n")[:4]].append([float(close),sector,date.strip("\n")])


values =  (collections.OrderedDict(sorted(values.items())))

name_and_perc = defaultdict(list)


for key,list_value in values.items():
	name_society = key[0]
	data_minima = datetime.datetime(2018, 12, 31)
	data_massima = datetime.datetime(2004, 1, 1)
	valore_data_min = np.inf
	valore_data_max = 0

	for value in list_value:
		#####
		year, month, day = value[2].split("-")
		data = datetime.datetime(int(year), int(month), int(day))
		if (data <= data_minima):
			data_minima = data
			valore_data_min = value[0]
		if (data >= data_massima):
			data_massima = data
			valore_data_max = value[0]
		####

	percent = ((valore_data_max-valore_data_min)/valore_data_min)*100
	percent = round(percent)
	name_and_perc[name_society].append(percent)
	#print('%s\t%s' % (key,(str(percent) + "%")))

for key,list_value in name_and_perc.items():
	if len(list_value)==3:
		if (key,"2016") in values.keys():
			sector = values[key,"2016"][0][1]
			print('%s\t%s\t%s\t%s\t%s' % (key, list_value[0],list_value[1],list_value[2], sector))


