from collections import defaultdict
import sys
import numpy as np
from collections import defaultdict
import sys
import datetime
import collections

values = defaultdict(list)
for line in sys.stdin:
    value_2016, value_2017, value_2018, name, sector = line.split('\t')

    values[value_2016,value_2017,value_2018].append([name,sector.strip("\n")])



for key,list_value in values.items():
	for i in range(0,len(list_value)-1):
		for j in range(i,len(list_value)):
			if list_value[i][1]!=list_value[j][1]:
				print ('%s\t%s\t%s\t%s\t%s' % (list_value[i][0],list_value[j][0],("2016: " + key[0]+"%"),("2017: "+key[1]+"%"),("2018: "+ key[2]+"%")))



