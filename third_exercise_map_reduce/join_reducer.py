#!/usr/bin/python

import sys
from collections import defaultdict

historical_corto = {}
historical = defaultdict(list)

for line in sys.stdin:
	line = line.strip()
	ticker,close,date,name,sector = line.split('\t')
	if sector is not "@":
		historical_corto[ticker] = [sector,name]

	else:
		historical[ticker].append([close,date])

for key,list_value in historical.items():
	for value in list_value:
		ticker = key
		close = value[0]
		date = value[1]
		sector = historical_corto[key][0]
		name = historical_corto[key][1]
		if sector != "N/A":
			print('%s\t%s\t%s\t%s\t%s' % (name, close, sector, date,ticker))
