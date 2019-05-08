#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from collections import defaultdict

historical_corto = {}
historical = defaultdict(list)

for line in sys.stdin:
	line = line.strip()
	ticker,close,volume,date,sector = line.split('\t')
	if sector is not "@":
		historical_corto[ticker] = sector

	else:
		historical[ticker].append([close,volume,date])

for key,list_value in historical.items():
	for value in list_value:
		ticker = key
		close = value[0]
		volume = value[1]
		date = value[2]
		sector = historical_corto[key]
		if sector != "N/A":
			print('%s\t%s\t%s\t%s\t%s' % (sector, close, volume, date,ticker))
