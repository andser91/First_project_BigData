#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys

for line in sys.stdin:
	line = line.strip()
	name,value_2016,value_2017,value_2018,sector = line.split("\t")
	print('%s\t%s\t%s\t%s\t%s' % (value_2016, value_2017, value_2018, name, sector))
