#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip("\n")
    line = line.strip("\r")
    line = line.split(',')
    if line[0]!="ticker":
        #FIRST FILE:
        ticker = "@"
        #open = "@"
        close = "@"
        #lowThe = "@"
        #highThe = "@"
        #volume = "@"
        date = "@"

        #SECOND FILE
        #exchange = "@"
        name = "@"
        sector = "@"
        #industry = "@"

        #IF LEN == 8 IS HISTORICAL WITH PRICES
        if len(line) == 8:
            if 2016 <= int(line[7][:4]) <= 2018:
                ticker = line[0]
                #open = line[1]
                close = line[2]
                #lowThe = line[4]
                #highThe = line[5]
                #volume = line[6]
                date = line[7]


        else:
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
            #exchange = correct_line[1]
            name = correct_line[2]
            sector = correct_line[3]

            #industry = correct_line[4]
        if not(close=="@" and sector =="@"):
            print('%s\t%s\t%s\t%s\t%s' % (ticker,close,date,name,sector))
