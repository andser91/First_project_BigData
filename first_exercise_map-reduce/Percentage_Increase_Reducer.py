import sys
from collections import defaultdict
import datetime
import numpy as np

dictionary = defaultdict(list)
lista = []
for line in sys.stdin:
    ticker, close, low, high, volume, date = line.split('\t')
    dictionary[ticker].append([float(close),float(low),float(high),float(volume),date.strip("\n")])

for key, list_value in dictionary.items():
    data_minima = datetime.datetime(2018, 12, 31)
    data_massima = datetime.datetime(1998, 1, 1)
    valore_data_min = np.inf
    valore_data_max = 0
    for el in list_value:
        year, month, day = el[4].split("-")
        data = datetime.datetime(int(year), int(month), int(day))
        if (data <= data_minima):
            data_minima = data
            valore_data_min = el[0]
        if (data >= data_massima):
            data_massima = data
            valore_data_max = el[0]
    lista.append((key, ((valore_data_max - valore_data_min)/valore_data_min)*100, list_value[0][1], list_value[0][2], list_value[0][3]))


def sortSecond(val):
    return val[1]


lista.sort(key=sortSecond, reverse=True)
lista = lista[:10]

for el in lista:
    print('%s\t%s\t%s\t%s\t%s' %( el[0], el[1], el[2], el[3], el[4] ))



