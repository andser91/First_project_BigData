import sys
from collections import defaultdict
import numpy as np


dictionary = defaultdict(list)
for line in sys.stdin:
    ticker, close, low, high, volume, date = line.split('\t')
    dictionary[ticker].append([float(close),float(low),float(high),int(volume),date.strip("\n")])


for key, list_value in dictionary.items():
    minimo = np.inf
    massimo = 0
    volume = 0
    i = 0
    for el in list_value:
        # print(key, list_value)
        minimo = min(minimo, el[1])
        massimo = max(massimo, el[2])
        volume = volume + el[3]
        i = i + 1
    volume = volume / len(list_value)
    for el in list_value:
        print('%s\t%s\t%s\t%s\t%s\t%s' %(key, el[0], round(minimo,4), round(massimo,4), round(volume,4), el[4]))
