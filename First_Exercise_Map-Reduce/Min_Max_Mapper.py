import sys

CAMPI = {"ticker": 0, "open": 1, "close": 2, "adj_close": 3, "lowThe": 4, "highThe": 5, "volume": 6, "date": 7}

# input comes from STDIN (standard input)
for line in sys.stdin:
    values = line.split(',')
    if len(values) == 8:
        if 1998 <= int(values[CAMPI["date"]][:4]) <= 2018:
            print('%s\t%s\t%s\t%s\t%s\t%s' %(values[CAMPI["ticker"]], values[CAMPI["close"]], values[CAMPI["lowThe"]],
                                             values[CAMPI["highThe"]], values[CAMPI["volume"]], values[CAMPI["date"]].strip('\n')))

