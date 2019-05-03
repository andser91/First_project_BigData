import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    ticker, close, low, high, volume, data = line.split('\t')
    print('%s\t%s\t%s\t%s\t%s\t%s' %(ticker, close, low, high, volume, data.strip('\n')))