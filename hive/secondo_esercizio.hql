-- IMPORT DEI FILE NELLE TABLE

CREATE TABLE prices (ticker STRING, open FLOAT, close FLOAT, adj_close FLOAT, lowThe FLOAT, highThe FLOAT, volume INT,data DATE) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../historical_stock_prices2.csv' OVERWRITE INTO TABLE prices;

CREATE TABLE stocks_no_parsed (riga STRING, riga2 STRING) row format delimited fields terminated by '@';
LOAD DATA LOCAL INPATH '../historical_stocks2.csv' OVERWRITE INTO TABLE stocks_no_parsed;

add file ../parsing.py;

create table stocks as select transform (riga, riga2) using 'python3 parsing.py' as (ticker string, sector string) from stocks_no_parsed;

create table aggregate_ticker_anno as select ticker, year(data) as anno, sum(close) as close, sum(volume) as volume from prices where data > cast('2003-12-31' as date) group by ticker, year(data);

create table sum_volume_avg_quote as select sector, anno, sum(volume) as tot_volume, (sum(close))/365 as avg_quote from aggregate_ticker_anno a join stocks s on a.ticker = s.ticker group by sector,anno;

create table min_max_date_2 as select ticker, year(data) as anno, min(data) as min_data, max(data) as max_data from prices where data > cast('2003-12-31' as date) group by ticker,year(data);

create table data_min_value_2 as select p.ticker, anno, close from prices p join min_max_date_2 m where p.ticker = m.ticker and p.data = m.min_data; 

create table data_max_value_2 as select p.ticker, anno , close from prices p join min_max_date_2 m where p.ticker = m.ticker and p.data = m.max_data; 

create table annual_percentage_increment_ticker as select min.ticker, min.anno, ((max.close - min.close)/min.close) as percentage from data_min_value_2 min join data_max_value_2 max on min.ticker = max.ticker and min.anno = max.anno;

create table annual_percentage_increment_sector as select sector,anno, sum(percentage) as tot_percentage from annual_percentage_increment_ticker a join stocks j on a.ticker = j.ticker group by sector,anno;

create table result_2 as select a.sector,a.anno,s.tot_volume, a.tot_percentage, s.avg_quote from annual_percentage_increment_sector a join sum_volume_avg_quote s where a.sector = s.sector and a.anno = s.anno;
