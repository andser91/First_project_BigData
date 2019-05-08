
CREATE TABLE prices (ticker STRING, open FLOAT, close FLOAT, adj_close FLOAT, lowThe FLOAT, highThe FLOAT, volume INT,data DATE) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '../historical_stock_prices2.csv' OVERWRITE INTO TABLE prices;

create table min_max as select ticker, min(lowThe) as minimo, max(highThe) as massimo from prices where data > cast('1997-12-31' as date) group by ticker;

create table media_volume as select ticker, avg(volume) as volume_medio from prices where data > cast('1997-12-31' as date) group by ticker;

create table min_max_date as select ticker, min(data) as min_data, max(data) as max_data from prices where data > cast('1997-12-31' as date) group by ticker;

create table data_min_value as select p.ticker, close from prices p join min_max_date m where p.ticker = m.ticker and p.data = m.min_data; 

create table data_max_value as select p.ticker, close from prices p join min_max_date m where p.ticker = m.ticker and p.data = m.max_data; 

create table percentage_increment as select a.ticker, ((b.close - a.close)/a.close) as percentage from data_min_value a join data_max_value  b on a.ticker = b.ticker order by percentage desc limit 10;

create table result as select p.ticker, percentage, minimo, massimo, volume_medio from percentage_increment p join min_max mm on p.ticker = mm.ticker join media_volume mv on mv.ticker = p.ticker order by percentage desc


