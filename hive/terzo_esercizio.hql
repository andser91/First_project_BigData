CREATE TABLE prices3 (ticker STRING, open FLOAT, close FLOAT, adj_close FLOAT, lowThe FLOAT, highThe FLOAT, volume INT,data DATE) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '../historical_stock_prices2.csv' OVERWRITE INTO TABLE prices3;

CREATE TABLE stocks_no_parsed (riga STRING, riga2 STRING) row format delimited fields terminated by '@';

LOAD DATA LOCAL INPATH '../historical_stocks2.csv' OVERWRITE INTO TABLE stocks_no_parsed;

add file ../parsing_es3.py;

create table stocks2 as select transform (riga, riga2) using 'python3 parsing_es3.py' as (ticker string, name string, sector string) from stocks_no_parsed;

create table stocks_no2 as select name,sector from stocks2;

create table join_table2 as select name, close, sector, data from prices3 p join stocks2 s on p.ticker = s.ticker where data > cast('2015-12-31' as date) and sector != 'N/A';

create table min_max_date_3 as select name, year(data) as anno, min(data) as min_data, max(data) as max_data from join_table2 group by name,year(data);

create table data_min_value_3 as select j.name, anno, avg(close) as close from join_table2 j join min_max_date_3 m where j.name = m.name and j.data = m.min_data group by j.name, anno;

create table data_max_value_3 as select j.name, anno , avg(close) as close from join_table2 j join min_max_date_3 m where j.name = m.name and j.data = m.max_data group by j.name, anno;

create table annual_increment_16 as select min.name, min.anno, ((max.close - min.close)/min.close) as percentage from data_min_value_3 min join data_max_value_3 max on min.name = max.name and min.anno = max.anno where max.anno = 2016;

create table annual_increment_17 as select min.name, min.anno, ((max.close - min.close)/min.close) as percentage from data_min_value_3 min join data_max_value_3 max on min.name = max.name and min.anno = max.anno where max.anno = 2017;

create table annual_increment_18 as select min.name, min.anno, ((max.close - min.close)/min.close) as percentage from data_min_value_3 min join data_max_value_3 max on min.name = max.name and min.anno = max.anno where max.anno = 2018;

create table percentages3 as select a.name, cast(a.percentage*100 as int) as p16, cast(b.percentage*100 as int) as p17, cast(c.percentage*100 as int) as p18 from annual_increment_16 a join annual_increment_17 b on a.name = b.name join annual_increment_18 c on a.name = c.name;

create table percentages_sector as select a.name, p16, p17, p18, sector from percentages3 a join stocks_no2 s on a.name = s.name where sector != 'N/A';

create table result_duplicates as select distinct a.name as first, b.name as second, a.p16, a.p17, a.p18 from percentages_sector a join percentages_sector b on (a.p16 = b.p16 and a.p17 = b.p17 and a.p18 = b.p18) where a.sector != b.sector and a.name != b.name;

create table result3 as select distinct first,second, p16, p17, p18 from result_duplicates r where r.first > r.second or not exists(select * from result_duplicates r2 where r2.first = r.second and r2.second = r.first);