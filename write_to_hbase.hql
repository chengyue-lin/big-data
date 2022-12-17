create table weather_crime_by_month (
    yearsmonth string, total_crime bigint, fog_total bigint, rain_total bigint, snow_total bigint, 
    hail_total bigint, thunder_total bigint, tornado_total bigint, clear_total bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,crime:total_crime#b,crime:fog_total#b,crime:rain_total#b,crime:snow_total#b,crime:hail_total#b,crime:thunder_total#b,crime:tornado_total#b,crime:clear_total#b')
TBLPROPERTIES ('hbase.table.name' = 'weather_crime_by_month');

insert overwrite table weather_crime_by_month
select concat(years, months), total_crime, fog_total, rain_total, snow_total, hail_total,
thunder_total, tornado_total, clear_total from crime_by_month;