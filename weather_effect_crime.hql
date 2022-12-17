create table crime_by_month (
  years smallint, months smallint, total_crime int, fog_total bigint, rain_total bigint, snow_total bigint, 
  hail_total bigint, thunder_total bigint, tornado_total bigint, clear_total bigint
);

insert overwrite table crime_by_month
  select years, months, count(1), sum(fog_delay), sum(rain_delay), sum(snow_delay), sum(hail_delay),
  sum(thunder_delay), sum(tornado_delay), sum(clear_delay) from crime_and_weather group by years, months;