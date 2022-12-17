create table crime_and_weather (
  years smallint, months tinyint, days tinyint,
  ID string, CaseNumber string, new_happeddate string, block string, primaryType string,
  description string, location string, arrest string, domestic string,
  beat smallint, district smallint, ward smallint, community smallint,
  FBIcode smallint, cityname string, station_code string,
  mean_temperature double, mean_visibility double, mean_windspeed double,
  fog boolean, rain boolean, snow boolean, hail boolean, thunder boolean, tornado boolean,
  fog_delay double, rain_delay double, snow_delay double, hail_delay double,
  thunder_delay double, tornado_delay double, clear_delay double) stored as orc;

  insert overwrite table crime_and_weather
  select t.years as years, t.months as months, t.days as days, t.ID as ID, t.CaseNumber as CaseNumber, t.new_happeddate as new_happeddate,
  t.block as block, t.primaryType as primaryType, t.description as description, t.location as location, t.arrest as arrest,
  t.domestic as domestic, t.beat as beat, t.district as district, t.ward as ward, t.community as community, 
  t.FBIcode as FBIcode, t.cityname as cityname, t.station_code as station_code, 
  w.meantemperature as mean_temperature, w.meanvisibility as mean_visibility, w.meanwindspeed as mean_windspeed,
  w.fog as fog, w.rain as rain, w.snow as snow, w.hail as hail, w.thunder as thunder, w.tornado as tornado,
  if(w.fog,1 ,0) as fog_delay, if(w.rain,1,0) as rain_delay, if(w.snow, 1, 0) as snow_delay,
  if(w.hail, 1, 0) as hail_delay, if(w.thunder, 1, 0) as thunder_delay,
  if(w.tornado,  1, 0) as tornado_delay,
  if(w.fog or w.rain or w.snow or w.hail or w.thunder or w.tornado, 0, 1) as clear_delay
  from joinstation t join weathersummary w
  on t.years = w.year and t.months = w.month and t.days = w.day and t.station_code = w.station;