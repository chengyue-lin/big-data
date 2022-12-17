create table joinstation (
  years smallint, months tinyint, days tinyint,
  ID string, CaseNumber string, new_happeddate string, block string, primaryType string,
  description string, location string, arrest string, domestic string,
  beat smallint, district smallint, ward smallint, community smallint,
  FBIcode smallint, cityname string, station_code string)
  stored as orc;

  insert overwrite table joinstation
  select year(t.new_happeddate) as years, month(t.new_happeddate) as months,
  day(t.new_happeddate) as days, t.ID as ID, t.CaseNumber as CaseNumber, t.new_happeddate as new_happeddate,
  t.block as block, t.primaryType as primaryType, t.description as description, t.location as location, t.arrest as arrest,
  t.domestic as domestic, t.beat as beat, t.district as district, t.ward as ward, t.community as community, 
  t.FBIcode as FBIcode, t.cityname as cityname, so.code as station_code from crime t join stations so 
  on t.cityname = so.name;