add jar hdfs:///tmp/spertus/IngestWeather-1.0-SNAPSHOT.jar;  -- need waether table 

create external table crime_csv(
    unuse string,
    ID string,
    CaseNumber string,
    happeddate string,
    block string,
    IUCR string,
    primaryType string,
    description string,
    location string,
    arrest string,
    domestic string,
    beat smallint,
    district smallint,
    ward smallint,
    community smallint,
    FBIcode smallint,
    Xcoordinate string,
    Ycoordinate string,
    year smallint,
    updatedate string,
    latitude string,
    longitude string,
    LatLong string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/tmp/chengyuel/chicago_crime';


create table crime(
    unuse string,
    ID string,
    CaseNumber string,
    new_happeddate string,
    block string,
    IUCR string,
    primaryType string,
    description string,
    location string,
    arrest string,
    domestic string,
    beat smallint,
    district smallint,
    ward smallint,
    community smallint,
    FBIcode smallint,
    Xcoordinate string,
    Ycoordinate string,
    year smallint,
    updatedate string,
    latitude string,
    longitude string,
    LatLong string,
    cityname string)
    stored as orc;

insert overwrite table crime select unuse, ID, CaseNumber,
    from_unixtime(unix_timestamp(happeddate ,'MM/dd/yyyy'), 'yyyy-MM-dd') as new_happeddate,
    block, IUCR, primaryType, description,
    location, arrest, domestic, beat, district, ward,
    community, FBIcode, Xcoordinate, Ycoordinate,
    year, updatedate, latitude, longitude,
    LatLong, 
    case when happeddate is not null then "ORD"
        else   "ORD"
    end cityname
      from crime_csv
where unuse is not null and happeddate is not null;

