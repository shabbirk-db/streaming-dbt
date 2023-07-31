{{
    config(
        materialized='streaming_table',
        zorder="Date"
    )
}}

SELECT 
  ActualElapsedTime
  ,ArrDelay::INT
  ,ArrTime 
  ,CRSArrTime 
  ,CRSDepTime 
  ,CRSElapsedTime 
  ,Cancelled::INT
  ,DayOfWeek
  ,DayOfMonth
  ,Month
  ,Year
  ,DepDelay::INT
  ,DepTime 
  ,Dest 
  ,Distance 
  ,Diverted::INT
  ,FlightNum 
  ,IsArrDelayed 
  ,IsDepDelayed
  ,Origin 
  ,UniqueCarrier
  ,TO_DATE(STRING(INT(Year*10000+Month*100+DayofMonth)),'yyyyMMdd') AS Date
  ,airlines.name as airline_name
  ,origin.municipality origin_city
  ,origin.name as origin_airport_name
  ,origin.elevation_ft::INT origin_elevation_ft
  ,split(origin.coordinates,',') as origin_coordinates_array
  ,dest.municipality dest_city
  ,dest.name as dest_airport_name
  ,dest.elevation_ft::INT dest_elevation_ft
  ,split(dest.coordinates,',') as dest_coordinates_array
FROM STREAM({{ref("airline_trips_bronze")}}) raw
LEFT JOIN {{ref("airport_codes")}} origin
  ON raw.Origin = origin.iata_code
LEFT JOIN {{ref("airport_codes")}} dest
  ON raw.Dest = dest.iata_code  
LEFT JOIN {{ref("airline_codes")}} airlines
  ON raw.UniqueCarrier = airlines.iata