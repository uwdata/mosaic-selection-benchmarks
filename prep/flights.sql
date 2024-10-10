-- first download data from https://transtats.bts.gov/PREZIP/
-- get 'On_Time_Marketing_Carrier' files for 2019-2024
-- unzip files locally under data/flights

-- load flights data from constiutent csv files
CREATE VIEW flights AS SELECT * FROM 'data/flights/*/*.csv';

-- extract airlines, time, delay and distance data
-- parse timestamps, map to [0-24] hour range
-- filter records with null values for measures of interest
COPY (SELECT
  Operating_Airline AS airline,
  FLOOR(DepTime::SMALLINT / 100 + (DepTime::SMALLINT % 100) / 60)::FLOAT AS time,
  ArrDelay::SMALLINT AS delay,
  Distance::SMALLINT AS distance
FROM flights
WHERE DepTime IS NOT NULL AND ArrDelay IS NOT NULL AND Distance IS NOT NULL)
TO 'data/flights.parquet' (FORMAT PARQUET);
