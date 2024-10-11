-- first download data from https://transtats.bts.gov/PREZIP/
-- get 'On_Time_Marketing_Carrier' files for 2018-2024
-- unzip files locally under data/flights
-- fix non-unicode chars in 2018 data using iconv

-- load flights data from constituent csv files
-- extract airlines, time, delay and distance data
-- parse timestamps, map to [0-24] hour range
-- filter records with null values for measures of interest
CREATE VIEW flights AS SELECT
  Operating_Airline AS airline,
  (DepTime::SMALLINT / 100 + (DepTime::SMALLINT % 100) / 60)::FLOAT AS time,
  ArrDelay::SMALLINT AS delay,
  Distance::SMALLINT AS distance
FROM read_csv('data/flights/*/*.csv')
WHERE DepTime IS NOT NULL AND ArrDelay IS NOT NULL AND Distance IS NOT NULL;

COPY flights TO 'data/flights.parquet' (FORMAT PARQUET);
