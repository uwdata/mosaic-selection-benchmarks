-- first download taxi data files from
-- https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{0..19}.gz
-- gunzip files locally under data/trips
-- strip non-unicode characters using iconv, write as clean_trips_0, etc.

-- use duckdb spatial extension
LOAD spatial;

-- load taxi data and project lat/lon points
CREATE VIEW taxis AS SELECT
  pickup_datetime::TIMESTAMP AS datetime,
  ST_Transform(ST_Point(pickup_latitude, pickup_longitude), 'EPSG:4326', 'ESRI:102718') AS pick,
  ST_Transform(ST_Point(dropoff_latitude, dropoff_longitude), 'EPSG:4326', 'ESRI:102718') AS drop
FROM read_csv('data/trips/clean_trips_*', delim='\t');

-- map timestamps to [0, 24] hour range
-- extract projected pickup/dropoff coordinates
CREATE VIEW trips AS SELECT
  ((HOUR(datetime) + MINUTE(datetime)/60))::FLOAT AS time,
  ST_X(pick)::FLOAT AS px,
  ST_Y(pick)::FLOAT AS py,
  ST_X(drop)::FLOAT AS dx,
  ST_Y(drop)::FLOAT AS dy
FROM taxis;

-- write result to new parquet file
COPY trips TO 'data/nyc-taxi-trips.parquet' (FORMAT PARQUET);
