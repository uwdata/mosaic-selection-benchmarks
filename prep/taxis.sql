-- first download taxi data files from urls listed in
-- https://raw.githubusercontent.com/pdet/taxi-benchmark/refs/tags/0.1/files.txt
-- save files under data/trips

-- use duckdb spatial extension
LOAD spatial;

-- create taxi trips table schema
CREATE TABLE trips (
  trip_id                 BIGINT,
  vendor_id               VARCHAR,
  pickup_datetime         TIMESTAMP,
  dropoff_datetime        TIMESTAMP,
  store_and_fwd_flag      VARCHAR,
  rate_code_id            BIGINT,
  pickup_longitude        DOUBLE,
  pickup_latitude         DOUBLE,
  dropoff_longitude       DOUBLE,
  dropoff_latitude        DOUBLE,
  passenger_count         BIGINT,
  trip_distance           DOUBLE,
  fare_amount             DOUBLE,
  extra                   DOUBLE,
  mta_tax                 DOUBLE,
  tip_amount              DOUBLE,
  tolls_amount            DOUBLE,
  ehail_fee               DOUBLE,
  improvement_surcharge   DOUBLE,
  total_amount            DOUBLE,
  payment_type            VARCHAR,
  trip_type               VARCHAR,
  pickup                  VARCHAR,
  dropoff                 VARCHAR,
  cab_type                VARCHAR,
  precipitation           BIGINT,
  snow_depth              BIGINT,
  snowfall                BIGINT,
  max_temperature         BIGINT,
  min_temperature         BIGINT,
  average_wind_speed      BIGINT,
  pickup_nyct2010_gid     BIGINT,
  pickup_ctlabel          VARCHAR,
  pickup_borocode         BIGINT,
  pickup_boroname         VARCHAR,
  pickup_ct2010           VARCHAR,
  pickup_boroct2010       BIGINT,
  pickup_cdeligibil       VARCHAR,
  pickup_ntacode          VARCHAR,
  pickup_ntaname          VARCHAR,
  pickup_puma             VARCHAR,
  dropoff_nyct2010_gid    BIGINT,
  dropoff_ctlabel         VARCHAR,
  dropoff_borocode        BIGINT,
  dropoff_boroname        VARCHAR,
  dropoff_ct2010          VARCHAR,
  dropoff_boroct2010      BIGINT,
  dropoff_cdeligibil      VARCHAR,
  dropoff_ntacode         VARCHAR,
  dropoff_ntaname         VARCHAR,
  dropoff_puma            VARCHAR);

-- load taxi data from compressed csvs
SET preserve_insertion_order = false;
COPY trips FROM 'data/trips/trips_*.csv.gz' (header 0);

-- load taxi data and project lat/lon points
CREATE VIEW projected AS SELECT
  pickup_datetime::TIMESTAMP AS datetime,
  ST_Transform(ST_Point(pickup_latitude, pickup_longitude), 'EPSG:4326', 'ESRI:102718') AS pick,
  ST_Transform(ST_Point(dropoff_latitude, dropoff_longitude), 'EPSG:4326', 'ESRI:102718') AS drop
FROM trips;

-- map timestamps to [0, 24] hour range
-- extract projected pickup/dropoff coordinates
CREATE VIEW taxis AS SELECT
  ((HOUR(datetime) + MINUTE(datetime)/60))::FLOAT AS time,
  ST_X(pick)::FLOAT AS px,
  ST_Y(pick)::FLOAT AS py,
  ST_X(drop)::FLOAT AS dx,
  ST_Y(drop)::FLOAT AS dy
FROM projected
WHERE NOT (ISINF(dy) OR ISINF(dx) OR ISINF(px) OR ISINF(py));

-- write result to new parquet file
COPY taxis TO 'data/taxis-1b.parquet' (FORMAT PARQUET);
