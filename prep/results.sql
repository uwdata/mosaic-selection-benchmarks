-- combines benchmark output files into single tables

-- combine data for preaggregation optimization benchmark
CREATE TABLE std AS
  SELECT 'unopt' AS condition, *
  FROM 'results/bench/*-node-std-1*.csv';

CREATE TABLE node AS
  SELECT 'node' AS condition, *
  FROM 'results/bench/*-node-opt-1*.csv';

CREATE TABLE wasm AS
  SELECT 'wasm' AS condition, *
  FROM 'results/bench/*-wasm-opt-1*.csv';

CREATE TABLE external AS
  SELECT * FROM 'results/vegaplus.csv';

CREATE VIEW combined AS
  SELECT * FROM std UNION ALL
  SELECT * FROM node UNION ALL
  SELECT * FROM wasm UNION ALL
  SELECT * FROM external;

COPY combined TO 'results/results.parquet' (FORMAT PARQUET);

-- combine data for panning (sort / prefetch) benchmark
CREATE TABLE tilepan AS
 SELECT
   name,
   sorted,
   CASE
     WHEN tiled AND prefetch THEN 'Prefetch Tiles'
     WHEN tiled THEN 'Tiles'
     ELSE 'Query' END AS condition,
   time
 FROM 'results/tile-pan/*.json';

COPY tilepan TO 'results/tile-pan.parquet' (FORMAT PARQUET);
