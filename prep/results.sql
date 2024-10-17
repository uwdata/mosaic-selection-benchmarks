CREATE TABLE std AS
  SELECT 'unopt' AS condition, *
  FROM 'results/*-node-std-1*.csv';

CREATE TABLE node AS
  SELECT 'node' AS condition, *
  FROM 'results/*-node-opt-1*.csv';

CREATE TABLE wasm AS
  SELECT 'wasm' AS condition, *
  FROM 'results/*-wasm-opt-1*.csv';

CREATE TABLE external AS
  SELECT * FROM 'results/vegaplus.csv';

CREATE VIEW combined AS
  SELECT * FROM std UNION ALL
  SELECT * FROM node UNION ALL
  SELECT * FROM wasm UNION ALL
  SELECT * FROM external;

COPY combined TO 'results/results.parquet' (FORMAT PARQUET);
