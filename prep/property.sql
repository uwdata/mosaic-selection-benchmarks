-- first download csv to data folder from
-- http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv

-- parse csv data
CREATE VIEW parse AS SELECT
  column01::INTEGER AS price,
  column02::DATE AS date,
  random() AS r
FROM 'data/pp-complete.csv' ORDER BY r;

-- prepare data, map dates to years and fractions
CREATE VIEW property AS SELECT
  price,
  (YEAR(date) + DAYOFYEAR(date) / DAYOFYEAR(MAKE_DATE(YEAR(date), 12, 31)))::FLOAT AS date
FROM parse
WHERE price IS NOT NULL AND date IS NOT NULL;

-- write result to new parquet file
COPY property TO 'data/property.parquet' (FORMAT PARQUET);
