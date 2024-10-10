-- load original data
CREATE VIEW gaia AS SELECT * FROM 'data/gaia.parquet';

-- convert ra, dec to galactic coordinates l, b
-- see https://astronomy.stackexchange.com/questions/53397/how-can-i-convert-my-sky-coordinate-system-ra-dec-into-galactic-coordinate-sy
-- apply projection to l, b to make galaxy map
-- filter to valid values and desired parallax range
-- project to desired columns only
CREATE VIEW gp AS
WITH prep AS (
  SELECT
    asin(sin(0.473479) * sin(dec) + cos(0.473479) * cos(dec) * cos(ra - 3.36603)) AS b,
    2.14557 - asin(cos(dec) * sin(ra - 3.36603) / cos(b)) AS l,
    radians((-l + 540) % 360 - 180) AS lambda,
    radians(b) AS phi,
    asin(sqrt(3)/2 * sin(phi)) AS t,
    t^2 AS t2,
    t2^3 AS t6,
    *
  FROM gaia
  WHERE parallax BETWEEN -5 AND 20 AND phot_g_mean_mag IS NOT NULL AND bp_rp IS NOT NULL
)
SELECT
  ((1.340264 * lambda * cos(t)) / (sqrt(3)/2 * (1.340264 + (-0.081106 * 3 * t2) + (t6 * (0.000893 * 7 + 0.003796 * 9 * t2)))))::FLOAT AS u,
  (t * (1.340264 + (-0.081106 * t2) + (t6 * (0.000893 + 0.003796 * t2))))::FLOAT AS v,
  parallax,
  phot_g_mean_mag,
  bp_rp
FROM prep;

-- write result to new parquet file
COPY gp TO 'data/gaia_projected.parquet' (FORMAT PARQUET);
