-- download data at http://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/
-- save files under data/gaia

-- load original data
CREATE VIEW gaia AS
SELECT ra, dec, parallax, phot_g_mean_mag, bp_rp
FROM 'data/gaia/*.csv.gz';

-- first convert ra, dec to galactic coordinates l, b
-- see https://astronomy.stackexchange.com/questions/53397/how-can-i-convert-my-sky-coordinate-system-ra-dec-into-galactic-coordinate-sy
-- then apply equal earth projection to l, b to make galaxy map
-- filter to valid values and desired parallax range
CREATE VIEW projected AS
WITH prep AS (
  SELECT
    radians(ra) AS a,
    radians(dec) AS d,
    asin(sin(0.473479) * sin(d) + cos(0.473479) * cos(d) * cos(a - 3.36603)) AS phi,
    2.14557 - atan2(cos(d) * sin(a - 3.36603), cos(0.473479) * sin(d) - sin(0.473479) * cos(d) * cos(a - 3.36603)) AS l,
    radians((-degrees(l) + 540) % 360 - 180) AS lambda,
    asin(sqrt(3)/2 * sin(phi)) AS t,
    t^2 AS t2,
    t2^3 AS t6,
    *
  FROM gaia
  WHERE parallax BETWEEN -5 AND 20
    AND phot_g_mean_mag IS NOT NULL
    AND bp_rp IS NOT NULL
    AND ra IS NOT NULL
    AND dec IS NOT NULL
)
SELECT
  ((1.340264 * lambda * cos(t)) / (sqrt(3)/2 * (1.340264 + (-0.081106 * 3 * t2) + (t6 * (0.000893 * 7 + 0.003796 * 9 * t2)))))::FLOAT AS u,
  (t * (1.340264 + (-0.081106 * t2) + (t6 * (0.000893 + 0.003796 * t2))))::FLOAT AS v,
  parallax,
  phot_g_mean_mag,
  bp_rp
FROM prep;

-- write result to new parquet file
COPY projected TO 'data/gaia.parquet' (FORMAT PARQUET);
