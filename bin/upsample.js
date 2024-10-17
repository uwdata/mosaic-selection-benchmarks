import { tableFromIPC } from '@uwdata/flechette';
import { DuckDB } from '@uwdata/mosaic-duckdb';

async function upsample(size, table, loadQuery, sampleQuery) {
  const db = new DuckDB();

  // load table
  await db.exec(`CREATE VIEW ${table} AS ${loadQuery}`);

  // get table size
  const bytes = await db.arrowBuffer(`SELECT COUNT(*) AS count FROM ${table}`);
  const [{ count }] = tableFromIPC(bytes).toArray();
  console.log(`Upsampling ${table} from ${count} to ${size}...`);

  // create view to sample from
  await db.exec(`CREATE VIEW sample AS ${sampleQuery} FROM ${table}`);

  const iter = Math.ceil(size / count) - 1;
  const calls = Array.from({ length: iter}, () => `SELECT * FROM sample`);
  const query = [`SELECT * FROM ${table}`, ...calls].join(' UNION ALL ');

  await db.exec(`COPY (${query}) TO 'data/${table}-1b.parquet' (FORMAT PARQUET)`);
}

// upsample to 1B rows
const size = 1_000_000_000;

const airlinesLoad = `SELECT * FROM 'data/flights.parquet' WHERE airline != 'KS' AND airline != '9K'`;
const airlinesSample =
`SELECT
  airline,
  LEAST(23.99, time + random())::FLOAT as time,
  FLOOR(delay + (0.5 - random()) * delay)::SMALLINT as delay,
  FLOOR(distance + (0.5 - random()) * distance)::SMALLINT as distance`;
await upsample(size, 'airlines', airlinesLoad, airlinesSample);

const flightsLoad = `SELECT
  time,
  GREATEST(-60, LEAST(delay, 180))::SMALLINT as delay,
  LEAST(distance, 3000)::SMALLINT as distance
FROM 'data/flights.parquet'`;
const flightsSample =
`SELECT
  LEAST(23.99, time + random())::FLOAT as time,
  GREATEST(-60, LEAST(FLOOR(delay + (0.5 - random()) * delay), 180))::SMALLINT as delay,
  LEAST(FLOOR(distance + (0.5 - random()) * distance), 3000)::SMALLINT as distance`;
await upsample(size, 'flights', flightsLoad, flightsSample);

const propertyLoad = `SELECT * FROM 'data/property.parquet'`;
const propertySample =
`SELECT
  FLOOR(price + (0.5 - random()) * price)::INTEGER AS price,
  (date + (0.5 * random()))::FLOAT AS date`;
await upsample(size, 'property', propertyLoad, propertySample);

const taxisLoad = `SELECT * FROM 'data/nyc-taxi-trips.parquet'`;
const taxisSample =
`SELECT
  LEAST(23.99, time + random())::FLOAT as time,
  (px + FLOOR((0.5 - random()) * 1000))::FLOAT as px,
  (py + FLOOR((0.5 - random()) * 1000))::FLOAT as py,
  (dx + FLOOR((0.5 - random()) * 1000))::FLOAT as dx,
  (dy + FLOOR((0.5 - random()) * 1000))::FLOAT as dy`;
await upsample(size, 'taxis', taxisLoad, taxisSample);
