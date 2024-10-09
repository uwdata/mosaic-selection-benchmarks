import { readFile } from 'node:fs/promises';
import { Benchmark } from './benchmark.js';
// import { nodeConnector } from './util/node-connector.js';
import { wasmConnector } from './util/wasm-connector.js';

// const load = `CREATE TEMP TABLE IF NOT EXISTS flights10m AS
// SELECT
//   GREATEST(-60, LEAST(ARR_DELAY, 180))::DOUBLE AS delay,
//   DISTANCE AS distance,
//   DEP_TIME AS time
// FROM 'https://idl.uw.edu/mosaic-datasets/data/flights-10m.parquet'`;

const load = `CREATE TEMP TABLE IF NOT EXISTS flights10m AS
SELECT 180 AS delay, 1000 AS distance, 7 AS time UNION ALL
SELECT 0 AS delay, 5000 AS distance, 24 AS time`;

export async function run() {
  try {
    console.log('LOADING TASKS');
    const tasks = JSON.parse(
      await readFile('tasks/test.json')
    );

    const c = wasmConnector();
    const b = new Benchmark('test', c);

    console.log('LOADING DATA');
    await b.init(load);

    console.log('RUNNING TASKS');
    await b.run(tasks);
    console.log(b.data);

    await c.close?.();
  } catch (error) {
    console.error(error);
  }
}
