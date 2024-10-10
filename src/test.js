import { readFile } from 'node:fs/promises';
import { benchmark } from './benchmark.js';
import { analyze } from './analyze.js';

export async function runTest(options = { connector: 'wasm', parallel: true }) {
  try {
    const data = await benchmark({
      metadata: { name: 'test' },
      ...options,
      verbose: true,
      load: `
        CREATE TEMP TABLE IF NOT EXISTS flights10m AS
        SELECT
          GREATEST(-60, LEAST(ARR_DELAY, 180))::DOUBLE AS delay,
          DISTANCE AS distance,
          DEP_TIME AS time
        FROM 'data/flights-10m.parquet'`,
      tasks: JSON.parse(await readFile('tasks/test.json'))
    });
    console.log(analyze(data).objects());
  } catch (error) {
    console.error(error);
  }
}
