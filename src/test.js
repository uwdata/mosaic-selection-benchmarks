import { readFile } from 'node:fs/promises';
import { benchmark } from './benchmark.js';

export async function runTest() {
  try {
    const data = await benchmark({
      metadata: { name: 'test' },
      connector: 'node',
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
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}
