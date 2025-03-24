/*
 * This benchmark computes panning performance as reported in Appendix A
 *
 * The benchmark uses proprietary data from neuroscience collaborators:
 *  - data/neuro-by-col.parquet
 *  - data/neuro-by-row.parquet
 *
 * The neuron recording data is NOT included in this repo!
 * Please contact the authors if you are interested in data access.
 */
import { writeFile } from 'node:fs/promises';
import { tilePan } from '../src/index.js';
import { nodeConnector } from '../src/util/node-connector.js';

const args = process.argv.slice(2);

// benchmark parameters
const name = args[0] || 'neuro';
const skips = 10; // number of "skips" when panning (jumping to new area)
const steps = 100; // number of "steps" when panning (local increments)

// run benchmark batches
for (const view of [false]) { // load data as view (true) or table (false)
  for (const sorted of [false]) { // use sorted data
    await runBatch(name, sorted, view);
  }
}

// run batch of benchmarks against same table
async function runBatch(name, sorted, view) {
  const suf = sorted ? 'row' : 'col';
  const table = `${name}_${suf}`;
  const type = view ? 'VIEW' : 'TABLE';
  console.log(`LOADING ${type} data/neuro-by-${suf}.parquet...`);

  const db = nodeConnector();
  await db.query({
    type: 'exec',
    sql: [
      `PRAGMA max_temp_directory_size='200GiB'`,
      `CREATE ${type} ${table} AS (SELECT * FROM 'data/neuro-by-${suf}.parquet')`
    ].join('; ')
  });

  // untiled
  await run(db, table, name, sorted, false, false, view, skips, steps);
  // tiled, no prefetching (only caching)
  await run(db, table, name, sorted, true, false, view, skips, steps);
  // tiled, with prefetching
  await run(db, table, name, sorted, true, true, view, skips, steps);
}

// run benchmark for a single condition
async function run(db, table, name, sorted, tiled, prefetch, view, skips, steps) {
  const prefix = [
    name,
    sorted ? 'sorted' : 'unsorted',
    tiled ? 'tile' : 'direct',
    prefetch ? 'prefetch' : null,
    view ? 'view' : null
  ].filter(x => x).join('-');
  console.log(`RUNNING ${prefix}`);

  try {
    const t0 = Date.now();
    const data = await tilePan({
      metadata: { name, sorted, tiled, prefetch, view },
      db,
      table,
      steps,
      skips,
      tiled,
      prefetch
    });
    const time = Date.now() - t0;

    const total = data.reduce((s, d) => s + d.time, 0);
    const avg = (total / data.length).toFixed(2);

    const outputFile = `results/tile-pan/${prefix}.json`;
    console.log(`WRITING ${outputFile} (${time} ms total, ${avg} ms avg update)`);
    await writeFile(outputFile, JSON.stringify(data));
  } catch (err) {
    console.error(err);
  }
}
