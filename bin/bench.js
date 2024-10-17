import { readFile, writeFile } from 'node:fs/promises';
import { analyze, run } from '../src/index.js';

const args = process.argv.slice(2);

// benchmark conditions
const name = args[0] || 'flights';
const connector = args[1] || 'node';
const optimized = ({ opt: true, std: false })[args[2] || 'opt'];
if (optimized == null) throw new Error(`Unrecognized optimization option: ${args[2]}`);

// WARNING: the 1e9 condition will take a VERY LONG TIME under 'std' queries
const sizes = [1e4, 1e5, 1e6, 1e7, 1e8, 1e9];
console.log(`RUNNING BENCHMARKS FOR ${name} (${connector})`);

// duckdb settings
const parallel = true; // submit task queries in parallel (true) or sequence (false)
const view = false; // load data as view (true) or table (false)

const tasks = JSON.parse(await readFile(`tasks/${optimized ? 'optimized' : 'standard'}/${name}.json`));

const prefix = `${name}-${connector}-${optimized ? 'opt' : 'std'}${view ? '-view' : ''}`;

for (const size of sizes) {
  try {
    const t0 = Date.now();
    const data = await run({ name, size, connector, tasks, parallel, view });
    const time = Date.now() - t0;

    const results = analyze(data);
    const { avg } = results
      .filter(d => d.stage === 'update')
      .rollup({ avg: d => op.mean(d.time) })
      .object(0);

    const outputFile = `results/${prefix}-1e${Math.log10(size)}.csv`;
    console.log(`WRITING RESULTS TO ${outputFile} (${time} ms total, ${avg.toFixed(1)} ms avg update)`);
    await writeFile(outputFile, results.toCSV());
  } catch (err) {
    console.error(err);
  }
}
