import { benchmark } from './benchmark.js';

export async function run({
  name = 'flights',
  size = 1e5,
  connector = 'wasm',
  tasks = [],
  verbose = true,
  parallel = true,
  view = false
} = {}) {
  const type = view ? 'VIEW' : 'TABLE';
  const file = `data/${name}-1b.parquet`;
  const load = `CREATE ${type} ${name} AS SELECT * FROM '${file}' LIMIT ${size}`;

  return benchmark({
    metadata: { name, size, parallel, view },
    connector,
    parallel,
    verbose,
    tasks,
    load
  });
}
