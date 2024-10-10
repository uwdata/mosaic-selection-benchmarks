import { performance } from 'node:perf_hooks';
import { nodeConnector } from './util/node-connector.js';
import { wasmConnector } from './util/wasm-connector.js';

const CONNECTORS = {
  node: nodeConnector,
  wasm: wasmConnector
};

export async function benchmark({
  metadata = {},
  connector = 'node',
  verbose = false,
  load,
  tasks = []
}) {
  const db = typeof connector === 'string'
    ? CONNECTORS[connector]()
    : connector;
  const data = [];

  if (load) {
    const sql = [load].flat().join('; ');
    if (verbose) console.log('LOADING DATA', sql);

    const t0 = performance.now();
    await db.query({ type: 'exec', sql });
    const t1 = performance.now();
    data.push({
      ...metadata,
      stage: 'load',
      start: t0,
      end: t1,
      time: t1 - t0
    });
  }

  if (verbose) console.log('RUNNING TASKS');
  for (const task of tasks) {
    try {
      const sql = task.query;
      const type = task.stage === 'create' ? 'exec' : 'arrow';

      const t0 = performance.now();
      const result = await db.query({ type, sql });
      const t1 = performance.now();

      data.push({
        ...metadata,
        ...task,
        start: t0,
        end: t1,
        time: t1 - t0,
        rows: result?.numRows ?? -1,
        cols: result?.numCols ?? -1
      });
    } catch (err) {
      console.error(err, task);
    }
  }

  await db.close?.();

  return data;
}
