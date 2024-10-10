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
  parallel = false,
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
      taskid: 0,
      stage: 'load',
      query: sql,
      start: t0,
      end: t1,
      time: t1 - t0
    });
  }

  if (parallel) {
    if (verbose) console.log('RUNNING TASKS IN PARALLEL');
    let curTaskId = -1;
    let taskQueue = [];

    async function processQueue(id) {
      const results = await Promise.allSettled(taskQueue);
      for (const result of results) {
        if (result.status === 'fulfilled') {
          data.push(result.value);
        } else {
          console.error(result.reason);
        }
      }
      taskQueue = [];
      curTaskId = id;
    }

    for (const task of tasks) {
      try {
        const id = task.taskid;
        const sql = task.query;
        const type = task.stage === 'create' ? 'exec' : 'arrow';

        if (id !== curTaskId) {
          await processQueue(id);
        }

        taskQueue.push(
          query(db, type, sql)
            .then(result => ({ ...metadata, ...task, ...result }))
        );
      } catch (err) {
        console.error(err, task);
      }
    }

    await processQueue(curTaskId);
  } else {
    if (verbose) console.log('RUNNING TASKS SEQUENTIALLY');
    for (const task of tasks) {
      try {
        const sql = task.query;
        const type = task.stage === 'create' ? 'exec' : 'arrow';
        const result = await query(db, type, sql);
        data.push({ ...metadata, ...task, ...result });
      } catch (err) {
        console.error(err, task);
      }
    }
  }

  await db.close?.();

  return data;
}

async function query(db, type, sql) {
  const t0 = performance.now();
  const result = await db.query({ type, sql });
  const t1 = performance.now();
  return {
    start: t0,
    end: t1,
    time: t1 - t0,
    rows: result?.numRows ?? -1,
    cols: result?.numCols ?? -1
  };
}