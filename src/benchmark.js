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
  let taskid = 0;

  if (load) {
    try {
      const sql = [load].flat().join('; ');
      if (verbose) console.log('LOADING DATA', sql);

      const t0 = performance.now();
      await db.query({ type: 'exec', sql });
      const t1 = performance.now();
      data.push({
        ...metadata,
        taskid,
        activeView: 'none',
        updateId: -1,
        stage: 'load',
        query: sql,
        start: t0,
        end: t1,
        time: t1 - t0
      });
    } catch (err) {
      console.error(err);
      return null;
    }
  }

  if (parallel) {
    if (verbose) console.log('RUNNING TASKS IN PARALLEL');
    let curKey = null;
    let taskQueue = [];
    let counter = 0;
    const decile = Math.floor(tasks.length / 10);

    async function processQueue() {
      const results = await Promise.allSettled(taskQueue);
      for (const result of results) {
        if (result.status === 'fulfilled') {
          data.push(result.value);
          ++counter;
          if (verbose && counter % decile === 0) {
            console.log(`PROGRESS: ${counter} / ${tasks.length} (${(100 * counter / tasks.length).toFixed(1)}%)`);
          }
        } else {
          console.error(result.reason);
          return false;
        }
      }
      taskQueue = [];
      return true;
    }

    for (const task of tasks) {
      try {
        const key = `${task.stage}-${task.activeView ?? 'none'}-${task.updateId ?? 'x'}`;
        const sql = task.query;
        const type = task.stage === 'create' ? 'exec' : 'arrow';

        if (key !== curKey) {
          const signal = await processQueue();
          if (!signal) return null;
          curKey = key;
          ++taskid;
        }

        taskQueue.push(
          query(db, type, sql)
            .then(result => ({
              ...metadata,
              taskid,
              stage: task.stage,
              query: task.query,
              activeView: task.activeView ?? 'none',
              updateId: task.updateId ?? -1,
              ...result
            }))
            .catch(err => {
              console.error(err);
              console.error('TASK', task);
            })
        );
      } catch (err) {
        console.error(err, task);
      }
    }

    await processQueue();
  } else {
    if (verbose) console.log('RUNNING TASKS SEQUENTIALLY');
    let curKey = null;
    for (const task of tasks) {
      try {
        const key = `${task.stage}-${task.activeView ?? 'none'}-${task.updateId ?? 'x'}`;
        const sql = task.query;
        const type = task.stage === 'create' ? 'exec' : 'arrow';

        if (key !== curKey) {
          curKey = key;
          ++taskid;
        }
        const result = await query(db, type, sql);
        data.push({
          ...metadata,
          taskid,
          stage: task.stage,
          query: task.query,
          activeView: task.activeView ?? 'none',
          updateId: task.updateId ?? -1,
          ...result
        });
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