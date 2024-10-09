import { performance } from 'node:perf_hooks';

export class Benchmark {
  constructor(name, connector) {
    this.name = name;
    this.connector = connector;
    this.data = [];
  }

  async init(loadQueries) {
    const sql = [loadQueries].flat().join('; ');
    const t0 = performance.now();
    await this.connector.query({ type: 'exec', sql });
    const t1 = performance.now();
    this.data.push({
      name: this.name,
      stage: 'load',
      time: t1 - t0
    });
  }

  async run(tasks) {
    for (const task of tasks) {
      try {
        const sql = task.query;
        const type = task.stage === 'create' ? 'exec' : 'arrow';

        const t0 = performance.now();
        const result = await this.connector.query({ type, sql });
        const t1 = performance.now();

        this.data.push({
          name: this.name,
          ...task,
          time: t1 - t0,
          rows: result?.numRows ?? -1,
          cols: result?.numCols ?? -1
        });
      } catch (err) {
        console.error(err);
      }
    }
  }
}