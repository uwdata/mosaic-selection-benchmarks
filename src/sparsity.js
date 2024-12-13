import { nodeConnector } from './util/node-connector.js';

export async function sparsity({
  name,
  size,
  load,
  tasks = []
} = {}) {
  const data = [];
  const db = nodeConnector();
  const createTasks = tasks.filter(t => t.stage === 'create');

  // load base table
  await db.query({ type: 'exec', sql: load });

  // get base column names
  const desc = await db.query({
    type: 'arrow',
    sql: `DESCRIBE SELECT * FROM ${name}`
  });
  const cols = desc.getChild('column_name').toArray();

  for (const task of createTasks) {
    try {
      const { activeView, query } = task;

      // identify base columns referenced in query
      const refs = referencedColumns(cols, query);

      // create materialized view
      await db.query({ type: 'exec', sql: query });

      // get row count
      const view = viewName(query);
      const result = await db.query({
        type: 'arrow',
        sql: `SELECT COUNT(*) AS rows FROM ${view}`
      });
      const rows = (result.toArray())[0].rows;

      data.push({ name, size, activeView, refs, rows });
    } catch (err) {
      console.log(err);
      return [];
    }
  }
  return data;
}

function viewName(query) {
  const i1 = query.indexOf('mosaic.cube_');
  const i2 = query.indexOf(' ', i1);
  return query.slice(i1, i2);
}

function referencedColumns(columns, query) {
  const ref = columns.filter(name => query.includes(`"${name}"`));
  ref.sort();
  return ref.join('|');
}
