import { wasmConnector } from '@uwdata/mosaic-core';

export function benchmarkConnector(connector = wasmConnector()) {
  let queries = [];
  let currentStage = 'init';
  let currentVisualization = 'none';
  let currentProps = {};

  return {
    /**
     * Issue a query and also add it to the internal query log.
     * @param {object} query
     * @param {'exec' | 'arrow' | 'json'} [query.type] The query type.
     * @param {string} [query.sql] A SQL query string.
     * @returns the query result
     */
    async query(query) {
      const queryObj = {
        name: currentVisualization,
        stage: currentStage,
        query: query.sql,
        ...currentProps
      }
      queries.push(queryObj);
      return connector.query(query);
    },
    dumpQueries() {
      const ret = queries;
      queries = [];
      return ret;
    },
    stage(stage) {
      console.log('stage', stage);
      currentStage = stage;
    },
    visualization(visualization) {
      console.log('visualization', visualization);
      currentVisualization = visualization;
    },
    props(props) {
      currentProps = props;
    },
    async reset() {
      console.log('reset');
      currentStage = 'init';
      currentVisualization = 'none';
      currentProps = {};
      queries = [];
      const tables = await connector.query({ type: 'json', sql: 'SHOW ALL TABLES' });
      for (const table of tables) {
        await connector.query({
          type: 'exec',
          sql: `DROP TABLE ${table.schema ? `${table.schema}.${table.name}` : table.name}`
        });
      }
    }
  };
}
