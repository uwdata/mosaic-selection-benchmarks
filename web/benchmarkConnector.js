import { wasmConnector } from '@uwdata/mosaic-core';

export function benchmarkConnector(connector = wasmConnector()) {
  let queries = [];
  let currentStage = 'init';
  let currentVisualization = 'none';
  let currentProps = {};
  
  return {
    /**
     * Query the underlying connector first with a EXPLAIN ANALYZE
     * prefixed query which gives detailed information on runtime metrics then
     * with the original query to get the result.
     * 
     * NOTE: This connector was designed to be used with DuckDB as the backing database so may not work 
     * with other databases that don't have same output formate for EXPLAIN ANALYZE.
     * NOTE: If using above functions to change output to JSON certain queries return an error instead
     * of timing information which is why they are currently unsused.
     * 
     * @param {object} query
     * @param {'exec' | 'arrow' | 'json' | 'create-bundle' | 'load-bundle'} [query.type] The query type.
     * @param {string} [query.sql] A SQL query string.
     * @param {string[]} [query.queries] The queries used to create a bundle.
     * @param {string} [query.name] The name of a bundle to create or load.
     * @returns the query result
     */
    async query(query) {
        const queryObj = {
            visualization: currentVisualization,
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
      const tables = await connector.query({ type: 'json', sql: "SHOW ALL TABLES"});
      for (const table of tables) {
        await connector.query({ type: 'exec', sql: `DROP TABLE
          ${table.schema ? `${table.schema}.${table.name}` : table.name}` });
      }
    }
  };
}
