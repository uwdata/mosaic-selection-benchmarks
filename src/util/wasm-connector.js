import * as path from 'node:path';
import * as duckdb from '@duckdb/duckdb-wasm';
import { tableFromIPC } from '@uwdata/flechette';
import { Worker } from './web-worker.js';

const DUCKDB_DIST = 'node_modules/@duckdb/duckdb-wasm/dist';

// bypass duckdb-wasm query method to get Arrow IPC bytes directly
// https://github.com/duckdb/duckdb-wasm/issues/267#issuecomment-2252749509
function getArrowIPC(con, query) {
  return new Promise((resolve, reject) => {
    con.useUnsafe(async (bindings, conn) => {
      try {
        const buffer = await bindings.runQuery(conn, query);
        resolve(buffer);
      } catch (error) {
        reject(error);
      }
    });
  });
}

export function wasmConnector(options = {}) {
  const { duckdb, connection, ...opts } = options;
  let db = duckdb;
  let con = connection;
  let loadPromise;

  function load() {
    if (!loadPromise) {
      // use a loading promise to avoid race conditions
      // synchronizes multiple callees on the same load
      loadPromise = (db
        ? Promise.resolve(db)
        : initDatabase(opts).then(result => db = result))
        .then(db => db.connect())
        .then(result => con = result);
    }
    return loadPromise;
  }

  /**
   * Get the backing DuckDB-WASM instance.
   * Will lazily initialize DuckDB-WASM if not already loaded.
   * @returns {Promise<duckdb.AsyncDuckDB>} The DuckDB-WASM instance.
   */
  async function getDuckDB() {
    if (!db) await load();
    return db;
  }

  /**
   * Get the backing DuckDB-WASM connection.
   * Will lazily initialize DuckDB-WASM if not already loaded.
   * @returns {Promise<duckdb.AsyncDuckDBConnection>} The DuckDB-WASM connection.
   */
  async function getConnection() {
    if (!con) await load();
    return con;
  }

  return {
    getDuckDB,
    getConnection,
    /**
     * Query the DuckDB-WASM instance.
     * @param {object} query
     * @param {'exec' | 'arrow' | 'json'} [query.type] The query type.
     * @param {string} query.sql A SQL query string.
     * @returns the query result
     */
    query: async query => {
      const { type, sql } = query;
      const con = await getConnection();
      const result = await getArrowIPC(con, sql);
      return type === 'exec' ? undefined
        : type === 'arrow' ? tableFromIPC(result)
        : tableFromIPC(result).toArray();
    },
    async close() {
      if (db) {
        await db.terminate();
        await db.__worker.terminate();
      }
    }
  };
}

async function initDatabase({
  log = false
} = {}) {
  const DUCKDB_CONFIG = await duckdb.selectBundle({
    mvp: {
      mainModule: path.resolve(DUCKDB_DIST, './duckdb-mvp.wasm'),
      mainWorker: path.resolve(DUCKDB_DIST, './duckdb-node-mvp.worker.cjs'),
    },
    eh: {
      mainModule: path.resolve(DUCKDB_DIST, './duckdb-eh.wasm'),
      mainWorker: path.resolve(DUCKDB_DIST, './duckdb-node-eh.worker.cjs'),
    }
  });

  // Instantiate the asynchronus version of DuckDB-wasm
  const worker = new Worker(DUCKDB_CONFIG.mainWorker);
  const logger = log ? new duckdb.ConsoleLogger() : new duckdb.VoidLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(DUCKDB_CONFIG.mainModule, DUCKDB_CONFIG.pthreadWorker);

  db.__worker = worker;
  return db;
}
