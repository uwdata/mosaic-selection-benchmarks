# mosaic-selection-benchmarks

Query performance benchmarks for Mosaic selections.

Loads DuckDB either in-process or via WASM, issues benchmark task queries against the database, and records the results.

Source data files to load should be placed in `/data`.

Recorded queries should be provided in JSON format in `/tasks`.
See `tasks/test.json` for an example.

## Running Instructions

- Run `npm i` to install dependencies.
- Run `node bin/run-test.js` to run the test benchmark example.
- Run `npm run dev` to launch visualization examples.
