# mosaic-selection-benchmarks

Query performance benchmarks for Mosaic selections.

This repository is intended to accompany the research paper "Mosaic Selections: Managing and Optimizing User Selections for Scalable Data Visualization Systems".

The benchmarks load DuckDB either in-process or via WASM, issues benchmark task queries against the database, and records the results.

Source data files to load should be placed in `data/`.

Recorded queries should be provided in JSON format in `tasks/`.
See the `tasks/` folder for examples.

## Running Instructions

_Note: for review purposes, this repo includes example datasets as 100k row samples to keep the total file size down. See the files in the prep/ folder for instructions to retrieve full datasets._

### Preliminaries

- Ensure you have node.js version 20 or higher installed.
- Run `npm i` to install dependencies.

### Benchmark Query Generation

_For review purposes, this step can be skipped. Benchmark queries are already in the `tasks/` folder._

- Run `npm run dev` to launch visualization examples.
- Select a template using the "Specification" menu and click the `Run` button to load the example, simulate interactions, and generate benchmark queries. Resulting query logs will be downloaded as a JSON file. The "Optimize" checkbox controls whether or not pre-aggregated materialized views are created.

### Run Benchmarks

_For review purposes, this step can also be skipped. Benchmark results are in the `results/` folder._

- Ensure benchmark queries have been generated and reside in the `tasks/` folder.
- Download and prepare datasets as needed. The scripts in `prep` include download instructions and SQL queries for data prep. Prepared datasets must reside in the `data` folder.
- Run `node bin/upsample.js` to create upsampled datasets (up to 1 billion rows).
- Run benchmarks using the `bin/bench.js` script. For example:
  - `npm run bench flights node opt` - benchmark 'flights' example queries in standard DuckDB (loaded within node.js) with materialized view optimizations
  - `npm run bench airlines node std` - benchmark 'flights' example queries in DuckDB-WASM *without* materialized view optimizations
  - `npm run bench airlines wasm` - benchmark 'airlines' example queries in DuckDB-WASM with materialized view optimizations

### Analyze Results

- Upon completion of benchmarks, run the `prep/results.sql` script in DuckDB to consolidate all benchmark results. _You can safely skip this step if reviewing, `results/results.parquet` should already exist._
- Run `npm run dev` and browse to `http://localhost:5173/web/results/` to see result visualization.
