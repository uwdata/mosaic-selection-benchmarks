import { performance } from 'node:perf_hooks';
import { Coordinator } from '@uwdata/mosaic-core';
import { isBetween, Query, sql } from '@uwdata/mosaic-sql';
import { nodeConnector } from './util/node-connector.js';

export async function tilePan({
  metadata = {},
  steps = 100,
  skips = 10,
  db = nodeConnector(),
  tiled = false,
  prefetch = false,
  table
}) {
  const mc = new Coordinator(db, { logger: null });
  const data = [];
  const max = 10739994;
  const span = 1100; // span in data space for one "screen" of data
  const step = span / 2; // panning step size in data space
  const skipSize = Math.floor((max - steps * step) / skips);
  const tasks = [];
  for (let k = -1, skip = 0; skip < skips; ++skip) {
    const base = skip * skipSize;
    for (let i = 0; i < steps; ++i) {
      const lo = base + step * i;
      tasks.push({
        index: ++k,
        step: i,
        extent: [lo, lo + span - 1],
        prefetch
      });
    }
  }

  const method = tiled ? requestTiles : requestAll;
  const request = (extent, opt) => get(mc, method, table, extent, opt);

  for (const task of tasks) {
    try {
      const { result, promise } = await request(task.extent, { prefetch });
      data.push({ ...metadata, ...task, ...result });
      await promise;
    } catch (err) {
      console.error(err);
    }
  }

  return data;
}

async function get(mc, request, table, extent, opt) {
  const t0 = performance.now();
  const { grid, promise } = await request(mc, table, extent, opt);
  const t1 = performance.now();
  return {
    result: { start: t0, end: t1, time: t1 - t0, size: grid.length },
    promise
  };
}

function tileQuery(table, extent) {
  const w = 1100;
  return Query
    .select({
      index: sql`(row - ${extent[0]}) * ${w} + col`,
      value: 'value'
    })
    .from(table)
    .where(isBetween('row', extent));
}

async function requestAll(mc, table, extent) {
  // also possible: groupby index, sum(value)
  const w = 1100;
  const h = 384;
  const data = await mc.query(tileQuery(table, extent));
  const nrows = data.numRows;
  const I = data.getChild('index').toArray();
  const V = data.getChild('value').toArray();
  const grid = new Float64Array(w * h);
  for (let i = 0; i < nrows; ++i) {
    grid[I[i]] = V[i];
  }
  return { grid };
}

async function requestTiles(mc, table, extent, opt) {
  const w = 1100;
  const h = 384;
  const span = w / 2;

  // break extent into tile regions, query for those
  // get tile coords that overlap current view extent
  const [x0, x1] = extent;
  const y0 = 0;
  const i0 = Math.floor(x0 / span);
  const i1 = tileFloor(x1 / span);
  const j0 = 0;
  const j1 = 0;
  const xx = Math.floor(x0 * w / span);
  const yy = Math.floor(y0 * h / span); // yspan

  // query for currently needed data tiles
  const coords = [];
  for (let i = i0; i <= i1; ++i) {
    for (let j = j0; j <= j1; ++j) {
      coords.push([i, j]);
    }
  }
  const queries = coords.map(
    ([i]) => {
      const q = tileQuery(table, [i * span, tileFloor((i + 1) * span)]);
      return mc.query(q);
    }
  );
  const tiles = await Promise.all(queries);

  let promise = null;
  if (opt.prefetch) {
    // prefetch tiles along periphery of current tiles
    const coords = [];
    for (let j = j0; j <= j1; ++j) {
      coords.push([i1 + 1, j]);
      if (i0 > 0) coords.push([i0 - 1, j]);
    }
    promise = Promise.all(coords.map(
      ([i]) => mc.prefetch(tileQuery(table, [i * span, tileFloor((i + 1) * span)]))
    ));
  }

  return {
    grid: stitchTiles(w, h, xx, yy, coords, tiles),
    promise
  };
}

function stitchTiles(w, h, x, y, coords, tiles) {
  const grid = new Float64Array(w * h);
  tiles.forEach((data, index) => {
    const [i, j] = coords[index];
    const tx = i * w - x;
    const ty = j * h - y;
    copy(w, h, grid, data, tx, ty);
  });
  return grid;
}

function copy(w, h, grid, values, tx, ty) {
  // index = row + col * width
  const num = values.numRows;
  if (num === 0) return;
  const I = values.getChild('index').toArray();
  const V = values.getChild('value').toArray();
  for (let row = 0; row < num; ++row) {
    const idx = I[row];
    const i = tx + (idx % w);
    const j = ty + Math.floor(idx / w);
    if (0 <= i && i < w && 0 <= j && j < h) {
      grid[i + j * w] = V[row];
    }
  }
}

function tileFloor(value) {
  const floored = Math.floor(value);
  return floored === value ? floored - 1 : floored;
}
