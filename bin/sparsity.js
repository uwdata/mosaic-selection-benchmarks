import * as aq from 'arquero';
import { readFile, writeFile } from 'node:fs/promises';
import { sparsity } from '../src/index.js';
const { op } = aq;

// visualization template
const args = process.argv.slice(2);
const name = args[0] || 'flights';

const sizes = [1e4, 1e5, 1e6, 1e7, 1e8, 1e9];
const tasks = JSON.parse(await readFile(`tasks/optimized/${name}.json`));

// rewrite rules to disambiguate multiply-referenced columns
const rewrite = {
  mag: {
    'phot_g_mean_mag|u|v': 'mag1|u|v',
    'parallax|phot_g_mean_mag': 'parallax|mag1',
    'bp_rp|phot_g_mean_mag': 'bp_rp|mag1|mag2'
  },
  par: {
    'parallax|phot_g_mean_mag': 'parallax|mag1',
    'bp_rp|parallax|phot_g_mean_mag': 'bp_rp|parallax|mag2'
  },
  sky: {
    'phot_g_mean_mag|u|v': 'mag1|u|v',
    'bp_rp|phot_g_mean_mag|u|v': 'bp_rp|mag2|u|v'
  },
  hrd: {
    'bp_rp|phot_g_mean_mag|u|v': 'bp_rp|mag2|u|v',
    'bp_rp|phot_g_mean_mag': 'bp_rp|mag1|mag2',
    'bp_rp|parallax|phot_g_mean_mag': 'bp_rp|parallax|mag2'
  }
};

// map active view keys to column names
const actives = {
  airlines: {
    'Depart By slider': ['time']
  },
  property: {
    'property price': ['date']
  },
  flights: {
    'delay': ['delay'],
    'time': ['time'],
    'distance': ['distance']
  },
  gaia: {
    'mag': ['mag1'],
    'par': ['parallax'],
    'sky': ['u', 'v'],
    'hrd': ['bp_rp', 'mag2']
  },
  taxis: {
    'pickup time': ['time'],
    'pickup location': ['px', 'py'],
    'dropoff location': ['dx', 'dy']
  }
};

// dimension cardinality
const dims = {
  airlines: {
    time: { active: [180] },
    airline: { bins: [26] },
    delay: { bins: [1] }
  },
  property: {
    date: { active: [440], bins: [220] },
    price: { active: [350], bins: [1] }
  },
  flights: {
    delay:    { active: [540], bins: [25] },
    time:     { active: [540], bins: [24] },
    distance: { active: [540], bins: [30] }
  },
  gaia: {
    u: { active: [282], bins: [282] },
    v: { active: [178], bins: [178] },
    mag1: { active: [235], bins: [22] },
    parallax: { active: [235], bins: [25] },
    bp_rp: { active: [178], bins: [178] },
    mag2: { active: [278], bins: [278] }
  },
  taxis: {
    time: { active: [620], bins: [24] },
    px: { active: [168], bins: [168] },
    py: { active: [275], bins: [275] },
    dx: { active: [168], bins: [168] },
    dy: { active: [275], bins: [275] }
  }
}

console.log(`ANALYZING SPARSITY FOR ${name}`);
const _act = actives[name];
const $ = dims[name];

for (const size of sizes) {
  try {
    const file = `data/${name}-1b.parquet`;
    const load = `CREATE TABLE ${name} AS SELECT * FROM '${file}' WHERE NOT (ISINF(dy) OR ISINF(dx) OR ISINF(px) OR ISINF(py)) LIMIT ${size}`;

    const t0 = Date.now();
    const results = await sparsity({ name, size, load, tasks });
    const time = Date.now() - t0;

    const dt = aq.from(results)
      .derive({
        max: aq.escape(d => {
          const act = _act[d.activeView];
          const re = rewrite[d.activeView];
          const cols = op.split((re && re[d.refs]) || d.refs, '|');
          return cols.reduce((s, r) => s * (act.includes(r) ? $[r].active : $[r].bins), 1);
        })
      })
      .derive({
        perc: aq.escape(d => (d.rows / d.max).toFixed(6))
      })
      .select('name', 'size', 'activeView', 'refs', 'rows', 'max', 'perc');

    const outputFile = `results/sparsity/${name}-1e${Math.log10(size)}.csv`;
    console.log(`WRITING ${outputFile} (${time} ms total)`);
    await writeFile(outputFile, dt.toCSV());
  } catch (err) {
    console.error(err);
  }
}
