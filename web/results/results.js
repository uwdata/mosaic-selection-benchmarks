import { coordinator, vg } from '../setup.js';

export default async function(el) {
  const table = 'results';

  // load data
  await coordinator.exec(`
   CREATE TABLE ${table} AS SELECT * FROM '${location.origin}/results/results.parquet';
   CREATE TABLE update AS SELECT * FROM ${table} WHERE stage = 'update';
   CREATE TABLE build AS SELECT * FROM ${table} WHERE stage = 'create';
  `);

  function tickFormat(v) {
    return v < 1e3 ? v
      : v < 1e6 ? (v / 1e3) + 'k'
      : v < 1e9 ? (v / 1e6) + 'M'
      : (v / 1e9) + 'B';
  }

  const labels = [
    { fx: 'airlines', text: 'Airlines' },
    { fx: 'property', text: 'Property' },
    { fx: 'flights', text: 'Flights' },
    { fx: 'gaia', text: 'Gaia' },
    { fx: 'taxis', text: 'Taxis' }
  ];

  const colorDomain = ['wasm', 'node', 'unopt', 'VegaPlus', 'vegaFusion'];
  const colorLabels = {
    wasm: 'Mosaic WASM',
    node: 'Mosaic Local',
    unopt: 'Unoptimized Mosaic Local',
    VegaPlus: 'VegaPlus',
    vegaFusion: 'VegaFusion'
  };

  const unoptimized = ['unopt', 'VegaPlus', 'vegaFusion']

  function plot(name, title, threshold, minFps) {
    return vg.plot(
      vg.name(name),
      vg.frame(),
      vg.text(labels, { fx: 'fx', text: 'text', frameAnchor: 'top', dy: 5 }),
      vg.ruleY([threshold], { stroke: '#ccc', strokeDasharray: '3,3' }),
      minFps ? [
        vg.ruleY([1000 / minFps], { stroke: '#858585', strokeDasharray: '4,5' }),
        vg.text([{ fx: 'taxis', text: `${minFps}fps` }], { fx: 'fx', text: 'text', frameAnchor: 'right', dx: 30, dy: 12, fill: '#858585' }),
        vg.marginRight(30),
      ] : [],
      vg.areaY(vg.from(name, { optimize: false }), {
        fx: 'name',
        x: 'size',
        y1: vg.quantile('time', 0.25),
        y2: vg.quantile('time', 0.75),
        fill: 'condition',
        fillOpacity: 0.15,
        curve: 'monotone-x'
      }),
      vg.lineY(vg.from(name, { optimize: false }), {
        fx: 'name',
        x: 'size',
        y: vg.median('time'),
        strokeLinecap: 'butt',
        stroke: 'condition',
        curve: 'monotone-x'
      }),
      vg.fxDomain(labels.map(l => l.fx)),
      vg.fxLabel(title),
      vg.fxTickFormat(() => ''),
      vg.fxPadding(0.1),
      vg.xScale('log'),
      vg.xInset(5),
      vg.xTicks(4),
      vg.xTickFormat(tickFormat),
      vg.xLabel(null),
      vg.yScale('log'),
      vg.yLabel('Time (ms)'),
      vg.yLabelAnchor('center'),
      vg.yDomain([0.1, 1e5]),
      vg.yTicks(5),
      vg.yTickFormat(tickFormat),
      vg.colorDomain(colorDomain),
      vg.colorTickFormat(v => colorLabels[v]),
      vg.width(900),
      vg.height(130),
      vg.marginTop(18),
      vg.marginLeft(45),
      vg.marginBottom(20)
    );
  }

  const view = vg.vconcat(
    plot('build', 'Materialized View Creation', 1000),
    vg.vspace(10),
    plot('update', 'Update Queries', 100, 30),
    vg.hconcat(
      vg.hspace(45),
      vg.colorLegend({ for: 'update' })
    )
  );

  el.replaceChildren(view);
}
