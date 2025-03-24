import { coordinator, vg } from '../setup.js';

export default async function(el) {
  const table = 'results';

  // load data
  await coordinator.exec(`
    CREATE TABLE ${table} AS SELECT * FROM '${location.origin}/results/tile-pan.parquet';
    CREATE TABLE sorted AS SELECT * FROM ${table} WHERE sorted;
    CREATE TABLE unsorted AS SELECT * FROM ${table} WHERE NOT sorted
  `);

  const fxDomain = [true, false];
  const conditions = ['Query', 'Tiles', 'Prefetch Tiles'];
  const minFps = 60;
  const threshold = 100;

  function split() {
    return vg.vconcat(
      plot1('sorted'),
      plot1('unsorted')
    );
  }

  function plot1(name) {
    return vg.plot(
      vg.name(name),
      vg.frame({ stroke: '#eee'}),
      vg.text([{ fy: conditions[0], text: name[0].toUpperCase() + name.slice(1) }], {
        fy: 'fy', text: 'text', fontSize: 11, frameAnchor: 'top', dy: -15
      }),
      vg.ruleX([threshold], { stroke: '#ccc', strokeDasharray: '3,3' }),
      minFps ? [
        vg.ruleX([1000 / minFps], { stroke: '#999', strokeDasharray: '2,2' }),
        vg.text([{ fy: conditions[2], text: `${minFps}fps`, x: 1000 / minFps }], {
          fy: 'fy', text: 'text', x: 'x', frameAnchor: 'bottom',
          dx: 17, dy: -6, fill: '#999'
        }),
        vg.marginRight(30),
      ] : [],
      vg.densityY(vg.from(name), {
        bandwidth: 15,
        stack: true,
        offset: 'center',
        fy: 'condition',
        x: 'time',
        fill: 'condition',
        fillOpacity: 0.6
      }),
      vg.tickX(vg.from(name), {
        fy: 'condition',
        x: vg.median('time'),
        stroke: 'white'
      }),
      vg.tickX(vg.from(name), {
        fy: 'condition',
        x: vg.avg('time'),
        stroke: 'condition',
        strokeWidth: 1
      }),
      vg.fyLabel(null),
      vg.fyTickFormat(d => d === 'Query' ? 'Direct' : d === 'Tiles' ? 'Tile' : 'Prefetch'),
      vg.fyPadding(0),
      vg.fyDomain(conditions),
      vg.xScale('log'),
      vg.xDomain([1, 1000]),
      vg.xLabel('Time (ms)'),
      vg.xLabelAnchor('right'),
      vg.yAxis(null),
      vg.yInset(4),
      vg.width(640),
      vg.height(210),
      vg.marginLeft(50),
      vg.marginRight(5)
    );
  }

  function plot(name) {
    return vg.plot(
      vg.name(name),
      vg.frame({ stroke: '#eee'}),
      vg.ruleX([threshold], { stroke: '#ccc', strokeDasharray: '3,3' }),
      minFps ? [
        vg.ruleX([1000 / minFps], { stroke: '#999', strokeDasharray: '2,2' }),
        vg.text([{ fy: conditions[2], text: `${minFps}fps`, x: 1000 / minFps }], {
          fy: 'fy', text: 'text', x: 'x', frameAnchor: 'bottom',
          dx: 17, dy: -6, fill: '#999'
        }),
        vg.marginRight(30),
      ] : [],
      vg.densityY(vg.from(name), {
        bandwidth: 15,
        stack: true,
        offset: 'center',
        fx: 'sorted',
        fy: 'condition',
        x: 'time',
        fill: 'condition',
        fillOpacity: 0.6
      }),
      vg.tickX(vg.from(name), {
        fx: 'sorted',
        fy: 'condition',
        x: vg.median('time'),
        stroke: 'white'
      }),
      vg.tickX(vg.from(name), {
        fx: 'sorted',
        fy: 'condition',
        x: vg.avg('time'),
        stroke: 'condition',
        strokeWidth: 1,
        // strokeDasharray: '3,3'
      }),
      vg.fxLabel(null),
      vg.fxDomain(fxDomain),
      vg.fxTickFormat(d => d ? 'Sorted' : 'Unsorted'),
      vg.fxPadding(0.05),
      vg.fyLabel(null),
      vg.fyTickFormat(d => d === 'Query' ? 'Direct' : d === 'Tiles' ? 'Tile' : 'Prefetch'),
      vg.fyPadding(0),
      vg.fyDomain(conditions),
      vg.xScale('log'),
      vg.xDomain([1, 1000]),
      vg.xLabel('Time (ms)'),
      vg.xLabelAnchor('right'),
      vg.yAxis(null),
      vg.yInset(4),
      // vg.yRound(true),
      vg.width(1024),
      vg.height(210),
      vg.marginLeft(50),
      vg.marginRight(5)
    );
  }

  function plot2(name) {
    return vg.plot(
      vg.frame({ stroke: '#ccc' }),
      vg.rectY(vg.from(name), {
        x: vg.bin('time', { step: 2 }),
        y: vg.count(),
        fx: 'sorted',
        fy: 'condition',
        fillOpacity: 0.15,
        clip: true
      }),
      vg.tickX(vg.from(name), {
        x: vg.avg('time'),
        fx: 'sorted',
        fy: 'condition'
      }),
      vg.width(1000),
      vg.height(400),
      vg.xDomain([0, 100]),
      // vg.marginTop(18),
      // vg.marginLeft(45),
      vg.marginRight(100),
      // vg.marginBottom(20)
    );
  }

  const view = split();
  // plot('results');

  el.replaceChildren(view);
}
