import { coordinator, vg, watchRender, namedPlots } from '../setup.js';
import { run, createIndex, slideInterval1D, slideInterval2D, downloadJSON } from '../experiment.js';

export default async function(el) {
  let experimentResolver;
  const experimentPromise = new Promise(resolve => experimentResolver = resolve);
  const table = 'trips';
  const names1D = ['pickup time']
  const names2D = ['pickup location', 'dropoff location'];
  const connector = coordinator.databaseConnector();
  await connector.reset();
  connector.visualization(table);

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT *
    FROM '${location.origin}/data/nyc-taxi-trips.parquet'
  `);

  // Add experiment to render watcher:
  watchRender(3, async () => {
    connector.stage('create');
    const ival1D = names1D.map(x => namedPlots.get(x).interactors[0]);
    const ival2D = names2D.map(x => namedPlots.get(x).interactors[0]);

    // generate indices
    for (let i = 0; i < ival1D.length; i++) {
      const ival = ival1D[i];
      await createIndex(ival, [0, 1], names1D[i]);
    }
    for (let i = 0; i < ival2D.length; i++) {
      const ival = ival2D[i];
      await createIndex(ival, [[0, 1], [0, 1]], names2D[i]);
    }

    // simulate brushing
    connector.stage('update');
    const n = namedPlots.size - 1;
    const p = [0.1, 0.2, 0.3];
    const tasks1d = ival1D.flatMap((ival, i) => slideInterval1D(p, ival, n, names1D[i]));
    // We have n-1 here because the 2d plots only affect each other
    const tasks2d = ival2D.flatMap((ival, i) => slideInterval2D(p, ival, n - 1, names2D[i], 1, 'bottom-left'));
    const tasks = tasks1d.concat(tasks2d);
    await run(tasks);
    downloadJSON(
      connector.dumpQueries(),
      `nyc-taxis-${coordinator.dataCubeIndexer.enabled ? 'optimized' : 'not-optimized'}.json`
    );
    experimentResolver();
  });

  const $filter = vg.Selection.crossfilter();

  const view = vg.vconcat(
    vg.hconcat(
      vg.plot(
        vg.name(names2D[0]),
        vg.raster(vg.from('trips', { filterBy: $filter }), {
          x: 'px',
          y: 'py',
          bandwidth: 0,
          pixelSize: 1,
          imageRendering: 'pixelated'
        }),
        vg.intervalXY({ as: $filter, pixelSize: 2 }),
        vg.text([{ label: 'Taxi Pickups' }], {
          dx: 10,
          dy: 10,
          text: 'label',
          fill: 'black',
          fontSize: '1.2em',
          frameAnchor: 'top-left'
        }),
        vg.width(335),
        vg.height(550),
        vg.margin(0),
        vg.xAxis(null),
        vg.yAxis(null),
        vg.xDomain([9.75e5, 1.005e6]),
        vg.yDomain([1.9e5, 2.4e5]),
        vg.colorScale('symlog'),
        vg.colorScheme('blues')
      ),
      vg.hspace(10),
      vg.plot(
        vg.name(names2D[1]),
        vg.raster(vg.from('trips', { filterBy: $filter }), {
          x: 'dx',
          y: 'dy',
          bandwidth: 0,
          pixelSize: 1,
          imageRendering: 'pixelated'
        }),
        vg.intervalXY({ as: $filter, pixelSize: 2 }),
        vg.text([{ label: 'Taxi Dropoffs' }], {
          dx: 10,
          dy: 10,
          text: 'label',
          fill: 'black',
          fontSize: '1.2em',
          frameAnchor: 'top-left'
        }),
        vg.width(335),
        vg.height(550),
        vg.margin(0),
        vg.xAxis(null),
        vg.yAxis(null),
        vg.xDomain([9.75e5, 1.005e6]),
        vg.yDomain([1.9e5, 2.4e5]),
        vg.colorScale('symlog'),
        vg.colorScheme('oranges')
      )
    ),
    vg.vspace(10),
    vg.plot(
      vg.name(names1D[0]),
      vg.rectY(vg.from('trips'), {
        x: vg.bin('time'),
        y: vg.count(),
        fill: 'steelblue',
        insetLeft: 0.5,
        insetRight: 0.5
      }),
      vg.intervalX({ as: $filter }),
      vg.xDomain([0, 24]),
      vg.yTickFormat('s'),
      vg.xLabel('Pickup Hour →'),
      vg.width(680),
      vg.height(100)
    )
  );

  el.replaceChildren(view);
  return experimentPromise;
}
