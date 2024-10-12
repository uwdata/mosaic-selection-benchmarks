import { coordinator, vg, watchRender, namedPlots } from './setup.js';
import { run, createIndex, slideInterval1D, slideInterval2D, downloadJSON } from './experiment.js';

export default async function(el) {
  let experimentResolver;
  const experimentPromise = new Promise(resolve => experimentResolver = resolve);
  const table = 'gaia';
  const names1D = ['mag', 'par'];
  const names2D = ['sky', 'hrd'];
  const connector = coordinator.databaseConnector();
  await connector.reset();
  connector.visualization(table);

  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS gaia AS
    SELECT * FROM '${location.origin}/data/gaia.parquet'
  `);

  // Add experiment to render watcher:
  watchRender(4, async () => {
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
    const tasks2d = ival2D.flatMap((ival, i) => slideInterval2D(p, ival, n, names2D[i], 2));
    const tasks = tasks1d.concat(tasks2d);
    await run(tasks);
    downloadJSON(
      connector.dumpQueries(),
      `gaia-${coordinator.dataCubeIndexer.enabled ? 'optimized' : 'not-optimized'}.json`
    );
    experimentResolver();
  });

  const $brush = vg.Selection.crossfilter();
  const bandwidth = 0;
  const histScale = 'sqrt';
  const pixelSize = 2;
  const scheme = 'viridis';

  const view = vg.hconcat(
    vg.vconcat(
      vg.plot(
        vg.name(names2D[0]),
        vg.raster(vg.from(table, { filterBy: $brush }), {
          x: 'u',
          y: 'v',
          fill: 'density',
          bandwidth,
          pixelSize,
          imageRendering: 'pixelated'
        }),
        vg.intervalXY({ as: $brush, pixelSize }),
        vg.xDomain([-3.63, 3.63]),
        vg.yDomain([-1.318, 1.318]),
        vg.colorScale('sqrt'),
        vg.colorScheme(scheme),
        vg.width(600),
        vg.height(400),
        vg.marginLeft(35),
        vg.marginTop(15),
        vg.marginRight(1)
      ),
      vg.hconcat(
        vg.plot(
          vg.name(names1D[0]),
          vg.rectY(vg.from(table, { filterBy: $brush }), {
            x: vg.bin('phot_g_mean_mag', { step: 1 }),
            y: vg.count(),
            fill: 'steelblue',
            insetLeft: 0.5,
            insetRight: 0.5
          }),
          vg.intervalX({ as: $brush }),
          vg.yScale(histScale),
          vg.yGrid(true),
          vg.yTickFormat('s'),
          vg.xDomain([1, 23]),
          vg.width(300),
          vg.height(200),
          vg.marginLeft(45)
        ),
        vg.plot(
          vg.name(names1D[1]),
          vg.rectY(vg.from(table, { filterBy: $brush }), {
            x: vg.bin('parallax', { step: 1 }),
            y: vg.count(),
            fill: 'steelblue',
            insetLeft: 0.5
          }),
          vg.intervalX({ as: $brush }),
          vg.yScale(histScale),
          vg.yGrid(true),
          vg.yTickFormat('s'),
          vg.xDomain([-5, 20]),
          vg.width(300),
          vg.height(200),
          vg.marginLeft(45)
        )
      )
    ),
    vg.hspace(30),
    vg.plot(
      vg.name(names2D[1]),
      vg.raster(vg.from(table, { filterBy: $brush }), {
        x: 'bp_rp',
        y: 'phot_g_mean_mag',
        fill: 'density',
        bandwidth,
        pixelSize,
        imageRendering: 'pixelated'
      }),
      vg.intervalXY({ as: $brush, pixelSize }),
      vg.colorScale('sqrt'),
      vg.colorScheme(scheme),
      vg.yReverse(true),
      vg.xDomain([-2, 6]),
      vg.yDomain([1.5, 23]),
      vg.width(400),
      vg.height(600),
      vg.marginLeft(25),
      vg.marginTop(15)
    )
  );

  el.replaceChildren(view);
  return experimentPromise;
}
