import { coordinator, vg, watchRender, namedPlots } from '../setup.js';
import { run, createIndex, slideInterval1D, downloadJSON } from '../experiment.js';

export default async function(el) {
  let experimentResolver;
  const experimentPromise = new Promise(resolve => experimentResolver = resolve);
  const table = 'property';
  const names = ['property price'];
  const connector = coordinator.databaseConnector();
  await connector.reset();
  connector.visualization(table);

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT *
    FROM '${location.origin}/data/property.parquet'
    ${coordinator.dataCubeIndexer.enabled ? '' : 'LIMIT 10000'}
  `);

    // Add experiment to render watcher:
    watchRender(1, async () => {
      connector.stage('create');
      const ival1D = names.map(x => namedPlots.get(x).interactors[0]);
      // generate indices
      for (let i = 0; i < ival1D.length; i++) {
        const ival = ival1D[i];
        await createIndex(ival, [0, 1], names[i]);
      }

      // simulate brushing
      connector.stage('update');
      const n = namedPlots.size; // Not -1 because plot interacts with itself
      const p = [0.1, 0.2, 0.3];
      const tasks = ival1D.flatMap((ival, i) => slideInterval1D(p, ival, n, names[i]));
      await run(tasks);

      const prefix = coordinator.dataCubeIndexer.enabled ? 'opt' : 'std';
      downloadJSON(connector.dumpQueries(), `${prefix}-property.json`);
      experimentResolver();
    });

  const $brush = vg.Selection.intersect();

  const view = vg.vconcat(
    vg.plot(
      vg.name(names[0]),
      vg.raster(vg.from(table), {
        x: 'date',
        y: 'price',
        fill: 'density',
        bandwidth: 0,
        pixelSize: 2,
        imageRendering: 'pixelated'
      }),
      vg.intervalX({
        as: $brush,
        brush: {
          fillOpacity: 0,
          strokeWidth: 1,
          stroke: 'white',
          strokeDasharray: '2,2'
        }
      }),
      vg.regressionY(vg.from(table), {
        x: 'date',
        y: 'price',
        stroke: 'white',
        strokeWidth: 1,
        strokeDasharray: '4,4'
      }),
      vg.regressionY(vg.from(table, { filterBy: $brush }), {
        x: 'date',
        y: 'price',
        stroke: 'white',
        ci: 0.95
      }),
      vg.colorScheme('turbo'),
      vg.xDomain([1995, 2025]),
      vg.yDomain([0, 1_000_000]),
      vg.xTickFormat('d'),
      vg.yTickFormat('s'),
      vg.xLabel('Date'),
      vg.yLabel('Property Price (£)'),
      vg.width(500),
      vg.height(400)
    )
  );

  el.replaceChildren(view);
  await experimentPromise;
}
