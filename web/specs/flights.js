import { coordinator, vg, watchRender, namedPlots } from '../setup.js';
import { run, createIndex, slideInterval1D, downloadJSON } from '../experiment.js';

export default async function(el) {
  let experimentResolver;
  const experimentPromise = new Promise(resolve => experimentResolver = resolve);
  const table = 'flights';
  const names = ['delay', 'time', 'distance'];
  const connector = coordinator.databaseConnector();
  await connector.reset();
  connector.visualization(table);

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT
      GREATEST(-60, LEAST(delay, 180))::FLOAT AS delay,
      LEAST(distance, 3000)::FLOAT AS distance,
      time
    FROM '${location.origin}/data/flights.parquet'
    ${coordinator.preaggregator.enabled ? '' : 'LIMIT 10000'}
  `);


  // Add experiment to render watcher:
  watchRender(3, async () => {
    connector.stage('create');
    const ival1D = names.map(x => namedPlots.get(x).interactors[0]);
    // generate indices
    for (let i = 0; i < ival1D.length; i++) {
      const ival = ival1D[i];
      await createIndex(ival, [0, 1], names[i]);
    }

    // simulate brushing
    connector.stage('update');
    const n = namedPlots.size - 1;
    const p = [0.1, 0.2, 0.3];
    const tasks = ival1D.flatMap((ival, i) => slideInterval1D(p, ival, n, names[i]));
    await run(tasks);

    const prefix = coordinator.preaggregator.enabled ? 'opt' : 'std';
    downloadJSON(connector.dumpQueries(), `${prefix}-flights.json`);
    experimentResolver();
  });

  const $brush = vg.Selection.crossfilter();
  const view = vg.vconcat(
    vg.plot(
      vg.name(names[0]),
      vg.rectY(
        vg.from('flights', {filterBy: $brush}),
        {
          x: vg.bin('delay', { step: 10 }),
          y: vg.count(),
          fill: 'steelblue',
          insetLeft: 0.5,
          insetRight: 0.5
        }
      ),
      vg.intervalX({as: $brush}),
      vg.xDomain([-60, 190]),
      vg.xLabel('Arrival Delay (min)'),
      vg.yTickFormat('s'),
      vg.width(600),
      vg.height(200)
    ),
    vg.plot(
      vg.name(names[1]),
      vg.rectY(
        vg.from('flights', {filterBy: $brush}),
        {
          x: vg.bin('time', { step: 1 }),
          y: vg.count(),
          fill: 'steelblue',
          insetLeft: 0.5,
          insetRight: 0.5
        }
      ),
      vg.intervalX({as: $brush}),
      vg.xDomain([0, 24]),
      vg.xLabel('Departure Time (hour)'),
      vg.yTickFormat('s'),
      vg.width(600),
      vg.height(200)
    ),
    vg.plot(
      vg.name(names[2]),
      vg.rectY(
        vg.from('flights', {filterBy: $brush}),
        {
          x: vg.bin('distance', { step: 100 }),
          y: vg.count(),
          fill: 'steelblue',
          insetLeft: 0.5,
          insetRight: 0.5
        }
      ),
      vg.intervalX({as: $brush}),
      vg.xDomain([0, 3000]),
      vg.xLabel('Distance Flown (miles)'),
      vg.yTickFormat('s'),
      vg.width(600),
      vg.height(200)
    )
  );

  el.replaceChildren(view);
  return experimentPromise;
}
