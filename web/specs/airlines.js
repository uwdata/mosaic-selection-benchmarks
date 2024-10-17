import { coordinator, vg, watchRender, namedPlots } from '../setup.js';
import { run, createIndexSlider, slideIntervalSlider, downloadJSON } from '../experiment.js';

const airlineNames = new Map([
  ['KS', 'Peninsula'],
  ['AX', 'Trans States'],
  ['9K', 'Cape Air'],
  ['C5', 'CommuteAir'],
  ['EV', 'ExpressJet'],
  ['YV', 'Mesa'],
  ['ZW', 'Air Wisconsin'],
  ['F9', 'Frontier'],
  ['OO', 'SkyWest'],
  ['AA', 'American'],
  ['MQ', 'Envoy'],
  ['NK', 'Spirit'],
  ['HA', 'Hawaiian'],
  ['G7', 'GoJet'],
  ['QX', 'Horizon'],
  ['CP', 'Alis Cargo'],
  ['G4', 'Allegiant'],
  ['EM', 'Empire'],
  ['OH', 'PSA'],
  ['9E', 'Endeavor'],
  ['AS', 'Alaska'],
  ['B6', 'JetBlue'],
  ['UA', 'United'],
  ['DL', 'Delta'],
  ['VX', 'Virgin America'],
  ['PT', 'Piedmont'],
  ['YX', 'Republic'],
  ['WN', 'Southwest']
]);

export default async function(el) {
  let experimentResolver;
  const experimentPromise = new Promise(resolve => experimentResolver = resolve);
  const table = 'airlines';
  const interactorIds = ['slider_1'];
  const connector = coordinator.databaseConnector();
  await connector.reset();
  connector.visualization(table);

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT
      airline,
      delay,
      time
    FROM '${location.origin}/data/flights.parquet'
    WHERE airline != 'KS' AND airline != '9K'
    ${coordinator.dataCubeIndexer.enabled ? '' : 'LIMIT 10000'}
  `);

    // Add experiment to render watcher:
    watchRender(1, async () => {
      connector.stage('create');
      const sliders = Array.from(coordinator.clients).map(x => {
        if (interactorIds.includes(x.id)) {
          return x;
        }
      }).filter(x => x !== undefined);

      // generate indices
      for (let i = 0; i < sliders.length; i++) {
        const slider = sliders[i];
        await createIndexSlider(slider, slider.max);
      }

      // simulate brushing
      connector.stage('update');
      const n = namedPlots.size; // Not -1 because we are not interacting with a plot
      const tasks = sliders.flatMap((slider, i) => slideIntervalSlider(slider, n));
      await run(tasks);

      const prefix = coordinator.dataCubeIndexer.enabled ? 'opt' : 'std';
      downloadJSON(connector.dumpQueries(), `${prefix}-airlines.json`);
      experimentResolver();
    });

  const $ci = vg.Param.value(0.95);
  const $filter = vg.Selection.single();

  const view = vg.vconcat(
    vg.hconcat(
      vg.slider({
        select: 'interval',
        field: 'time',
        as: $filter,
        min: 6,
        max: 24,
        step: 0.1,
        value: 24,
        label: 'Depart By'
      }),
      vg.slider({
        as: $ci,
        min: 0.5,
        max: 0.999,
        step: 0.001,
        label: 'Conf. Level'
      })
    ),
    vg.plot(
      vg.name('delay'),
      vg.tickX([0], {
        stroke: '#ccc',
        strokeDasharray: '3 3'
      }),
      vg.errorbarX(
        vg.from('airlines', { filterBy: $filter }),
        {
          ci: $ci,
          x: 'delay',
          y: 'airline',
          strokeWidth: 1,
          marker: 'tick',
          sort: {y: '-x'}
        }
      ),
      vg.text(
        vg.from('airlines', { filterBy: $filter }),
        {
          frameAnchor: 'right',
          fontSize: 8,
          fill: '#999',
          dx: 50,
          text: vg.count(),
          y: 'airline'
        }
      ),
      vg.xDomain([-15, 15]),
      vg.xLabel('Arrival Delay (minutes)'),
      vg.xLabelAnchor('center'),
      vg.yDomain(vg.Fixed),
      vg.yTickFormat(v => airlineNames.get(v)),
      vg.yGrid(true),
      vg.yLabel(null),
      vg.marginTop(5),
      vg.marginLeft(83),
      vg.marginRight(52),
      vg.width(565),
      vg.height(400)
    )
  );

  el.replaceChildren(view);
  return experimentPromise;
}
