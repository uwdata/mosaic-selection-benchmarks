import { coordinator, vg } from './setup.js';

export default async function(el) {
  const table = 'flights';

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT
      GREATEST(-60, LEAST(delay, 180))::FLOAT AS delay,
      LEAST(distance, 3000)::FLOAT AS distance,
      time
    FROM '${location.origin}/data/flights.parquet'
  `);

  const $brush = vg.Selection.crossfilter();

  const view = vg.vconcat(
    vg.plot(
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
      vg.yTickFormat('s'),
      vg.width(600),
      vg.height(200)
    ),
    vg.plot(
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
      vg.yTickFormat('s'),
      vg.width(600),
      vg.height(200)
    ),
    vg.plot(
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
      vg.yTickFormat('s'),
      vg.width(600),
      vg.height(200)
    )
  );

  el.replaceChildren(view);
}
