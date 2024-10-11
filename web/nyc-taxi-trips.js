import { coordinator, vg } from './setup.js';

export default async function(el) {
  const table = 'trips';

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT *
    FROM '${location.origin}/data/nyc-taxi-trips.parquet'
  `);

  const $filter = vg.Selection.crossfilter();

  const view = vg.vconcat(
    vg.hconcat(
      vg.plot(
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
}
