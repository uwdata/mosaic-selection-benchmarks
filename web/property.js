import { coordinator, vg } from './setup.js';

export default async function(el) {
  const table = 'property';

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT *
    FROM '${location.origin}/data/property.parquet'
  `);

  const $brush = vg.Selection.intersect();

  const view = vg.vconcat(
    vg.plot(
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
}
