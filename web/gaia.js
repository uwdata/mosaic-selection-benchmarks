import { coordinator, vg } from './setup.js';

export default async function(el) {
  const table = 'gaia';

  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS gaia AS
    SELECT * FROM '${location.origin}/data/gaia.parquet'
  `);

  const $brush = vg.Selection.crossfilter();
  const bandwidth = 0;
  const histScale = 'sqrt';
  const pixelSize = 2;
  const scheme = 'viridis';

  const view = vg.hconcat(
    vg.vconcat(
      vg.plot(
        vg.name('sky'),
        vg.raster(vg.from(table, { filterBy: $brush }), {
          x: 'u', y: 'v', fill: 'density', bandwidth, pixelSize
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
          vg.name('mag'),
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
          vg.name('par'),
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
      vg.name('hrd'),
      vg.raster(vg.from(table, { filterBy: $brush }), {
        x: 'bp_rp',
        y: 'phot_g_mean_mag',
        fill: 'density',
        bandwidth,
        pixelSize
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
}
