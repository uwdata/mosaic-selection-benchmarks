import { coordinator, vg } from './setup.js';

const airlineNames = new Map([
	['KS', 'Peninsula'],
	['AX', 'Trans States'],
	['EV', 'ExpressJet'],
	['C5', 'CommuteAir'],
	['YV', 'Mesa'],
	['F9', 'Frontier'],
	['ZW', 'Air Wisconsin'],
	['AA', 'American'],
	['OO', 'SkyWest'],
	['NK', 'Spirit'],
	['HA', 'Hawaiian'],
	['G4', 'Allegiant'],
	['MQ', 'Envoy'],
	['G7', 'GoJet'],
	['QX', 'Horizon'],
	['CP', 'Alis Cargo'],
	['OH', 'PSA'],
	['EM', 'Empire'],
	['AS', 'Alaska'],
	['B6', 'JetBlue'],
	['DL', 'Delta'],
	['UA', 'United'],
	['9E', 'Endeavor'],
	['PT', 'Piedmont'],
	['WN', 'Southwest'],
	['YX', 'Republic']
]);

export default async function(el) {
  const table = 'airlines';

  // load data
  await coordinator.exec(`
    CREATE TABLE IF NOT EXISTS ${table} AS SELECT
      airline,
      delay,
      time
    FROM '${location.origin}/data/flights.parquet'
  `);

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
        value: 10,
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
      vg.xDomain([-12, 30]),
      vg.yDomain(vg.Fixed),
      vg.yTickFormat(v => airlineNames.get(v)),
      vg.yGrid(true),
      vg.yLabel(null),
      vg.marginTop(5),
      vg.marginLeft(83),
      vg.marginRight(52),
      vg.height(420)
    )
  );

  el.replaceChildren(view);
}
