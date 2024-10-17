import { from, op } from 'arquero';

const fields = [
  'name',
  'size',
  'taskid',
  'stage',
  'activeView',
  'updateId',
  'parallel',
  'view'
];

export function analyze(results) {
  return from(results)
    .groupby(fields)
    .rollup({
      count: d => op.count(),
      time: d => op.max(d.end) - op.min(d.start)
    });
}
