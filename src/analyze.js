import { from, op } from 'arquero';

export function analyze(results) {
  return from(results)
    .groupby('taskid', 'stage')
    .rollup({
      time: d => op.max(d.end) - op.min(d.start)
    });
}