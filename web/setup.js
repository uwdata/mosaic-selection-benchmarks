import { createAPIContext } from '@uwdata/vgplot';
import { Plot } from '@uwdata/mosaic-plot';
import { benchmarkConnector } from './benchmarkConnector';

export const vg = createAPIContext();

// make API accesible for console debugging
self.vg = vg;

export const { coordinator, namedPlots } = vg.context;

coordinator.databaseConnector(benchmarkConnector());

const _plotRender = Plot.prototype.render;
let renderCount;
let renderDone;

const onRender = () => {
  if (renderDone) {
    if (--renderCount === 0) {
      const done = renderDone;
      renderDone = null;
      done();
    }
  }
}

Plot.prototype.render = async function() {
  await _plotRender.call(this);
  onRender();
}

export function watchRender(count, done) {
  renderCount = count;
  renderDone = done;
}

export function clear() {
  coordinator.clear();
  namedPlots.clear();
}
