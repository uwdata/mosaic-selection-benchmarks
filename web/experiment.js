import { coordinator, watchRender } from './setup.js';
import { clauseInterval } from '@uwdata/mosaic-core';

let abort = false;

export async function run(tasks) {
  for (const task of tasks) {
    if (!abort) await task();
  }
  abort = false;
}

export function stop() {
  abort = true;
}

export async function createIndex(interval, value, name) {
  const selection = interval.selection;
  const { clients } = coordinator.filterGroups.get(selection);
  const active = interval.clause(value);

  coordinator.databaseConnector().props({ activeView: name });

  let promises = [];
  for (const client of clients) {
    const info = coordinator.dataCubeIndexer.index(client, selection, active);
    if (info) promises.push(info.result); // if an index is getting created wait for it
  }
  await Promise.all(promises);
}

export async function createIndexSlider(slider, value) {
  const { field, selectionType, selection } = slider;
  const interval = {
    selection,
    clause: value => {
      if (selectionType === 'interval') {
        const domain = [slider.min ?? 0, value];
        return clauseInterval(field, domain, {
          source: slider,
          bin: 'ceil',
          scale: { type: 'identity', domain },
          pixelSize: slider.step
        });
      } else {
        return clausePoint(field, value, { source: slider });
      }
    }
  }
  await createIndex(interval, value, slider.element.firstChild.innerText + ' slider');
}

function updateInterval(interval, renderCount) {
  const connnector = coordinator.databaseConnector();
  return (selection, props) => () => new Promise(resolve => {
    watchRender(renderCount, () => {
      resolve();
    });
    connnector.props({...props, selection});
    interval.publish(selection);
  });
}

export function slideInterval2D(percs, interval, renderCount, name, step = 1, start = 'top-left') {
  const updater = updateInterval(interval, renderCount);
  const tasks = [];
  const { xscale, yscale } = interval;
  const [x0, x1] = xscale.range.slice().sort((a, b) => a - b);
  const [y0, y1] = yscale.range.slice().sort((a, b) => a - b);
  const xext = Math.floor(x1 - x0);
  const yext = Math.floor(y1 - y0);
  percs.forEach(brushSize => {
    const w = Math.round(brushSize * xext);
    const h = Math.round(brushSize * yext);
    for (let i = 0; i < xext - w; i += step) {
      const j = Math.round(i * yext / xext);
      let brush;
      switch (start) {
        case 'top-left':
          brush = [
            [x0 + i, y0 + j],
            [x0 + i + w, y0 + j + h]
          ];
          break;
        case 'top-right':
          brush = [
            [x1 - i - w, y0 + j],
            [x1 - i, y0 + j + h]
          ];
          break;
        case 'bottom-left':
          brush = [
            [x0 + i, y1 - j - h],
            [x0 + i + w, y1 - j]
          ];
          break;
        case 'bottom-right':
          brush = [
            [x1 - i - w, y1 - j - h],
            [x1 - i, y1 - j]
          ];
          break;
      }
      tasks.push(updater(brush, { brushSize, updateId: (i / step), activeView: name  }));
    }
    tasks.push(updater(null, { brushSize: 1, updateId: -1, activeView: name  })); // clear
  });
  return tasks;
}

export function slideInterval1D(percs, interval, renderCount, name, step = 1) {
  const updater = updateInterval(interval, renderCount);
  const tasks = [];
  const { range } = interval.scale;
  const [lo, hi] = range;
  const extent = hi - lo;
  percs.forEach(brushSize => {
    const w = Math.round(brushSize * extent);
    for (let updateId = 0; (updateId * step) < extent - w; updateId++) {
      tasks.push(updater(
        [lo + (updateId * step), lo + (updateId * step) + w],
        { brushSize, updateId, activeView: name }
      ));
    }
    tasks.push(updater(null, { brushSize: 1, updateId: -1, activeView: name })); // clear
  });
  return tasks;
}

export function slideIntervalSlider(slider, renderCount, step = 0.1) {
  const updater = updateInterval(slider, renderCount);
  const tasks = [];
  const { min, max } = slider;
  const extent = max - min;
  if (slider.step % step !== 0) {
    throw new Error(`step ${step} must be a multiple of slider step size ${slider.step}`);
  }

  for (let updateId = 0; (updateId * step) < extent; updateId++) {
    tasks.push(updater(min + (updateId * step), { updateId, activeView: slider.element.firstChild.innerText + ' slider' }));
  }
  return tasks;
}

export function downloadJSON(data, name) {
  // drop initial table load query
  // strip extra data cube creation queries
  const results = data.filter((d, i) => {
    return i > 0 && !(d.stage === 'update' && d.query.startsWith('CREATE '));
  });

  const blob = new Blob([JSON.stringify(results, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

// TODO: add benchmarks using the following:
export function growInterval2D(interval, renderCount, name, step = 1) {
  const updater = updateInterval(interval, renderCount);
  const tasks = [];
  const { xscale, yscale } = interval;
  const [x0, x1] = xscale.range.slice().sort((a, b) => a - b);
  const [y0, y1] = yscale.range.slice().sort((a, b) => a - b);
  const xext = Math.floor(x1 - x0);
  const yext = Math.floor(y1 - y0);
  if (yext < xext) {
    const ar = xext / yext;
    for (let yi = 0; yi < yext; yi += step) {
      const xi = x0 + Math.round(yi * ar);
      tasks.push(updater([[x0, y0], [xi, y0 + yi]], { updateId: yi, activeView: name  }));
    }
  } else {
    const ar = yext / xext;
    for (let xi = 0; xi < xext; xi += step) {
      const yi = y0 + Math.round(xi * ar);
      tasks.push(updater([[x0, y0], [x0 + xi, yi]], { updateId: xi, activeView: name  }));
    }
  }
  tasks.push(updater(null, { updateId: -1, activeView: name  })); // clear
  return tasks;
}

export function growInterval1D(interval, renderCount, name, step = 1) {
  const updater = updateInterval(interval, renderCount);
  const tasks = [];
  const { range } = interval.scale;
  const [lo, hi] = range;
  const extent = Math.floor(hi - lo);
  for (let updateId = 0; updateId < extent; updateId += step) {
    tasks.push(updater([lo, lo + updateId], { updateId, activeView: name  }));
  }
  tasks.push(updater(null, { updateId: -1, activeView: name  })); // clear
  return tasks;
}