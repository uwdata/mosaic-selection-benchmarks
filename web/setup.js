import { wasmConnector } from '@uwdata/mosaic-core';
import { createAPIContext } from '@uwdata/vgplot';

export const vg = createAPIContext();

// make API accesible for console debugging
self.vg = vg;

export const { coordinator, namedPlots } = vg.context;

coordinator.databaseConnector( wasmConnector());

export function clear() {
  coordinator.clear();
  namedPlots.clear();
}
