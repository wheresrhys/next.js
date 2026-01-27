// Re-export with yet another name
export { renamedInMiddleLayer as finalExportName } from './b'
export { middleLayerFunction as finalFunctionName } from './b'

// Also add a local export
export const localInC = 'local-c'
