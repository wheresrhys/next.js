// Test re-export chains work correctly with mangling
// Each module should independently mangle its export keys
// The wiring between modules should use correct mangled names

import { finalExportName, finalFunctionName, localInC } from './c'

it('should handle re-export chain with mangling', () => {
  // Values should flow through the chain correctly
  expect(finalExportName).toBe('from-a')
  expect(finalFunctionName()).toBe('func-a')
  expect(localInC).toBe('local-c')
})

// Also test direct imports from middle of chain
import { renamedInMiddleLayer, middleLayerFunction } from './b'

it('should handle imports from middle of chain', () => {
  expect(renamedInMiddleLayer).toBe('from-a')
  expect(middleLayerFunction()).toBe('func-a')
})

// Also test direct imports from source with __webpack_exports_info__
import {
  veryLongOriginalExportName,
  anotherLongFunctionName,
  exportsInfo,
} from './a'

it('should handle imports from source', () => {
  expect(veryLongOriginalExportName).toBe('from-a')
  expect(anotherLongFunctionName()).toBe('func-a')
})

it('should have exports info in source module (via __webpack_exports_info__)', () => {
  // Modules are only split (and mangled) when they have re-exports.
  // Since a.js has no re-exports, mangledName will be null.
  console.log('exportsInfo from a.js:', JSON.stringify(exportsInfo, null, 2))

  // Verify the structure
  expect(exportsInfo).toBeDefined()
  expect(exportsInfo.veryLongOriginalExportName).toBeDefined()
  expect(exportsInfo.anotherLongFunctionName).toBeDefined()

  // For modules without re-exports, mangledName is null
  // (mangling only applies to modules that are split into locals+facade)
  expect(exportsInfo.veryLongOriginalExportName.used).toBe(true)
  expect(exportsInfo.anotherLongFunctionName.used).toBe(true)
})
