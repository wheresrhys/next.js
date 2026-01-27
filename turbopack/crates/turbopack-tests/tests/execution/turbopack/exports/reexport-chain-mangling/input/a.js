// Original module with long export name
// Now that all ESM modules are split, this will have mangled names
export const veryLongOriginalExportName = 'from-a'
export function anotherLongFunctionName() {
  return 'func-a'
}

// Export __webpack_exports_info__ to verify mangling
export const exportsInfo = __webpack_exports_info__
