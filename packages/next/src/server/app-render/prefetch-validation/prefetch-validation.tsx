import type {
  CacheNodeSeedData,
  FlightRouterState,
  HeadData,
  InitialRSCPayload,
  Segment,
} from '../../../shared/lib/app-router-types'
import type { VaryParamsThenable } from '../../../shared/lib/segment-cache/vary-params-decoding'
import { InvariantError } from '../../../shared/lib/invariant-error'
import { RenderStage } from '../staged-rendering'
import { getServerModuleMap } from '../manifests-singleton'
import {
  pipelineInSequentialTasks,
  scheduleInSequentialTasks,
} from '../app-render-render-utils'
import { workAsyncStorage } from '../work-async-storage.external'
import {
  Phase,
  printDebugThrownValueForProspectiveRender,
} from '../prospective-render-utils'
import { getDigestForWellKnownError } from '../create-error-handler'
import { PrefetchValidationBoundary } from './prefetch-validation-boundary'
import {
  getLayoutOrPageModule,
  type LoaderTree,
} from '../../lib/app-dir-module'
import { parseLoaderTree } from '../../../shared/lib/router/utils/parse-loader-tree'
import type { GetDynamicParamFromSegment } from '../app-render'
import type {
  AppSegmentConfig,
  Prefetch,
} from '../../../build/segment-config/app/app-segment-config'
import { Readable } from 'node:stream'
import {
  createNodeStreamWithLateRelease,
  createNodeStreamFromChunks,
} from './utils'
import { createDebugChannel } from '../debug-channel-server'

// eslint-disable-next-line import/no-extraneous-dependencies
import { createFromNodeStream } from 'react-server-dom-webpack/client'
// eslint-disable-next-line import/no-extraneous-dependencies
import { renderToReadableStream } from 'react-server-dom-webpack/server'

type StageChunks = Record<SegmentStage, Uint8Array[]>

// FIXME: this causes typescript errors related to 'flight-client-entry-plugin.d.ts'
// type ClientReferenceManifest = ReturnType<
//   (typeof import('../manifests-singleton'))['getClientReferenceManifest']
// >
type ClientReferenceManifest = Record<string, any>

const filterStackFrame =
  process.env.NODE_ENV !== 'production'
    ? (
        require('../../lib/source-maps') as typeof import('../../lib/source-maps')
      ).filterStackFrameDEV
    : undefined
const findSourceMapURL =
  process.env.NODE_ENV !== 'production'
    ? (
        require('../../lib/source-maps') as typeof import('../../lib/source-maps')
      ).findSourceMapURLDEV
    : undefined

function createStagedStreamFromChunks(stageChunks: StageChunks) {
  // The successive stages are supersets of one another,
  // so we can index into the dynamic chunks everywhere
  // and just look at the lengths of the Static/Runtime arrays
  const allChunks = stageChunks[RenderStage.Dynamic]

  const numStaticChunks = stageChunks[RenderStage.Static].length
  const numRuntimeChunks = stageChunks[RenderStage.Runtime].length
  const numDynamicChunks = stageChunks[RenderStage.Dynamic].length

  let chunkIx = 0
  let currentStage:
    | RenderStage.Static
    | RenderStage.Runtime
    | RenderStage.Dynamic = RenderStage.Static
  let closed = false

  function push(chunk: Uint8Array) {
    stream.push(chunk)
  }

  function close() {
    closed = true
    stream.push(null)
  }

  const stream = new Readable({
    read() {
      // Emit static chunks
      for (; chunkIx < numStaticChunks; chunkIx++) {
        push(allChunks[chunkIx])
      }

      // If there's no more chunks after this stage, finish the stream.
      if (chunkIx >= allChunks.length) {
        close()
        return
      }
    },
  })

  function advanceStage(
    stage: RenderStage.Runtime | RenderStage.Dynamic
  ): boolean {
    if (closed) return true

    switch (stage) {
      case RenderStage.Runtime: {
        currentStage = RenderStage.Runtime
        for (; chunkIx < numRuntimeChunks; chunkIx++) {
          push(allChunks[chunkIx])
        }
        break
      }

      case RenderStage.Dynamic: {
        currentStage = RenderStage.Dynamic
        for (; chunkIx < numDynamicChunks; chunkIx++) {
          push(allChunks[chunkIx])
        }
        break
      }

      default: {
        stage satisfies never
      }
    }

    // If there's no more chunks after this stage, finish the stream.
    if (chunkIx >= allChunks.length) {
      close()
      return true
    } else {
      return false
    }
  }

  return {
    stream,
    controller: {
      get currentStage() {
        return currentStage
      },
      get closed() {
        return closed
      },
      advanceStage,
    },
  }
}

export type StageEndTimes = {
  [RenderStage.Static]: number
  [RenderStage.Runtime]: number
}

export async function collectStagedSegmentData(
  stageChunks: StageChunks,
  debugChunks: Uint8Array[] | null,
  startTime: number,
  hasRuntimePrefetch: boolean,
  clientReferenceManifest: ClientReferenceManifest
) {
  const debugChannelAbortController = new AbortController()
  const debugStream = debugChunks
    ? createNodeStreamFromChunks(
        debugChunks,
        debugChannelAbortController.signal
      )
    : null

  const { stream, controller } = createStagedStreamFromChunks(stageChunks)
  stream.on('end', () => {
    // When the stream finishes, we have to close the debug stream too,
    // but delay it to avoid "Connection closed." errors.
    setImmediate(() => debugChannelAbortController.abort())
  })

  // Technically we're just re-encoding, so nothing new should be emitted,
  // but we add an environment name just in case.
  const environmentName = () => {
    const currentStage = controller.currentStage
    switch (currentStage) {
      case RenderStage.Static:
        return 'Prerender'
      case RenderStage.Runtime:
        return hasRuntimePrefetch ? 'Prefetch' : 'Prefetchable'
      case RenderStage.Dynamic:
        return 'Server'
      default:
        currentStage satisfies never
        throw new InvariantError(`Invalid render stage: ${currentStage}`)
    }
  }

  // NOTE: the stream will go up to the static stage for now.
  // we expect the structure of the payload to be readable in this state.
  const payload = await decodeFromStream<InitialRSCPayload>(
    stream,
    debugStream,
    clientReferenceManifest,
    null // Do not pass start/end timings - we do not want to omit any debug info.
  )

  const segments = new Map<SegmentPath, SegmentData>()
  traverseRootSeedDataSegments(payload, (segmentPath, seedData) => {
    segments.set(segmentPath, createSegmentData(seedData))
  })

  const cache = createValidationCache()
  const pendingTasks: Promise<void>[] = []

  const stageEndTimes: StageEndTimes = {
    [RenderStage.Static]: -1,
    [RenderStage.Runtime]: -1,
  }

  await pipelineInSequentialTasks(
    () => {
      for (const [segmentPath, segmentData] of segments) {
        const segmentChunks: ValidationSegmentCacheValue = {
          chunks: {
            [RenderStage.Static]: [],
            [RenderStage.Runtime]: [],
            [RenderStage.Dynamic]: [],
          },
          debugChunks: debugChunks ? [] : null,
        }
        cache.set(segmentPath, segmentChunks)

        const segmentTask = async () => {
          const segmentDebugChannel = debugChunks
            ? createDebugChannel()
            : undefined

          const segmentStream = renderToReadableStream(
            segmentData,
            clientReferenceManifest.clientModules,
            {
              filterStackFrame,
              debugChannel: segmentDebugChannel?.serverSide,
              environmentName,
              startTime,
              onError(error: unknown) {
                const digest = getDigestForWellKnownError(error)
                if (digest) {
                  return digest
                }
                // We don't need to log the errors because we would have already done that
                // when generating the original Flight stream for the whole page.
                if (
                  process.env.NEXT_DEBUG_BUILD ||
                  process.env.__NEXT_VERBOSE_LOGGING
                ) {
                  const workStore = workAsyncStorage.getStore()
                  printDebugThrownValueForProspectiveRender(
                    error,
                    workStore?.route ?? 'unknown route',
                    Phase.PrefetchValidation
                  )
                }
              },
            }
          )

          await Promise.all([
            // accumulate Flight chunks
            (async () => {
              for await (const chunk of segmentStream.values()) {
                writeChunk(segmentChunks.chunks, controller.currentStage, chunk)
              }
            })(),
            // accumulate Debug chunks
            segmentDebugChannel &&
              (async () => {
                for await (const chunk of segmentDebugChannel.clientSide.readable.values()) {
                  segmentChunks.debugChunks!.push(chunk)
                }
              })(),
          ])
        }
        pendingTasks.push(segmentTask())
      }
    },
    () => {
      stageEndTimes[RenderStage.Static] =
        performance.now() + performance.timeOrigin

      controller.advanceStage(RenderStage.Runtime)
    },
    () => {
      stageEndTimes[RenderStage.Runtime] =
        performance.now() + performance.timeOrigin

      controller.advanceStage(RenderStage.Dynamic)
    }
  )
  await Promise.all(pendingTasks)

  return { cache, payload, stageEndTimes }
}

type Timings = {
  startTime?: number
  endTime?: number
}

function decodeFromStream<T>(
  stream: Readable,
  debugStream: Readable | null,
  clientReferenceManifest: ClientReferenceManifest,
  timings: Timings | null
) {
  const serverConsumerManifest = {
    // moduleLoading must be null because we don't want to trigger preloads of ClientReferences
    // to be added to the consumer. Instead, we'll wait for any ClientReference to be emitted
    // which themselves will handle the preloading.
    moduleLoading: null,
    moduleMap: clientReferenceManifest.rscModuleMapping,
    serverModuleMap: getServerModuleMap(),
  }

  return createFromNodeStream(stream, serverConsumerManifest, {
    findSourceMapURL,
    debugChannel: debugStream ?? undefined,
    startTime: timings?.startTime,
    endTime: timings?.endTime,
  }) as Promise<T>
}

function decodeFromChunks<T>(
  chunks: Uint8Array[],
  allChunks: Uint8Array[],
  debugChunks: Uint8Array[] | null,
  releaseSignal: AbortSignal,
  clientReferenceManifest: ClientReferenceManifest,
  timings: Timings | null
) {
  const debugChannelAbortController = new AbortController()
  const debugStream = debugChunks
    ? createNodeStreamFromChunks(
        debugChunks,
        debugChannelAbortController.signal
      )
    : null

  const serverConsumerManifest = {
    // moduleLoading must be null because we don't want to trigger preloads of ClientReferences
    // to be added to the consumer. Instead, we'll wait for any ClientReference to be emitted
    // which themselves will handle the preloading.
    moduleLoading: null,
    moduleMap: clientReferenceManifest.rscModuleMapping,
    serverModuleMap: getServerModuleMap(),
  }

  const segmentStream =
    chunks.length < allChunks.length
      ? createNodeStreamWithLateRelease(chunks, allChunks, releaseSignal)
      : createNodeStreamFromChunks(chunks)

  segmentStream.on('end', () => {
    // When the stream finishes, we have to close the debug stream too,
    // but delay it to avoid "Connection closed." errors.
    setImmediate(() => debugChannelAbortController.abort())
  })

  return createFromNodeStream(segmentStream, serverConsumerManifest, {
    findSourceMapURL,
    debugChannel: debugStream ?? undefined,
    startTime: timings?.startTime,
    endTime: timings?.endTime,
  }) as Promise<T>
}

//===================================

export async function createCombinedPayloadStream(
  createPayload: (
    extraChunksReleaseSignal: AbortSignal
  ) => Promise<InitialRSCPayload>,
  renderSignal: AbortSignal,
  clientReferenceManifest: ClientReferenceManifest,
  startTime: number,
  isDebugChannelEnabled: boolean
) {
  const extraChunksAbortController = new AbortController()

  const payload = await createPayload(extraChunksAbortController.signal)

  // Collect all the chunks so that we're not dependent on timing of the above render.

  let isRenderable = true
  const renderableChunks: Uint8Array[] = []
  const allChunks: Uint8Array[] = []

  const debugChunks: Uint8Array[] | null = isDebugChannelEnabled ? [] : null
  const debugChannel = isDebugChannelEnabled ? createDebugChannel() : null

  let streamFinished: Promise<any> = null!

  await scheduleInSequentialTasks(
    () => {
      const stream = renderToReadableStream(
        payload,
        clientReferenceManifest.clientModules,
        {
          filterStackFrame,
          debugChannel: debugChannel?.serverSide,
          startTime,
          onError(error: unknown) {
            const digest = getDigestForWellKnownError(error)
            if (digest) {
              return digest
            }
            // We don't need to log the errors because we would have already done that
            // when generating the original Flight stream for the whole page.
            if (
              process.env.NEXT_DEBUG_BUILD ||
              process.env.__NEXT_VERBOSE_LOGGING
            ) {
              const workStore = workAsyncStorage.getStore()
              printDebugThrownValueForProspectiveRender(
                error,
                workStore?.route ?? 'unknown route',
                Phase.PrefetchValidation
              )
            }
          },
        }
      )

      streamFinished = Promise.all([
        // Accumulate Flight chunks
        (async () => {
          for await (const chunk of stream.values()) {
            allChunks.push(chunk)
            if (isRenderable) {
              renderableChunks.push(chunk)
            }
          }
        })(),
        // Accumulate debug chunks
        debugChannel &&
          (async () => {
            for await (const chunk of debugChannel.clientSide.readable.values()) {
              debugChunks!.push(chunk)
            }
          })(),
      ])
    },
    () => {
      isRenderable = false
      extraChunksAbortController.abort()
    }
  )

  await streamFinished

  // {
  //   console.log(
  //     '\n\n###################### Combined stream ##########################'
  //   )
  //   const chunksToString = (chunks: Uint8Array[]) =>
  //     Buffer.concat(chunks.map((chunk) => Buffer.from(chunk))).toString('utf-8')

  //   console.log(`========= Renderable (${renderableChunks.length}) ==========`)
  //   console.log(chunksToString(renderableChunks))
  //   console.log(`========= Extra (${allChunks.length}) ==========`)
  //   console.log(chunksToString(allChunks.slice(renderableChunks.length)))
  //   console.log('\n')
  // }

  return {
    stream: createNodeStreamWithLateRelease(
      renderableChunks,
      allChunks,
      renderSignal
    ),
    debugStream: debugChunks
      ? createNodeStreamFromChunks(debugChunks, renderSignal)
      : null,
  }
}

export async function createCombinedPayload(
  initialRSCPayload: InitialRSCPayload,
  cache: ValidationSegmentCache,
  validationRouteTree: ValidationRouteTree,
  navigationParent: SegmentPath,
  releaseSignal: AbortSignal,
  clientReferenceManifest: ClientReferenceManifest,
  stageEndTimes: StageEndTimes,
  useRuntimeStageForPartialSegments: boolean,
  usedSegmentKinds: Set<SegmentStage>
): Promise<InitialRSCPayload> {
  const { head, flightRouterState } = getRootDataFromPayload(initialRSCPayload)
  const combinedSeedData = await createValidationSeedData(
    cache,
    validationRouteTree,
    navigationParent,
    releaseSignal,
    clientReferenceManifest,
    stageEndTimes,
    useRuntimeStageForPartialSegments,
    usedSegmentKinds
  )
  const combinedRSCPayload: InitialRSCPayload = {
    ...initialRSCPayload,
    f: [
      // We expect the root path to only have three elements.
      [
        flightRouterState satisfies FlightRouterState,
        combinedSeedData satisfies CacheNodeSeedData,
        head satisfies HeadData, // TODO: handle head better
      ],
    ],
  }
  return combinedRSCPayload
}

function getRootDataFromPayload(initialRSCPayload: InitialRSCPayload) {
  // FlightDataPath is an unsound type, hence the additional checks.
  const flightDataPaths = initialRSCPayload.f
  if (flightDataPaths.length !== 1 && flightDataPaths[0].length !== 3) {
    throw new InvariantError(
      'InitialRSCPayload does not match the expected shape during prefetch validation.'
    )
  }
  const flightRouterState: FlightRouterState = flightDataPaths[0][0]
  const seedData: CacheNodeSeedData = flightDataPaths[0][1]
  // TODO: handle head
  const head: HeadData = flightDataPaths[0][2]

  return { flightRouterState, seedData, head }
}

//=====================================
// Validation tree
//=====================================

export type SegmentPath = string & { _tag: 'SegmentPath' }

export type ValidationRouteTree = {
  path: SegmentPath
  segment: Segment
  module: null | {
    type: 'layout' | 'page'
    // TODO(prefetch-validation): We should know if a layout segment is shared
    prefetchConfig: Prefetch | null
    conventionPath: string
  }

  slots: { [parallelRouteKey: string]: ValidationRouteTree } | null
}

export async function createValidationRouteTree(
  rootLoaderTree: LoaderTree,
  getDynamicParamFromSegment: GetDynamicParamFromSegment
) {
  type ValidationTask = { target: SegmentPath; parents: SegmentPath[] }

  const validationTasks: ValidationTask[] = []
  let navigationParents: SegmentPath[] = []

  const segmentsWithPrefetchConfigs: SegmentPath[] = []
  const treeNodes = new Map<SegmentPath, ValidationRouteTree>()

  function getSegment(loaderTree: LoaderTree): Segment {
    const dynamicParam = getDynamicParamFromSegment(loaderTree)
    return dynamicParam ? dynamicParam.treeSegment : loaderTree[0]
  }

  async function visit(
    loaderTree: LoaderTree,
    parentPath: SegmentPath | null,
    key: string | null,
    parentLayoutPath: SegmentPath | null,
    isInsideParallelSlot: boolean
  ): Promise<ValidationRouteTree> {
    const { conventionPath, parallelRoutes } = parseLoaderTree(loaderTree)
    const { mod: layoutOrPageMod, modType } =
      await getLayoutOrPageModule(loaderTree)

    const segment = getSegment(loaderTree)
    const segmentPath =
      parentPath === null
        ? stringifySegment(segment)
        : createChildSegmentPath(parentPath, key!, segment)

    let moduleInfo: ValidationRouteTree['module'] = null
    if (layoutOrPageMod !== undefined) {
      // TODO(restart-on-cache-miss): Does this work correctly for client page/layout modules?
      const prefetchConfig =
        (layoutOrPageMod as AppSegmentConfig).unstable_prefetch ?? null
      moduleInfo = {
        type: modType!,
        prefetchConfig,
        conventionPath: conventionPath!,
      }

      if (isInsideParallelSlot) {
        // For now, we ignore parallel routes for purposes of finding configs to validate
        // and finding shared layout parents.
        if (prefetchConfig !== null) {
          console.error(
            `${conventionPath}: \`unstable_prefetch\` validation is not fully implemented for parallel routes yet.`
          )
        }
      } else {
        if (modType === 'layout') {
          // All layouts will be checked as navigation parents, so
          // if a layout has a prefetch config, we'll check navigations into it
          // because we'll be navigating from its parents.

          if (prefetchConfig !== null) {
            if (prefetchConfig === false) {
              // we don't want to validate navigations into this segment,
              // but still want to validate inside it.
              navigationParents = []
            } else {
              const isRootLayout = parentLayoutPath === null
              if (isRootLayout && prefetchConfig.mode === 'runtime') {
                throw new Error(
                  `${conventionPath}: \`unstable_prefetch\` with mode 'runtime' is not supported in root layouts.`
                )
              }

              const task: ValidationTask = {
                target: segmentPath,
                parents: navigationParents,
              }
              validationTasks.push(task)
              navigationParents = []
            }
          }
          navigationParents.push(segmentPath)
        } else if (modType === 'page') {
          if (prefetchConfig !== null) {
            if (prefetchConfig === false) {
              navigationParents = []
            } else {
              // If the page itself has a prefetch config, then
              // make sure we always validate a navigation from its parent
              // to ensure `__PAGE__?p=foo -> __PAGE__?p=bar` works.
              //
              // This is relevant if the parent layout is implicit, as in
              //   my-segment/
              //     loading.tsx
              //     page.tsx
              // because the above code for layouts wouldn't add it.
              // TODO: what if this is runtime-prefetched? how does that affect a search-param navigation?
              // TODO: this can cause double validation if the parent segment is empty
              //       but we have a parent layout that'd be validated
              if (parentPath === null) {
                throw new InvariantError('A page must have a root layout')
              }
              if (!navigationParents.includes(parentPath)) {
                navigationParents.push(parentPath)
              }
              const task: ValidationTask = {
                target: segmentPath,
                parents: navigationParents,
              }
              validationTasks.push(task)
              navigationParents = []
            }
          }
        }

        if (prefetchConfig && typeof prefetchConfig === 'object') {
          segmentsWithPrefetchConfigs.push(segmentPath)
        }
      }
    }

    const parentLayoutPathForChildren =
      modType === 'layout' ? segmentPath : parentLayoutPath

    let slots: ValidationRouteTree['slots'] = null
    for (const parallelRouteKey in parallelRoutes) {
      const childLoaderTree = parallelRoutes[parallelRouteKey]
      const isChildInParallelSlot =
        isInsideParallelSlot || parallelRouteKey !== 'children'
      slots ??= {}
      slots[parallelRouteKey] = await visit(
        childLoaderTree,
        segmentPath,
        parallelRouteKey,
        parentLayoutPathForChildren,
        isChildInParallelSlot
      )
    }

    const treeNode: ValidationRouteTree = {
      path: segmentPath,
      segment,
      module: moduleInfo,
      slots,
    }
    treeNodes.set(segmentPath, treeNode)
    return treeNode
  }

  const routeTree = await visit(rootLoaderTree, null, null, null, false)
  return {
    tree: routeTree,
    treeNodes,
    // TODO: do we want to preserve info about which config caused a validation to occur?
    navigationParents: validationTasks.flatMap((task) => task.parents),
    segmentsWithPrefetchConfigs,
  }
}

function traverseRootSeedDataSegments(
  initialRSCPayload: InitialRSCPayload,
  processSegment: (
    segmentPath: SegmentPath,
    seedData: CacheNodeSeedData
  ) => void
) {
  // TODO: handle head as well
  const { flightRouterState, seedData } =
    getRootDataFromPayload(initialRSCPayload)

  const [rootSegment] = flightRouterState
  const rootPath = stringifySegment(rootSegment)
  return traverseCacheNodeSegments(
    rootPath,
    flightRouterState,
    seedData,
    processSegment
  )
}

function traverseCacheNodeSegments(
  path: SegmentPath,
  route: FlightRouterState,
  seedData: CacheNodeSeedData,
  processSegment: (
    segmentPath: SegmentPath,
    seedData: CacheNodeSeedData
  ) => void
): void {
  processSegment(path, seedData)

  const [_segment, childRoutes] = route
  const [_node, parallelRoutesData, _loading, _isPartial] = seedData

  for (const parallelRouteKey in childRoutes) {
    const childSeedData = parallelRoutesData[parallelRouteKey]
    if (!childSeedData) {
      throw new InvariantError(
        `Got unexpected empty seed data during prefetch validation`
      )
    }

    const childRoute = childRoutes[parallelRouteKey]
    const [childSegment] = childRoute
    const childPath = createChildSegmentPath(
      path,
      parallelRouteKey,
      childSegment
    )

    traverseCacheNodeSegments(
      childPath,
      childRoute,
      childSeedData,
      processSegment
    )
  }
}

function createChildSegmentPath(
  parentPath: SegmentPath,
  parallelRouteKey: string,
  segment: Segment
): SegmentPath {
  const parallelRoutePrefix =
    parallelRouteKey === 'children'
      ? ''
      : `@${encodeURIComponent(parallelRouteKey)}/`
  return `${parentPath}/${parallelRoutePrefix}${stringifySegment(segment)}` as SegmentPath
}

function stringifySegment(segment: Segment): SegmentPath {
  return (
    typeof segment === 'string'
      ? encodeURIComponent(segment)
      : encodeURIComponent(segment[0]) + '|' + segment[1] + '|' + segment[2]
  ) as SegmentPath
}

function createValidationSeedData(
  cache: ValidationSegmentCache,
  rootRouteTree: ValidationRouteTree,
  navigationParent: SegmentPath,
  releaseSignal: AbortSignal,
  clientReferenceManifest: ClientReferenceManifest,
  stageEndTimes: StageEndTimes,
  useRuntimeStageForPartialSegments: boolean,
  usedSegmentKinds: Set<SegmentStage>
): Promise<CacheNodeSeedData> {
  type TraversalState =
    | { kind: 'shared-tree' }
    | { kind: 'new-tree'; isInsideRuntimePrefetch: boolean }

  async function createSeedDataFromValidationTreeImpl(
    routeTree: ValidationRouteTree,
    state: TraversalState,
    parentState: TraversalState | null
  ) {
    const { path, slots } = routeTree

    let stage: SegmentStage
    let nextState: TraversalState
    switch (state.kind) {
      case 'shared-tree': {
        stage = RenderStage.Dynamic
        if (path === navigationParent) {
          // We reached the last shared segment. Everything below is a new subtree.
          nextState = { kind: 'new-tree', isInsideRuntimePrefetch: false }
        } else {
          nextState = state
        }
        break
      }
      case 'new-tree': {
        if (!state.isInsideRuntimePrefetch) {
          // We're not already inside a runtime prefetch, so by default we prefetch statically.
          // Check if we need to switch to runtime prefetching instead.
          const prefetchConfig = routeTree.module?.prefetchConfig
          if (
            prefetchConfig &&
            typeof prefetchConfig === 'object' &&
            prefetchConfig.mode === 'runtime'
          ) {
            // We have a runtime prefetch config. The client router will
            // prefetch this segment and all segments below using a runtime prefetch.
            stage = RenderStage.Runtime
            nextState = { kind: 'new-tree', isInsideRuntimePrefetch: true }
          } else {
            // No runtime prefetch config. Continue using static prefetching.
            //
            // Note that we can also get here for `unstable_prefetch = false` with a `mode: 'static'` parent.
            // `false` doesn't currently affect router behavior, so we act like it's not there.
            //
            // If the initial validation failed, we retry the render and use the runtime stage
            // for static segments. This lets us discriminate runtime and dynamic holes.
            stage = useRuntimeStageForPartialSegments
              ? RenderStage.Runtime
              : RenderStage.Static
            nextState = state
          }
        } else {
          // We're already inside a runtime prefetch, so we stay this way.
          // Note that we can also get here for `unstable_prefetch = false` with a `mode: 'runtime'` parent.
          // `false` doesn't currently affect router behavior, so we act like it's not there.
          stage = RenderStage.Runtime
          nextState = state
        }

        break
      }
      default: {
        state satisfies never
        throw new InvariantError(
          `Unexpected state while traversing route tree: ${(state as any).kind}`
        )
      }
    }

    console.log(`  ${path || '/'} - ${RenderStage[stage]}`)
    const segmentChunks = cache.get(path)
    if (!segmentChunks) {
      throw new InvariantError(`Missing segment data: ${path}`)
    }

    // TODO: for runtime-only validations, empty segments can throw this off
    // and make us retry even though there's no *real* static segments in the tree
    usedSegmentKinds.add(stage)

    let segmentData = await decodeFromChunks<SegmentData>(
      segmentChunks.chunks[stage],
      segmentChunks.chunks[RenderStage.Dynamic],
      segmentChunks.debugChunks,
      releaseSignal,
      clientReferenceManifest,
      stage === RenderStage.Dynamic
        ? null
        : { startTime: undefined, endTime: stageEndTimes[stage] }
    )

    // We place the validation boundary right below the shared parent segment
    // This means that a dynamic hole is accepted as long as it has a Suspense boundary
    // in the new subtree (i.e. it wouldn't block the navigation).
    const isValidationBoundary =
      state.kind === 'new-tree' &&
      parentState &&
      parentState.kind === 'shared-tree'

    if (isValidationBoundary) {
      console.log(`adding validation boundary around '${path}'`)
      segmentData = {
        ...segmentData,
        node: (
          // bundled in the server layer
          // eslint-disable-next-line @next/internal/no-ambiguous-jsx
          <PrefetchValidationBoundary key="c" /* matching `cacheNodeKey` */>
            {segmentData.node}
          </PrefetchValidationBoundary>
        ),
      }
    }

    const slotsSeedData: CacheNodeSeedDataSlots = {}
    if (slots) {
      for (const parallelRouteKey in slots) {
        slotsSeedData[parallelRouteKey] =
          await createSeedDataFromValidationTreeImpl(
            slots[parallelRouteKey],
            nextState,
            state
          )
      }
    }
    return getCacheNodeSeedDataFromSegment(segmentData, slotsSeedData)
  }

  return createSeedDataFromValidationTreeImpl(
    rootRouteTree,
    // Root layouts are always shared. Navigating to a new root layout is an MPA navigation.
    { kind: 'shared-tree' },
    null
  )
}

//=====================================
// Segment seed data
//=====================================

/** An object version of `CacheNodeSeedData`, without slots. */
type SegmentData = {
  node: React.ReactNode | null
  isPartial: boolean
  hasRuntimePrefetch: boolean
  varyParams: VaryParamsThenable | null
}

function createSegmentData(seedData: CacheNodeSeedData): SegmentData {
  const [
    node,
    _parallelRoutesData,
    _unused,
    isPartial,
    hasRuntimePrefetch,
    varyParams,
  ] = seedData
  return {
    node,
    isPartial,
    hasRuntimePrefetch,
    varyParams,
  }
}
type CacheNodeSeedDataSlots = CacheNodeSeedData[1]

function getCacheNodeSeedDataFromSegment(
  data: SegmentData,
  slots: CacheNodeSeedDataSlots
): CacheNodeSeedData {
  return [
    data.node,
    slots,
    /* unused (previously `loading`) */ null,
    data.isPartial,
    data.hasRuntimePrefetch,
    data.varyParams,
  ]
}

//=====================================
// Validation segment cache
//=====================================

function createValidationCache(): ValidationSegmentCache {
  return new Map()
}

export type SegmentStage =
  | RenderStage.Static
  | RenderStage.Runtime
  | RenderStage.Dynamic

export type ValidationSegmentCache = Map<
  SegmentPath,
  ValidationSegmentCacheValue
>
type ValidationSegmentCacheValue = {
  chunks: StageChunks
  debugChunks: Uint8Array[] | null
}

function writeChunk(
  stageChunks: StageChunks,
  stage: SegmentStage,
  chunk: Uint8Array
) {
  switch (stage) {
    case RenderStage.Static: {
      stageChunks[RenderStage.Static].push(chunk)
      // fallthrough
    }
    case RenderStage.Runtime: {
      stageChunks[RenderStage.Runtime].push(chunk)
      // fallthrough
    }
    case RenderStage.Dynamic: {
      stageChunks[RenderStage.Dynamic].push(chunk)
      break
    }
    default: {
      stage satisfies never
    }
  }
}
