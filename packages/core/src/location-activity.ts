export * as LocationActivity from "./location-activity.js"

import path from "path"
import { parse, type ParseError } from "jsonc-parser"
import { Clock, Context, Duration, Effect, Layer, Option, PubSub, RcMap, Schema, Stream } from "effect"
import { Info } from "@opencode/schema/config"
import { FSUtil } from "@opencode/util/fs-util"
import { Global } from "@opencode/util/global"
import { Bus } from "./bus.js"
import { ConfigDiscovery } from "./config/discovery.js"
import { ConfigVariable } from "./config/variable.js"
import { Watcher } from "./filesystem/watcher.js"
import { Location } from "./location.js"
import { LocationServiceMap } from "./location-service-map.js"
import { SessionEvent } from "./session/event.js"
import { SessionExecution } from "./session/execution.js"
import { SessionStore } from "./session/store.js"
import { makeGlobalNode } from "@opencode/util/effect/app-node"

const isSessionEvent = Schema.is(SessionEvent.Durable)

export class Service extends Context.Service<Service, {}>()("@opencode/LocationActivity") {}

export function layer(options: { readonly timeToLive?: Duration.Input; readonly sweepInterval?: Duration.Input } = {}) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const clock = yield* Clock.Clock
      const bus = yield* Bus.Service
      const locations = yield* LocationServiceMap.Service
      const execution = yield* SessionExecution.Service
      const sessions = yield* SessionStore.Service
      const global = yield* Global.Service
      const fs = yield* FSUtil.Service
      const watcher = yield* Watcher.Service
      const fallback = Duration.toMillis(options.timeToLive ?? "60 minutes")
      const readTimeout = readGlobalTimeout(fs, global.config, fallback)
      const initial = yield* readTimeout.pipe(Effect.orDie)
      if (initial === null) yield* Effect.logWarning("invalid global location inactivity timeout; using default")
      let timeToLive = initial === null ? fallback : initial
      const changes = yield* PubSub.sliding<void>(1)
      const subscription = yield* PubSub.subscribe(changes)
      const entries = new Map<string, { readonly ref: Location.Ref; expiresAt: number }>()
      const key = (ref: Location.Ref) => `${LocationServiceMap.canonical(ref).directory}\0${ref.workspaceID ?? ""}`
      const touch = (ref: Location.Ref) =>
        Effect.sync(() => {
          if (timeToLive === false) return
          entries.set(key(ref), { ref, expiresAt: clock.currentTimeMillisUnsafe() + timeToLive })
        })

      const refresh = Effect.fn("LocationActivity.refreshTimeout")(function* () {
        const next = yield* readTimeout
        if (next === null) {
          yield* Effect.logWarning("invalid global location inactivity timeout; keeping previous value")
          return
        }
        if (next === timeToLive) return
        timeToLive = next
        entries.clear()
        yield* PubSub.publish(changes, undefined)
      })
      const refreshSafely = refresh().pipe(
        Effect.catchCause((cause) => Effect.logWarning("failed to reload location inactivity timeout", { cause })),
      )
      const updates = yield* watcher.subscribe(
        { path: global.config, type: "entries", names: ConfigDiscovery.names },
        refreshSafely,
      )
      yield* updates.pipe(
        Stream.debounce("100 millis"),
        Stream.runForEach(() => refreshSafely),
        Effect.forkScoped({ startImmediately: true }),
      )

      const unsubscribe = yield* bus.listen((event) => {
        if (!isSessionEvent(event)) return Effect.void
        const location = event.location
        if (!location) return Effect.void
        return RcMap.has(locations.rcMap, location).pipe(
          Effect.flatMap((active) => (active ? touch(location) : Effect.void)),
        )
      })
      yield* Effect.addFinalizer(() => unsubscribe)
      yield* Effect.gen(function* () {
        yield* Effect.race(
          Effect.sleep(
            options.sweepInterval ??
              Duration.millis(timeToLive === false ? 60_000 : Math.max(1_000, Math.min(60_000, timeToLive / 2))),
          ),
          PubSub.take(subscription),
        )
        const refs = Array.from(yield* RcMap.keys(locations.rcMap))
        if (timeToLive === false) {
          entries.clear()
          return
        }
        const cached = new Set(refs.map(key))
        yield* Effect.forEach(refs, (ref) => (entries.has(key(ref)) ? Effect.void : touch(ref)), { discard: true })
        for (const id of entries.keys()) {
          if (!cached.has(id)) entries.delete(id)
        }
        const now = clock.currentTimeMillisUnsafe()
        const expired = Array.from(entries.values()).filter((entry) => entry.expiresAt <= now)
        if (expired.length === 0) return
        const active = yield* Effect.forEach(yield* execution.active, (sessionID) => sessions.get(sessionID))
        yield* Effect.forEach(
          expired,
          (entry) =>
            Effect.gen(function* () {
              const owners = active.flatMap((session) =>
                session && key(session.location) === key(entry.ref) ? [session] : [],
              )
              // Invalidation only detaches the cache entry; borrowers retain the old
              // graph. Stop its executions and settle tool cleanup before detaching it.
              yield* Effect.forEach(
                owners,
                (session) => execution.interrupt(session.id, { reason: "inactivity", awaitSettlement: true }),
                {
                  discard: true,
                  concurrency: "unbounded",
                },
              )
              const remaining = yield* Effect.forEach(yield* execution.active, (sessionID) => sessions.get(sessionID))
              // New work admitted during cleanup may now own the cached graph.
              if (remaining.some((session) => session && key(session.location) === key(entry.ref))) {
                yield* touch(entry.ref)
                return
              }
              entries.delete(key(entry.ref))
              yield* Effect.logInfo("location services evicted", {
                directory: entry.ref.directory,
                workspaceID: entry.ref.workspaceID,
              }).pipe(Effect.andThen(locations.invalidate(entry.ref)))
            }),
          { discard: true, concurrency: "unbounded" },
        )
      }).pipe(Effect.forever, Effect.forkScoped)

      return Service.of({})
    }),
  )
}

const readGlobalTimeout = Effect.fn("LocationActivity.readGlobalTimeout")(function* (
  fs: FSUtil.Interface,
  directory: string,
  fallback: number,
) {
  const values = yield* Effect.forEach(ConfigDiscovery.names, (name) =>
    Effect.gen(function* () {
      const file = path.join(directory, name)
      const text = yield* fs.readFileStringSafe(file)
      if (text === undefined) return undefined
      const substituted = yield* ConfigVariable.substitute({ type: "path", path: file, text }).pipe(
        Effect.provideService(FSUtil.Service, fs),
      )
      const errors: ParseError[] = []
      const input: unknown = parse(substituted, errors, { allowTrailingComma: true })
      if (errors.length || !input || typeof input !== "object" || Array.isArray(input)) return null
      if (!("location_inactivity_timeout" in input)) return undefined
      const decoded = Schema.decodeUnknownOption(Info.fields.location_inactivity_timeout)(input.location_inactivity_timeout)
      if (Option.isNone(decoded) || decoded.value === undefined) return null
      return decoded.value === false ? false : Duration.toMillis(decoded.value)
    }),
  )
  const selected = values.findLast((value) => value !== undefined)
  return selected === undefined ? fallback : selected
})

export const node = makeGlobalNode({
  service: Service,
  layer: layer(),
  deps: [
    Bus.node,
    LocationServiceMap.node,
    SessionExecution.node,
    SessionStore.node,
    Global.node,
    FSUtil.node,
    Watcher.node,
  ],
})
