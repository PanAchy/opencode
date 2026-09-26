import path from "path"
import fs from "fs/promises"
import { describe, expect } from "bun:test"
import { Context, Deferred, Duration, Effect, Fiber, Layer, LayerMap, RcMap, Schema } from "effect"
import { TestClock } from "effect/testing"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Global } from "@opencode/util/global"
import { FSUtil } from "@opencode/util/fs-util"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { Form } from "@opencode/core/form"
import { Watcher } from "@opencode/core/filesystem/watcher"
import { Location } from "@opencode/core/location"
import { LocationActivity } from "@opencode/core/location-activity"
import { LocationServiceMap, type LocationServices } from "@opencode/core/location-services"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionRunner } from "@opencode/core/session/runner/index"
import { SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { Workspace } from "@opencode/core/workspace"
import { testEffect } from "./lib/effect"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdirScoped } from "./fixture/tmpdir"

// Keep real execution ownership, location caching, forms, and eviction. The fixture
// runner waits on a form instead of making a model request before asking a question.
const locations = Layer.effect(
  LocationServiceMap.Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const map = yield* LayerMap.make(
      (ref: Location.Ref) =>
        // The fixture only exercises these three Location services.
        // oxlint-disable-next-line typescript-eslint/no-unsafe-type-assertion
        Layer.merge(
          Layer.succeed(
            Location.Service,
            Location.Service.of({
              directory: ref.directory,
              workspaceID: ref.workspaceID,
              project: { id: Project.ID.global, directory: ref.directory, canonical: ref.directory },
            }),
          ),
          Layer.effect(
            SessionRunner.Service,
            Effect.gen(function* () {
              const forms = yield* Form.Service
              return SessionRunner.Service.of({
                drain: ({ sessionID }) =>
                  forms
                    .ask({
                      sessionID,
                      title: "Questions",
                      fields: [{ key: "runtime", type: "string" }],
                    })
                    .pipe(
                      Effect.orDie,
                      Effect.as(SessionRunner.DrainResult.Complete()),
                      Effect.onInterrupt(() => Effect.sleep("5 minutes")),
                    ),
              })
            }),
          ),
        ).pipe(
          Layer.provideMerge(Form.layer),
          Layer.provide(Layer.succeed(Bus.Service, bus)),
          Layer.fresh,
        ) as unknown as Layer.Layer<LocationServices>,
      { idleTimeToLive: Duration.infinity },
    )
    return {
      ...map,
      get: (ref: Location.Ref) => map.get(LocationServiceMap.canonical(ref)),
      contextEffect: (ref: Location.Ref) => map.contextEffect(LocationServiceMap.canonical(ref)),
      contextEffectOption: (ref: Location.Ref) => map.contextEffectOption(LocationServiceMap.canonical(ref)),
      invalidate: (ref: Location.Ref) => map.invalidate(LocationServiceMap.canonical(ref)),
    }
  }),
)

const watcher = Watcher.testLayer
const it = testEffect(
  Layer.mergeAll(
    AppNodeBuilder.build(
      LayerNode.group([
        Database.node,
        Global.node,
        Watcher.node,
        Bus.node,
        SessionStore.node,
        LocationServiceMap.node,
        SessionExecution.node,
        LocationActivity.node,
      ]),
      [
        Global.node.replace(tempGlobalLayer),
        Watcher.node.replace(watcher),
        LocationServiceMap.node.replace(
          makeGlobalNode({
            service: LocationServiceMap.Service,
            layer: locations,
            deps: [Bus.node],
          }),
        ),
      ],
    ),
    watcher,
  ),
)

describe("LocationActivity eviction", () => {
  it.effect("sweeps short global timeouts without waiting a full minute", () =>
    Effect.gen(function* () {
      const map = yield* LocationServiceMap.Service
      const global = yield* Global.Service
      const watcher = yield* Watcher.Test
      const file = path.join(global.config, "opencode.jsonc")
      yield* TestClock.adjust("1 second")
      yield* Effect.promise(() => fs.writeFile(file, '{"location_inactivity_timeout":"2 seconds"}'))
      yield* watcher.emit({ path: file, type: "update" })
      yield* TestClock.adjust("1 minute")
      const ref = LocationServiceMap.canonical({ directory: AbsolutePath.make("/short-timeout") })
      yield* Location.Service.pipe(Effect.provide(map.get(ref)), Effect.scoped)
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([ref])
      yield* TestClock.adjust("5 seconds")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([])
    }),
  )

  it.effect("applies global duration and disabled edits live, resetting existing deadlines", () =>
    Effect.gen(function* () {
      const map = yield* LocationServiceMap.Service
      const global = yield* Global.Service
      const watcher = yield* Watcher.Test
      const file = path.join(global.config, "opencode.jsonc")
      yield* TestClock.adjust("1 second")
      expect(yield* watcher.subscriptions()).toContainEqual({
        path: global.config,
        type: "entries",
        names: ["opencode.json", "opencode.jsonc"],
      })
      const ref = LocationServiceMap.canonical({ directory: AbsolutePath.make("/project") })
      yield* Location.Service.pipe(Effect.provide(map.get(ref)), Effect.scoped)
      yield* TestClock.adjust("1 minute")

      const update = (value: string) =>
        Effect.promise(() => fs.writeFile(file, value)).pipe(
          Effect.andThen(watcher.emit({ path: file, type: "update" })),
          Effect.andThen(TestClock.adjust("200 millis")),
        )
      yield* update('{"location_inactivity_timeout":"2 minutes"}')
      yield* TestClock.adjust("1 minute")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([ref])
      yield* TestClock.adjust("3 minutes")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([])

      yield* Location.Service.pipe(Effect.provide(map.get(ref)), Effect.scoped)
      yield* update('{"location_inactivity_timeout":false}')
      yield* TestClock.adjust("90 minutes")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([ref])

      yield* update('{"location_inactivity_timeout":"0 seconds"}')
      yield* TestClock.adjust("62 minutes")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([ref])

      yield* update('{"location_inactivity_timeout":"2 minutes"}')
      yield* TestClock.adjust("1 minute")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([ref])
      yield* TestClock.adjust("3 minutes")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([])
    }),
  )

  it.effect("ignores invalid global edits and project overrides", () =>
    Effect.gen(function* () {
      const map = yield* LocationServiceMap.Service
      const global = yield* Global.Service
      const watcher = yield* Watcher.Test
      const file = path.join(global.config, "opencode.jsonc")
      const project = (yield* tmpdirScoped()).path
      const ref = LocationServiceMap.canonical({ directory: AbsolutePath.make(project) })
      yield* TestClock.adjust("1 second")
      expect(yield* watcher.subscriptions()).toContainEqual({
        path: global.config,
        type: "entries",
        names: ["opencode.json", "opencode.jsonc"],
      })
      yield* Effect.promise(() =>
        fs.writeFile(path.join(project, "opencode.jsonc"), '{"location_inactivity_timeout":false}'),
      )
      yield* Location.Service.pipe(Effect.provide(map.get(ref)), Effect.scoped)
      yield* Effect.promise(() => fs.writeFile(file, '{"location_inactivity_timeout":"0 seconds"}'))
      yield* watcher.emit({ path: file, type: "update" })
      yield* TestClock.adjust("200 millis")
      yield* TestClock.adjust("62 minutes")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([])
    }),
  )

  it.effect("uses a valid higher-priority global file despite an invalid lower-priority file", () =>
    Effect.gen(function* () {
      const map = yield* LocationServiceMap.Service
      const global = yield* Global.Service
      const watcher = yield* Watcher.Test
      yield* TestClock.adjust("1 second")
      const lower = path.join(global.config, "opencode.json")
      const higher = path.join(global.config, "opencode.jsonc")
      yield* Effect.promise(() =>
        Promise.all([
          fs.writeFile(lower, '{"location_inactivity_timeout":"0 seconds"}'),
          fs.writeFile(higher, '{"location_inactivity_timeout":"2 seconds"}'),
        ]),
      )
      yield* watcher.emit({ path: higher, type: "update" })
      yield* TestClock.adjust("1 minute")
      const ref = LocationServiceMap.canonical({ directory: AbsolutePath.make("/priority") })
      yield* Location.Service.pipe(Effect.provide(map.get(ref)), Effect.scoped)
      yield* TestClock.adjust("5 seconds")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([])
    }),
  )

  it.effect("gives a waiting execution a fresh deadline when the timeout is shortened", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const bus = yield* Bus.Service
      const map = yield* LocationServiceMap.Service
      const execution = yield* SessionExecution.Service
      const global = yield* Global.Service
      const watcher = yield* Watcher.Test
      const ref = LocationServiceMap.canonical({ directory: AbsolutePath.make("/waiting-shortened") })
      const sessionID = Session.ID.make("ses_waiting_shortened")
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: ref.directory, sandboxes: [] })
        .run()
        .pipe(Effect.orDie)
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: Project.ID.global,
          slug: "question",
          directory: ref.directory,
          title: "Waiting question",
          version: "test",
        })
        .run()
        .pipe(Effect.orDie)
      const created = yield* Deferred.make<Form.Info>()
      const unsubscribe = yield* bus.listen((event) => {
        if (event.type !== Form.Event.Created.type) return Effect.void
        return Deferred.succeed(created, Schema.decodeUnknownSync(Form.Event.Created.data)(event.data).form)
      })
      yield* Effect.addFinalizer(() => unsubscribe)
      const running = yield* execution.resume(sessionID).pipe(Effect.exit, Effect.forkScoped)
      yield* Effect.addFinalizer(() =>
        execution.interrupt(sessionID).pipe(Effect.andThen(TestClock.adjust("5 minutes"))),
      )
      const form = yield* Deferred.await(created)
      const context = yield* map.contextEffect(ref).pipe(Effect.scoped)
      const forms = Context.get(context, Form.Service)
      yield* TestClock.adjust("1 minute")
      yield* TestClock.adjust("57 minutes")
      const file = path.join(global.config, "opencode.jsonc")
      yield* Effect.promise(() => fs.writeFile(file, '{"location_inactivity_timeout":"2 minutes"}'))
      yield* watcher.emit({ path: file, type: "update" })
      yield* TestClock.adjust("200 millis")
      yield* TestClock.adjust("1 minute")
      expect(yield* forms.state(form.id)).toEqual({ status: "pending" })
      expect(Array.from(yield* execution.active)).toEqual([sessionID])
      yield* TestClock.adjust("3 minutes")
      expect(yield* forms.state(form.id)).toEqual({ status: "cancelled" })
      yield* TestClock.adjust("5 minutes")
      expect((yield* Fiber.join(running))._tag).toBe("Failure")
      expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([])
    }),
  )
  for (const [count, admission] of [
    [1, "none"],
    [2, "none"],
    [1, "other"],
    [1, "same"],
  ] as const) {
    const newWork = admission !== "none"
    it.effect(
      `interrupts ${count} waiting executions before eviction (${admission} session admitted during cleanup)`,
      () =>
        Effect.gen(function* () {
          const db = (yield* Database.Service).db
          const bus = yield* Bus.Service
          const map = yield* LocationServiceMap.Service
          const execution = yield* SessionExecution.Service
          const store = yield* SessionStore.Service
          const sessionIDs = Array.from({ length: count }, (_, index) =>
            Session.ID.make(`ses_waiting_question_${index}`),
          )
          const newcomer = admission === "same" ? sessionIDs[0] : Session.ID.make("ses_new_question")
          const ref = LocationServiceMap.canonical({ directory: AbsolutePath.make("/project") })
          const idle = Location.Ref.make({ directory: ref.directory, workspaceID: Workspace.ID.make("wrk_idle") })
          yield* db
            .insert(ProjectTable)
            .values({ id: Project.ID.global, worktree: ref.directory, sandboxes: [] })
            .run()
            .pipe(Effect.orDie)
          yield* db
            .insert(SessionTable)
            .values(
              Array.from(new Set([...sessionIDs, newcomer]), (sessionID) => ({
                id: sessionID,
                project_id: Project.ID.global,
                slug: "question",
                directory: ref.directory,
                title: "Waiting question",
                version: "test",
              })),
            )
            .run()
            .pipe(Effect.orDie)

          const created = yield* Deferred.make<void>()
          const newCreated = yield* Deferred.make<void>()
          const pending: Form.Info[] = []
          const interrupted: SessionEvent.Execution.Interrupted["data"][] = []
          const unsubscribe = yield* bus.listen((event) =>
            Effect.gen(function* () {
              if (event.type === SessionEvent.Execution.Interrupted.type) {
                interrupted.push(Schema.decodeUnknownSync(SessionEvent.Execution.Interrupted.data)(event.data))
              }
              if (event.type !== Form.Event.Created.type) return
              pending.push(Schema.decodeUnknownSync(Form.Event.Created.data)(event.data).form)
              if (pending.length === count) yield* Deferred.succeed(created, undefined)
              if (pending.length > count) yield* Deferred.succeed(newCreated, undefined)
            }),
          )
          yield* Effect.addFinalizer(() => unsubscribe)
          const running = yield* Effect.forEach(sessionIDs, (sessionID) =>
            execution.resume(sessionID).pipe(Effect.exit, Effect.forkScoped),
          )
          yield* Effect.addFinalizer(() =>
            Effect.forEach([...sessionIDs, newcomer], (sessionID) => execution.interrupt(sessionID)).pipe(
              Effect.andThen(TestClock.adjust("5 minutes")),
            ),
          )
          yield* Deferred.await(created)
          const context = yield* map.contextEffect(ref).pipe(Effect.scoped)
          const forms = Context.get(context, Form.Service)
          expect((yield* store.listSuspended()).toSorted()).toEqual(sessionIDs.toSorted())
          yield* Location.Service.pipe(Effect.provide(map.get(idle)), Effect.scoped)

          // Human input produces no durable activity while the question is pending.
          yield* TestClock.adjust("1 minute")
          yield* TestClock.adjust("62 minutes")
          // Interruption has cancelled each question, but slow cleanup still owns the graph.
          expect(Array.from(yield* execution.active).toSorted()).toEqual(sessionIDs.toSorted())
          expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([ref])
          expect(yield* forms.list()).toEqual([])
          for (const form of pending) expect(yield* forms.state(form.id)).toEqual({ status: "cancelled" })

          if (newWork) {
            yield* execution.wake(newcomer)
            if (admission === "other") yield* Deferred.await(newCreated)
          }
          yield* TestClock.adjust("5 minutes")
          if (newWork) yield* Deferred.await(newCreated)
          const results = yield* Effect.forEach(running, Fiber.join)
          expect(results.every((exit) => exit._tag === "Failure")).toBe(true)
          expect(Array.from(yield* execution.active)).toEqual(newWork ? [newcomer] : [])
          expect(yield* store.listSuspended()).toEqual(newWork ? [newcomer] : [])
          expect(interrupted.toSorted((a, b) => a.sessionID.localeCompare(b.sessionID))).toEqual(
            sessionIDs.map((sessionID) => ({ sessionID, reason: "inactivity" })),
          )
          expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual(newWork ? [ref] : [])
          if (newWork) {
            expect(yield* forms.list({ sessionID: newcomer })).toEqual([pending[count]])
            if (admission === "same") {
              const later = LocationServiceMap.canonical({ directory: AbsolutePath.make("/later") })
              yield* Location.Service.pipe(Effect.provide(map.get(later)), Effect.scoped)
              yield* TestClock.adjust("30 minutes")
              // Keep fresh work active while a different graph reaches its own deadline.
              yield* bus.publish(SessionEvent.Execution.Started, { sessionID: newcomer }, { location: ref })
              yield* TestClock.adjust("32 minutes")
              expect(Array.from(yield* execution.active)).toEqual([newcomer])
              expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([ref])
            }
            yield* execution.interrupt(newcomer)
            yield* TestClock.adjust("5 minutes")
            yield* execution.awaitIdle(newcomer)
            yield* TestClock.adjust("62 minutes")
            expect(yield* store.listSuspended()).toEqual([])
            expect(Array.from(yield* RcMap.keys(map.rcMap))).toEqual([])
          }
        }),
    )
  }
})
