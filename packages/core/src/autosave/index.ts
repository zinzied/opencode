export * as Autosave from "./index"

import { Context, Effect, Layer, Schema } from "effect"
import { Database } from "../database/database"
import { Global } from "../global"
import { SessionV2 } from "../session"
import { EventV2 } from "../event"
import { SessionSchema } from "../session/schema"
import { SessionMessage } from "../session/message"
import fs from "fs/promises"
import path from "path"
import { randomUUID } from "crypto"

export const AUTOSAVE_INTERVAL_MS = 60000 // 60 seconds
export const MAX_BACKUPS = 5

export interface AutosaveMetadata {
  readonly timestamp: number
  readonly sessionCount: number
  readonly messageCount: number
  readonly lastSessionID?: string
}

const AutosaveMetadataSchema = Schema.Struct({
  timestamp: Schema.Number,
  sessionCount: Schema.Number,
  messageCount: Schema.Number,
  lastSessionID: Schema.optional(Schema.String),
})

export interface Interface {
  readonly start: Effect.Effect<void>
  readonly stop: Effect.Effect<void>
  readonly trigger: Effect.Effect<void>
  readonly checkForRecovery: Effect.Effect<AutosaveMetadata | null>
  readonly restore: (timestamp: number) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/Autosave") {}

const createAutosaveDirectory = Effect.gen(function* () {
  const global = yield* Global.Service
  const autosaveDir = path.join(global.state, "autosave")
  yield* Effect.tryPromise({
    try: () => fs.mkdir(autosaveDir, { recursive: true }),
    catch: (error) => new Error(`Failed to create autosave directory: ${error}`),
  })
  return autosaveDir
})

const writeLockFile = (dir: string, timestamp: number) =>
  Effect.gen(function* () {
    const lockPath = path.join(dir, "lock")
    const lockContent = JSON.stringify({ timestamp, pid: process.pid })
    yield* Effect.tryPromise({
      try: () => fs.writeFile(lockPath, lockContent, "utf-8"),
      catch: (error) => new Error(`Failed to write lock file: ${error}`),
    })
  })

const removeLockFile = (dir: string) =>
  Effect.tryPromise({
    try: () => fs.unlink(path.join(dir, "lock")).catch(() => {}),
    catch: (error) => new Error(`Failed to remove lock file: ${error}`),
  })

const readLockFile = (dir: string) =>
  Effect.gen(function* () {
    const lockPath = path.join(dir, "lock")
    const content = yield* Effect.tryPromise({
      try: () => fs.readFile(lockPath, "utf-8"),
      catch: () => null,
    })
    if (!content) return null
    try {
      return JSON.parse(content) as { timestamp: number; pid: number }
    } catch {
      return null
    }
  })

const createBackup = (dir: string, timestamp: number) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    const backupDir = path.join(dir, `backup-${timestamp}`)

    // Create backup directory
    yield* Effect.tryPromise({
      try: () => fs.mkdir(backupDir, { recursive: true }),
      catch: (error) => new Error(`Failed to create backup directory: ${error}`),
    })

    // Backup database
    const dbPath = yield* Effect.sync(() => path.join(Global.Path.data, "opencode.db"))
    const backupDbPath = path.join(backupDir, "opencode.db")

    // Check if database file exists
    const dbExists = yield* Effect.tryPromise({
      try: () => fs.access(dbPath).then(() => true),
      catch: () => false,
    })

    if (dbExists) {
      yield* Effect.tryPromise({
        try: () => fs.copyFile(dbPath, backupDbPath),
        catch: (error) => new Error(`Failed to backup database: ${error}`),
      })
    }

    // Collect metadata
    const sessions = yield* SessionV2.Service.pipe(
      Effect.flatMap((service) => service.list()),
      Effect.catchAll(() => Effect.succeed([]))
    )

    let messageCount = 0
    for (const session of sessions) {
      const messages = yield* SessionV2.Service.pipe(
        Effect.flatMap((service) => service.messages({ sessionID: session.id })),
        Effect.catchAll(() => Effect.succeed([]))
      )
      messageCount += messages.length
    }

    const metadata: AutosaveMetadata = {
      timestamp,
      sessionCount: sessions.length,
      messageCount,
      lastSessionID: sessions[0]?.id,
    }

    // Write metadata
    const metadataPath = path.join(backupDir, "metadata.json")
    yield* Effect.tryPromise({
      try: () => fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2), "utf-8"),
      catch: (error) => new Error(`Failed to write metadata: ${error}`),
    })

    // Clean up old backups
    yield* cleanupOldBackups(dir)

    return metadata
  })

const cleanupOldBackups = (dir: string) =>
  Effect.gen(function* () {
    const entries = yield* Effect.tryPromise({
      try: () => fs.readdir(dir, { withFileTypes: true }),
      catch: () => [],
    })

    const backups = entries
      .filter((e) => e.isDirectory() && e.name.startsWith("backup-"))
      .map((e) => ({
        name: e.name,
        timestamp: Number.parseInt(e.name.replace("backup-", ""), 10),
      }))
      .filter((b) => !Number.isNaN(b.timestamp))
      .sort((a, b) => b.timestamp - a.timestamp)

    // Remove excess backups
    for (const backup of backups.slice(MAX_BACKUPS)) {
      const backupPath = path.join(dir, backup.name)
      yield* Effect.tryPromise({
        try: () => fs.rm(backupPath, { recursive: true, force: true }),
        catch: () => {},
      })
    }
  })

const restoreBackup = (dir: string, timestamp: number) =>
  Effect.gen(function* () {
    const backupDir = path.join(dir, `backup-${timestamp}`)
    const backupDbPath = path.join(backupDir, "opencode.db")
    const dbPath = path.join(Global.Path.data, "opencode.db")

    // Check if backup exists
    const backupExists = yield* Effect.tryPromise({
      try: () => fs.access(backupDbPath).then(() => true),
      catch: () => false,
    })

    if (!backupExists) {
      return yield* new Error(`Backup not found for timestamp: ${timestamp}`)
    }

    // Create a backup of current database before restoring
    const currentBackupPath = path.join(dir, `pre-restore-${Date.now()}.db`)
    const currentDbExists = yield* Effect.tryPromise({
      try: () => fs.access(dbPath).then(() => true),
      catch: () => false,
    })

    if (currentDbExists) {
      yield* Effect.tryPromise({
        try: () => fs.copyFile(dbPath, currentBackupPath),
        catch: (error) => new Error(`Failed to backup current database: ${error}`),
      })
    }

    // Restore from backup
    yield* Effect.tryPromise({
      try: () => fs.copyFile(backupDbPath, dbPath),
      catch: (error) => new Error(`Failed to restore database: ${error}`),
    })
  })

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const autosaveDir = yield* createAutosaveDirectory
    let intervalId: NodeJS.Timeout | null = null

    const startAutosave = Effect.gen(function* () {
      // Write lock file on startup
      yield* writeLockFile(autosaveDir, Date.now())

      // Set up periodic autosave
      intervalId = setInterval(() => {
        Effect.runPromise(
          Effect.gen(function* () {
            const timestamp = Date.now()
            yield* createBackup(autosaveDir, timestamp).pipe(
              Effect.catchAll((error) =>
                Effect.sync(() => console.error("Autosave failed:", error))
              )
            )
          })
        )
      }, AUTOSAVE_INTERVAL_MS)
    })

    const stopAutosave = Effect.gen(function* () {
      // Remove lock file on graceful shutdown
      yield* removeLockFile(autosaveDir)

      // Clear interval
      if (intervalId) {
        clearInterval(intervalId)
        intervalId = null
      }
    })

    return Service.of({
      start: startAutosave,
      stop: stopAutosave,
      trigger: Effect.gen(function* () {
        const timestamp = Date.now()
        yield* createBackup(autosaveDir, timestamp)
      }),
      checkForRecovery: Effect.gen(function* () {
        const lock = yield* readLockFile(autosaveDir)
        if (!lock) return null

        // Check if process is still running
        try {
          process.kill(lock.pid, 0)
          // Process is still running, no recovery needed
          return null
        } catch {
          // Process is not running, recovery available
          const backupDir = path.join(autosaveDir, `backup-${lock.timestamp}`)
          const metadataPath = path.join(backupDir, "metadata.json")

          const content = yield* Effect.tryPromise({
            try: () => fs.readFile(metadataPath, "utf-8"),
            catch: () => null,
          })

          if (!content) return null

          return yield* Effect.tryPromise({
            try: () => Promise.resolve(JSON.parse(content) as AutosaveMetadata),
            catch: () => null,
          })
        }
      }),
      restore: (timestamp: number) => restoreBackup(autosaveDir, timestamp),
    })
  })
)

export const defaultLayer = layer.pipe(Layer.provide(Global.defaultLayer))
