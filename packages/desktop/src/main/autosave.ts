import { ipcMain } from "electron"
import type { IpcMainInvokeEvent } from "electron"
import type { AutosaveMetadata } from "@opencode/core"

export interface AutosaveHandler {
  checkForRecovery: () => Promise<AutosaveMetadata | null>
  restore: (timestamp: number) => Promise<void>
  trigger: () => Promise<void>
}

export function registerAutosaveHandlers(handler: AutosaveHandler) {
  ipcMain.handle("autosave:checkForRecovery", async () => {
    return handler.checkForRecovery()
  })

  ipcMain.handle("autosave:restore", async (_event: IpcMainInvokeEvent, timestamp: number) => {
    return handler.restore(timestamp)
  })

  ipcMain.handle("autosave:trigger", async () => {
    return handler.trigger()
  })
}
