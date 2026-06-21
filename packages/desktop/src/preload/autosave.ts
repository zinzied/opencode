import { ipcRenderer } from "electron"

export interface AutosaveAPI {
  checkForRecovery: () => Promise<AutosaveMetadata | null>
  restore: (timestamp: number) => Promise<void>
  trigger: () => Promise<void>
}

export interface AutosaveMetadata {
  readonly timestamp: number
  readonly sessionCount: number
  readonly messageCount: number
  readonly lastSessionID?: string
}

export const autosaveApi: AutosaveAPI = {
  checkForRecovery: () => ipcRenderer.invoke("autosave:checkForRecovery"),
  restore: (timestamp: number) => ipcRenderer.invoke("autosave:restore", timestamp),
  trigger: () => ipcRenderer.invoke("autosave:trigger"),
}
