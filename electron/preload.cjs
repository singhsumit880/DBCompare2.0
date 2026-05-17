const { contextBridge, ipcRenderer } = require("electron");

const apiArg = process.argv.find((arg) => arg.startsWith("--api-base-url="));
const apiBaseUrl = apiArg ? apiArg.slice("--api-base-url=".length) : "http://127.0.0.1:8765";
const openFileArg = process.argv.find((arg) => arg.startsWith("--open-file="));
const initialOpenFile = openFileArg ? decodeURIComponent(openFileArg.slice("--open-file=".length)) : "";

contextBridge.exposeInMainWorld("dbcompare", {
  openDatabase: () => ipcRenderer.invoke("dialog:openDatabase"),
  saveGeneratedFile: (sourcePath, defaultName) =>
    ipcRenderer.invoke("dialog:saveGeneratedFile", sourcePath, defaultName),
  apiBaseUrl,
  initialOpenFile,
  backendStatus: () => ipcRenderer.invoke("backend:status")
});
