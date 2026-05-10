const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("dbcompare", {
  openDatabase: () => ipcRenderer.invoke("dialog:openDatabase"),
  saveGeneratedFile: (sourcePath, defaultName) =>
    ipcRenderer.invoke("dialog:saveGeneratedFile", sourcePath, defaultName),
  apiBaseUrl: "http://127.0.0.1:8765"
});
