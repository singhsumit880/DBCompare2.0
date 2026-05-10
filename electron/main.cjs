const { app, BrowserWindow, dialog, ipcMain } = require("electron");
const fs = require("node:fs");
const path = require("node:path");
const { execFileSync, spawn } = require("node:child_process");
const http = require("node:http");

const isDev = !app.isPackaged;
let apiProcess = null;
const apiBaseUrl = "http://127.0.0.1:8765";
const requiredDevRoutes = [
  "/api/compare",
  "/api/tools/sanitize",
  "/api/tools/convert",
  "/api/tools/fts",
  "/api/tools/settings-repair",
  "/api/sql/tables",
  "/api/sql/table-info",
  "/api/sql/checks",
  "/api/sql/version",
  "/api/sql/rows",
  "/api/sql/related-rows",
  "/api/sql/query"
];

function backendExecutable() {
  if (isDev) {
    return {
      command: process.platform === "win32" ? "python" : "python3",
      args: ["backend/run.py"],
      cwd: path.join(__dirname, "..")
    };
  }

  const exeName = process.platform === "win32" ? "dbcompare-api.exe" : "dbcompare-api";
  return {
    command: path.join(process.resourcesPath, "backend", exeName),
    args: [],
    cwd: process.resourcesPath
  };
}

function startBackend() {
  const backend = backendExecutable();
  apiProcess = spawn(backend.command, backend.args, {
    cwd: backend.cwd,
    stdio: isDev ? "inherit" : "ignore",
    windowsHide: true
  });

  apiProcess.on("exit", () => {
    apiProcess = null;
  });
}

function requestJson(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => {
        body += chunk;
      });
      res.on("end", () => {
        try {
          resolve(JSON.parse(body));
        } catch (error) {
          reject(error);
        }
      });
    });
    req.on("error", reject);
    req.setTimeout(1000, () => {
      req.destroy(new Error("Backend route check timed out"));
    });
  });
}

async function hasCurrentBackendRoutes() {
  try {
    const openApi = await requestJson(`${apiBaseUrl}/openapi.json`);
    const paths = openApi?.paths ?? {};
    return requiredDevRoutes.every((route) => Object.prototype.hasOwnProperty.call(paths, route));
  } catch {
    return false;
  }
}

function listeningPidsForPort(port) {
  if (process.platform !== "win32") return [];
  try {
    const output = execFileSync("netstat", ["-ano"], { encoding: "utf8" });
    const pids = new Set();
    for (const line of output.split(/\r?\n/)) {
      if (!line.includes(`:${port}`) || !/\bLISTENING\b/.test(line)) continue;
      const parts = line.trim().split(/\s+/);
      const pid = Number(parts[parts.length - 1]);
      if (Number.isInteger(pid) && pid > 0) {
        pids.add(pid);
      }
    }
    return [...pids];
  } catch {
    return [];
  }
}

async function prepareDevBackendPort() {
  if (!isDev) return;
  if (await hasCurrentBackendRoutes()) return;
  for (const pid of listeningPidsForPort(8765)) {
    try {
      process.kill(pid);
    } catch {
      // Ignore stale process cleanup failures; backend spawn will surface real port issues.
    }
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
}

function createWindow() {
  const win = new BrowserWindow({
    width: 1440,
    height: 920,
    minWidth: 1120,
    minHeight: 760,
    title: "DB Explorer Pro",
    icon: path.join(__dirname, "assets", process.platform === "darwin" ? "icon.icns" : "icon-512.png"),
    backgroundColor: "#111827",
    webPreferences: {
      preload: path.join(__dirname, "preload.cjs"),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  if (isDev) {
    win.loadURL("http://127.0.0.1:5173");
  } else {
    win.loadFile(path.join(__dirname, "..", "dist", "index.html"));
  }
}

ipcMain.handle("dialog:openDatabase", async () => {
  const result = await dialog.showOpenDialog({
    properties: ["openFile"],
    filters: [
      { name: "Database Files", extensions: ["vyp", "vyb", "sqlite", "db", "zip"] },
      { name: "All Files", extensions: ["*"] }
    ]
  });
  return result.canceled ? null : result.filePaths[0];
});

ipcMain.handle("dialog:saveGeneratedFile", async (_event, sourcePath, defaultName) => {
  if (!sourcePath || !fs.existsSync(sourcePath)) {
    return { saved: false, error: "Generated file was not found." };
  }

  const extension = path.extname(sourcePath).replace(".", "").toLowerCase();
  const suggestedName = defaultName || path.basename(sourcePath);
  const filters = extension
    ? [
        { name: `${extension.toUpperCase()} file`, extensions: [extension] },
        { name: "All Files", extensions: ["*"] }
      ]
    : [{ name: "All Files", extensions: ["*"] }];

  const result = await dialog.showSaveDialog({
    defaultPath: suggestedName,
    filters
  });

  if (result.canceled || !result.filePath) {
    return { saved: false };
  }

  await fs.promises.copyFile(sourcePath, result.filePath);
  return { saved: true, path: result.filePath };
});

app.whenReady().then(async () => {
  await prepareDevBackendPort();
  startBackend();
  createWindow();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});

app.on("before-quit", () => {
  if (apiProcess) {
    apiProcess.kill();
  }
});
