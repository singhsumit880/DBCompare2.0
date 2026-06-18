const { app, BrowserWindow, Menu, dialog, ipcMain } = require("electron");
const fs = require("node:fs");
const path = require("node:path");
const { execFileSync, spawn } = require("node:child_process");
const http = require("node:http");
const net = require("node:net");

const isDev = !app.isPackaged;
let apiProcess = null;
let apiPort = null;
let apiBaseUrl = null;
let backendStartupError = null;
let backendReady = false;
let initialOpenFile = null;
const requiredDevRoutes = [
  "/api/compare",
  "/api/compare/jobs",
  "/api/compare/jobs/{job_id}",
  "/api/compare/jobs/{job_id}/result",
  "/api/compare/jobs/{job_id}/cancel",
  "/api/tools/sanitize",
  "/api/tools/convert",
  "/api/tools/fts",
  "/api/tools/settings-repair",
  "/api/sql/tables",
  "/api/sql/table-info",
  "/api/sql/schema",
  "/api/sql/checks",
  "/api/sql/version",
  "/api/sql/rows",
  "/api/sql/update-row",
  "/api/sql/update-rows-batch",
  "/api/sql/related-rows",
  "/api/sql/query",
  "/api/sql/compare-query",
  "/api/sql/export"
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

function isDatabaseFile(filePath) {
  if (!filePath || filePath.startsWith("-")) return false;
  const extension = path.extname(filePath).toLowerCase();
  return [".vyp", ".vyb", ".db", ".sqlite", ".sqlite3"].includes(extension);
}

function getLaunchDatabaseFile(argv) {
  return argv.find((arg) => isDatabaseFile(arg)) || null;
}

function findFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : null;
      server.close(() => {
        if (port) resolve(port);
        else reject(new Error("Could not allocate a backend port"));
      });
    });
  });
}

function startBackend() {
  const backend = backendExecutable();
  apiProcess = spawn(backend.command, backend.args, {
    cwd: backend.cwd,
    env: {
      ...process.env,
      DB_EXPLORER_API_PORT: String(apiPort)
    },
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

async function waitForBackend(timeoutMs = 15000) {
  const started = Date.now();
  let lastError = null;
  while (Date.now() - started < timeoutMs) {
    try {
      if (await hasCurrentBackendRoutes()) return true;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error(lastError?.message || `Backend did not start on ${apiBaseUrl}`);
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
  if (!isDev || !apiPort) return;
  if (await hasCurrentBackendRoutes()) return;
  for (const pid of listeningPidsForPort(apiPort)) {
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
    icon: path.join(__dirname, "assets", process.platform === "darwin" ? "icon.icns" : "icon.ico"),
    autoHideMenuBar: true,
    backgroundColor: "#f5f6fb",
    webPreferences: {
      preload: path.join(__dirname, "preload.cjs"),
      contextIsolation: true,
      nodeIntegration: false,
      additionalArguments: [
        `--api-base-url=${apiBaseUrl}`,
        `--open-file=${encodeURIComponent(initialOpenFile || "")}`
      ]
    }
  });

  win.setMenuBarVisibility(false);

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
  Menu.setApplicationMenu(null);
  app.setAppUserModelId("com.dbexplorerpro.desktop");
  initialOpenFile = getLaunchDatabaseFile(process.argv);
  apiPort = await findFreePort();
  apiBaseUrl = `http://127.0.0.1:${apiPort}`;
  await prepareDevBackendPort();
  createWindow();
  try {
    startBackend();
    await waitForBackend();
    backendReady = true;
  } catch (error) {
    backendStartupError = error instanceof Error ? error.message : String(error);
  }
});

ipcMain.handle("backend:status", async () => {
  if (backendReady) {
    return { ok: true, apiBaseUrl, error: null };
  }

  const ok = await hasCurrentBackendRoutes().catch(() => false);
  if (ok) {
    backendReady = true;
    backendStartupError = null;
  }

  return {
    ok,
    apiBaseUrl,
    error: ok ? null : backendStartupError
  };
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
