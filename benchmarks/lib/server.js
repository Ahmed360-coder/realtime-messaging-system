// The server under test, for the benchmarks.
//
// By default we START our own server as a separate child process (`node server/index.js`), on its
// own port and a fresh temporary database, exactly like `PORT=3109 DB_PATH=... npm start`:
//   - separate PROCESS: the server's event loop is not shared with the benchmark clients, so the
//     clients' work (JSON parsing, timestamps) is not added to the server's time;
//   - fresh DATABASE (deleted first): every benchmark run starts from the same empty state, and the
//     real chat.db is never touched.
// With --url the benchmarks use a server that is already running instead (e.g. the laptop, measured
// from another computer over Wi-Fi); then nothing is started or deleted.

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..');
const sleep = ms => new Promise(r => setTimeout(r, ms));

/** GET /api/stats once: the Step 6 metrics snapshot (bytes, counters, server-side latency). */
async function getStats(httpUrl) {
  const res = await fetch(`${httpUrl}/api/stats`);
  return res.json();
}

// Poll /api/health until the server answers (it needs a moment to open SQLite and listen).
async function waitUntilUp(httpUrl, ms = 10_000) {
  const end = Date.now() + ms;
  for (;;) {
    try {
      const res = await fetch(`${httpUrl}/api/health`);
      if (res.ok) return;
    } catch (e) { /* not listening yet */ }
    if (Date.now() > end) throw new Error(`server at ${httpUrl} did not start`);
    await sleep(100);
  }
}

/**
 * Start (or attach to) the server. Returns { httpUrl, wsUrl, dbPath, pid, stop() }.
 * httpUrl = http://host:port, wsUrl = ws://host:port (the transports add /ws or /mqtt).
 */
async function startServer({ url = null, port = 3109, dbPath = path.join(os.tmpdir(), 'chat-bench.db') } = {}) {
  if (url) {
    const httpUrl = url.replace(/^ws/, 'http').replace(/\/$/, '');
    await waitUntilUp(httpUrl);
    return { httpUrl, wsUrl: httpUrl.replace(/^http/, 'ws'), dbPath: null, pid: null, stop: async () => {} };
  }
  for (const f of [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]) fs.rmSync(f, { force: true });
  const child = spawn(process.execPath, [path.join(ROOT, 'server', 'index.js')], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(port), DB_PATH: dbPath },
    // The server prints a line per join/leave: thrown away (writing to a pipe nobody reads would
    // eventually block it). Errors still reach our terminal.
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  const exited = new Promise(r => child.once('exit', r));
  const httpUrl = `http://127.0.0.1:${port}`;
  await waitUntilUp(httpUrl);
  return {
    httpUrl,
    wsUrl: `ws://127.0.0.1:${port}`,
    dbPath,
    pid: child.pid,
    stop: async () => {
      child.kill();
      await exited;
    },
  };
}

module.exports = { startServer, getStats };
