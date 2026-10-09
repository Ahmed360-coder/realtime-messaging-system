// Entry point of the messaging server.
// Step 1: serve the client web page over HTTP so phones on the same Wi-Fi can reach it.
// Step 2: ChatProto v1 over WebSocket on the same port (path /ws).
// Step 3: reliability - messages and users persisted in SQLite (chat.db).
// Step 5: MQTT over WebSocket (path /mqtt), a second protocol on the same hub.
// Step 6: live metrics (server/core/metrics.js) and the /stats dashboard (server/stats-routes.js).
// Step 8: end-to-end encryption happens in the browser; the server only serves TweetNaCl.

const http = require('http');
const path = require('path');
const express = require('express');
const qrcode = require('qrcode-terminal');
const { getLocalIP } = require('./network');
const { Hub } = require('./core/hub');
const { Store } = require('./core/store');
const { Metrics } = require('./core/metrics');
const { mountStatsRoutes } = require('./stats-routes');
const { createWebSocketEndpoint } = require('./transports/websocket');
const { createMqttEndpoint } = require('./transports/mqtt');
const MqttBinding = require('../protocols/mqtt-binding');

const PORT = process.env.PORT || 3000;
// 0.0.0.0 = listen on every network interface, not only localhost,
// otherwise phones on the Wi-Fi could not connect.
const HOST = '0.0.0.0';
// SQLite database file (git-ignored). Tests pass ':memory:' or a temp file instead.
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'chat.db');

/**
 * Build and start the server. Returns a Promise of { port, hub, store, metrics, close() }.
 * port 0 lets the OS pick a free port (used by the tests).
 */
async function startServer({ port = PORT, host = HOST, dbPath = DB_PATH, log = console.log } = {}) {
  const app = express();

  // Serve everything in /client (index.html, css, js) as static files.
  app.use(express.static(path.join(__dirname, '..', 'client')));
  // The browser loads the same protocol files the server uses (ChatProto codec, MQTT topics).
  app.use('/protocols', express.static(path.join(__dirname, '..', 'protocols')));
  // The MQTT client library for the browser, served by us: phones on the LAN need no internet.
  // (The package's "exports" map turns "mqtt/dist/mqtt.min" into dist/mqtt.min.js.)
  const mqttBundle = require.resolve('mqtt/dist/mqtt.min');
  // root: only the file name is checked, so the path may contain folders like ".claude".
  app.get('/vendor/mqtt.min.js', (req, res) => res.sendFile(path.basename(mqttBundle), { root: path.dirname(mqttBundle) }));
  // Chart.js for the dashboard chart, served the same way. ("chart.js" resolves to dist/chart.cjs;
  // the browser build chart.umd.min.js sits next to it.)
  const chartDir = path.dirname(require.resolve('chart.js'));
  app.get('/vendor/chart.umd.min.js', (req, res) => res.sendFile('chart.umd.min.js', { root: chartDir }));
  // Step 8: TweetNaCl (end-to-end encryption in the browser), the same way. nacl-fast is the
  // same API as nacl.js with unrolled loops: same results, several times faster on a phone.
  const naclDir = path.dirname(require.resolve('tweetnacl'));
  app.get('/vendor/nacl.min.js', (req, res) => res.sendFile('nacl-fast.min.js', { root: naclDir }));

  const store = new Store(dbPath);
  const hub = new Hub({ store, log });
  // Listens to the hub's events; the hub works the same without it.
  const metrics = new Metrics({ hub, store });
  const stats = mountStatsRoutes(app, metrics);

  // Simple health check: lets a phone (or a script) confirm the server is alive.
  app.get('/api/health', (req, res) => {
    res.json({
      status: 'ok',
      serverTime: Date.now(),
      yourIP: req.socket.remoteAddress, // the phone's IP as seen by the server
      online: hub.onlineUsers().length,
    });
  });

  let mqtt;
  try {
    mqtt = await createMqttEndpoint(hub, { log });
  } catch (err) {
    stats.close();
    store.close();
    throw err;
  }
  const ws = createWebSocketEndpoint(hub, { log });

  // One HTTP server carries every protocol: normal HTTP requests go to Express; a request with
  // "Upgrade: websocket" is handed to the endpoint for its path. (Two ws servers attached to
  // one HTTP server would each refuse the other's path, so we route here.)
  const endpoints = { '/ws': ws, [MqttBinding.PATH]: mqtt.wss };
  const protocolOf = { '/ws': 'ws', [MqttBinding.PATH]: 'mqtt' };
  const httpServer = http.createServer(app);
  httpServer.on('upgrade', (req, socket, head) => {
    const pathname = new URL(req.url, 'http://localhost').pathname;
    const wss = endpoints[pathname];
    if (!wss) {
      socket.end('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
      return;
    }
    // Count this TCP connection and its bytes (both directions) under its protocol.
    metrics.trackSocket(socket, protocolOf[pathname]);
    wss.handleUpgrade(req, socket, head, conn => wss.emit('connection', conn, req));
  });

  return new Promise((resolve, reject) => {
    httpServer.once('error', async (err) => { stats.close(); await mqtt.close(); store.close(); reject(err); });
    httpServer.listen(port, host, () => {
      resolve({
        port: httpServer.address().port,
        hub,
        store,
        metrics,
        close: async () => {
          stats.close(); // end the dashboards' SSE streams, or httpServer.close() would wait forever
          // Wait for every connection's close handler (which calls hub.leave -> store)
          // before closing the database underneath it.
          const closed = [...ws.clients].map(c => new Promise(r => c.once('close', r)));
          for (const c of ws.clients) c.terminate();
          await Promise.all(closed);
          ws.close();
          await mqtt.close();
          await new Promise(done => httpServer.close(done));
          store.close();
        },
      });
    });
  });
}

// Only start listening when run directly (npm start), not when required by tests.
if (require.main === module) {
  startServer().then(({ port }) => {
    const url = `http://${getLocalIP()}:${port}`;
    console.log('Messaging server running');
    console.log(`  On this laptop: http://localhost:${port}`);
    console.log(`  On phones:      ${url}  (same Wi-Fi)`);
    console.log(`  WebSocket:      ws://${getLocalIP()}:${port}/ws  (ChatProto v1)`);
    console.log(`  MQTT:           ws://${getLocalIP()}:${port}${MqttBinding.PATH}  (MQTT 3.1.1 over WebSocket)`);
    console.log(`  Live metrics:   ${url}/stats`);
    console.log(`  Database:       ${DB_PATH}`);
    console.log('Scan to open on a phone:');
    qrcode.generate(url, { small: true });
  });
}

module.exports = { startServer };
