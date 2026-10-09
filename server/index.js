// Entry point of the messaging server.
// Step 1: serve the client web page over HTTP so phones on the same Wi-Fi can reach it.
// Step 2: ChatProto v1 over WebSocket on the same port (path /ws).
// Step 3: reliability - messages and users persisted in SQLite (chat.db).
// Step 5: MQTT over WebSocket (path /mqtt), a second protocol on the same hub.

const http = require('http');
const path = require('path');
const express = require('express');
const qrcode = require('qrcode-terminal');
const { getLocalIP } = require('./network');
const { Hub } = require('./core/hub');
const { Store } = require('./core/store');
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
 * Build and start the server. Returns a Promise of { port, hub, store, close() }.
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

  const store = new Store(dbPath);
  const hub = new Hub({ store, log });

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
    store.close();
    throw err;
  }
  const ws = createWebSocketEndpoint(hub, { log });

  // One HTTP server carries every protocol: normal HTTP requests go to Express; a request with
  // "Upgrade: websocket" is handed to the endpoint for its path. (Two ws servers attached to
  // one HTTP server would each refuse the other's path, so we route here.)
  const endpoints = { '/ws': ws, [MqttBinding.PATH]: mqtt.wss };
  const httpServer = http.createServer(app);
  httpServer.on('upgrade', (req, socket, head) => {
    const wss = endpoints[new URL(req.url, 'http://localhost').pathname];
    if (!wss) {
      socket.end('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
      return;
    }
    wss.handleUpgrade(req, socket, head, conn => wss.emit('connection', conn, req));
  });

  return new Promise((resolve, reject) => {
    httpServer.once('error', async (err) => { await mqtt.close(); store.close(); reject(err); });
    httpServer.listen(port, host, () => {
      resolve({
        port: httpServer.address().port,
        hub,
        store,
        close: async () => {
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
    console.log(`  Database:       ${DB_PATH}`);
    console.log('Scan to open on a phone:');
    qrcode.generate(url, { small: true });
  });
}

module.exports = { startServer };
