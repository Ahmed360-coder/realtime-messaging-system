// Entry point of the messaging server.
// Step 1: serve the client web page over HTTP so phones on the same Wi-Fi can reach it.
// Step 2: ChatProto v1 over WebSocket on the same port (path /ws).

const http = require('http');
const path = require('path');
const express = require('express');
const qrcode = require('qrcode-terminal');
const { getLocalIP } = require('./network');
const { Hub } = require('./core/hub');
const { attachWebSocket } = require('./transports/websocket');

const PORT = process.env.PORT || 3000;
// 0.0.0.0 = listen on every network interface, not only localhost,
// otherwise phones on the Wi-Fi could not connect.
const HOST = '0.0.0.0';

/**
 * Build and start the server. Returns a Promise of { port, hub, close() }.
 * port 0 lets the OS pick a free port (used by the tests).
 */
function startServer({ port = PORT, host = HOST, log = console.log } = {}) {
  const app = express();

  // Serve everything in /client (index.html, css, js) as static files.
  app.use(express.static(path.join(__dirname, '..', 'client')));
  // The browser loads the same ChatProto codec the server uses.
  app.use('/protocols', express.static(path.join(__dirname, '..', 'protocols')));

  const hub = new Hub({ log });

  // Simple health check: lets a phone (or a script) confirm the server is alive.
  app.get('/api/health', (req, res) => {
    res.json({
      status: 'ok',
      serverTime: Date.now(),
      yourIP: req.socket.remoteAddress, // the phone's IP as seen by the server
      online: hub.onlineUsers().length,
    });
  });

  // One HTTP server carries both protocols: normal HTTP requests go to Express,
  // requests with "Upgrade: websocket" on /ws are taken over by the ws library.
  const httpServer = http.createServer(app);
  const wss = attachWebSocket(httpServer, hub, { path: '/ws', log });

  return new Promise((resolve, reject) => {
    httpServer.once('error', reject);
    httpServer.listen(port, host, () => {
      resolve({
        port: httpServer.address().port,
        hub,
        close: () => new Promise(done => {
          for (const ws of wss.clients) ws.terminate();
          wss.close();
          httpServer.close(done);
        }),
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
    console.log('Scan to open on a phone:');
    qrcode.generate(url, { small: true });
  });
}

module.exports = { startServer };
