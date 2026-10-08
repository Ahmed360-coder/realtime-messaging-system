// Entry point of the messaging server.
// Step 1: serve the client web page over HTTP so phones on the same Wi-Fi can reach it.

const path = require('path');
const express = require('express');
const qrcode = require('qrcode-terminal');
const { getLocalIP } = require('./network');

const PORT = process.env.PORT || 3000;
// 0.0.0.0 = listen on every network interface, not only localhost,
// otherwise phones on the Wi-Fi could not connect.
const HOST = '0.0.0.0';

const app = express();

// Serve everything in /client (index.html, css, js) as static files.
app.use(express.static(path.join(__dirname, '..', 'client')));

// Simple health check: lets a phone (or a script) confirm the server is alive.
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    serverTime: Date.now(),
    yourIP: req.socket.remoteAddress, // the phone's IP as seen by the server
  });
});

app.listen(PORT, HOST, () => {
  const url = `http://${getLocalIP()}:${PORT}`;
  console.log('Messaging server running');
  console.log(`  On this laptop: http://localhost:${PORT}`);
  console.log(`  On phones:      ${url}  (same Wi-Fi)`);
  console.log('Scan to open on a phone:');
  qrcode.generate(url, { small: true });
});
