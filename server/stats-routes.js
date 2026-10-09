// HTTP routes of the live metrics dashboard (Step 6).
//
//   GET /stats              the dashboard page (client/stats.html)
//   GET /api/stats          one snapshot as JSON (for curl and the Step 9 benchmark script)
//   GET /api/stats/stream   Server-Sent Events: a new snapshot pushed every second
//
// Why SSE: the dashboard only needs data from server to page. SSE is one ordinary HTTP response
// that never ends; each update is a text block "data: <json>\n\n". Browsers support it natively
// (EventSource) and reconnect by themselves. Polling would cost one full HTTP request per update;
// a WebSocket on /ws would make the dashboard count as a chat connection in its own numbers.
// We push on a fixed 1 s tick (not on every message), and compute one snapshot for all viewers.

const path = require('path');

const PUSH_MS = 1000;
const RETRY_MS = 2000; // tells EventSource how long to wait before reconnecting

function mountStatsRoutes(app, metrics) {
  const streams = new Set(); // open SSE responses

  app.get('/stats', (req, res) => res.sendFile('stats.html', { root: path.join(__dirname, '..', 'client') }));

  app.get('/api/stats', (req, res) => res.json(metrics.snapshot()));

  app.get('/api/stats/stream', (req, res) => {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',  // never cache a live stream
      Connection: 'keep-alive',
    });
    res.write(`retry: ${RETRY_MS}\n\n`);
    res.write(event(metrics.snapshot())); // the first values at once, not after 1 s
    streams.add(res);
    req.on('close', () => streams.delete(res)); // tab closed or network gone
  });

  const timer = setInterval(() => {
    if (streams.size === 0) return; // nobody watching: do not even compute it
    const data = event(metrics.snapshot());
    for (const res of streams) res.write(data);
  }, PUSH_MS);
  timer.unref(); // this timer alone must not keep the process alive

  return {
    // An SSE response never ends by itself, so the HTTP server could not close while one is open.
    close() {
      clearInterval(timer);
      for (const res of streams) res.end();
      streams.clear();
    },
  };
}

// One SSE event: "data: " + one line of JSON + an empty line.
const event = obj => `data: ${JSON.stringify(obj)}\n\n`;

module.exports = { mountStatsRoutes };
