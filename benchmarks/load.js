// Background load for the scalability experiment (benchmarks/experiments.js -> scalability).
// Started by the benchmark with fork(): a SEPARATE process, so the work of hundreds of clients
// does not run on the event loop that times the probe's round trips.
//
// Protocol with the parent (Node IPC messages):
//   parent -> { wsUrl, protocol, clients, rate }   connect `clients` users, in pairs (0<->1, 2<->3 ...)
//   load   -> { ready: true }                      all joined; each now sends `rate` msg/s to its partner
//   parent -> { stop: true }                       stop sending, disconnect
//   load   -> { sent, acked, errors, seconds,      then exit
//                achievedMsgPerS, lagP99Ms }
//
// Is the LOAD GENERATOR keeping up (so that a slow result is the server's fault, not ours)?
//   achievedMsgPerS  messages really sent per second; should be clients x rate
//   lagP99Ms         this process's event-loop delay, p99. On Windows a timer fires only every
//                    ~15.6 ms, so ~16-25 ms here is the OS timer, not overload.

const { monitorEventLoopDelay, performance } = require('node:perf_hooks');
const ChatProto = require('../protocols/chatproto');
const { connect } = require('./lib/client');

const TEXT = 'background load message, 64 characters long, for the benchmark.';

process.once('message', async ({ wsUrl, protocol, clients, rate }) => {
  const counts = { sent: 0, acked: 0, errors: 0 };
  const users = [];
  const timers = [];
  try {
    for (let i = 0; i < clients; i++) {
      // Connect one by one: K joins at once would measure a join storm, not steady chatting.
      users.push(await connect(protocol, wsUrl, { username: `ld_${protocol}_${i}` }));
    }
  } catch (err) {
    process.send({ error: err.message });
    process.exit(1);
  }
  for (const u of users) {
    u.on(m => {
      if (m.type === 'ACK') counts.acked++;
      if (m.type === 'ERROR') counts.errors++;
    });
  }

  const lag = monitorEventLoopDelay({ resolution: 10 });
  lag.enable();
  const period = 1000 / rate;
  users.forEach((u, i) => {
    const partner = users[i ^ 1] || users[0]; // i ^ 1 pairs 0<->1, 2<->3 ...
    const tick = () => { u.send(ChatProto.make('MSG', { to: partner.username, body: TEXT })); counts.sent++; };
    // A random start offset spreads the clients over the period, so they do not all send in the
    // same millisecond (that would be a burst test, not a steady load).
    timers.push(setTimeout(() => { tick(); timers.push(setInterval(tick, period)); }, Math.random() * period));
  });
  const started = performance.now();
  process.send({ ready: true });

  process.once('message', async () => {
    for (const t of timers) { clearTimeout(t); clearInterval(t); }
    lag.disable();
    const seconds = (performance.now() - started) / 1000; // the sending time only, not the closing
    for (const u of users) await u.close();
    const achievedMsgPerS = Math.round((counts.sent / seconds) * 10) / 10;
    process.send({ ...counts, seconds: Math.round(seconds * 10) / 10, achievedMsgPerS, lagP99Ms: Math.round(lag.percentile(99) / 1e4) / 100 }, () => process.exit(0));
  });
});
