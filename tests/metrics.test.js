// Step 6: live metrics. Unit tests of the sliding window, the latency samples and the Metrics
// observer (with a fake hub and a fake clock), then integration tests against the real server:
// traffic over both protocols must show up in /api/stats and in the SSE stream.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const ChatProto = require('../protocols/chatproto');
const MqttBinding = require('../protocols/mqtt-binding');
const { Metrics, RateWindow, Samples } = require('../server/core/metrics');
const { startServer } = require('../server');
const { joinAs, joinMqttAs, until } = require('./helpers');
const { TYPES } = ChatProto;

// ---------- Unit tests ----------

test('RateWindow: per-second counts, oldest first, current second left out', () => {
  const w = new RateWindow(5);
  w.add(100); w.add(100); w.add(101); w.add(103, 3);
  // At second 104 the whole seconds 99..103 are in the window.
  assert.deepEqual(w.history(104), [0, 2, 1, 0, 3]);
  assert.equal(w.rate(104, 5), 6 / 5);
  assert.equal(w.rate(104, 2), 3 / 2);
});

test('RateWindow: buckets are reused, old seconds drop out', () => {
  const w = new RateWindow(5);
  w.add(100, 7);
  w.add(102);
  assert.deepEqual(w.history(105), [7, 0, 1, 0, 0]); // seconds 100..104
  assert.deepEqual(w.history(106), [0, 1, 0, 0, 0]); // 100 has left the window
  w.add(160, 2);                                      // a long pause: every bucket is reset
  assert.deepEqual(w.history(161), [0, 0, 0, 0, 2]);
});

test('RateWindow: works for seconds near 0 (no negative bucket index)', () => {
  const w = new RateWindow(5);
  w.add(0); w.add(1);
  assert.deepEqual(w.history(2), [0, 0, 0, 1, 1]);
});

test('Samples: nearest-rank percentiles, avg and max', () => {
  const s = new Samples(100);
  for (let v = 1; v <= 100; v++) s.add(v);
  assert.deepEqual(s.summary(), { count: 100, avg: 50.5, p50: 50, p95: 95, max: 100 });
  assert.deepEqual(new Samples().summary(), { count: 0, avg: null, p50: null, p95: null, max: null });
});

test('Samples: keeps only the last `size` values, but counts all of them', () => {
  const s = new Samples(3);
  for (const v of [100, 100, 1, 2, 3]) s.add(v);
  const sum = s.summary();
  assert.equal(sum.count, 5);
  assert.equal(sum.max, 3); // the two 100s were overwritten
});

// A fake hub (just an EventEmitter with the online-users map) and a clock we move by hand.
function fixture() {
  const hub = new EventEmitter();
  hub.users = new Map();
  let t = 50_000;
  const clock = { now: () => t, advance: msMore => { t += msMore; } };
  const metrics = new Metrics({ hub, now: clock.now });
  return { hub, clock, metrics };
}

test('Metrics: hub events become per-protocol counters, rate and errors', () => {
  const { hub, clock, metrics } = fixture();
  hub.emit('message', { protocol: 'ws', status: 'delivered', receivedAt: clock.now() });
  hub.emit('message', { protocol: 'ws', status: 'stored', receivedAt: clock.now() });
  hub.emit('message', { protocol: 'ws', status: 'duplicate', receivedAt: clock.now() });
  hub.emit('message', { protocol: 'mqtt', status: 'delivered', receivedAt: clock.now() });
  hub.emit('synced', { protocol: 'mqtt', count: 4 });
  hub.emit('failure', { protocol: 'ws', code: 'BAD_JSON' });
  hub.emit('failure', { protocol: 'ws', code: 'BAD_JSON' });
  hub.emit('failure', { protocol: 'mqtt', code: 'UNKNOWN_USER' });
  clock.advance(1000); // the second with the messages is now complete

  const snap = metrics.snapshot();
  const { ws, mqtt } = snap.protocols;
  assert.deepEqual(ws.messages, { sent: 2, delivered: 1, stored: 1, duplicate: 1, received: 0, synced: 0 });
  assert.equal(mqtt.messages.sent, 1);
  assert.equal(mqtt.messages.synced, 4);
  assert.deepEqual(ws.errors, { BAD_JSON: 2 });
  assert.equal(ws.errorsTotal, 2);
  assert.deepEqual(mqtt.errors, { UNKNOWN_USER: 1 });
  // A duplicate is a retry, not a new message: 2 ws messages in the last second, not 3.
  assert.equal(ws.history.at(-1), 2);
  assert.equal(ws.rate, 0.2); // 2 messages / 10 s
  assert.equal(snap.totals.sent, 3);
  assert.equal(snap.totals.errors, 3);
  assert.equal(snap.windowS, 60);
  assert.equal(ws.history.length, 60);
});

test('Metrics: latency = clock when written minus receivedAt, per protocol', () => {
  const { hub, clock, metrics } = fixture();
  const receivedAt = clock.now();
  clock.advance(4);
  hub.emit('acked', { protocol: 'mqtt', receivedAt });       // ACK written 4 ms after arrival
  clock.advance(2);
  hub.emit('delivered', { protocol: 'ws', receivedAt });     // MSG written 6 ms after arrival
  const { ws, mqtt } = metrics.snapshot().protocols;
  assert.equal(mqtt.latency.ack.p50, 4);
  assert.equal(mqtt.latency.ack.count, 1);
  assert.equal(ws.latency.deliver.max, 6);
  assert.equal(ws.messages.received, 1);
  assert.equal(ws.latency.ack.count, 0);
  assert.equal(ws.latency.ack.p50, null);
});

test('Metrics: online users are read from the hub at snapshot time (a gauge)', () => {
  const { hub, metrics } = fixture();
  hub.users.set('alice', { protocol: 'ws' });
  hub.users.set('bob', { protocol: 'mqtt' });
  hub.users.set('carol', { protocol: 'mqtt' });
  let snap = metrics.snapshot();
  assert.equal(snap.protocols.ws.online, 1);
  assert.equal(snap.protocols.mqtt.online, 2);
  assert.equal(snap.totals.online, 3);
  hub.users.delete('bob');
  snap = metrics.snapshot();
  assert.equal(snap.protocols.mqtt.online, 1);
});

test('Metrics: sockets count as open connections; their bytes are kept after close', () => {
  const { metrics } = fixture();
  const sock = () => Object.assign(new EventEmitter(), { bytesRead: 0, bytesWritten: 0 });
  const a = sock(), b = sock();
  metrics.trackSocket(a, 'ws');
  metrics.trackSocket(b, 'mqtt');
  a.bytesRead = 300; a.bytesWritten = 200;
  b.bytesRead = 50;
  let { ws, mqtt } = metrics.snapshot().protocols;
  assert.deepEqual(ws.connections, { open: 1, total: 1 });
  assert.deepEqual(ws.bytes, { in: 300, out: 200 });
  assert.deepEqual(mqtt.bytes, { in: 50, out: 0 });

  a.emit('close');
  ({ ws } = metrics.snapshot().protocols);
  assert.deepEqual(ws.connections, { open: 0, total: 1 });
  assert.deepEqual(ws.bytes, { in: 300, out: 200 }); // a closed connection still counts in the totals
});

// ---------- Integration tests (real server, real clients) ----------

let server, base, wsUrl, mqttUrl;

test.before(async () => {
  server = await startServer({ port: 0, host: '127.0.0.1', dbPath: ':memory:', log: () => {} });
  base = `http://127.0.0.1:${server.port}`;
  wsUrl = `ws://127.0.0.1:${server.port}/ws`;
  mqttUrl = `ws://127.0.0.1:${server.port}${MqttBinding.PATH}`;
});
test.after(() => server.close());

const stats = async () => (await fetch(`${base}/api/stats`)).json();

test('/api/stats counts traffic of both protocols', async () => {
  const before = await stats();
  const a = await joinAs(wsUrl, 'm1_ws');
  const b = await joinMqttAs(mqttUrl, 'm1_mqtt');

  let snap = await stats();
  assert.equal(snap.protocols.ws.online - before.protocols.ws.online, 1);
  assert.equal(snap.protocols.mqtt.online - before.protocols.mqtt.online, 1);
  assert.ok(snap.protocols.ws.connections.open >= 1);
  assert.ok(snap.protocols.mqtt.connections.open >= 1);

  // ws -> mqtt and mqtt -> ws, then a retry (same id) and a bad frame on ws.
  const m1 = ChatProto.make(TYPES.MSG, { to: 'm1_mqtt', body: 'hello over ws' });
  a.send(m1);
  await a.next(TYPES.ACK);
  await b.next(TYPES.MSG);
  b.send(ChatProto.make(TYPES.MSG, { to: 'm1_ws', body: 'hello over mqtt' }));
  await b.next(TYPES.ACK);
  await a.next(TYPES.MSG);
  a.send(m1);
  assert.equal((await a.next(TYPES.ACK)).body.status, 'duplicate');
  a.send('{not json');
  await a.next(TYPES.ERROR);

  // The 'acked' / 'delivered' callbacks run after the bytes are written: wait for them.
  await until(() => server.metrics.snapshot().protocols.ws.latency.deliver.count > before.protocols.ws.latency.deliver.count);
  snap = await stats();
  const d = name => {
    const now = snap.protocols[name], was = before.protocols[name];
    return {
      sent: now.messages.sent - was.messages.sent,
      delivered: now.messages.delivered - was.messages.delivered,
      duplicate: now.messages.duplicate - was.messages.duplicate,
      received: now.messages.received - was.messages.received,
      badJson: (now.errors.BAD_JSON || 0) - (was.errors.BAD_JSON || 0),
      acks: now.latency.ack.count - was.latency.ack.count,
    };
  };
  assert.deepEqual(d('ws'), { sent: 1, delivered: 1, duplicate: 1, received: 1, badJson: 1, acks: 2 });
  assert.deepEqual(d('mqtt'), { sent: 1, delivered: 1, duplicate: 0, received: 1, badJson: 0, acks: 1 });

  for (const name of ['ws', 'mqtt']) {
    const p = snap.protocols[name];
    assert.ok(p.bytes.in > 0 && p.bytes.out > 0, `${name} bytes counted`);
    assert.ok(p.latency.ack.p50 >= 0 && p.latency.ack.max < 1000, `${name} ack latency is plausible`);
  }
  assert.ok(snap.database.messages >= 2);
  assert.ok(snap.database.users >= 2);

  await a.close();
  await b.close();
  await until(() => server.metrics.snapshot().protocols.ws.online === before.protocols.ws.online);
});

test('/api/stats/stream pushes snapshots as Server-Sent Events', async () => {
  const ac = new AbortController();
  const res = await fetch(`${base}/api/stats/stream`, { signal: ac.signal });
  assert.equal(res.status, 200);
  assert.match(res.headers.get('content-type'), /^text\/event-stream/);

  // Read until two "data:" events arrived (the first is sent at once, the next after ~1 s).
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let text = '';
  const events = () => text.split('\n\n').filter(block => block.startsWith('data: '));
  while (events().length < 2) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  ac.abort();

  assert.match(text, /^retry: \d+\n\n/);
  const snap = JSON.parse(events()[0].slice('data: '.length));
  assert.ok(snap.protocols.ws && snap.protocols.mqtt);
  assert.equal(snap.protocols.ws.history.length, 60);
});

test('the dashboard page and its scripts are served', async () => {
  const page = await fetch(`${base}/stats`);
  assert.equal(page.status, 200);
  const html = await page.text();
  const refs = [...html.matchAll(/(?:src|href)="(\/[^"]*)"/g)].map(m => m[1]);
  assert.deepEqual(refs, ['/style.css', '/stats.css', '/', '/vendor/chart.umd.min.js', '/stats.js']);
  for (const ref of refs) assert.equal((await fetch(base + ref)).status, 200, `${ref} should be served`);
});

test('server.close() does not hang while a dashboard is connected', async () => {
  const s = await startServer({ port: 0, host: '127.0.0.1', dbPath: ':memory:', log: () => {} });
  const res = await fetch(`http://127.0.0.1:${s.port}/api/stats/stream`);
  const reader = res.body.getReader();
  await reader.read(); // the stream is open
  await s.close();     // must end the SSE response itself
  for (let r = await reader.read(); !r.done; r = await reader.read()) { /* drain */ }
});
