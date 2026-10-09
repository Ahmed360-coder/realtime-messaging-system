// Live metrics (Step 6): counts what the server does, per protocol, for the /stats dashboard
// and for the Step 9 WebSocket-vs-MQTT benchmarks.
//
// It is an *observer*: the hub and the ChatProto state machine only announce what happened
// (hub.emit('message', ...)), and this file alone decides what to count. Remove it and the chat
// works exactly the same. It has three sources:
//
//   1. hub events        message / delivered / acked / synced / failure  -> counters, rate, latency
//   2. TCP sockets       trackSocket(socket, protocol) from server/index.js -> connections, bytes
//   3. live state        hub.users, store.counts() read at snapshot time  -> online users, DB totals
//
// Kinds of metric: COUNTER = only goes up (messages sent); GAUGE = current value, up or down
// (connections open), never stored, read from the live state; RATE = counter growth per second.

const { performance } = require('node:perf_hooks');

const PROTOCOLS = ['ws', 'mqtt'];
const WINDOW_S = 60;     // the dashboard chart shows the last 60 seconds
const RATE_S = 10;       // "messages per second" = average over the last 10 whole seconds
const SAMPLES = 1000;    // latency percentiles over the last 1000 messages

// x mod n that is never negative (JavaScript's % keeps the sign of x).
const mod = (x, n) => ((x % n) + n) % n;

/**
 * Sliding window of per-second counts: a ring buffer with one bucket per second.
 * The count of second s lives in bucket s mod (size + 1); when time moves on, the buckets of the
 * seconds that passed are reset to 0 and reused. Memory never grows.
 * size + 1 buckets: `size` whole seconds for history() plus the current, unfinished one.
 */
class RateWindow {
  constructor(size = WINDOW_S) {
    this.size = size;
    this.counts = new Array(size + 1).fill(0);
    this.second = null; // the newest second the window has reached
  }

  // Move the window forward to `second`: clear the buckets of every second we skipped.
  advance(second) {
    if (this.second === null) this.second = second;
    if (second <= this.second) return;
    const skipped = Math.min(second - this.second, this.counts.length);
    for (let i = 1; i <= skipped; i++) this.counts[mod(this.second + i, this.counts.length)] = 0;
    this.second = second;
  }

  add(second, n = 1) {
    this.advance(second);
    this.counts[mod(second, this.counts.length)] += n;
  }

  /** Counts of the `size` whole seconds before `second`, oldest first (the current, unfinished second is left out). */
  history(second) {
    this.advance(second);
    const out = [];
    for (let i = this.size; i >= 1; i--) out.push(this.counts[mod(second - i, this.counts.length)]);
    return out;
  }

  /** Average per second over the last `seconds` whole seconds. */
  rate(second, seconds = RATE_S) {
    const recent = this.history(second).slice(-seconds);
    return recent.reduce((a, b) => a + b, 0) / seconds;
  }
}

/** The last `size` latency samples (ring buffer) and their summary: avg, p50, p95, max. */
class Samples {
  constructor(size = SAMPLES) {
    this.size = size;
    this.values = [];
    this.next = 0;   // where the next sample is written once the buffer is full
    this.count = 0;  // samples ever recorded (a counter)
  }

  add(v) {
    if (this.values.length < this.size) this.values.push(v);
    else this.values[this.next] = v;
    this.next = (this.next + 1) % this.size;
    this.count++;
  }

  summary() {
    const n = this.values.length;
    if (n === 0) return { count: this.count, avg: null, p50: null, p95: null, max: null };
    const sorted = [...this.values].sort((a, b) => a - b);
    // Nearest-rank percentile: the smallest sample with at least p % of the samples <= it.
    const pct = p => sorted[Math.ceil((p / 100) * n) - 1];
    const ms = x => Math.round(x * 100) / 100; // 0.01 ms is plenty
    return {
      count: this.count,
      avg: ms(sorted.reduce((a, b) => a + b, 0) / n),
      p50: ms(pct(50)),
      p95: ms(pct(95)),
      max: ms(sorted[n - 1]),
    };
  }
}

function emptyProtocolStats() {
  return {
    connectionsTotal: 0,     // TCP connections ever upgraded on this path (counter)
    closedBytesIn: 0,        // bytes of connections that already closed
    closedBytesOut: 0,
    messages: { sent: 0, delivered: 0, stored: 0, duplicate: 0, received: 0, synced: 0 },
    errors: {},              // ChatProto error code -> count
    rate: new RateWindow(),  // new messages sent per second
    ackLatency: new Samples(),
    deliverLatency: new Samples(),
  };
}

class Metrics {
  /**
   * hub: the Hub (an EventEmitter); store: for the database totals (optional).
   * now: a MONOTONIC clock in ms. performance.now() never jumps (unlike Date.now(), which moves
   * when Windows corrects its clock), and the tests pass a fake one.
   */
  constructor({ hub, store = null, now = () => performance.now() }) {
    this.hub = hub;
    this.store = store;
    this.now = now;
    this.startedAt = now();
    this.sockets = new Map(); // open TCP socket -> 'ws' | 'mqtt'
    this.p = Object.fromEntries(PROTOCOLS.map(name => [name, emptyProtocolStats()]));

    // Every hub event carries the protocol of the session it is about, and for a chat message
    // `receivedAt` = the monotonic time its frame reached the server.
    hub.on('message', e => this.onMessage(e));
    hub.on('acked', e => this.p[e.protocol].ackLatency.add(this.now() - e.receivedAt));
    hub.on('delivered', e => {
      this.p[e.protocol].messages.received++;
      this.p[e.protocol].deliverLatency.add(this.now() - e.receivedAt);
    });
    hub.on('synced', e => { this.p[e.protocol].messages.synced += e.count; });
    hub.on('failure', e => {
      const errors = this.p[e.protocol].errors;
      errors[e.code] = (errors[e.code] || 0) + 1;
    });
  }

  second() {
    return Math.floor(this.now() / 1000);
  }

  // A chat message from a sender of protocol e.protocol: status delivered | stored | duplicate.
  onMessage({ protocol, status }) {
    const s = this.p[protocol];
    s.messages[status]++;
    if (status === 'duplicate') return; // a retry is not a new message
    s.messages.sent++;
    s.rate.add(this.second());
  }

  /**
   * Count a TCP connection that was upgraded to `protocol`. Node's net.Socket already counts
   * every byte it read and wrote (bytesRead / bytesWritten): HTTP Upgrade, WebSocket headers,
   * MQTT headers and topics, PUBACKs, pings and our JSON. That is what really crossed the network.
   */
  trackSocket(socket, protocol) {
    const s = this.p[protocol];
    s.connectionsTotal++;
    this.sockets.set(socket, protocol);
    socket.once('close', () => {
      this.sockets.delete(socket);
      s.closedBytesIn += socket.bytesRead;
      s.closedBytesOut += socket.bytesWritten;
    });
  }

  /** Everything the dashboard shows, as plain JSON. Gauges are read from the live state now. */
  snapshot() {
    const second = this.second();
    const online = { ws: 0, mqtt: 0 };
    for (const session of this.hub.users.values()) online[session.protocol]++;

    const protocols = {};
    for (const name of PROTOCOLS) {
      const s = this.p[name];
      let open = 0, bytesIn = s.closedBytesIn, bytesOut = s.closedBytesOut;
      for (const [socket, protocol] of this.sockets) {
        if (protocol !== name) continue;
        open++;
        bytesIn += socket.bytesRead;
        bytesOut += socket.bytesWritten;
      }
      protocols[name] = {
        connections: { open, total: s.connectionsTotal },
        online: online[name],
        rate: Math.round(s.rate.rate(second) * 10) / 10,
        history: s.rate.history(second),
        messages: { ...s.messages },
        errors: { ...s.errors },
        errorsTotal: Object.values(s.errors).reduce((a, b) => a + b, 0),
        bytes: { in: bytesIn, out: bytesOut },
        latency: { ack: s.ackLatency.summary(), deliver: s.deliverLatency.summary() },
      };
    }

    const sum = f => PROTOCOLS.reduce((acc, name) => acc + f(protocols[name]), 0);
    return {
      time: Date.now(), // wall clock, only for showing "updated at" on the page
      uptimeS: Math.round((this.now() - this.startedAt) / 1000),
      windowS: WINDOW_S,
      rateS: RATE_S,
      totals: {
        connections: sum(p => p.connections.open),
        online: sum(p => p.online),
        rate: Math.round(sum(p => p.rate) * 10) / 10,
        sent: sum(p => p.messages.sent),
        errors: sum(p => p.errorsTotal),
      },
      database: this.store ? this.store.counts() : null,
      protocols,
    };
  }
}

module.exports = { Metrics, RateWindow, Samples, PROTOCOLS };
