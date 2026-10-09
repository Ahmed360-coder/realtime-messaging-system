// The Step 9 experiments: ChatProto over WebSocket vs ChatProto over MQTT over WebSocket.
//
// Each experiment is an async function (target, cfg) -> result object, where
//   target = { httpUrl, wsUrl }   the server under test (benchmarks/lib/server.js)
//   cfg    = that experiment's settings (benchmarks/run.js: FULL / QUICK)
// and the result is plain JSON: { config, summary, raw } (raw = the individual samples, for the CSV).
//
// Rules every experiment follows (the methodology in the README):
//   - ONE CLOCK. Every interval starts and ends in this process, on performance.now() (monotonic,
//     sub-millisecond). Phones' and laptops' wall clocks are never compared.
//   - WARM-UP. The first messages of every run are sent but not recorded (JIT, SQLite cache).
//   - INDEPENDENT RUNS. Each run opens fresh connections; the protocol order alternates from run
//     to run (ws, mqtt / mqtt, ws ...), so a slow drift of the laptop does not favour one protocol.
//   - ONE MESSAGE AT A TIME for latency (closed loop): a queue can never build up inside the
//     measurement, so we measure the path, not our own backlog.

const { performance } = require('node:perf_hooks');
const { fork } = require('node:child_process');
const path = require('node:path');
const ChatProto = require('../protocols/chatproto');
const E2E = require('../client/e2e');
const { connect } = require('./lib/client');
const { getStats } = require('./lib/server');
const { summarize, acrossRuns, cdf, round } = require('./lib/stats');

const PROTOCOLS = ['ws', 'mqtt'];
const now = () => performance.now();
// Run r measures the protocols in this order: alternate, so neither always goes first.
const orderFor = r => (r % 2 === 0 ? PROTOCOLS : [...PROTOCOLS].reverse());
// A typical short chat message (64 characters of text).
const TEXT = 'The quick brown fox jumps over the lazy dog, then naps by the TCP';
const textOf = n => TEXT.repeat(Math.ceil(n / TEXT.length)).slice(0, n);

const msgTo = (to, body) => ChatProto.make('MSG', { to, body });

// Bytes counted by the server's TCP sockets for one protocol (Step 6 metrics), from two snapshots.
function bytesDelta(before, after, protocol) {
  const a = before.protocols[protocol].bytes;
  const b = after.protocols[protocol].bytes;
  return { in: b.in - a.in, out: b.out - a.out };
}

/** B answers every MSG from A with the same body: A's round trip A -> server -> B -> server -> A. */
function startEcho(a, b) {
  return b.on(m => { if (m.type === 'MSG' && m.from === a.username) b.send(msgTo(a.username, m.body)); });
}

/**
 * One ping: A sends, and we time (on A's clock) the ACK (A -> server -> A, after the SQLite write)
 * and the echo (A -> B -> A: two messages, each stored and delivered). Returns { ackMs, rttMs }.
 */
async function ping(a, b, body) {
  const msg = msgTo(b.username, body);
  const ackP = a.next('ACK', m => m.body.ref === msg.id);
  const echoP = a.next('MSG', m => m.from === b.username && m.body === body);
  const t0 = now();
  a.send(msg);
  const [ackMs, rttMs] = await Promise.all([ackP.then(() => now() - t0), echoP.then(() => now() - t0)]);
  return { ackMs, rttMs };
}

// ---------------------------------------------------------------------------------------------
// 1. Latency: round-trip time of one message, one at a time.
// ---------------------------------------------------------------------------------------------
async function latency(target, { runs, warmup, samples }) {
  const raw = [];
  const perRun = { ws: [], mqtt: [] };
  for (let r = 0; r < runs; r++) {
    for (const protocol of orderFor(r)) {
      const a = await connect(protocol, target.wsUrl, { username: `lat_a_${protocol}` });
      const b = await connect(protocol, target.wsUrl, { username: `lat_b_${protocol}` });
      startEcho(a, b);
      const rtts = [];
      for (let i = 0; i < warmup + samples; i++) {
        const { ackMs, rttMs } = await ping(a, b, `${TEXT} ${r}.${i}`);
        if (i < warmup) continue; // warm-up: sent, not recorded
        rtts.push(rttMs);
        raw.push({ protocol, run: r, i: i - warmup, rtt_ms: round(rttMs), ack_ms: round(ackMs) });
      }
      perRun[protocol].push(summarize(rtts));
      await a.close();
      await b.close();
    }
  }
  // Server-side ACK latency (parse + SQLite write with fsync + routing) from the Step 6 metrics:
  // the last 1000 ACKs of each protocol, so mostly the last runs of this experiment.
  const stats = await getStats(target.httpUrl);
  const summary = {};
  for (const protocol of PROTOCOLS) {
    const mine = raw.filter(x => x.protocol === protocol);
    summary[protocol] = {
      rtt: summarize(mine.map(x => x.rtt_ms)),
      ack: summarize(mine.map(x => x.ack_ms)),
      runs: perRun[protocol],
      p50AcrossRuns: acrossRuns(perRun[protocol], 'p50'),
      p95AcrossRuns: acrossRuns(perRun[protocol], 'p95'),
      p99AcrossRuns: acrossRuns(perRun[protocol], 'p99'),
      meanAcrossRuns: acrossRuns(perRun[protocol], 'mean'),
      serverAck: stats.protocols[protocol].latency.ack,
      cdf: cdf(mine.map(x => x.rtt_ms)),
    };
  }
  return { config: { runs, warmup, samples, textChars: TEXT.length }, summary, raw };
}

// ---------------------------------------------------------------------------------------------
// 2. Throughput: A sends as fast as it may (at most `window` un-ACKed), B counts.
// ---------------------------------------------------------------------------------------------
async function blast(a, b, count, window) {
  let sent = 0, inflight = 0, acked = 0, received = 0;
  let resolveDone;
  const done = new Promise(r => { resolveDone = r; });
  let tLast = 0;
  const check = () => { if (received === count && acked === count) resolveDone(); };
  const offB = b.on(m => {
    if (m.type !== 'MSG' || m.from !== a.username) return;
    received++;
    if (received === count) tLast = now();
    check();
  });
  const pump = () => {
    while (sent < count && inflight < window) {
      a.send(msgTo(b.username, TEXT));
      sent++;
      inflight++;
    }
  };
  const offA = a.on(m => {
    if (m.type !== 'ACK') return;
    inflight--;
    acked++;
    pump();
    check();
  });
  const t0 = now();
  pump();
  await done;
  offA();
  offB();
  return tLast - t0;
}

async function throughput(target, { runs, warmup, count, window }) {
  const raw = [];
  for (let r = 0; r < runs; r++) {
    for (const protocol of orderFor(r)) {
      const a = await connect(protocol, target.wsUrl, { username: `tp_a_${protocol}` });
      const b = await connect(protocol, target.wsUrl, { username: `tp_b_${protocol}` });
      await blast(a, b, warmup, window); // warm-up
      const ms = await blast(a, b, count, window);
      raw.push({ protocol, run: r, messages: count, ms: round(ms), msg_per_s: round(count / (ms / 1000), 1) });
      await a.close();
      await b.close();
    }
  }
  const summary = {};
  for (const protocol of PROTOCOLS) summary[protocol] = summarize(raw.filter(x => x.protocol === protocol).map(x => x.msg_per_s));
  return { config: { runs, warmup, count, window, textChars: TEXT.length }, summary, raw };
}

// ---------------------------------------------------------------------------------------------
// 3. Bytes on the wire per message, from the server's TCP socket counters (Step 6 /api/stats).
// ---------------------------------------------------------------------------------------------
async function bytes(target, { sizes, count }) {
  const raw = [];
  for (const protocol of PROTOCOLS) {
    for (const size of sizes) {
      const a = await connect(protocol, target.wsUrl, { username: `by_a_${protocol}` });
      const b = await connect(protocol, target.wsUrl, { username: `by_b_${protocol}` });
      // The JSON alone (what an "ideal" transport would carry): A's MSG, the ACK, B's MSG.
      const json = { up: 0, ack: 0, down: 0 };
      a.on((m, text) => { if (m.type === 'ACK') json.ack = Buffer.byteLength(text); });
      b.on((m, text) => { if (m.type === 'MSG') json.down = Buffer.byteLength(text); });
      const before = await getStats(target.httpUrl);
      for (let i = 0; i < count; i++) {
        const msg = msgTo(b.username, textOf(size));
        const acked = a.next('ACK', m => m.body.ref === msg.id);
        const got = b.next('MSG', m => m.id === msg.id);
        json.up = a.send(msg);
        await Promise.all([acked, got]);
      }
      const after = await getStats(target.httpUrl);
      const d = bytesDelta(before, after, protocol);
      const jsonTotal = json.up + json.ack + json.down;
      const wireTotal = (d.in + d.out) / count;
      raw.push({
        protocol, text_chars: size, messages: count,
        wire_in_per_msg: round(d.in / count, 1),   // client -> server (A's MSG; MQTT also PUBACKs)
        wire_out_per_msg: round(d.out / count, 1), // server -> client (ACK to A, MSG to B; MQTT also PUBACK)
        wire_total_per_msg: round(wireTotal, 1),
        json_up: json.up, json_ack: json.ack, json_down: json.down, json_total: jsonTotal,
        overhead_per_msg: round(wireTotal - jsonTotal, 1),
      });
      await a.close();
      await b.close();
    }
  }
  return { config: { sizes, count }, summary: raw, raw };
}

// ---------------------------------------------------------------------------------------------
// 4. Connection setup: time per phase until SYNCED, and the bytes it costs.
// ---------------------------------------------------------------------------------------------
async function setup(target, { runs, warmup, connections }) {
  const raw = [];
  for (let r = 0; r < runs; r++) {
    for (const protocol of orderFor(r)) {
      for (let i = 0; i < warmup + connections; i++) {
        const before = await getStats(target.httpUrl);
        const c = await connect(protocol, target.wsUrl, { username: `setup_${protocol}` });
        const after = await getStats(target.httpUrl);
        await c.close();
        if (i < warmup) continue; // the first one also registers the user: not a typical join
        const d = bytesDelta(before, after, protocol);
        raw.push({ protocol, run: r, i: i - warmup, ...Object.fromEntries(Object.entries(c.phases).map(([k, v]) => [`${k}_ms`, round(v)])), bytes_in: d.in, bytes_out: d.out });
      }
    }
  }
  const summary = {};
  for (const protocol of PROTOCOLS) {
    const mine = raw.filter(x => x.protocol === protocol);
    const phases = Object.keys(mine[0]).filter(k => k.endsWith('_ms'));
    summary[protocol] = {
      phases: Object.fromEntries(phases.map(k => [k, summarize(mine.map(x => x[k]))])),
      bytesIn: summarize(mine.map(x => x.bytes_in)),
      bytesOut: summarize(mine.map(x => x.bytes_out)),
    };
  }
  return { config: { runs, warmup, connections }, summary, raw };
}

// ---------------------------------------------------------------------------------------------
// 5. Group size x end-to-end encryption: frame size, sender's sealing time, fan-out time, bytes.
// ---------------------------------------------------------------------------------------------
async function groups(target, { sizes, warmup, messages }) {
  const raw = [];
  const summary = [];
  for (const protocol of PROTOCOLS) {
    for (const size of sizes) {
      // size members, all online, all with a key pair (Step 8 HELLO.key). Member 0 sends.
      const ids = Array.from({ length: size }, () => E2E.newIdentity());
      const members = [];
      for (let i = 0; i < size; i++) {
        members.push(await connect(protocol, target.wsUrl, { username: `g${size}${protocol}_${i}`, key: ids[i].publicKey }));
      }
      const [sender, ...others] = members;
      const group = `#g${size}_${protocol}`;
      const create = ChatProto.make('GROUP', { to: group, body: { op: 'create', users: others.map(m => m.username) } });
      const created = sender.next('ACK', m => m.body.ref === create.id);
      sender.send(create);
      await created;

      // Fan-out: a message is "out" when the LAST other member has received it.
      const waiting = new Map(); // message id -> { left, resolve }
      for (const m of others) {
        m.on(msg => {
          const w = msg.type === 'MSG' && waiting.get(msg.id);
          if (w && --w.left === 0) { waiting.delete(msg.id); w.resolve(now()); }
        });
      }
      const memberKeys = Object.fromEntries(members.map((m, i) => [m.username, E2E.fromB64(ids[i].publicKey)]));
      const mySecret = E2E.fromB64(ids[0].secretKey);

      for (const mode of ['plain', 'e2e']) {
        const rows = [];
        let before = null;
        for (let i = 0; i < warmup + messages; i++) {
          if (i === warmup) before = await getStats(target.httpUrl);
          const msg = msgTo(group, TEXT);
          let sealMs = 0;
          if (mode === 'e2e') {
            const t = now();
            msg.body = E2E.sealGroup({ ...msg, from: sender.username }, TEXT, memberKeys, mySecret);
            sealMs = now() - t;
          }
          const allIn = others.length ? new Promise(resolve => waiting.set(msg.id, { left: others.length, resolve })) : Promise.resolve(null);
          const acked = sender.next('ACK', m => m.body.ref === msg.id);
          const t0 = now();
          const frameBytes = sender.send(msg);
          const [tAck, tAll] = await Promise.all([acked.then(now), allIn]);
          if (i < warmup) continue;
          rows.push({ protocol, members: size, mode, i: i - warmup, frame_bytes: frameBytes, seal_ms: round(sealMs, 4), ack_ms: round(tAck - t0), fanout_ms: round((tAll ?? tAck) - t0) });
        }
        const d = bytesDelta(before, await getStats(target.httpUrl), protocol);
        raw.push(...rows);
        summary.push({
          protocol, members: size, mode,
          frameBytes: rows[0].frame_bytes,
          seal: summarize(rows.map(x => x.seal_ms)),
          ack: summarize(rows.map(x => x.ack_ms)),
          fanout: summarize(rows.map(x => x.fanout_ms)),
          // Server bytes per group message: in = the sender's frame (+ MQTT PUBACKs from every member),
          // out = N - 1 copies + the ACK (+ MQTT PUBACK to the sender).
          wireInPerMsg: round(d.in / messages, 1),
          wireOutPerMsg: round(d.out / messages, 1),
        });
      }
      for (const m of members) await m.close();
    }
  }
  return { config: { sizes, warmup, messages, textChars: TEXT.length }, summary, raw };
}

// ---------------------------------------------------------------------------------------------
// 6. Scalability: the probe pair's round trip while K other clients chat in the background.
// ---------------------------------------------------------------------------------------------

// Start benchmarks/load.js (a separate process, so its work does not slow down our probe's
// timing) with K clients; resolves when they are all connected and sending.
function startLoad(target, protocol, clients, rate) {
  return new Promise((resolve, reject) => {
    const child = fork(path.join(__dirname, 'load.js'), [], { stdio: ['ignore', 'inherit', 'inherit', 'ipc'] });
    child.once('error', reject);
    child.once('message', m => (m.ready ? resolve(child) : reject(new Error(m.error))));
    child.send({ wsUrl: target.wsUrl, protocol, clients, rate });
  });
}
const stopLoad = child => new Promise(resolve => {
  child.once('message', report => child.once('exit', () => resolve(report)));
  child.send({ stop: true });
});

async function scalability(target, { levels, runs, rate, warmup, samples }) {
  const raw = [];
  const summary = [];
  for (const clients of levels) {
    for (let r = 0; r < runs; r++) {
      for (const protocol of orderFor(r)) {
        const load = clients > 0 ? await startLoad(target, protocol, clients, rate) : null;
        const a = await connect(protocol, target.wsUrl, { username: `sc_a_${protocol}` });
        const b = await connect(protocol, target.wsUrl, { username: `sc_b_${protocol}` });
        startEcho(a, b);
        const rtts = [];
        for (let i = 0; i < warmup + samples; i++) {
          const { rttMs } = await ping(a, b, `${TEXT} ${clients}.${r}.${i}`);
          if (i >= warmup) rtts.push(rttMs);
        }
        await a.close();
        await b.close();
        const report = load ? await stopLoad(load) : { sent: 0, acked: 0, errors: 0, seconds: 0, achievedMsgPerS: 0, lagP99Ms: null };
        for (let i = 0; i < rtts.length; i++) raw.push({ protocol, clients, run: r, i, rtt_ms: round(rtts[i]) });
        summary.push({ protocol, clients, run: r, rtt: summarize(rtts), load: report });
      }
    }
  }
  // One line per (protocol, clients): the samples of all runs pooled, plus run-to-run spread.
  const pooled = [];
  for (const protocol of PROTOCOLS) {
    for (const clients of levels) {
      const mine = summary.filter(s => s.protocol === protocol && s.clients === clients);
      pooled.push({
        protocol, clients,
        offeredMsgPerS: clients * rate,
        rtt: summarize(raw.filter(x => x.protocol === protocol && x.clients === clients).map(x => x.rtt_ms)),
        p50AcrossRuns: acrossRuns(mine.map(s => s.rtt), 'p50'),
        p95AcrossRuns: acrossRuns(mine.map(s => s.rtt), 'p95'),
        achievedMsgPerS: round(mine.reduce((acc, s) => acc + s.load.achievedMsgPerS, 0) / mine.length, 1),
        loadErrors: mine.reduce((acc, s) => acc + s.load.errors, 0),
        loadLagP99Ms: Math.max(...mine.map(s => s.load.lagP99Ms ?? 0)),
      });
    }
  }
  return { config: { levels, runs, rate, warmup, samples }, summary: pooled, runs: summary, raw };
}

// ---------------------------------------------------------------------------------------------
// 7. Storage: how much of the server's time per message is the SQLite write + fsync?
// No network at all: our own Store (server/core/store.js) in this process, on a temp file, timing
// saveMessage() with synchronous=FULL (what the server uses: wait for the disk) and OFF (hand the
// write to the OS and return; a power cut could lose it). The difference is the fsync.
// ---------------------------------------------------------------------------------------------
async function storage(target, { warmup, samples }) {
  const os = require('node:os');
  const fs = require('node:fs');
  const { Store } = require('../server/core/store');
  const file = path.join(os.tmpdir(), 'chat-bench-storage.db');
  const raw = [];
  for (const sync of ['FULL', 'OFF']) {
    for (const f of [file, `${file}-wal`, `${file}-shm`]) fs.rmSync(f, { force: true });
    const store = new Store(file);
    store.db.exec(`PRAGMA synchronous = ${sync}`);
    store.claimUser('st_a', 'bench-st_a______');
    store.claimUser('st_b', 'bench-st_b______');
    for (let i = 0; i < warmup + samples; i++) {
      const t0 = now();
      store.saveMessage({ id: ChatProto.uuid(), from: 'st_a', to: 'st_b', body: TEXT, ts: Date.now() });
      if (i >= warmup) raw.push({ synchronous: sync, i: i - warmup, save_ms: round(now() - t0, 4) });
    }
    store.close();
  }
  const summary = Object.fromEntries(['FULL', 'OFF'].map(s => [s, summarize(raw.filter(x => x.synchronous === s).map(x => x.save_ms))]));
  return { config: { warmup, samples, file: 'temp file (os.tmpdir())' }, summary, raw };
}

module.exports = { latency, throughput, bytes, setup, groups, scalability, storage, PROTOCOLS, TEXT };
