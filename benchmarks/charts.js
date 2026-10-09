// Draw the Step 9 charts from saved results: benchmarks/results/<label>/*.json -> <label>/charts/*.svg.
// Run by benchmarks/run.js after the experiments, or on its own to redraw without re-measuring:
//   node benchmarks/charts.js [results folder]        (default benchmarks/results/local)
// Each chart is drawn only if its experiment's JSON exists.

const fs = require('node:fs');
const path = require('node:path');
const { lineChart, barChart } = require('./lib/svg');

// Every chart compares the same two stacks; the legends name the transport under ChatProto.
const NAME = { ws: 'WebSocket', mqtt: 'MQTT over WebSocket' };
const CLS = { ws: 's1', mqtt: 's2' };
const PROTOCOLS = ['ws', 'mqtt'];
const ms = v => `${v} ms`;

function load(dir, name) {
  const file = path.join(dir, `${name}.json`);
  return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : null;
}

const charts = {
  // Whole distribution of the round trip: how many % of messages took at most x ms.
  'latency-cdf': dir => {
    const r = load(dir, 'latency');
    if (!r) return null;
    const cut = Math.max(...PROTOCOLS.map(p => r.summary[p].rtt.p99)) * 1.25; // keep the body readable
    return lineChart({
      title: 'Round-trip time A → B → A, distribution (CDF)',
      subtitle: `${r.config.runs} runs × ${r.config.samples} messages per protocol, after ${r.config.warmup} warm-up; x axis cut at 1.25 × p99, max in the table`,
      xLabel: 'round-trip time (ms)', yLabel: '% of messages at or below',
      xMin: 0, xMax: cut, yMax: 100, yFmt: v => `${v}%`,
      series: PROTOCOLS.map(p => ({ name: NAME[p], cls: CLS[p], points: r.summary[p].cdf.map(([x, f]) => [x, f * 100]) })),
    });
  },

  // p50 / p95 / p99 side by side; whiskers = smallest and largest run's value.
  'latency-percentiles': dir => {
    const r = load(dir, 'latency');
    if (!r) return null;
    const keys = ['p50', 'p95', 'p99'];
    return barChart({
      title: 'Round-trip time percentiles',
      subtitle: `pooled over ${r.config.runs} runs; whisker = lowest–highest run`,
      xLabel: 'percentile', yLabel: 'round-trip time (ms)', yFmt: v => v,
      categories: keys,
      series: PROTOCOLS.map(p => ({
        name: NAME[p], cls: CLS[p],
        values: keys.map(k => ({ v: r.summary[p].rtt[k], lo: r.summary[p][`${k}AcrossRuns`].min, hi: r.summary[p][`${k}AcrossRuns`].max, label: ms(r.summary[p].rtt[k]) })),
      })),
    });
  },

  'throughput': dir => {
    const r = load(dir, 'throughput');
    if (!r) return null;
    return barChart({
      title: 'Throughput: one sender → one receiver',
      subtitle: `${r.config.count} messages, at most ${r.config.window} un-ACKed; bar = mean of ${r.config.runs} runs, whisker = lowest–highest run`,
      xLabel: '', yLabel: 'messages per second', yFmt: v => v,
      categories: ['messages / s'],
      series: PROTOCOLS.map(p => ({ name: NAME[p], cls: CLS[p], values: [{ v: r.summary[p].mean, lo: r.summary[p].min, hi: r.summary[p].max, label: String(Math.round(r.summary[p].mean)) }] })),
    });
  },

  // Bytes per 1-to-1 message (MSG up + ACK + MSG down) against the JSON alone.
  'bytes-per-message': dir => {
    const r = load(dir, 'bytes');
    if (!r) return null;
    const rows = p => r.summary.filter(x => x.protocol === p);
    return lineChart({
      title: 'Bytes on the wire per 1-to-1 message',
      subtitle: 'MSG up + ACK back + MSG down, counted by the server\'s TCP sockets (TCP/IP headers not included)',
      xLabel: 'text length (characters)', yLabel: 'bytes per message', markers: true, xMin: 0, xMax: 1100, yFmt: v => v,
      series: [
        ...PROTOCOLS.map(p => ({ name: NAME[p], cls: CLS[p], points: rows(p).map(x => [x.text_chars, x.wire_total_per_msg]) })),
        { name: 'JSON only', cls: 's3', dash: true, points: rows('ws').map(x => [x.text_chars, x.json_total]) },
      ],
    });
  },

  // Connection setup: cumulative time at the end of each phase (median).
  'setup': dir => {
    const r = load(dir, 'setup');
    if (!r) return null;
    const phase = (p, k) => r.summary[p].phases[`${k}_ms`];
    const cats = [['connected', { ws: 'open', mqtt: 'connack' }], ['subscribed', { mqtt: 'suback' }], ['WELCOME', { ws: 'welcome', mqtt: 'welcome' }], ['SYNCED', { ws: 'synced', mqtt: 'synced' }]];
    return barChart({
      title: 'Connection setup: time until each phase is done (median)',
      subtitle: `${r.config.runs} runs × ${r.config.connections} connections; whisker = p50–p95. WebSocket has no SUBSCRIBE phase`,
      xLabel: 'phase (cumulative from the start of connect)', yLabel: 'ms since connect started', yFmt: v => v,
      categories: cats.map(c => c[0]),
      series: PROTOCOLS.map(p => ({
        name: NAME[p], cls: CLS[p],
        values: cats.map(([, k]) => (k[p] ? { v: phase(p, k[p]).p50, lo: phase(p, k[p]).p50, hi: phase(p, k[p]).p95, label: ms(phase(p, k[p]).p50) } : null)),
      })),
    });
  },

  // Group message: server bytes out per message vs group size, plain vs end-to-end encrypted.
  'group-bytes': dir => {
    const r = load(dir, 'groups');
    if (!r) return null;
    return lineChart({
      title: 'Fan-out cost: server bytes sent per group message',
      subtitle: 'N − 1 copies + the ACK; solid = plain text, dashed = end-to-end encrypted (Step 8); exact values in the README table',
      xLabel: 'group size (members, all online)', yLabel: 'bytes out per message', markers: true, xMin: 0,
      yFmt: v => (v >= 1000 ? `${v / 1000}k` : v),
      series: groupSeries(r, x => x.wireOutPerMsg),
    });
  },

  'group-fanout': dir => {
    const r = load(dir, 'groups');
    if (!r) return null;
    return lineChart({
      title: 'Fan-out time: send → last member has it (median)',
      subtitle: `${r.config.messages} messages per point; solid = plain text, dashed = end-to-end encrypted`,
      xLabel: 'group size (members, all online)', yLabel: 'ms', markers: true, xMin: 0, yFmt: v => v,
      series: groupSeries(r, x => x.fanout.p50),
    });
  },

  // Round trip of the probe pair while K other clients chat.
  'scalability': dir => {
    const r = load(dir, 'scalability');
    if (!r) return null;
    const rows = p => r.summary.filter(x => x.protocol === p);
    return lineChart({
      title: 'Round-trip time under load (many simultaneous clients)',
      subtitle: `K background clients, each sending ${r.config.rate} msg/s; solid = p50, dashed = p95; ${r.config.runs} runs pooled`,
      xLabel: 'background clients (same protocol)', yLabel: 'probe round-trip time (ms)', markers: true, xMin: 0, yFmt: v => v,
      series: PROTOCOLS.flatMap(p => [
        { name: `${p === 'ws' ? 'WebSocket' : 'MQTT'} p50`, cls: CLS[p], points: rows(p).map(x => [x.clients, x.rtt.p50]) },
        { name: `${p === 'ws' ? 'WebSocket' : 'MQTT'} p95`, cls: CLS[p], dash: true, points: rows(p).map(x => [x.clients, x.rtt.p95]) },
      ]),
    });
  },
};

function groupSeries(r, value) {
  return PROTOCOLS.flatMap(p => ['plain', 'e2e'].map(mode => ({
    name: `${p === 'ws' ? 'WebSocket' : 'MQTT'}${mode === 'e2e' ? ' + E2E' : ''}`,
    cls: CLS[p],
    dash: mode === 'e2e',
    points: r.summary.filter(x => x.protocol === p && x.mode === mode).map(x => [x.members, value(x)]),
  })));
}

/** Draw every chart whose results exist; returns the files written. */
function drawCharts(dir) {
  const out = path.join(dir, 'charts');
  fs.mkdirSync(out, { recursive: true });
  const written = [];
  for (const [name, draw] of Object.entries(charts)) {
    const svg = draw(dir);
    if (!svg) continue;
    const file = path.join(out, `${name}.svg`);
    fs.writeFileSync(file, svg);
    written.push(file);
  }
  return written;
}

if (require.main === module) {
  const dir = process.argv[2] || path.join(__dirname, 'results', 'local');
  console.log(drawCharts(dir).join('\n'));
}

module.exports = { drawCharts };
