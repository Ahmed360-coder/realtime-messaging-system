// Step 9: the benchmark code itself. The statistics must be right (they are the numbers in the
// README), and every experiment must run end to end and give results of the expected shape.
// The experiments run here with tiny counts against an in-process server with an in-memory DB.

const test = require('node:test');
const assert = require('node:assert/strict');
const { percentile, mean, sd, summarize, acrossRuns, cdf } = require('../benchmarks/lib/stats');
const { ticks, lineChart, barChart } = require('../benchmarks/lib/svg');
const experiments = require('../benchmarks/experiments');
const { startServer } = require('../server/index');

test('nearest-rank percentiles, mean and sample standard deviation', () => {
  const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  assert.equal(percentile(sorted, 50), 5);  // rank ceil(0.5 * 10) = 5
  assert.equal(percentile(sorted, 95), 10); // rank ceil(9.5) = 10
  assert.equal(percentile(sorted, 10), 1);
  assert.equal(percentile([], 50), null);
  assert.equal(mean([2, 4, 6]), 4);
  assert.equal(sd([2, 4, 4, 4, 5, 5, 7, 9]), Math.sqrt(32 / 7)); // n - 1 in the denominator
  assert.equal(sd([3]), 0);
});

test('summarize sorts its input and acrossRuns measures run-to-run spread', () => {
  const s = summarize([5, 1, 4, 2, 3]);
  assert.deepEqual(s, { n: 5, mean: 3, sd: 1.581, min: 1, p50: 3, p95: 5, p99: 5, max: 5 });
  const runs = [{ p50: 2 }, { p50: 4 }, { p50: 6 }];
  assert.deepEqual(acrossRuns(runs, 'p50'), { mean: 4, sd: 2, min: 2, max: 6 });
});

test('the CDF rises from the smallest sample to 1 at the largest', () => {
  const points = cdf([3, 1, 2, 4]);
  assert.deepEqual(points[0], [1, 0.25]);
  assert.deepEqual(points[points.length - 1], [4, 1]);
  for (let i = 1; i < points.length; i++) assert.ok(points[i][1] >= points[i - 1][1]);
});

test('chart axes use 1-2-5 steps and cover the data; charts are valid SVG text', () => {
  assert.deepEqual(ticks(0, 9.3), [0, 2, 4, 6, 8, 10]);
  assert.deepEqual(ticks(0, 100), [0, 20, 40, 60, 80, 100]);
  const line = lineChart({ title: 'a < b', xLabel: 'x', yLabel: 'y', series: [{ name: 'ws', cls: 's1', points: [[0, 1], [10, 3]] }] });
  assert.match(line, /^<svg /);
  assert.match(line, /a &lt; b/); // text is escaped
  const bars = barChart({ title: 't', xLabel: '', yLabel: 'y', categories: ['p50'], series: [{ name: 'ws', cls: 's1', values: [{ v: 2, lo: 1, hi: 3 }] }] });
  assert.match(bars, /class="whisker"/);
});

test('every experiment runs end to end over both protocols (tiny counts)', { timeout: 60_000 }, async () => {
  const server = await startServer({ port: 0, dbPath: ':memory:', log: () => {} });
  const target = { httpUrl: `http://127.0.0.1:${server.port}`, wsUrl: `ws://127.0.0.1:${server.port}` };
  try {
    const lat = await experiments.latency(target, { runs: 2, warmup: 2, samples: 5 });
    for (const p of ['ws', 'mqtt']) {
      assert.equal(lat.summary[p].rtt.n, 10);
      assert.ok(lat.summary[p].rtt.p50 > 0);
      assert.ok(lat.summary[p].ack.p50 <= lat.summary[p].rtt.p99, 'the ACK comes back before the echo');
    }
    assert.equal(lat.raw.length, 20);

    const tp = await experiments.throughput(target, { runs: 1, warmup: 5, count: 20, window: 5 });
    assert.ok(tp.summary.ws.mean > 0 && tp.summary.mqtt.mean > 0);

    const by = await experiments.bytes(target, { sizes: [16, 200], count: 5 });
    const row = (p, n) => by.summary.find(x => x.protocol === p && x.text_chars === n);
    // The wire carries at least the JSON; a longer text costs about its extra characters more.
    for (const p of ['ws', 'mqtt']) {
      assert.ok(row(p, 16).wire_total_per_msg >= row(p, 16).json_total);
      assert.ok(Math.abs((row(p, 200).wire_total_per_msg - row(p, 16).wire_total_per_msg) - 2 * 184) < 20);
    }
    assert.ok(row('mqtt', 16).overhead_per_msg > row('ws', 16).overhead_per_msg, 'MQTT adds topics and PUBACKs');

    const su = await experiments.setup(target, { runs: 1, warmup: 1, connections: 2 });
    assert.ok(su.summary.ws.phases.synced_ms.p50 > 0);
    assert.ok(su.summary.mqtt.phases.suback_ms, 'MQTT has a SUBSCRIBE phase');

    const gr = await experiments.groups(target, { sizes: [2, 3], warmup: 1, messages: 3 });
    const g = (p, n, mode) => gr.summary.find(x => x.protocol === p && x.members === n && x.mode === mode);
    assert.ok(g('ws', 3, 'e2e').frameBytes > g('ws', 3, 'plain').frameBytes, 'a sealed frame carries a key box per member');
    assert.ok(g('ws', 3, 'e2e').frameBytes > g('ws', 2, 'e2e').frameBytes);

    const sc = await experiments.scalability(target, { levels: [0, 4], runs: 1, rate: 5, warmup: 2, samples: 5 });
    const loaded = sc.summary.find(x => x.protocol === 'mqtt' && x.clients === 4);
    assert.equal(loaded.rtt.n, 5);
    assert.equal(loaded.loadErrors, 0);
  } finally {
    await server.close();
  }
});
