// Step 9 benchmark runner.
//
//   node benchmarks/run.js                      all experiments, full size (several minutes)
//   node benchmarks/run.js --quick              tiny counts, to check that everything works
//   node benchmarks/run.js --only=latency,bytes some experiments only
//   node benchmarks/run.js --url=ws://192.168.1.7:3000   use a server that is already running
//                                               (e.g. measure the laptop from another computer
//                                               over Wi-Fi); default: start our own on port 3109
//   node benchmarks/run.js --label=wifi-far     results go to benchmarks/results/<label>/ (default "local")
//
// For each experiment it writes benchmarks/results/<label>/<experiment>.json (config, summary,
// environment) and <experiment>.csv (every raw sample), then draws the charts (benchmarks/charts.js).

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const experiments = require('./experiments');
const { startServer } = require('./lib/server');
const { drawCharts } = require('./charts');

// Sizes of each experiment. FULL is what the README reports; QUICK only proves the code runs.
const FULL = {
  latency: { runs: 5, warmup: 50, samples: 300 },
  throughput: { runs: 5, warmup: 200, count: 2000, window: 50 },
  bytes: { sizes: [16, 64, 256, 1024], count: 100 },
  setup: { runs: 5, warmup: 3, connections: 20 },
  groups: { sizes: [2, 5, 10, 20, 50], warmup: 10, messages: 100 },
  scalability: { levels: [0, 10, 50, 100, 200], runs: 3, rate: 1, warmup: 30, samples: 150 },
  storage: { warmup: 50, samples: 1000 },
};
const QUICK = {
  latency: { runs: 2, warmup: 5, samples: 20 },
  throughput: { runs: 2, warmup: 20, count: 100, window: 10 },
  bytes: { sizes: [16, 256], count: 10 },
  setup: { runs: 2, warmup: 1, connections: 3 },
  groups: { sizes: [2, 5], warmup: 2, messages: 10 },
  scalability: { levels: [0, 10], runs: 2, rate: 2, warmup: 5, samples: 20 },
  storage: { warmup: 5, samples: 50 },
};

function parseArgs(argv) {
  const args = {};
  for (const a of argv) {
    const [k, v = true] = a.replace(/^--/, '').split('=');
    args[k] = v;
  }
  return args;
}

// What the numbers were measured on: needed to compare them with anybody else's.
function environment(target, args) {
  return {
    date: new Date().toISOString(),
    node: process.version,
    os: `${os.type()} ${os.release()} (${os.arch()})`,
    cpu: os.cpus()[0].model,
    cores: os.cpus().length,
    memoryGB: Math.round(os.totalmem() / 2 ** 30),
    server: args.url ? `already running at ${target.httpUrl}` : `child process on ${target.httpUrl}, temp DB ${target.dbPath}`,
    path: args.url ? 'network (see label)' : 'loopback (127.0.0.1): client and server on the same laptop',
  };
}

// Raw samples -> CSV: one header line, then one line per sample.
function toCsv(rows) {
  if (!rows.length) return '';
  const cols = Object.keys(rows[0]);
  return [cols.join(','), ...rows.map(r => cols.map(c => r[c]).join(','))].join('\n') + '\n';
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cfg = args.quick ? QUICK : FULL;
  const only = args.only ? String(args.only).split(',') : Object.keys(cfg);
  const label = args.label || (args.quick ? 'quick' : 'local');
  const outDir = path.join(__dirname, 'results', label);
  fs.mkdirSync(outDir, { recursive: true });

  const target = await startServer({ url: args.url || null });
  const env = environment(target, args);
  console.log(`Benchmarking ${target.httpUrl} -> ${path.relative(process.cwd(), outDir)}`);
  try {
    for (const name of only) {
      const t = Date.now();
      process.stdout.write(`  ${name} ... `);
      const result = await experiments[name](target, cfg[name]);
      const { raw, ...rest } = result;
      fs.writeFileSync(path.join(outDir, `${name}.json`), JSON.stringify({ experiment: name, environment: env, ...rest }, null, 2) + '\n');
      fs.writeFileSync(path.join(outDir, `${name}.csv`), toCsv(raw));
      console.log(`done in ${((Date.now() - t) / 1000).toFixed(1)} s`);
    }
  } finally {
    await target.stop();
  }
  const charts = drawCharts(outDir);
  console.log(`Charts: ${charts.map(c => path.relative(process.cwd(), c)).join(', ')}`);
}

main().catch(err => { console.error(err); process.exit(1); });
