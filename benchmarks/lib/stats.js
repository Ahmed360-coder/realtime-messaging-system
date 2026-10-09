// Descriptive statistics for the Step 9 benchmarks (no library: every formula is here).
//
//   mean      sum / n
//   sd        sample standard deviation, sqrt( sum (x - mean)^2 / (n - 1) ): the typical distance
//             of a sample from the mean (n - 1, "Bessel's correction", because the mean itself was
//             estimated from the same samples)
//   pXX       nearest-rank percentile: the smallest sample with at least XX % of the samples <= it
//             (the same definition as the Step 6 dashboard, server/core/metrics.js)
//
// acrossRuns() answers "how repeatable is this?": the same statistic from each independent run,
// and how much it moves from run to run.

/** Nearest-rank percentile of an already SORTED array (ascending). */
function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const rank = Math.max(1, Math.ceil((p / 100) * sorted.length));
  return sorted[rank - 1];
}

function mean(values) {
  if (values.length === 0) return null;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

/** Sample standard deviation (n - 1). 0 for a single value. */
function sd(values) {
  if (values.length < 2) return values.length ? 0 : null;
  const m = mean(values);
  return Math.sqrt(values.reduce((acc, x) => acc + (x - m) ** 2, 0) / (values.length - 1));
}

// Round for the result files: 3 decimals is 1 microsecond when the unit is ms.
const round = (x, d = 3) => (x === null ? null : Math.round(x * 10 ** d) / 10 ** d);

/** n, mean, sd, min, p50, p95, p99, max of a list of numbers. */
function summarize(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    n: sorted.length,
    mean: round(mean(sorted)),
    sd: round(sd(sorted)),
    min: round(sorted[0] ?? null),
    p50: round(percentile(sorted, 50)),
    p95: round(percentile(sorted, 95)),
    p99: round(percentile(sorted, 99)),
    max: round(sorted.length ? sorted[sorted.length - 1] : null),
  };
}

/**
 * The same statistic (e.g. 'p50') taken from several runs' summaries: its mean over the runs,
 * the run-to-run standard deviation, and the smallest / largest run.
 */
function acrossRuns(summaries, key) {
  const values = summaries.map(s => s[key]);
  return { mean: round(mean(values)), sd: round(sd(values)), min: round(Math.min(...values)), max: round(Math.max(...values)) };
}

/** Empirical CDF for a chart: up to `points` (value, fraction <= value) pairs. */
function cdf(values, points = 200) {
  const sorted = [...values].sort((a, b) => a - b);
  const out = [];
  const step = Math.max(1, Math.floor(sorted.length / points));
  for (let i = 0; i < sorted.length; i += step) out.push([round(sorted[i]), round((i + 1) / sorted.length, 4)]);
  if (sorted.length) out.push([round(sorted[sorted.length - 1]), 1]);
  return out;
}

module.exports = { percentile, mean, sd, round, summarize, acrossRuns, cdf };
