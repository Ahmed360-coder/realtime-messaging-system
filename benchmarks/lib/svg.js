// Tiny SVG chart writer for the benchmark charts (no library, no browser needed).
// An SVG file is text: <line>, <rect>, <path>, <text> elements in a coordinate system. GitHub shows
// SVG images in the README, and a browser shows them too, with a tooltip (<title>) on each mark.
//
// Two forms only:
//   lineChart  x is a number (latency, members, clients): one line per series
//   barChart   x is a category (WS / MQTT, p50 / p95 / p99): grouped bars, optional whiskers
// Bars always start at 0 (a bar's length must be proportional to its value). Light and dark
// colours are both defined; the dark ones apply when the viewer's system is in dark mode.

const W = 720;
const H = 420;
const M = { left: 70, right: 24, top: 86, bottom: 56 };
const PW = W - M.left - M.right; // plot width
const PH = H - M.top - M.bottom; // plot height

// Colour roles. Series slots 1-2 = WebSocket / MQTT everywhere, so a colour always means the same
// protocol. Slot 3 is a neutral grey for reference lines (e.g. "JSON only").
const STYLE = `
  .bg { fill: #fcfcfb } .t1 { fill: #0b0b0b } .t2 { fill: #52514e } .grid { stroke: #e4e3df } .axis { stroke: #8a8984 }
  .s1 { stroke: #2a78d6; fill: #2a78d6 } .s2 { stroke: #eb6834; fill: #eb6834 } .s3 { stroke: #8a8984; fill: #8a8984 }
  .whisker { stroke: #0b0b0b } .gap { stroke: #fcfcfb }
  @media (prefers-color-scheme: dark) {
    .bg { fill: #1a1a19 } .t1 { fill: #ffffff } .t2 { fill: #c3c2b7 } .grid { stroke: #34332f } .axis { stroke: #77766f }
    .s1 { stroke: #3987e5; fill: #3987e5 } .s2 { stroke: #d95926; fill: #d95926 } .s3 { stroke: #9a9992; fill: #9a9992 }
    .whisker { stroke: #ffffff } .gap { stroke: #1a1a19 }
  }
  text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif }`;

const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const fmt = v => (Math.abs(v) >= 100 ? Math.round(v).toString() : Number(v.toPrecision(3)).toString());

// "Nice" axis ticks: steps of 1, 2 or 5 x 10^k, about 5 of them from 0 (or min) to past max.
function ticks(min, max, count = 5) {
  const span = max - min || 1;
  const raw = span / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 5, 10].map(m => m * mag).find(s => s >= raw);
  const out = [];
  for (let v = Math.floor(min / step) * step; v <= max + step * 1e-9; v += step) out.push(Number(v.toFixed(10)));
  if (out[out.length - 1] < max) out.push(out[out.length - 1] + step);
  return out;
}

function frame({ title, subtitle, xLabel, yLabel }, body, legend) {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(title)}">
<title>${esc(title)}</title>
<style>${STYLE}</style>
<rect class="bg" width="${W}" height="${H}"/>
<text class="t1" x="${M.left}" y="26" font-size="17" font-weight="600">${esc(title)}</text>
${subtitle ? `<text class="t2" x="${M.left}" y="46" font-size="12.5">${esc(subtitle)}</text>` : ''}
${legend}
${body}
<text class="t2" x="${M.left + PW / 2}" y="${H - 14}" font-size="12.5" text-anchor="middle">${esc(xLabel)}</text>
<text class="t2" transform="translate(18 ${M.top + PH / 2}) rotate(-90)" font-size="12.5" text-anchor="middle">${esc(yLabel)}</text>
</svg>
`;
}

// Legend row under the subtitle: a short line (or square) in the series colour + its name.
function legendRow(series, kind) {
  let x = M.left;
  return series.map(s => {
    const mark = kind === 'line'
      ? `<line class="${s.cls}" x1="${x}" y1="64" x2="${x + 22}" y2="64" stroke-width="2.5" ${s.dash ? 'stroke-dasharray="6 4"' : ''}/>`
      : `<rect class="${s.cls}" x="${x + 4}" y="58" width="12" height="12" rx="2"/>`;
    const out = `${mark}<text class="t1" x="${x + 28}" y="68" font-size="12.5">${esc(s.name)}</text>`;
    x += 28 + s.name.length * 7 + 22;
    return out;
  }).join('\n');
}

function yAxis(yTicks, y, unitFmt = fmt) {
  return yTicks.map(v => `<line class="grid" x1="${M.left}" x2="${M.left + PW}" y1="${y(v)}" y2="${y(v)}"/>
<text class="t2" x="${M.left - 8}" y="${y(v) + 4}" font-size="11.5" text-anchor="end">${unitFmt(v)}</text>`).join('\n');
}

/**
 * series: [{ name, cls: 's1'|'s2'|'s3', dash, points: [[x, y], ...] }]
 * opts: title, subtitle, xLabel, yLabel, xMax (cut the x axis), yFmt, markers (draw dots)
 */
function lineChart(opts) {
  const all = opts.series.flatMap(s => s.points);
  const xMin = opts.xMin ?? Math.min(...all.map(p => p[0]));
  const xMax = opts.xMax ?? Math.max(...all.map(p => p[0]));
  const xT = ticks(xMin, xMax);
  const yT = ticks(0, opts.yMax ?? Math.max(...all.map(p => p[1])));
  const x = v => M.left + ((v - xT[0]) / (xT[xT.length - 1] - xT[0])) * PW;
  const y = v => M.top + PH - (v / yT[yT.length - 1]) * PH;
  const xAxis = xT.map(v => `<text class="t2" x="${x(v)}" y="${M.top + PH + 18}" font-size="11.5" text-anchor="middle">${fmt(v)}</text>`).join('\n');
  const lines = opts.series.map(s => {
    const pts = s.points.filter(p => p[0] <= xT[xT.length - 1]);
    const d = pts.map((p, i) => `${i ? 'L' : 'M'}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join('');
    // Each dot = a surface-coloured disc (a 2 px ring that separates overlapping dots) + the coloured dot.
    const dots = opts.markers ? pts.map(p => {
      const cx = x(p[0]).toFixed(1), cy = y(p[1]).toFixed(1);
      return `<circle class="bg" cx="${cx}" cy="${cy}" r="6.5"/><circle class="${s.cls}" cx="${cx}" cy="${cy}" r="4.5" style="stroke:none"><title>${esc(`${s.name}: ${fmt(p[1])} at ${fmt(p[0])}`)}</title></circle>`;
    }).join('') : '';
    return `<path class="${s.cls}" d="${d}" stroke-width="2" stroke-linejoin="round" ${s.dash ? 'stroke-dasharray="6 4"' : ''} style="fill:none"><title>${esc(s.name)}</title></path>${dots}`;
  }).join('\n');
  const body = `${yAxis(yT, y, opts.yFmt)}
<line class="axis" x1="${M.left}" x2="${M.left + PW}" y1="${M.top + PH}" y2="${M.top + PH}"/>
${xAxis}
${lines}`;
  return frame(opts, body, legendRow(opts.series, 'line'));
}

/**
 * categories: ['p50', 'p95', ...]; series: [{ name, cls, values: [{ v, lo, hi, label }] }]
 * lo / hi (optional) draw a whisker, e.g. the smallest and largest run. label overrides the
 * value printed above the bar. A null value draws no bar.
 */
function barChart(opts) {
  const vals = opts.series.flatMap(s => s.values.filter(Boolean).flatMap(v => [v.v, v.hi ?? v.v]));
  const yT = ticks(0, Math.max(...vals));
  const y = v => M.top + PH - (v / yT[yT.length - 1]) * PH;
  const groupW = PW / opts.categories.length;
  const barW = Math.min(64, (groupW * 0.7) / opts.series.length);
  const bars = [];
  opts.categories.forEach((cat, ci) => {
    const g0 = M.left + ci * groupW + (groupW - barW * opts.series.length) / 2;
    bars.push(`<text class="t1" x="${M.left + ci * groupW + groupW / 2}" y="${M.top + PH + 20}" font-size="12.5" text-anchor="middle">${esc(cat)}</text>`);
    opts.series.forEach((s, si) => {
      const val = s.values[ci];
      if (!val || val.v === null || val.v === undefined) return;
      const bx = g0 + si * barW + 1; // 1 px each side = a 2 px gap between neighbouring bars
      const w = barW - 2;
      const top = y(val.v);
      const h = M.top + PH - top;
      const r = Math.min(4, h); // rounded top only: the bar is anchored to the 0 line
      const tip = `${s.name}, ${cat}: ${val.label ?? fmt(val.v)}${val.lo !== undefined ? ` (runs ${fmt(val.lo)}–${fmt(val.hi)})` : ''}`;
      bars.push(`<path class="${s.cls}" d="M${bx},${M.top + PH}V${top + r}Q${bx},${top} ${bx + r},${top}H${bx + w - r}Q${bx + w},${top} ${bx + w},${top + r}V${M.top + PH}Z" style="stroke:none"><title>${esc(tip)}</title></path>`);
      if (val.lo !== undefined && val.hi !== undefined) {
        const cx = bx + w / 2;
        bars.push(`<path class="whisker" d="M${cx},${y(val.lo)}V${y(val.hi)}M${cx - 5},${y(val.lo)}H${cx + 5}M${cx - 5},${y(val.hi)}H${cx + 5}" stroke-width="1.5" fill="none"/>`);
      }
      const labelY = Math.min(top, val.hi !== undefined ? y(val.hi) : top) - 6;
      bars.push(`<text class="t1" x="${bx + w / 2}" y="${labelY}" font-size="11.5" text-anchor="middle">${esc(val.label ?? fmt(val.v))}</text>`);
    });
  });
  const body = `${yAxis(yT, y, opts.yFmt)}
<line class="axis" x1="${M.left}" x2="${M.left + PW}" y1="${M.top + PH}" y2="${M.top + PH}"/>
${bars.join('\n')}`;
  return frame(opts, body, legendRow(opts.series, 'bar'));
}

module.exports = { lineChart, barChart, ticks };
