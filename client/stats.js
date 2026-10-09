// Live metrics dashboard (Step 6).
// Opens a Server-Sent Events stream (/api/stats/stream). The server pushes one JSON snapshot per
// second (made by server/core/metrics.js); render() writes its numbers into the page.
// XSS rule as in the chat: text reaches the page only through textContent, never as HTML.

(function () {
  'use strict';

  const $ = id => document.getElementById(id);
  const PROTOCOLS = ['ws', 'mqtt'];

  // ---- Number formatting ----
  const int = n => Math.round(n).toLocaleString('en-US');
  const ms = x => (x === null ? '–' : `${x.toFixed(x < 10 ? 2 : 1)}`);
  function bytes(n) {
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / 1024 / 1024).toFixed(2)} MB`;
  }
  function duration(s) {
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
    return h ? `${h}h ${m}m` : m ? `${m}m ${s % 60}s` : `${s}s`;
  }
  const split = (snap, f) => `ws ${f(snap.protocols.ws)} · mqtt ${f(snap.protocols.mqtt)}`;

  // ---- Comparison table: [label, value(protocolStats)] or a group heading (a string) ----
  const ROWS = [
    'Connections',
    ['Open now', p => int(p.connections.open)],
    ['Opened since start', p => int(p.connections.total)],
    ['Users online', p => int(p.online)],
    'Messages',
    ['Per second (10 s avg)', p => p.rate.toFixed(1)],
    ['Sent (new)', p => int(p.messages.sent)],
    ['…of them to groups', p => int(p.messages.group)],
    // Step 8: the server can tell a sealed body from plain text, but cannot read it.
    ['…of them end-to-end encrypted', p => int(p.messages.e2e)],
    // Step 7: counted per RECIPIENT copy (a group message to 3 others counts 3); 1-to-1 = 1 copy.
    ['Copies delivered live', p => int(p.messages.delivered)],
    ['Copies stored for offline', p => int(p.messages.stored)],
    ['Duplicates (retries)', p => int(p.messages.duplicate)],
    ['Received live', p => int(p.messages.received)],
    ['Replayed by sync', p => int(p.messages.synced)],
    ['Group changes', p => int(p.groupChanges)],
    ['Errors', p => int(p.errorsTotal)],
    'Traffic on TCP',
    ['Bytes in', p => bytes(p.bytes.in)],
    ['Bytes out', p => bytes(p.bytes.out)],
    'Server latency (ms)',
    ['ACK p50 / p95', p => `${ms(p.latency.ack.p50)} / ${ms(p.latency.ack.p95)}`],
    ['ACK max', p => ms(p.latency.ack.max)],
    ['Delivery p50 / p95', p => `${ms(p.latency.deliver.p50)} / ${ms(p.latency.deliver.p95)}`],
    ['Delivery max', p => ms(p.latency.deliver.max)],
  ];

  // Build the rows once; keep the value cells so each update only changes their text.
  const cells = []; // [{ value, ws: <td>, mqtt: <td> }]
  for (const row of ROWS) {
    const tr = document.createElement('tr');
    const head = document.createElement('th');
    if (typeof row === 'string') {
      tr.className = 'group';
      head.scope = 'colgroup';
      head.colSpan = 3;
      head.textContent = row;
      tr.append(head);
    } else {
      head.scope = 'row';
      head.textContent = row[0];
      tr.append(head);
      const cell = { value: row[1] };
      for (const name of PROTOCOLS) {
        cell[name] = document.createElement('td');
        cell[name].textContent = '–';
        tr.append(cell[name]);
      }
      cells.push(cell);
    }
    $('compareBody').append(tr);
  }

  // ---- Chart (Chart.js, served by our server at /vendor/chart.umd.min.js) ----
  const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  let chart = null;
  if (window.Chart) {
    const labels = Array.from({ length: 60 }, (_, i) => `${i - 60}s`);
    chart = new window.Chart($('rateChart'), {
      type: 'line',
      data: {
        labels,
        datasets: PROTOCOLS.map(name => ({
          label: name === 'ws' ? 'WebSocket' : 'MQTT',
          data: new Array(60).fill(0),
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.25,
        })),
      },
      options: {
        animation: false,            // new data every second: animating would only lag behind
        responsive: true,
        maintainAspectRatio: false,  // take the height of .chart-box
        interaction: { mode: 'index', intersect: false },
        plugins: { legend: { display: false } }, // the HTML legend below the chart is used instead
        scales: {
          x: { ticks: { maxTicksLimit: 7, maxRotation: 0 } },
          y: { beginAtZero: true, ticks: { precision: 0 } }, // whole messages per second
        },
      },
    });
  }

  function drawChart(snap) {
    if (!chart) return;
    const colors = { ws: css('--accent'), mqtt: css('--mqtt') }; // read each time: follows light/dark
    const grid = css('--surface-2'), text = css('--muted');
    PROTOCOLS.forEach((name, i) => {
      const ds = chart.data.datasets[i];
      ds.data = snap.protocols[name].history;
      ds.borderColor = ds.backgroundColor = colors[name];
    });
    for (const axis of ['x', 'y']) {
      chart.options.scales[axis].grid = { color: grid };
      chart.options.scales[axis].ticks.color = text;
    }
    chart.update();
    $('rateChart').setAttribute('aria-label',
      `Messages per second over the last minute. Now: WebSocket ${snap.protocols.ws.rate}, MQTT ${snap.protocols.mqtt.rate}.`);
  }

  // ---- Lists ----
  function fillList(ul, lines, emptyText) {
    ul.replaceChildren();
    if (lines.length === 0) lines = [[emptyText, 'none']];
    for (const [text, className] of lines) {
      const li = document.createElement('li');
      li.textContent = text;
      if (className) li.className = className;
      ul.append(li);
    }
  }

  // ---- One snapshot -> the page ----
  function render(snap) {
    const t = snap.totals;
    $('tConn').textContent = int(t.connections);
    $('tConnSplit').textContent = split(snap, p => p.connections.open);
    $('tOnline').textContent = int(t.online);
    $('tOnlineSplit').textContent = split(snap, p => p.online);
    $('tRate').textContent = t.rate.toFixed(1);
    $('tRateSplit').textContent = split(snap, p => p.rate.toFixed(1));
    $('tSent').textContent = int(t.sent);
    $('tSentSplit').textContent = split(snap, p => p.messages.sent);

    for (const cell of cells) {
      for (const name of PROTOCOLS) cell[name].textContent = cell.value(snap.protocols[name]);
    }

    const codes = [...new Set(PROTOCOLS.flatMap(name => Object.keys(snap.protocols[name].errors)))].sort();
    fillList($('errors'), codes.map(code => [
      `${code}: ${split(snap, p => p.errors[code] || 0)}`,
    ]), 'No errors');

    const db = snap.database;
    fillList($('database'), db ? [
      [`Messages stored: ${int(db.messages)} (${int(db.encrypted)} encrypted)`],
      [`Registered users: ${int(db.users)}`],
      [`Groups: ${int(db.groups)}`],
      [`Last seq: ${int(db.lastSeq)}`],
    ] : [], 'Not available');

    drawChart(snap);

    const subtitle = $('subtitle');
    const dot = document.createElement('span');
    dot.className = 'live';
    dot.textContent = '● live';
    subtitle.replaceChildren(dot, ` · server up ${duration(snap.uptimeS)} · ${new Date(snap.time).toLocaleTimeString()}`);
  }

  function showBanner(text) {
    $('banner').hidden = !text;
    $('bannerText').textContent = text || '';
  }

  // ---- Live updates: Server-Sent Events ----
  const source = new EventSource('/api/stats/stream');
  source.onopen = () => showBanner(null);
  source.onmessage = (e) => {
    let snap;
    try { snap = JSON.parse(e.data); } catch { return; }
    render(snap);
  };
  // The server stopped or the Wi-Fi dropped. EventSource retries on its own (every 2 s,
  // the "retry:" the server sent), so we only tell the user.
  source.onerror = () => {
    showBanner('Connection to the server lost – reconnecting…');
    $('subtitle').textContent = 'offline';
  };
})();
