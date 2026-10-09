// Step 4: the mobile page is served, is set up for phones, and never renders text as HTML.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { startServer } = require('../server');

const clientDir = path.join(__dirname, '..', 'client');

test('the server serves the page and every script it loads', async () => {
  const server = await startServer({ host: '127.0.0.1', log: () => {}, port: 0, dbPath: ':memory:' });
  try {
    const base = `http://127.0.0.1:${server.port}`;
    const page = await fetch(`${base}/`);
    assert.equal(page.status, 200);
    const html = await page.text();
    // Every <script src> and stylesheet the page references must exist on the server.
    const refs = [...html.matchAll(/(?:src|href)="(\/[^"]+)"/g)].map(m => m[1]);
    assert.deepEqual(refs, ['/style.css', '/stats', '/protocols/chatproto.js', '/protocols/mqtt-binding.js',
                           '/vendor/mqtt.min.js', '/mqtt-socket.js', '/vendor/nacl.min.js', '/e2e.js',
                           '/chat-client.js', '/conversations.js', '/app.js']);
    for (const ref of refs) {
      const r = await fetch(base + ref);
      assert.equal(r.status, 200, `${ref} should be served`);
    }
  } finally {
    await server.close();
  }
});

test('the page has the phone viewport meta tag', () => {
  const html = fs.readFileSync(path.join(clientDir, 'index.html'), 'utf8');
  assert.match(html, /<meta name="viewport" content="width=device-width, initial-scale=1/);
});

test('no client code uses innerHTML / outerHTML / insertAdjacentHTML / document.write (XSS)', () => {
  for (const file of ['app.js', 'conversations.js', 'chat-client.js', 'mqtt-socket.js', 'e2e.js', 'index.html', 'stats.js', 'stats.html']) {
    const code = fs.readFileSync(path.join(clientDir, file), 'utf8');
    assert.doesNotMatch(code, /\.(innerHTML|outerHTML)\s*=|insertAdjacentHTML|document\.write/, file);
  }
});
