// Shared test helpers (not a test file itself: node --test only runs *.test.js).

const assert = require('node:assert/strict');
const WebSocket = require('ws');
const ChatProto = require('../protocols/chatproto');
const { TYPES } = ChatProto;

// A tiny raw test client: queues every decoded frame so tests can `await next(type)`.
async function rawClient(url) {
  const ws = new WebSocket(url);
  const inbox = [];
  const waiters = [];
  ws.on('message', data => {
    const r = ChatProto.decode(data.toString(), ChatProto.SERVER_TYPES);
    assert.ok(r.ok, `server sent an invalid frame: ${data}`);
    inbox.push(r.msg);
    flush();
  });
  function flush() {
    for (const w of [...waiters]) {
      const i = inbox.findIndex(m => m.type === w.type);
      if (i !== -1) {
        waiters.splice(waiters.indexOf(w), 1);
        w.resolve(inbox.splice(i, 1)[0]);
      }
    }
  }
  await new Promise((res, rej) => { ws.once('open', res); ws.once('error', rej); });
  return {
    ws,
    inbox,
    send: msg => ws.send(typeof msg === 'string' ? msg : ChatProto.encode(msg)),
    next: (type, ms = 2000) => new Promise((resolve, reject) => {
      const w = { type, resolve };
      waiters.push(w);
      flush();
      setTimeout(() => reject(new Error(`timed out waiting for ${type}`)), ms).unref();
    }),
    close: () => new Promise(res => {
      if (ws.readyState === WebSocket.CLOSED) return res();
      ws.once('close', res);
      ws.close();
    }),
  };
}

// Every test user gets its own device token (HELLO needs one since Step 3).
const tokenOf = username => `token-for-${username}`.padEnd(16, '_');
const hello = (username, extra = {}) =>
  ChatProto.make(TYPES.HELLO, { body: { username, token: tokenOf(username), ...extra } });

// Connect, HELLO, wait for WELCOME and SYNCED. Returns the client plus the SYNCED body.
async function joinAs(url, username, extra = {}) {
  const c = await rawClient(url);
  c.send(hello(username, extra));
  const welcome = await c.next(TYPES.WELCOME);
  assert.equal(welcome.body.username, username);
  c.synced = (await c.next(TYPES.SYNCED)).body;
  return c;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Resolve when emitter fires `event` (ChatClient-style .on()), or reject after ms.
const once = (emitter, event, ms = 3000) => new Promise((resolve, reject) => {
  const t = setTimeout(() => reject(new Error(`timed out waiting for ${event}`)), ms);
  emitter.on(event, (...args) => { clearTimeout(t); resolve(args); });
});

module.exports = { rawClient, tokenOf, hello, joinAs, sleep, once };
