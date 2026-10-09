// Shared test helpers (not a test file itself: node --test only runs *.test.js).

const assert = require('node:assert/strict');
const WebSocket = require('ws');
const mqtt = require('mqtt');
const ChatProto = require('../protocols/chatproto');
const MqttBinding = require('../protocols/mqtt-binding');
const { TYPES } = ChatProto;

// Decoded server frames, queued so tests can `await next(type)` in any order.
function makeInbox() {
  const inbox = [];
  const waiters = [];
  function flush() {
    for (const w of [...waiters]) {
      const i = inbox.findIndex(m => m.type === w.type);
      if (i !== -1) {
        waiters.splice(waiters.indexOf(w), 1);
        w.resolve(inbox.splice(i, 1)[0]);
      }
    }
  }
  return {
    inbox,
    push(text) {
      const r = ChatProto.decode(text, ChatProto.SERVER_TYPES);
      assert.ok(r.ok, `server sent an invalid frame: ${text}`);
      inbox.push(r.msg);
      flush();
    },
    next: (type, ms = 2000) => new Promise((resolve, reject) => {
      const w = { type, resolve };
      waiters.push(w);
      flush();
      setTimeout(() => reject(new Error(`timed out waiting for ${type}`)), ms).unref();
    }),
  };
}

// A tiny raw ChatProto-over-WebSocket test client.
async function rawClient(url) {
  const ws = new WebSocket(url);
  const box = makeInbox();
  ws.on('message', data => box.push(data.toString()));
  await new Promise((res, rej) => { ws.once('open', res); ws.once('error', rej); });
  return {
    ws,
    inbox: box.inbox,
    next: box.next,
    send: msg => ws.send(typeof msg === 'string' ? msg : ChatProto.encode(msg)),
    close: () => new Promise(res => {
      if (ws.readyState === WebSocket.CLOSED) return res();
      ws.once('close', res);
      ws.close();
    }),
  };
}

// A raw ChatProto-over-MQTT test client: CONNECT, SUBSCRIBE to its down topic, then publish
// frames to its up topic. `granted` is the SUBACK result.
async function rawMqtt(url, { clientId = MqttBinding.newClientId(ChatProto.uuid), subscribe = true } = {}) {
  const client = await mqtt.connectAsync(url, { clientId, clean: true, reconnectPeriod: 0, protocolVersion: 4 });
  const box = makeInbox();
  const down = MqttBinding.downTopic(clientId);
  client.on('message', (topic, payload) => { if (topic === down) box.push(payload.toString()); });
  const granted = subscribe ? await client.subscribeAsync(down, { qos: MqttBinding.QOS }) : null;
  return {
    client,
    clientId,
    granted,
    inbox: box.inbox,
    next: box.next,
    send: msg => client.publish(MqttBinding.upTopic(clientId), typeof msg === 'string' ? msg : ChatProto.encode(msg), { qos: 1 }),
    // endAsync() never settles on a client the broker already disconnected.
    close: () => (client.connected ? client.endAsync() : client.end(true)),
  };
}

// Every test user gets its own device token (HELLO needs one since Step 3).
const tokenOf = username => `token-for-${username}`.padEnd(16, '_');
const hello = (username, extra = {}) =>
  ChatProto.make(TYPES.HELLO, { body: { username, token: tokenOf(username), ...extra } });

// HELLO on an already connected raw client, then wait for WELCOME and SYNCED.
async function joined(c, username, extra = {}) {
  c.send(hello(username, extra));
  const welcome = await c.next(TYPES.WELCOME);
  assert.equal(welcome.body.username, username);
  c.synced = (await c.next(TYPES.SYNCED)).body;
  return c;
}

// Connect, HELLO, wait for WELCOME and SYNCED. Returns the client plus the SYNCED body.
const joinAs = async (url, username, extra = {}) => joined(await rawClient(url), username, extra);
const joinMqttAs = async (url, username, extra = {}) => joined(await rawMqtt(url), username, extra);

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Resolve when emitter fires `event` (ChatClient-style .on()), or reject after ms.
const once = (emitter, event, ms = 3000) => new Promise((resolve, reject) => {
  const t = setTimeout(() => reject(new Error(`timed out waiting for ${event}`)), ms);
  emitter.on(event, (...args) => { clearTimeout(t); resolve(args); });
});

// Wait until cond() is true (polling), or fail.
async function until(cond, ms = 3000) {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error('condition not met in time');
    await sleep(10);
  }
}

module.exports = { rawClient, rawMqtt, tokenOf, hello, joinAs, joinMqttAs, sleep, once, until };
