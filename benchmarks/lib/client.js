// A minimal ChatProto client for the benchmarks, over either protocol:
//   'ws'    ChatProto JSON in WebSocket text frames on /ws            (Step 2)
//   'mqtt'  ChatProto JSON in MQTT PUBLISH packets (QoS 1) on /mqtt   (Step 5)
//
// Why not client/chat-client.js: that one adds an outbox, 3 s retries, dedup and reconnect, i.e.
// timers and work that are not the protocol. A benchmark must measure the protocol, so this client
// does only what the wire needs: connect, HELLO, send a frame, hand each received frame to a
// listener. Everything above the transport is identical for both protocols, so any difference we
// measure comes from the transport.
//
// It records the connection setup phases (ms since connect() was called, on the monotonic clock):
//   ws:    open (TCP + HTTP Upgrade done)                       -> welcome -> synced
//   mqtt:  connack (TCP + HTTP Upgrade + CONNECT/CONNACK) -> suback -> welcome -> synced

const { performance } = require('node:perf_hooks');
const WebSocket = require('ws');
const mqtt = require('mqtt');
const ChatProto = require('../../protocols/chatproto');
const MqttBinding = require('../../protocols/mqtt-binding');

// HELLO needs a device token (16-128 of A-Z a-z 0-9 _ -); one fixed token per bench username.
const tokenOf = username => `bench-${username}`.padEnd(16, '_');

// The transport part: text in, text out. The two protocols differ only here.
async function openWs(wsUrl, phases, t0) {
  const ws = new WebSocket(`${wsUrl}/ws`);
  await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  phases.open = performance.now() - t0;
  return {
    send: text => ws.send(text),
    onText: fn => ws.on('message', data => fn(data.toString('utf8'))),
    close: () => new Promise(resolve => {
      if (ws.readyState === WebSocket.CLOSED) return resolve();
      ws.once('close', resolve);
      ws.close();
    }),
  };
}

async function openMqtt(wsUrl, phases, t0) {
  const clientId = MqttBinding.newClientId(ChatProto.uuid);
  const client = await mqtt.connectAsync(`${wsUrl}${MqttBinding.PATH}`, {
    clientId, clean: true, reconnectPeriod: 0, protocolVersion: 4, keepalive: MqttBinding.KEEPALIVE_S,
  });
  phases.connack = performance.now() - t0;
  const down = MqttBinding.downTopic(clientId);
  await client.subscribeAsync(down, { qos: MqttBinding.QOS });
  phases.suback = performance.now() - t0;
  const up = MqttBinding.upTopic(clientId);
  return {
    send: text => client.publish(up, text, { qos: MqttBinding.QOS }),
    onText: fn => client.on('message', (topic, payload) => { if (topic === down) fn(payload.toString('utf8')); }),
    close: () => (client.connected ? client.endAsync() : client.end(true)),
  };
}

/**
 * Connect and join as `username`. Returns a client:
 *   send(msg) -> bytes of JSON sent      on(fn)  every received frame (decoded object)
 *   next(type, pred, ms) -> Promise      close() -> Promise         phases, welcome, username
 */
async function connect(protocol, wsUrl, { username, key = null, lastSeq = 0 } = {}) {
  const t0 = performance.now();
  const phases = {};
  const transport = protocol === 'ws' ? await openWs(wsUrl, phases, t0) : await openMqtt(wsUrl, phases, t0);

  const listeners = new Set();
  transport.onText(text => {
    const msg = JSON.parse(text);
    for (const fn of [...listeners]) fn(msg, text);
  });
  const on = fn => { listeners.add(fn); return () => listeners.delete(fn); };
  const next = (type, pred = () => true, ms = 10_000) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => { off(); reject(new Error(`${username}: timed out waiting for ${type}`)); }, ms);
    const off = on(msg => {
      if (msg.type === type && pred(msg)) { clearTimeout(timer); off(); resolve(msg); }
      if (msg.type === 'ERROR' && type !== 'ERROR') { clearTimeout(timer); off(); reject(new Error(`${username}: ERROR ${msg.body.code} ${msg.body.message}`)); }
    });
  });
  const send = msg => {
    const text = ChatProto.encode(msg);
    transport.send(text);
    return Buffer.byteLength(text);
  };

  const body = { username, token: tokenOf(username), lastSeq };
  if (key) body.key = key;
  const welcomeP = next('WELCOME');
  const syncedP = next('SYNCED');
  send(ChatProto.make('HELLO', { body }));
  const welcome = await welcomeP;
  phases.welcome = performance.now() - t0;
  await syncedP;
  phases.synced = performance.now() - t0;

  return { protocol, username, phases, welcome, send, on, next, close: transport.close };
}

module.exports = { connect, tokenOf };
