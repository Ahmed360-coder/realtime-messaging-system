// Step 5: MQTT as the second protocol. The broker's access rules, ChatProto over MQTT,
// keepalive, cross-protocol chat (WebSocket <-> MQTT), offline sync, takeover, and ChatClient
// running over the MqttSocket adapter (reconnect, retry, dedup).
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const WebSocket = require('ws');
const mqtt = require('mqtt');
const ChatProto = require('../protocols/chatproto');
const MqttBinding = require('../protocols/mqtt-binding');
const ChatClient = require('../client/chat-client');
const MqttSocket = require('../client/mqtt-socket');
const { startServer } = require('../server');
const { rawMqtt, tokenOf, hello, joinAs, joinMqttAs, sleep, once, until } = require('./helpers');
const { TYPES, ERRORS } = ChatProto;

const quiet = { host: '127.0.0.1', log: () => {} };

test.describe('MQTT transport', () => {
  let server, wsUrl, mqttUrl;
  test.before(async () => {
    server = await startServer({ ...quiet, port: 0, dbPath: ':memory:' });
    wsUrl = `ws://127.0.0.1:${server.port}/ws`;
    mqttUrl = `ws://127.0.0.1:${server.port}${MqttBinding.PATH}`;
  });
  test.after(() => server.close());

  // A ChatClient that speaks MQTT, with short timers so tests are fast.
  function mqttClient(username, opts = {}) {
    const c = new ChatClient({
      token: tokenOf(username), openSocket: () => new MqttSocket(mqttUrl),
      ackTimeoutMs: 150, maxMissedAcks: 2, backoffMinMs: 30, backoffMaxMs: 200, ...opts,
    });
    c.received = [];
    c.on('message', m => c.received.push(m));
    return c;
  }
  function wsClient(username) {
    const c = new ChatClient({ url: wsUrl, token: tokenOf(username), WebSocket, backoffMinMs: 30, backoffMaxMs: 200 });
    c.received = [];
    c.on('message', m => c.received.push(m));
    return c;
  }
  async function start(c, username) {
    c.join(username);
    c.connect();
    await once(c, 'synced');
    return c;
  }

  test('HELLO, MSG with ACK + seq, LIST, and malformed payloads over MQTT', async () => {
    const a = await joinMqttAs(mqttUrl, 'm1_alice');
    const b = await joinMqttAs(mqttUrl, 'm1_bob');
    assert.deepEqual(a.granted.map(g => g.qos), [1], 'own down topic granted at QoS 1');

    const m = ChatProto.make(TYPES.MSG, { to: 'm1_bob', body: 'hi over mqtt', from: 'mallory' });
    a.send(m);
    const ack = await a.next(TYPES.ACK);
    assert.equal(ack.body.ref, m.id);
    assert.equal(ack.body.status, 'delivered');
    const got = await b.next(TYPES.MSG);
    assert.equal(got.body, 'hi over mqtt');
    assert.equal(got.from, 'm1_alice', 'from is set by the server');
    assert.equal(got.seq, ack.body.seq);

    a.send('{not json');
    assert.equal((await a.next(TYPES.ERROR)).body.code, ERRORS.BAD_JSON);
    a.send({ v: 1, type: 'WELCOME', id: ChatProto.uuid(), ts: Date.now() });
    assert.equal((await a.next(TYPES.ERROR)).body.code, ERRORS.BAD_TYPE);
    // Still connected and joined after the errors.
    a.send(ChatProto.make(TYPES.LIST));
    assert.deepEqual((await a.next(TYPES.USERS)).body.users.filter(u => u.startsWith('m1_')), ['m1_alice', 'm1_bob']);
    await a.close();
    await b.close();
  });

  test('before HELLO only HELLO is allowed; a name owned by another token is refused', async () => {
    const c = await rawMqtt(mqttUrl);
    const m = ChatProto.make(TYPES.MSG, { to: 'x', body: 'too early' });
    c.send(m);
    const err = await c.next(TYPES.ERROR);
    assert.equal(err.body.code, ERRORS.NOT_JOINED);
    assert.equal(err.body.ref, m.id);

    await (await joinAs(wsUrl, 'm2_owner')).close(); // registered via WebSocket
    const h = ChatProto.make(TYPES.HELLO, { body: { username: 'm2_owner', token: 'some-other-token-123' } });
    c.send(h);
    const refused = await c.next(TYPES.ERROR);
    assert.equal(refused.body.code, ERRORS.NAME_TAKEN, 'same rule for both protocols');
    assert.equal(refused.body.ref, h.id);
    await c.close();
  });

  test('SUBSCRIBE is refused (SUBACK 128) for any topic but your own down topic', async () => {
    const victim = await rawMqtt(mqttUrl);
    const spy = await rawMqtt(mqttUrl, { subscribe: false });
    for (const topic of [MqttBinding.downTopic(victim.clientId), 'chat/+/down', 'chat/#', '#',
                         MqttBinding.upTopic(spy.clientId)]) {
      // mqtt.js turns a SUBACK with 128 (= refused) into an error carrying the packet.
      await assert.rejects(spy.client.subscribeAsync(topic, { qos: 1 }),
        e => e.packet.cmd === 'suback' && e.packet.granted[0] === 128, `${topic} must be refused`);
    }
    const own = await spy.client.subscribeAsync(MqttBinding.downTopic(spy.clientId), { qos: 1 });
    assert.equal(own[0].qos, 1, 'own down topic is granted');
    await victim.close();
    await spy.close();
  });

  test('PUBLISH to another connection\'s topic, or retained, closes the connection', async () => {
    const victim = await rawMqtt(mqttUrl);
    for (const [topic, opts] of [[MqttBinding.upTopic(victim.clientId), {}],
                                 [MqttBinding.downTopic(victim.clientId), {}],
                                 [MqttBinding.upTopic('OWN'), { retain: true }]]) {
      const evil = await rawMqtt(mqttUrl);
      const t = topic.replace('OWN', evil.clientId);
      const closed = new Promise(r => evil.client.once('close', r));
      evil.client.publish(t, ChatProto.encode(hello('m4_evil')), { qos: 1, ...opts });
      await closed;
      await evil.close();
    }
    await sleep(50);
    assert.equal(victim.inbox.length, 0, 'victim received nothing');
    assert.ok(!server.hub.onlineUsers().includes('m4_evil'));
    await victim.close();
  });

  test('CONNECT: a malformed or already connected client id is rejected (CONNACK 2)', async () => {
    const opts = { clean: true, reconnectPeriod: 0, protocolVersion: 4 };
    await assert.rejects(mqtt.connectAsync(mqttUrl, { ...opts, clientId: 'chat/+/#' }), e => e.code === 2);
    const first = await rawMqtt(mqttUrl);
    await assert.rejects(mqtt.connectAsync(mqttUrl, { ...opts, clientId: first.clientId }), e => e.code === 2);
    // The original connection was not taken over.
    await joined(first, 'm5_first');
    await first.close();
  });

  test('cross-protocol: a WebSocket user and an MQTT user chat both ways, with presence', async () => {
    const ws = await joinAs(wsUrl, 'x1_ws');
    const mq = await joinMqttAs(mqttUrl, 'x1_mqtt');
    const online = await ws.next(TYPES.PRESENCE);
    assert.deepEqual(online.body, { username: 'x1_mqtt', status: 'online' });

    const m1 = ChatProto.make(TYPES.MSG, { to: 'x1_mqtt', body: 'from websocket' });
    ws.send(m1);
    const ack1 = await ws.next(TYPES.ACK);
    assert.equal(ack1.body.status, 'delivered');
    const got1 = await mq.next(TYPES.MSG);
    assert.deepEqual([got1.id, got1.from, got1.body, got1.seq], [m1.id, 'x1_ws', 'from websocket', ack1.body.seq]);

    const m2 = ChatProto.make(TYPES.MSG, { to: 'x1_ws', body: 'from mqtt' });
    mq.send(m2);
    const ack2 = await mq.next(TYPES.ACK);
    assert.equal(ack2.body.status, 'delivered');
    assert.ok(ack2.body.seq > ack1.body.seq, 'one seq space for both protocols');
    const got2 = await ws.next(TYPES.MSG);
    assert.deepEqual([got2.id, got2.from, got2.body], [m2.id, 'x1_mqtt', 'from mqtt']);

    await mq.close();
    assert.deepEqual((await ws.next(TYPES.PRESENCE)).body, { username: 'x1_mqtt', status: 'offline' });
    await ws.close();
  });

  test('cross-protocol offline sync and dedup: stored while offline, replayed after lastSeq', async () => {
    const mq = await joinMqttAs(mqttUrl, 'x2_mqtt');
    await mq.close();
    const ws = await joinAs(wsUrl, 'x2_ws');
    const sent = [];
    for (const body of ['while', 'you were', 'away']) {
      const m = ChatProto.make(TYPES.MSG, { to: 'x2_mqtt', body });
      ws.send(m);
      const ack = await ws.next(TYPES.ACK);
      assert.equal(ack.body.status, 'stored');
      sent.push({ id: m.id, seq: ack.body.seq });
    }
    // Retry of an already stored id: not stored or delivered again, same seq.
    ws.send(ChatProto.make(TYPES.MSG, { id: sent[0].id, to: 'x2_mqtt', body: 'while' }));
    const dup = await ws.next(TYPES.ACK);
    assert.deepEqual([dup.body.status, dup.body.seq], ['duplicate', sent[0].seq]);

    // Back online over MQTT, telling the server what it already has (nothing after the join).
    const back = await rawMqtt(mqttUrl);
    back.send(hello('x2_mqtt', { lastSeq: sent[0].seq - 1 }));
    await back.next(TYPES.WELCOME);
    const replay = [await back.next(TYPES.MSG), await back.next(TYPES.MSG), await back.next(TYPES.MSG)];
    assert.deepEqual(replay.map(m => m.seq), sent.map(s => s.seq), 'in seq order');
    const synced = await back.next(TYPES.SYNCED);
    assert.deepEqual(synced.body, { count: 3, lastSeq: sent[2].seq });

    // Reconnect with the new lastSeq: nothing to replay.
    await back.close();
    const again = await joinMqttAs(mqttUrl, 'x2_mqtt', { lastSeq: synced.body.lastSeq });
    assert.deepEqual(again.synced, { count: 0, lastSeq: synced.body.lastSeq });
    await again.close();
    await ws.close();
  });

  test('keepalive: a silent MQTT client is dropped and goes offline (no Last Will needed)', async () => {
    const watcher = await joinAs(wsUrl, 'k1_watch');
    // Hand-made MQTT 3.1.1 packets over a raw WebSocket, so nothing sends PINGREQ for us.
    const clientId = MqttBinding.newClientId(ChatProto.uuid);
    const ws = new WebSocket(mqttUrl, 'mqtt');
    await new Promise((res, rej) => { ws.once('open', res); ws.once('error', rej); });
    const str = s => { const b = Buffer.from(s); return Buffer.concat([Buffer.from([b.length >> 8, b.length & 255]), b]); };
    const packet = (header, body) => Buffer.concat([Buffer.from([header, body.length]), body]); // body < 128 bytes
    // CONNECT: protocol "MQTT", level 4, flags 0x02 (clean session), keepalive 1 s, client id.
    ws.send(packet(0x10, Buffer.concat([str('MQTT'), Buffer.from([4, 0x02, 0, 1]), str(clientId)])));
    // PUBLISH QoS 0 to our up topic, payload = ChatProto HELLO.
    const h = Buffer.from(ChatProto.encode(hello('k1_silent')));
    const pub = Buffer.concat([str(MqttBinding.upTopic(clientId)), h]);
    ws.send(Buffer.concat([Buffer.from([0x30, (pub.length & 127) | 128, pub.length >> 7]), pub]));
    assert.deepEqual((await watcher.next(TYPES.PRESENCE)).body, { username: 'k1_silent', status: 'online' });
    // ...then silence. The broker drops us after 1.5 x keepalive = 1.5 s.
    const t0 = Date.now();
    assert.deepEqual((await watcher.next(TYPES.PRESENCE, 4000)).body, { username: 'k1_silent', status: 'offline' });
    assert.ok(Date.now() - t0 >= 1000, 'not before the keepalive ran out');
    ws.terminate();
    await watcher.close();
  });

  test('ChatClient over MqttSocket talks to a ChatClient over WebSocket, with MQTT packet trace', async () => {
    const a = mqttClient('c1_mqtt');
    const trace = [];
    a.on('frame', (dir, text) => { if (text.startsWith('MQTT')) trace.push(`${dir} ${text}`); });
    const b = wsClient('c1_ws');
    await start(a, 'c1_mqtt');
    await start(b, 'c1_ws');

    const sent = a.send('c1_ws', 'hello websocket');
    const [ack] = await once(a, 'ack');
    assert.equal(ack.status, 'delivered');
    await until(() => b.received.length === 1);
    assert.equal(b.received[0].id, sent.id);

    b.send('c1_mqtt', 'hello mqtt');
    await until(() => a.received.length === 1);
    assert.equal(a.received[0].body, 'hello mqtt');
    assert.equal(a.lastSeq, a.received[0].seq);

    const text = trace.join('\n');
    assert.match(text, /out MQTT CONNECT clientId=c-[0-9a-f]{32} clean=true keepalive=30s/);
    assert.match(text, /in MQTT CONNACK returnCode=0/);
    assert.match(text, /out MQTT SUBSCRIBE chat\/c-[0-9a-f]{32}\/down qos1/);
    assert.match(text, /in MQTT SUBACK granted=\[1\]/);
    assert.match(text, /out MQTT PUBLISH chat\/c-[0-9a-f]{32}\/up qos1 id=\d+/);
    assert.match(text, /in MQTT PUBACK id=\d+/);
    a.close();
    b.close();
  });

  test('ChatClient over MQTT: lost connection -> reconnect, re-join, sync, outbox re-sent once', async () => {
    const a = await start(mqttClient('c2_mqtt'), 'c2_mqtt');
    const b = await start(wsClient('c2_ws'), 'c2_ws');

    // Cut the TCP connection under MQTT (like Wi-Fi dropping), no DISCONNECT packet.
    const firstClientId = a.ws.clientId;
    a.ws.client.stream.destroy();
    await once(a, 'reconnecting');
    // While a is offline: b writes to a (stored), and a queues a message in its outbox.
    b.send('c2_mqtt', 'missed you');
    await once(b, 'ack');
    const queued = a.send('c2_ws', 'sent while offline');
    assert.equal(a.pending.size, 1);

    await once(a, 'synced', 3000);
    assert.notEqual(a.ws.clientId, firstClientId, 'every connection gets a new client id');
    await until(() => a.pending.size === 0 && b.received.length === 1);
    assert.equal(b.received[0].id, queued.id);
    assert.deepEqual(a.received.map(m => m.body), ['missed you'], 'synced after reconnect');
    await sleep(300);
    assert.equal(b.received.length, 1, 'delivered exactly once');
    a.close();
    b.close();
  });

  test('takeover across protocols: the newer connection wins, the old MQTT one gets BYE 4001', async () => {
    const old = await start(mqttClient('t1_user'), 't1_user');
    const frames = [];
    old.on('frame', (dir, text) => frames.push(text));
    const replaced = once(old, 'replaced');
    const closed = once(old, 'status');

    const newer = await start(wsClient('t1_user'), 't1_user'); // same user, same token, WebSocket
    await replaced;
    const [status, code] = await closed;
    assert.deepEqual([status, code], ['closed', ChatProto.CLOSE_REPLACED]);
    assert.ok(frames.some(f => f.includes('"type":"BYE"')), 'BYE frame received');
    assert.equal(old.stopped, true, 'no auto-reconnect after being replaced');
    assert.deepEqual(server.hub.onlineUsers().filter(u => u === 't1_user'), ['t1_user']);

    // And back: an MQTT connection replaces the WebSocket one.
    const newest = mqttClient('t1_user');
    const wsReplaced = once(newer, 'replaced');
    await start(newest, 't1_user');
    await wsReplaced;
    await sleep(100);
    assert.equal(server.hub.users.get('t1_user').protocol, 'mqtt');
    newest.close();
  });
});

// The MQTT endpoint is a normal WebSocket path on the same port; other paths are refused.
test('only /ws and /mqtt accept WebSocket upgrades', async () => {
  const server = await startServer({ ...quiet, port: 0, dbPath: ':memory:' });
  try {
    const status = await new Promise(resolve => {
      const sock = net.connect(server.port, '127.0.0.1', () => {
        sock.write('GET /nope HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
                   'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n');
      });
      sock.once('data', d => { resolve(d.toString().split('\r\n')[0]); sock.destroy(); });
    });
    assert.equal(status, 'HTTP/1.1 404 Not Found');
  } finally {
    await server.close();
  }
});

async function joined(c, username) {
  c.send(hello(username));
  await c.next(TYPES.WELCOME);
  await c.next(TYPES.SYNCED);
}
