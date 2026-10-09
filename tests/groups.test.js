// Step 7: group chat over both protocols. Membership (create / add / leave) and its authorization
// checks, fan-out with ACK counts, WebSocket and MQTT members in the same group, offline sync
// from joined_seq, retry dedup of a group change, one order for everyone, metrics, and
// ChatClient's outbox for group changes.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const WebSocket = require('ws');
const ChatProto = require('../protocols/chatproto');
const MqttBinding = require('../protocols/mqtt-binding');
const ChatClient = require('../client/chat-client');
const MqttSocket = require('../client/mqtt-socket');
const { startServer } = require('../server');
const { tokenOf, joinAs, joinMqttAs, sleep, once, until } = require('./helpers');
const { TYPES, ERRORS } = ChatProto;

const quiet = { host: '127.0.0.1', log: () => {} };

const group = (to, op, users = []) => ChatProto.make(TYPES.GROUP, { to, body: op === 'leave' ? { op } : { op, users } });
const text = (to, body) => ChatProto.make(TYPES.MSG, { to, body });

test.describe('group chat', () => {
  let server, wsUrl, mqttUrl;
  test.before(async () => {
    server = await startServer({ ...quiet, port: 0, dbPath: ':memory:' });
    wsUrl = `ws://127.0.0.1:${server.port}/ws`;
    mqttUrl = `ws://127.0.0.1:${server.port}${MqttBinding.PATH}`;
  });
  test.after(() => server.close());

  // Send a request and wait for the ACK or ERROR whose ref is its id (taken out of the inbox).
  async function request(c, frame) {
    c.send(frame);
    const isReply = f => (f.type === TYPES.ACK || f.type === TYPES.ERROR) && f.body.ref === frame.id;
    await until(() => c.inbox.some(isReply));
    return c.inbox.splice(c.inbox.findIndex(isReply), 1)[0];
  }

  test('create over WebSocket: everyone (creator included) gets the GROUP event; MQTT member receives', async () => {
    const alice = await joinAs(wsUrl, 'g1_alice');
    const bob = await joinMqttAs(mqttUrl, 'g1_bob');
    const carol = await joinAs(wsUrl, 'g1_carol');

    const req = group('#g1', 'create', ['g1_bob', 'g1_carol']);
    const ack = await request(alice, req);
    assert.equal(ack.type, TYPES.ACK);
    assert.equal(ack.body.ref, req.id);
    assert.deepEqual([ack.body.status, ack.body.recipients, ack.body.delivered], ['delivered', 2, 2]);

    for (const c of [alice, bob, carol]) {
      const ev = await c.next(TYPES.GROUP);
      assert.equal(ev.id, req.id, 'the event keeps the request id (dedup)');
      assert.equal(ev.seq, ack.body.seq);
      assert.equal(ev.from, 'g1_alice');
      assert.equal(ev.to, '#g1');
      assert.deepEqual(ev.body, { op: 'create', users: ['g1_bob', 'g1_carol'], members: ['g1_alice', 'g1_bob', 'g1_carol'] });
    }
    assert.deepEqual(server.store.groupMembers('#g1'), ['g1_alice', 'g1_bob', 'g1_carol']);
    await Promise.all([alice.close(), bob.close(), carol.close()]);
  });

  test('a group message is stored once, fanned out to the other members over both protocols, not echoed', async () => {
    const alice = await joinAs(wsUrl, 'g2_alice');
    const bob = await joinMqttAs(mqttUrl, 'g2_bob');
    const carol = await joinAs(wsUrl, 'g2_carol');
    await request(alice, group('#g2', 'create', ['g2_bob', 'g2_carol']));

    // From the MQTT member to the group: the WebSocket members get it.
    const m = text('#g2', 'hello from mqtt');
    const ack = await request(bob, { ...m, from: 'mallory' });
    assert.deepEqual([ack.body.status, ack.body.recipients, ack.body.delivered], ['delivered', 2, 2]);
    for (const c of [alice, carol]) {
      const got = await c.next(TYPES.MSG);
      assert.deepEqual([got.id, got.from, got.to, got.body, got.seq], [m.id, 'g2_bob', '#g2', 'hello from mqtt', ack.body.seq]);
    }
    await sleep(50);
    assert.equal(bob.inbox.filter(f => f.type === TYPES.MSG).length, 0, 'the sender gets an ACK, not a copy');
    const rows = server.store.db.prepare('SELECT COUNT(*) AS n FROM messages WHERE id = ?').get(m.id).n;
    assert.equal(rows, 1, 'one row, whatever the group size');
    await Promise.all([alice.close(), bob.close(), carol.close()]);
  });

  test('authorization: only members send / add / leave; names, users and size are checked', async () => {
    const alice = await joinAs(wsUrl, 'g3_alice');
    const bob = await joinAs(wsUrl, 'g3_bob');
    const eve = await joinMqttAs(mqttUrl, 'g3_eve');
    await request(alice, group('#g3', 'create', ['g3_bob']));

    const code = async (c, frame) => (await request(c, frame)).body.code;
    assert.equal(await code(eve, text('#g3', 'let me in')), ERRORS.NOT_MEMBER);
    assert.equal(await code(eve, group('#g3', 'add', ['g3_eve'])), ERRORS.NOT_MEMBER, 'cannot add yourself');
    assert.equal(await code(eve, group('#g3', 'leave')), ERRORS.NOT_MEMBER);
    assert.equal(await code(eve, text('#nope', 'hi')), ERRORS.UNKNOWN_GROUP);
    assert.equal(await code(alice, group('#nope', 'add', ['g3_eve'])), ERRORS.UNKNOWN_GROUP);
    assert.equal(await code(eve, group('#g3', 'create', [])), ERRORS.GROUP_EXISTS);
    assert.equal(await code(alice, group('#g3', 'add', ['g3_ghost'])), ERRORS.UNKNOWN_USER);
    assert.equal(await code(alice, group('#g3_x', 'create', ['g3_ghost'])), ERRORS.UNKNOWN_USER);
    assert.equal(server.store.getGroup('#g3_x'), null, 'a refused create leaves nothing behind');
    assert.equal(await code(alice, group('#g3', 'add', ['g3_bob'])), ERRORS.BAD_FIELD, 'already a member');
    assert.equal(await code(alice, ChatProto.make(TYPES.GROUP, { to: '#g3', body: { op: 'kick', users: ['g3_bob'] } })), ERRORS.BAD_FIELD);

    // Size cap: 50 members. Register 49 more users directly in the store, then try to add one too many.
    const many = Array.from({ length: 49 }, (_, i) => `g3_u${i}`);
    for (const u of [...many, 'g3_last']) server.store.claimUser(u, tokenOf(u));
    assert.equal((await request(alice, group('#g3_big', 'create', many))).type, TYPES.ACK);
    assert.equal(server.store.groupMembers('#g3_big').length, 50);
    assert.equal(await code(alice, group('#g3_big', 'add', ['g3_last'])), ERRORS.GROUP_FULL);

    // Members may add and leave. Eve, added, now gets messages; Bob, gone, does not.
    assert.equal((await request(bob, group('#g3', 'add', ['g3_eve']))).type, TYPES.ACK);
    assert.equal((await request(bob, group('#g3', 'leave'))).type, TYPES.ACK);
    assert.deepEqual(server.store.groupMembers('#g3'), ['g3_alice', 'g3_eve']);
    assert.equal(await code(bob, text('#g3', 'still here?')), ERRORS.NOT_MEMBER);
    await Promise.all([alice.close(), bob.close(), eve.close()]);
  });

  test('membership changes reach the members before and after the change, with the new member list', async () => {
    const alice = await joinAs(wsUrl, 'g4_alice');
    const bob = await joinMqttAs(mqttUrl, 'g4_bob');
    const carol = await joinAs(wsUrl, 'g4_carol');
    await request(alice, group('#g4', 'create', ['g4_bob']));
    await alice.next(TYPES.GROUP);
    await bob.next(TYPES.GROUP);

    await request(bob, group('#g4', 'add', ['g4_carol']));
    for (const c of [alice, bob, carol]) {
      const ev = await c.next(TYPES.GROUP);
      assert.deepEqual([ev.body.op, ev.body.users, ev.body.members], ['add', ['g4_carol'], ['g4_alice', 'g4_bob', 'g4_carol']]);
    }
    await request(alice, group('#g4', 'leave'));
    for (const c of [alice, bob, carol]) {
      const ev = await c.next(TYPES.GROUP);
      assert.deepEqual([ev.from, ev.body.op, ev.body.members], ['g4_alice', 'leave', ['g4_bob', 'g4_carol']]);
    }
    // Alice left: the next message goes to bob's and carol's copies only.
    const ack = await request(carol, text('#g4', 'after alice left'));
    assert.deepEqual([ack.body.recipients, ack.body.delivered], [1, 1]);
    await sleep(50);
    assert.equal(alice.inbox.filter(f => f.type === TYPES.MSG).length, 0);
    await Promise.all([alice.close(), bob.close(), carol.close()]);
  });

  test('offline member: stored, ACK says delivered to 1 of 2, synced from joined_seq in seq order', async () => {
    const alice = await joinAs(wsUrl, 'g5_alice');
    const bob = await joinAs(wsUrl, 'g5_bob');
    const carol = await joinMqttAs(mqttUrl, 'g5_carol');
    await request(alice, group('#g5', 'create', ['g5_bob']));
    const before = await request(alice, text('#g5', 'before carol'));
    await carol.close(); // carol goes offline, still registered
    await until(() => !server.hub.users.has('g5_carol'));
    const lastSeq = before.body.seq;

    const add = await request(bob, group('#g5', 'add', ['g5_carol']));
    assert.deepEqual([add.body.status, add.body.recipients, add.body.delivered], ['stored', 2, 1]);
    const m1 = await request(alice, text('#g5', 'carol is offline'));
    assert.deepEqual([m1.body.status, m1.body.recipients, m1.body.delivered], ['stored', 2, 1]);
    const m2 = await request(bob, text('#g5', 'second'));

    // Carol comes back over WebSocket this time, from her old cursor.
    const back = await joinAs(wsUrl, 'g5_carol', { lastSeq });
    assert.deepEqual(back.synced, { count: 3, lastSeq: m2.body.seq });
    const frames = back.inbox.filter(f => f.type === TYPES.GROUP || f.type === TYPES.MSG);
    assert.deepEqual(frames.map(f => [f.type, f.seq]), [[TYPES.GROUP, add.body.seq], [TYPES.MSG, m1.body.seq], [TYPES.MSG, m2.body.seq]]);
    assert.deepEqual(frames[0].body.members, ['g5_alice', 'g5_bob', 'g5_carol']);

    // A fresh device (lastSeq 0) also starts at the "added" event, not at the group's start.
    const fresh = server.hub.store.messagesFor('g5_carol', 0).filter(m => m.to === '#g5');
    assert.equal(fresh[0].seq, add.body.seq);
    assert.ok(!fresh.some(m => m.body === 'before carol'));
    await Promise.all([alice.close(), bob.close(), back.close()]);
  });

  test('a retried group change (lost ACK) is applied once and ACKed as duplicate', async () => {
    const alice = await joinAs(wsUrl, 'g6_alice');
    const bob = await joinAs(wsUrl, 'g6_bob');
    const req = group('#g6', 'create', ['g6_bob']);
    alice.send(req);
    alice.send(req); // same id again, as ChatClient does after an ACK timeout
    const first = await alice.next(TYPES.ACK);
    const second = await alice.next(TYPES.ACK);
    assert.equal(first.body.status, 'delivered');
    assert.deepEqual([second.body.status, second.body.seq], ['duplicate', first.body.seq]);
    await sleep(50);
    assert.equal(bob.inbox.filter(f => f.type === TYPES.GROUP).length, 1, 'one event, not two');
    assert.equal(alice.inbox.filter(f => f.type === TYPES.ERROR).length, 0, 'no GROUP_EXISTS for a retry');

    // A retried group message after the sender LEFT still gets its ACK back (dedup comes first).
    const m = text('#g6', 'sent just before leaving');
    const ack = await request(alice, m);
    await request(alice, group('#g6', 'leave'));
    const again = await request(alice, m);
    assert.deepEqual([again.type, again.body.status, again.body.seq], [TYPES.ACK, 'duplicate', ack.body.seq]);
    await Promise.all([alice.close(), bob.close()]);
  });

  test('everyone sees the same order: interleaved senders, every member gets ascending seqs', async () => {
    const names = ['g7_a', 'g7_b', 'g7_c'];
    const [a, b, c] = [await joinAs(wsUrl, names[0]), await joinMqttAs(mqttUrl, names[1]), await joinAs(wsUrl, names[2])];
    await request(a, group('#g7', 'create', [names[1], names[2]]));
    // a and b fire 10 messages each without waiting.
    for (let i = 0; i < 10; i++) { a.send(text('#g7', `a${i}`)); b.send(text('#g7', `b${i}`)); }
    await until(() => c.inbox.filter(f => f.type === TYPES.MSG).length === 20);
    await until(() => a.inbox.filter(f => f.type === TYPES.MSG).length === 10 && b.inbox.filter(f => f.type === TYPES.MSG).length === 10);
    const seqs = cl => cl.inbox.filter(f => f.type === TYPES.MSG).map(f => f.seq);
    for (const cl of [a, b, c]) {
      const s = seqs(cl);
      assert.deepEqual(s, [...s].sort((x, y) => x - y), 'arrives in seq order');
    }
    // c saw everything; a and b saw each other's messages in the same relative order as c.
    const order = c.inbox.filter(f => f.type === TYPES.MSG).map(f => f.body);
    assert.deepEqual(a.inbox.filter(f => f.type === TYPES.MSG).map(f => f.body), order.filter(x => x.startsWith('b')));
    assert.deepEqual(b.inbox.filter(f => f.type === TYPES.MSG).map(f => f.body), order.filter(x => x.startsWith('a')));
    await Promise.all([a.close(), b.close(), c.close()]);
  });

  test('metrics: a group message counts once as sent, and once per recipient copy', async () => {
    const snap = () => server.metrics.snapshot().protocols;
    const alice = await joinMqttAs(mqttUrl, 'g8_alice');
    const bob = await joinAs(wsUrl, 'g8_bob');
    const carol = await joinMqttAs(mqttUrl, 'g8_carol');
    await joinAs(wsUrl, 'g8_dave').then(d => d.close());
    await until(() => !server.hub.users.has('g8_dave'));

    const was = snap();
    await request(alice, group('#g8', 'create', ['g8_bob', 'g8_carol', 'g8_dave']));
    await request(alice, text('#g8', 'to three others, two online'));
    await until(() => bob.inbox.some(f => f.type === TYPES.MSG) && carol.inbox.some(f => f.type === TYPES.MSG));
    await until(() => snap().ws.messages.received - was.ws.messages.received === 1 &&
                      snap().mqtt.messages.received - was.mqtt.messages.received === 1);
    const now = snap();
    const d = (name, key) => now[name].messages[key] - was[name].messages[key];
    assert.equal(d('mqtt', 'sent'), 1, 'one logical message');
    assert.equal(d('mqtt', 'group'), 1);
    assert.equal(d('mqtt', 'delivered'), 2, 'two live copies');
    assert.equal(d('mqtt', 'stored'), 1, 'one copy waits for dave');
    assert.equal(d('ws', 'received') + d('mqtt', 'received'), 2, 'received counts per copy, under the recipient protocol');
    assert.equal(now.mqtt.groupChanges - was.mqtt.groupChanges, 1, 'the create');
    assert.equal(now.mqtt.latency.deliver.count - was.mqtt.latency.deliver.count, 1);
    assert.equal(now.ws.latency.deliver.count - was.ws.latency.deliver.count, 1);
    assert.equal(server.metrics.snapshot().database.groups >= 1, true);
    await Promise.all([alice.close(), bob.close(), carol.close()]);
  });

  test('ChatClient: group change through the outbox survives a dropped connection; MQTT and WS clients chat', async () => {
    const opts = { ackTimeoutMs: 150, maxMissedAcks: 2, backoffMinMs: 30, backoffMaxMs: 200 };
    const mk = (name, mqttProto) => {
      const c = new ChatClient({
        url: wsUrl, WebSocket, token: tokenOf(name), ...opts,
        ...(mqttProto && { openSocket: () => new MqttSocket(mqttUrl) }),
      });
      c.events = [];
      c.received = [];
      c.on('group', m => c.events.push(m));
      c.on('message', m => c.received.push(m));
      return c;
    };
    const alice = mk('g9_alice', true);
    const bob = mk('g9_bob', false);
    for (const [c, n] of [[alice, 'g9_alice'], [bob, 'g9_bob']]) { c.join(n); c.connect(); await once(c, 'synced'); }

    // Offline: the change waits in the outbox, then goes out after the reconnect + SYNCED.
    alice.dropConnection('test');
    const req = alice.changeGroup('create', '#g9', ['g9_bob']);
    assert.deepEqual(alice.outbox().map(m => m.id), [req.id]);
    await until(() => alice.pending.size === 0 && alice.events.length === 1 && bob.events.length === 1);
    assert.deepEqual(alice.events[0].body.members, ['g9_alice', 'g9_bob']);
    assert.equal(alice.events[0].id, req.id, 'our own change comes back as the event');

    bob.send('#g9', 'hi from ws');
    await until(() => alice.received.length === 1);
    assert.deepEqual([alice.received[0].from, alice.received[0].to], ['g9_bob', '#g9']);
    assert.equal(alice.lastSeq, alice.received[0].seq, 'GROUP and MSG move the same sync cursor');

    // restore() after a "reload": a saved GROUP change is not marked as seen, so its event still applies.
    const c2 = mk('g9_alice2', false);
    c2.restore([ChatProto.make(TYPES.GROUP, { to: '#g9b', body: { op: 'create', users: [] } })]);
    assert.equal(c2.seen.size, 0);

    alice.close();
    bob.close();
  });
});
