// Step 3 reliability tests: ACK + seq, dedup, offline delivery, offline sync, takeover,
// server restart, and the ChatClient's retry / reconnect / dedup logic.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const ChatProto = require('../protocols/chatproto');
const ChatClient = require('../client/chat-client');
const { startServer } = require('../server');
const { rawClient, tokenOf, hello, joinAs, sleep, once } = require('./helpers');
const { TYPES, ERRORS } = ChatProto;

const quiet = { host: '127.0.0.1', log: () => {} };

function tempDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-rel-'));
  return { file: path.join(dir, 'chat.db'), cleanup: () => fs.rmSync(dir, { recursive: true, force: true }) };
}

// A ChatClient running in Node, with short timers so tests are fast.
function nodeClient(url, username, opts = {}) {
  const c = new ChatClient({
    url, token: tokenOf(username), WebSocket,
    ackTimeoutMs: 100, maxMissedAcks: 2, backoffMinMs: 30, backoffMaxMs: 200, ...opts,
  });
  c.received = [];
  c.on('message', m => c.received.push(m));
  return c;
}

// Wait until cond() is true (polling), or fail.
async function until(cond, ms = 3000) {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error('condition not met in time');
    await sleep(10);
  }
}

// ---------------------------------------------------------------------------------------
// Server side, raw frames

test.describe('server reliability (raw frames)', () => {
  let server, url;
  test.before(async () => {
    server = await startServer({ ...quiet, port: 0, dbPath: ':memory:' });
    url = `ws://127.0.0.1:${server.port}/ws`;
  });
  test.after(() => server.close());

  test('ACK carries the stored seq, and seqs increase', async () => {
    const a = await joinAs(url, 'r1_alice');
    const b = await joinAs(url, 'r1_bob');
    const seqs = [];
    for (const body of ['one', 'two', 'three']) {
      const m = ChatProto.make(TYPES.MSG, { to: 'r1_bob', body });
      a.send(m);
      const ack = await a.next(TYPES.ACK);
      assert.equal(ack.body.ref, m.id);
      assert.equal(ack.body.status, 'delivered');
      seqs.push(ack.body.seq);
      assert.equal((await b.next(TYPES.MSG)).seq, ack.body.seq);
    }
    assert.ok(seqs[0] < seqs[1] && seqs[1] < seqs[2]);
    // The message was on disk before the ACK went out.
    assert.equal(server.store.messagesFor('r1_bob').length, 3);
    await Promise.all([a.close(), b.close()]);
  });

  test('a retried id is ACKed again with the same seq but stored and delivered only once', async () => {
    const a = await joinAs(url, 'r2_alice');
    const b = await joinAs(url, 'r2_bob');
    const m = ChatProto.make(TYPES.MSG, { to: 'r2_bob', body: 'only once please' });
    a.send(m);
    a.send(m); // e.g. the first ACK was lost and the client retried
    const ack1 = await a.next(TYPES.ACK);
    const ack2 = await a.next(TYPES.ACK);
    assert.equal(ack1.body.status, 'delivered');
    assert.equal(ack2.body.status, 'duplicate');
    assert.equal(ack2.body.seq, ack1.body.seq);

    await b.next(TYPES.MSG);
    await sleep(100);
    assert.equal(b.inbox.filter(x => x.type === TYPES.MSG).length, 0, 'second copy must not be delivered');
    assert.equal(server.store.messagesFor('r2_bob').length, 1);
    await Promise.all([a.close(), b.close()]);
  });

  test('a retry after reconnecting (ACK lost with the connection) is still deduplicated', async () => {
    const b = await joinAs(url, 'r3_bob');
    let a = await joinAs(url, 'r3_alice');
    const m = ChatProto.make(TYPES.MSG, { to: 'r3_bob', body: 'did you get this?' });
    a.send(m);
    a.ws.terminate(); // connection dies; alice never reads the ACK
    await b.next(TYPES.MSG);

    a = await joinAs(url, 'r3_alice');
    a.send(m); // alice's client retries the same id on the new connection
    const ack = await a.next(TYPES.ACK);
    assert.equal(ack.body.status, 'duplicate');
    await sleep(100);
    assert.equal(b.inbox.filter(x => x.type === TYPES.MSG).length, 0);
    await Promise.all([a.close(), b.close()]);
  });

  test('messages to an offline user are stored, then replayed in order on reconnect', async () => {
    let b = await joinAs(url, 'r4_bob');
    assert.deepEqual(b.synced, { count: 0, lastSeq: 0 });
    await b.close();

    const a = await joinAs(url, 'r4_alice');
    const sent = [];
    for (const body of ['are', 'you', 'there?']) {
      const m = ChatProto.make(TYPES.MSG, { to: 'r4_bob', body });
      a.send(m);
      const ack = await a.next(TYPES.ACK);
      assert.equal(ack.body.status, 'stored');
      sent.push(ack.body.seq);
    }

    b = await rawClient(url);
    b.send(hello('r4_bob', { lastSeq: 0 }));
    await b.next(TYPES.WELCOME);
    const got = [await b.next(TYPES.MSG), await b.next(TYPES.MSG), await b.next(TYPES.MSG)];
    const synced = await b.next(TYPES.SYNCED);
    assert.deepEqual(got.map(m => m.body), ['are', 'you', 'there?']);
    assert.deepEqual(got.map(m => m.seq), sent);
    assert.ok(got.every(m => m.from === 'r4_alice'));
    assert.deepEqual(synced.body, { count: 3, lastSeq: sent[2] });
    await Promise.all([a.close(), b.close()]);
  });

  test('sync only replays messages after lastSeq', async () => {
    const a = await joinAs(url, 'r5_alice');
    let b = await joinAs(url, 'r5_bob');
    const seqs = [];
    for (const body of ['1', '2', '3']) {
      a.send(ChatProto.make(TYPES.MSG, { to: 'r5_bob', body }));
      seqs.push((await a.next(TYPES.ACK)).body.seq);
    }
    await b.close();

    b = await joinAs(url, 'r5_bob', { lastSeq: seqs[1] }); // "I already have up to message 2"
    assert.deepEqual(b.synced, { count: 1, lastSeq: seqs[2] });
    assert.equal(b.inbox.filter(m => m.type === TYPES.MSG).map(m => m.body).join(), '3');
    await b.close();

    b = await joinAs(url, 'r5_bob', { lastSeq: seqs[2] }); // fully up to date
    assert.deepEqual(b.synced, { count: 0, lastSeq: seqs[2] });
    await Promise.all([a.close(), b.close()]);
  });

  test('messages to never-registered users get UNKNOWN_USER', async () => {
    const a = await joinAs(url, 'r6_alice');
    const m = ChatProto.make(TYPES.MSG, { to: 'r6_ghost', body: 'hello?' });
    a.send(m);
    const err = await a.next(TYPES.ERROR);
    assert.equal(err.body.code, ERRORS.UNKNOWN_USER);
    assert.equal(err.body.ref, m.id);
    await a.close();
  });

  test('an id already used by another sender is refused, not acknowledged', async () => {
    const a = await joinAs(url, 'r7_alice');
    const c = await joinAs(url, 'r7_carol');
    const m = ChatProto.make(TYPES.MSG, { to: 'r7_carol', body: 'mine' });
    a.send(m);
    await a.next(TYPES.ACK);
    c.send({ ...m, to: 'r7_alice', body: 'stolen id' });
    assert.equal((await c.next(TYPES.ERROR)).body.code, ERRORS.BAD_FIELD);
    await Promise.all([a.close(), c.close()]);
  });

  test('same token reconnecting takes over the old (zombie) session with close code 4001', async () => {
    const watcher = await joinAs(url, 'r8_watcher');
    const old = await joinAs(url, 'r8_bob');
    await watcher.next(TYPES.PRESENCE); // bob online
    const closed = new Promise(r => old.ws.once('close', code => r(code)));

    const fresh = await joinAs(url, 'r8_bob'); // e.g. phone back on Wi-Fi before the heartbeat noticed
    assert.equal(await closed, ChatProto.CLOSE_REPLACED);

    // No offline/online flapping for the others, and the new session works.
    await sleep(100);
    assert.equal(watcher.inbox.filter(m => m.type === TYPES.PRESENCE).length, 0);
    watcher.send(ChatProto.make(TYPES.MSG, { to: 'r8_bob', body: 'still you?' }));
    assert.equal((await fresh.next(TYPES.MSG)).body, 'still you?');
    assert.deepEqual(server.hub.onlineUsers().filter(u => u.startsWith('r8_')), ['r8_bob', 'r8_watcher']);
    await Promise.all([watcher.close(), fresh.close()]);
  });
});

// ---------------------------------------------------------------------------------------
// Server restart with a real database file

test('server restart: stored messages and seq numbers survive', async () => {
  const db = tempDb();
  try {
    let server = await startServer({ ...quiet, port: 0, dbPath: db.file });
    let url = `ws://127.0.0.1:${server.port}/ws`;
    const b = await joinAs(url, 'rs_bob');
    await b.close();
    const a = await joinAs(url, 'rs_alice');
    a.send(ChatProto.make(TYPES.MSG, { to: 'rs_bob', body: 'sent before the restart' }));
    const before = (await a.next(TYPES.ACK)).body.seq;
    await a.close();
    await server.close(); // "crash" and restart

    server = await startServer({ ...quiet, port: 0, dbPath: db.file });
    url = `ws://127.0.0.1:${server.port}/ws`;
    const b2 = await joinAs(url, 'rs_bob');
    const replayed = b2.inbox.find(m => m.type === TYPES.MSG);
    assert.equal(replayed.body, 'sent before the restart');
    assert.equal(replayed.seq, before);

    const a2 = await joinAs(url, 'rs_alice');
    a2.send(ChatProto.make(TYPES.MSG, { to: 'rs_bob', body: 'after' }));
    assert.ok((await a2.next(TYPES.ACK)).body.seq > before, 'seq keeps increasing across restarts');
    await Promise.all([a2.close(), b2.close()]);
    await server.close();
  } finally {
    db.cleanup();
  }
});

// ---------------------------------------------------------------------------------------
// ChatClient (the browser's reliability logic) against a fake, misbehaving server

// A fake ChatProto server we fully control. `onMsg(msg, ws, connectionNumber)` decides
// whether to ACK; every HELLO gets WELCOME + optional extra frames + SYNCED.
async function fakeServer({ onMsg, afterWelcome = () => [] }) {
  const wss = new WebSocket.WebSocketServer({ port: 0, host: '127.0.0.1' });
  await new Promise(r => wss.once('listening', r));
  const log = { connections: 0, hellos: [], msgs: [] };
  wss.on('connection', ws => {
    const conn = ++log.connections;
    ws.on('message', raw => {
      const msg = JSON.parse(raw);
      const reply = m => ws.send(ChatProto.encode(m));
      if (msg.type === TYPES.HELLO) {
        log.hellos.push(msg.body);
        reply(ChatProto.make(TYPES.WELCOME, { body: { username: msg.body.username, users: [], known: [] } }));
        for (const m of afterWelcome()) reply(m);
        reply(ChatProto.make(TYPES.SYNCED, { body: { count: 0, lastSeq: msg.body.lastSeq } }));
      } else if (msg.type === TYPES.MSG) {
        log.msgs.push({ id: msg.id, conn });
        onMsg(msg, reply, conn);
      }
    });
  });
  return {
    url: `ws://127.0.0.1:${wss.address().port}`,
    log,
    close: () => new Promise(r => { for (const c of wss.clients) c.terminate(); wss.close(r); }),
  };
}

test('ChatClient re-sends until ACKed; after missed ACKs it reconnects and still delivers', async () => {
  // Connection 1 behaves like a half-open socket: it swallows every MSG.
  // Connection 2 works normally.
  const fake = await fakeServer({
    onMsg: (msg, reply, conn) => {
      if (conn > 1) reply(ChatProto.make(TYPES.ACK, { body: { ref: msg.id, seq: 1, status: 'delivered' } }));
    },
  });
  const c = nodeClient(fake.url, 'cc_alice');
  const retries = [];
  c.on('retry', (m, n) => retries.push(n));
  c.join('cc_alice');
  c.connect();
  await once(c, 'synced');

  const sent = c.send('cc_bob', 'please arrive');
  const [ack] = await once(c, 'ack', 3000);
  assert.equal(ack.ref, sent.id);
  assert.equal(c.pending.size, 0);

  // 2 tries on the silent connection, then reconnect + re-send once on the good one.
  assert.deepEqual(retries, [2]);
  assert.equal(fake.log.connections, 2);
  assert.deepEqual(fake.log.msgs, [{ id: sent.id, conn: 1 }, { id: sent.id, conn: 1 }, { id: sent.id, conn: 2 }]);
  assert.equal(fake.log.hellos.length, 2, 'client re-joined automatically');
  c.close();
  await fake.close();
});

test('ChatClient ignores a message id it has already seen, but still advances lastSeq', async () => {
  const dup = { ...ChatProto.make(TYPES.MSG, { to: 'cc_bob', body: 'twice' }), from: 'cc_x', seq: 7 };
  const fake = await fakeServer({ onMsg: () => {}, afterWelcome: () => [dup, dup] });
  const c = nodeClient(fake.url, 'cc_bob');
  const dups = [];
  c.on('duplicate', m => dups.push(m.id));
  c.join('cc_bob');
  c.connect();
  await once(c, 'synced');
  assert.equal(c.received.length, 1);
  assert.deepEqual(dups, [dup.id]);
  assert.equal(c.lastSeq, 7);
  c.close();
  await fake.close();
});

test('ChatClient: server restart mid-conversation loses nothing and duplicates nothing', async () => {
  const db = tempDb();
  try {
    let server = await startServer({ ...quiet, port: 0, dbPath: db.file });
    const port = server.port;
    const url = `ws://127.0.0.1:${port}/ws`;
    const alice = nodeClient(url, 'cr_alice');
    const bob = nodeClient(url, 'cr_bob');
    for (const [c, name] of [[alice, 'cr_alice'], [bob, 'cr_bob']]) { c.join(name); c.connect(); }
    await Promise.all([once(alice, 'synced'), once(bob, 'synced')]);

    alice.send('cr_bob', 'before');
    await until(() => bob.received.length === 1);

    await server.close(); // both clients lose their connection and start backing off
    await until(() => !alice.joined && !bob.joined);
    alice.send('cr_bob', 'while the server is down'); // stays pending
    assert.equal(alice.pending.size, 1);

    server = await startServer({ ...quiet, port, dbPath: db.file }); // same port, same database
    await until(() => alice.pending.size === 0 && bob.received.length === 2, 5000);

    alice.send('cr_bob', 'after');
    await until(() => bob.received.length === 3);
    await sleep(100);
    assert.deepEqual(bob.received.map(m => m.body), ['before', 'while the server is down', 'after']);
    assert.ok(bob.received[0].seq < bob.received[1].seq && bob.received[1].seq < bob.received[2].seq);
    assert.equal(bob.lastSeq, bob.received[2].seq);

    alice.close(); bob.close();
    await server.close();
  } finally {
    db.cleanup();
  }
});

test('ChatClient outbox survives a "page reload": restored messages are sent once, never twice', async () => {
  const server = await startServer({ ...quiet, port: 0, dbPath: ':memory:' });
  const url = `ws://127.0.0.1:${server.port}/ws`;
  const bob = nodeClient(url, 'ob_bob');
  bob.join('ob_bob'); bob.connect();
  await once(bob, 'synced');

  // Page 1: alice sends two messages while offline (never connected), saving her outbox.
  const page1 = nodeClient(url, 'ob_alice');
  let saved = [];
  page1.on('outbox', msgs => { saved = msgs; });
  page1.send('ob_bob', 'written offline 1');
  page1.send('ob_bob', 'written offline 2');
  assert.equal(saved.length, 2);
  // Pretend message 1 actually reached the server before the reload (its ACK was lost).
  const a = await joinAs(url, 'ob_alice');
  a.send(saved[0]);
  await a.next(TYPES.ACK);
  await a.close();
  page1.close(); // reload: page 1's memory is gone, only `saved` remains

  // Page 2 restores the outbox and connects.
  const page2 = nodeClient(url, 'ob_alice');
  const acks = [];
  page2.on('ack', body => acks.push(body.status));
  page2.restore(JSON.parse(JSON.stringify(saved)));
  page2.join('ob_alice'); page2.connect();
  await until(() => page2.pending.size === 0);
  assert.deepEqual(acks.sort(), ['delivered', 'duplicate']);

  await sleep(100);
  assert.deepEqual(bob.received.map(m => m.body), ['written offline 1', 'written offline 2']);
  page2.close(); bob.close();
  await server.close();
});

test('ChatClient replaced by a newer connection of the same user does not reconnect', async () => {
  const server = await startServer({ ...quiet, port: 0, dbPath: ':memory:' });
  const url = `ws://127.0.0.1:${server.port}/ws`;
  const tab1 = nodeClient(url, 'tab_user');
  tab1.join('tab_user'); tab1.connect();
  await once(tab1, 'synced');

  const replaced = once(tab1, 'replaced');
  const tab2 = nodeClient(url, 'tab_user');
  tab2.join('tab_user'); tab2.connect();
  await replaced;
  await sleep(300); // longer than the backoff: tab1 must stay quiet
  assert.equal(tab1.stopped, true);
  assert.equal(tab2.joined, true);
  tab2.close();
  await server.close();
});
