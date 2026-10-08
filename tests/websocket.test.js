// Integration tests: start the real server on a free port and talk to it with real
// WebSocket clients (the `ws` library acting as a client), like several phones would.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const WebSocket = require('ws');
const ChatProto = require('../protocols/chatproto');
const { startServer } = require('../server');
const { TYPES, ERRORS } = ChatProto;
const { rawClient, hello, joinAs } = require('./helpers');

let server;
let url;

test.before(async () => {
  server = await startServer({ port: 0, host: '127.0.0.1', dbPath: ':memory:', log: () => {} });
  url = `ws://127.0.0.1:${server.port}/ws`;
});
test.after(() => server.close());

const client = () => rawClient(url);
const joined = username => joinAs(url, username);

test('HELLO -> WELCOME with the online list', async () => {
  const a = await joined('t1_alice');
  const b = await client();
  b.send(hello('t1_bob'));
  const w = await b.next(TYPES.WELCOME);
  assert.ok(w.body.users.includes('t1_alice'));
  assert.ok(w.body.users.includes('t1_bob'));
  await a.close(); await b.close();
});

test('1-to-1 MSG is delivered with server-set from, and sender gets ACK', async () => {
  const a = await joined('t2_alice');
  const b = await joined('t2_bob');
  const c = await joined('t2_carol');

  // Alice tries to spoof `from`; the server must overwrite it.
  const out = ChatProto.make(TYPES.MSG, { to: 't2_bob', body: 'hi bob', from: 'mallory' });
  a.send(out);

  const ack = await a.next(TYPES.ACK);
  assert.equal(ack.body.ref, out.id);
  assert.equal(ack.body.status, 'delivered');
  assert.ok(ack.body.seq > 0);

  const got = await b.next(TYPES.MSG);
  assert.equal(got.id, out.id);
  assert.equal(got.from, 't2_alice');
  assert.equal(got.body, 'hi bob');
  assert.equal(got.seq, ack.body.seq);

  // Carol must not receive Alice's private message.
  await new Promise(r => setTimeout(r, 100));
  assert.equal(c.inbox.filter(m => m.type === TYPES.MSG).length, 0);
  await Promise.all([a.close(), b.close(), c.close()]);
});

test('PRESENCE online/offline and LIST', async () => {
  const a = await joined('t3_alice');
  const b = await joined('t3_bob');
  const on = await a.next(TYPES.PRESENCE);
  assert.deepEqual(on.body, { username: 't3_bob', status: 'online' });

  a.send(ChatProto.make(TYPES.LIST, {}));
  const users = await a.next(TYPES.USERS);
  assert.ok(users.body.users.includes('t3_bob'));

  await b.close();
  const off = await a.next(TYPES.PRESENCE);
  assert.deepEqual(off.body, { username: 't3_bob', status: 'offline' });
  await a.close();
});

test('one client dropping does not affect the others', async () => {
  const a = await joined('t4_alice');
  const b = await joined('t4_bob');
  const c = await joined('t4_carol');

  b.ws.terminate(); // abrupt: no close frame, like a phone losing Wi-Fi
  await a.next(TYPES.PRESENCE); // bob online
  await a.next(TYPES.PRESENCE); // carol online
  const off = await a.next(TYPES.PRESENCE);
  assert.deepEqual(off.body, { username: 't4_bob', status: 'offline' });

  // Alice <-> Carol still works.
  a.send(ChatProto.make(TYPES.MSG, { to: 't4_carol', body: 'still here?' }));
  assert.equal((await c.next(TYPES.MSG)).body, 'still here?');
  assert.equal((await a.next(TYPES.ACK)).body.status, 'delivered');

  // Messages to offline Bob are stored, not refused (Step 3).
  const toBob = ChatProto.make(TYPES.MSG, { to: 't4_bob', body: 'hello?' });
  a.send(toBob);
  const ack = await a.next(TYPES.ACK);
  assert.equal(ack.body.ref, toBob.id);
  assert.equal(ack.body.status, 'stored');

  // Bob comes back (same device token) and gets it.
  const b2 = await joined('t4_bob');
  assert.equal((await b2.next(TYPES.MSG)).body, 'hello?');
  await Promise.all([a.close(), b2.close(), c.close()]);
});

test('malformed frames get ERROR and the connection stays open', async () => {
  const a = await joined('t5_alice');
  const cases = [
    ['not json', ERRORS.BAD_JSON],
    ['[1,2,3]', ERRORS.BAD_JSON],
    [JSON.stringify({ ...ChatProto.make(TYPES.LIST, {}), v: 9 }), ERRORS.BAD_VERSION],
    [JSON.stringify(ChatProto.make('NOPE', {})), ERRORS.BAD_TYPE],
    [JSON.stringify(ChatProto.make(TYPES.WELCOME, { body: { users: [] } })), ERRORS.BAD_TYPE],
    [JSON.stringify({ ...ChatProto.make(TYPES.LIST, {}), id: 7 }), ERRORS.BAD_FIELD],
    [JSON.stringify(ChatProto.make(TYPES.MSG, { to: 't5_bob' })), ERRORS.BAD_FIELD],
    [JSON.stringify(hello('again')), ERRORS.ALREADY_JOINED],
  ];
  for (const [frame, code] of cases) {
    a.send(frame);
    const err = await a.next(TYPES.ERROR);
    assert.equal(err.body.code, code, `frame ${frame}`);
  }
  a.ws.send(Buffer.from([1, 2, 3]), { binary: true });
  assert.equal((await a.next(TYPES.ERROR)).body.code, ERRORS.BAD_JSON);

  // Still usable afterwards.
  a.send(ChatProto.make(TYPES.LIST, {}));
  await a.next(TYPES.USERS);
  assert.equal(a.ws.readyState, WebSocket.OPEN);
  await a.close();
});

test('must HELLO first; a username owned by another token is refused', async () => {
  const x = await client();
  x.send(ChatProto.make(TYPES.MSG, { to: 'anyone', body: 'hi' }));
  assert.equal((await x.next(TYPES.ERROR)).body.code, ERRORS.NOT_JOINED);

  const a = await joined('t6_alice');
  x.send(ChatProto.make(TYPES.HELLO, { body: { username: 't6_alice', token: 'someone-elses-token' } }));
  assert.equal((await x.next(TYPES.ERROR)).body.code, ERRORS.NAME_TAKEN);
  await Promise.all([a.close(), x.close()]);
});

test('frames over maxPayload close the socket with 1009', async () => {
  const a = await client();
  const code = await new Promise(res => {
    a.ws.once('close', res);
    a.ws.send('x'.repeat(ChatProto.MAX_FRAME_BYTES + 100));
  });
  assert.equal(code, 1009);
});

test('many simultaneous clients', async () => {
  const N = 50;
  const clients = await Promise.all(Array.from({ length: N }, (_, i) => joined(`t8_u${i}`)));
  // Everyone sends one message to the next user in a ring.
  clients.forEach((c, i) => c.send(ChatProto.make(TYPES.MSG, { to: `t8_u${(i + 1) % N}`, body: `from ${i}` })));
  const received = await Promise.all(clients.map(c => c.next(TYPES.MSG)));
  received.forEach((m, i) => assert.equal(m.body, `from ${(i - 1 + N) % N}`));
  await Promise.all(clients.map(c => c.close()));
});
