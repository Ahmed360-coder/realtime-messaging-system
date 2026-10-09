// Step 8: end-to-end encryption with TweetNaCl.
//   Unit:        client/e2e.js (seal / open, tampering, context binding, group key boxes, TOFU,
//                safety numbers, frame size) and the sealed-body checks in protocols/chatproto.js.
//   Integration: ChatClients with key rings over WebSocket AND MQTT: 1-to-1, groups, offline
//                sync, retries, STALE_MEMBERS re-sealing, KEY_MISMATCH, a key swapped by the
//                server, metrics, and proof that the server, its frames and its database file
//                never contain the plaintext.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const nacl = require('tweetnacl');
const ChatProto = require('../protocols/chatproto');
const MqttBinding = require('../protocols/mqtt-binding');
const E2E = require('../client/e2e');
const ChatClient = require('../client/chat-client');
const MqttSocket = require('../client/mqtt-socket');
const { startServer } = require('../server');
const { rawClient, tokenOf, hello, joinAs, joinMqttAs, once, until } = require('./helpers');
const { TYPES, ERRORS } = ChatProto;

const quiet = { host: '127.0.0.1', log: () => {} };
const decodeObj = (msg, allowed) => ChatProto.decode(ChatProto.encode(msg), allowed);

// A key ring for `me` with a fresh identity, and every user in `others` pinned (as if WELCOME had said so).
function ring(me, others = {}) {
  const r = new E2E.KeyRing({ me, identity: E2E.newIdentity() });
  for (const [user, other] of Object.entries(others)) r.learn(user, other.publicKey);
  return r;
}
// Rings for several users that all know each other's keys, and the groups they share.
function people(names, groups = {}) {
  const rings = Object.fromEntries(names.map(n => [n, ring(n)]));
  for (const a of names) {
    for (const b of names) if (a !== b) rings[a].learn(b, rings[b].publicKey);
    for (const [g, members] of Object.entries(groups)) rings[a].setMembers(g, members, 1);
  }
  return rings;
}
// An outgoing MSG sealed by `r`, as the server would forward it (from set, seq added).
function sealed(r, to, text, seq = 1) {
  const msg = ChatProto.make(TYPES.MSG, { to });
  msg.body = r.seal(msg, text);
  return { ...msg, from: r.me, seq };
}
// Flip one bit of a base64 value.
function flip(b64, i = 5) {
  const bytes = E2E.fromB64(b64);
  bytes[i] ^= 1;
  return E2E.toB64(bytes);
}

// ---------------------------------------------------------------------------------------------
// Unit tests: client/e2e.js
// ---------------------------------------------------------------------------------------------

test('identity: a Curve25519 key pair as base64; isIdentity checks that the halves belong together', () => {
  const id = E2E.newIdentity();
  assert.match(id.publicKey, ChatProto.KEY_RE);
  assert.equal(E2E.isIdentity(id), true);
  assert.equal(E2E.isIdentity({ ...id, publicKey: E2E.newIdentity().publicKey }), false);
  assert.equal(E2E.isIdentity(null), false);
  assert.equal(E2E.isIdentity({ publicKey: 'x', secretKey: 'y' }), false);
});

test('1-to-1: only the two parties can open it (the sender too); the frame is valid ChatProto', () => {
  const { alice, bob, carol } = people(['alice', 'bob', 'carol']);
  const msg = sealed(alice, 'bob', 'meet at 5 <b>sharp</b>');
  assert.deepEqual(Object.keys(msg.body), ['nonce', 'box']);
  assert.match(msg.body.nonce, ChatProto.NONCE_RE);
  assert.equal(decodeObj(msg, ChatProto.SERVER_TYPES).ok, true);
  assert.doesNotMatch(JSON.stringify(msg), /meet|sharp/);

  assert.deepEqual(bob.open(msg), { text: 'meet at 5 <b>sharp</b>', security: 'e2e' });
  assert.deepEqual(alice.open(msg), { text: 'meet at 5 <b>sharp</b>', security: 'e2e' }, 'box is symmetric: the sender reads her own history');
  assert.equal(carol.open(msg).security, 'failed');
  // reveal(): what the page gets
  const shown = bob.reveal(msg);
  assert.equal(shown.body, 'meet at 5 <b>sharp</b>');
  assert.equal(shown.security, 'e2e');
  assert.deepEqual(shown.sealed, msg.body);
});

test('every message gets a fresh random nonce: the same text twice gives different ciphertext', () => {
  const { alice } = people(['alice', 'bob']);
  const a = sealed(alice, 'bob', 'same');
  const b = sealed(alice, 'bob', 'same');
  assert.notEqual(a.body.nonce, b.body.nonce);
  assert.notEqual(a.body.box, b.body.box);
});

test('authentication: a changed bit, nonce, or envelope field makes the message fail to open', () => {
  const { alice, bob } = people(['alice', 'bob', 'mallory']);
  const msg = sealed(alice, 'bob', 'pay 10 EUR');
  // Poly1305: any modification of the ciphertext or nonce is detected.
  assert.equal(bob.open({ ...msg, body: { ...msg.body, box: flip(msg.body.box) } }).security, 'failed');
  assert.equal(bob.open({ ...msg, body: { ...msg.body, nonce: flip(msg.body.nonce, 0) } }).security, 'failed');
  // Context binding: the server cannot re-label the envelope.
  assert.equal(bob.open({ ...msg, id: ChatProto.uuid() }).security, 'failed', 'replayed under a new id');
  assert.equal(bob.open({ ...msg, ts: msg.ts + 1 }).security, 'failed', 'new time');
  assert.equal(bob.open({ ...msg, from: 'bob', to: 'alice' }).security, 'failed', 'from/to swapped (same shared key!)');
  // A message "from mallory" that alice made opens with mallory's key: fails.
  assert.equal(bob.open({ ...msg, from: 'mallory' }).security, 'failed');
  // Garbage that only looks like base64 does not throw, it just fails.
  assert.equal(bob.open({ ...msg, body: { nonce: msg.body.nonce, box: '!!!' } }).security, 'failed');
});

test('plain text is passed through but marked as not encrypted', () => {
  const { bob } = people(['alice', 'bob']);
  assert.deepEqual(bob.open({ id: ChatProto.uuid(), from: 'alice', to: 'bob', ts: 1, body: 'hi' }), { text: 'hi', security: 'plain' });
});

test('group: one ciphertext + one 80-byte key box per member (sender included); non-members cannot open', () => {
  const g = { '#g': ['alice', 'bob', 'carol'] };
  const { alice, bob, carol, dave } = people(['alice', 'bob', 'carol', 'dave'], g);
  const msg = sealed(alice, '#g', 'group secret');
  assert.deepEqual(Object.keys(msg.body.keys).sort(), ['alice', 'bob', 'carol']);
  for (const kb of Object.values(msg.body.keys)) assert.match(kb, ChatProto.KEYBOX_RE);
  assert.equal(decodeObj(msg, ChatProto.SERVER_TYPES).ok, true);
  for (const r of [alice, bob, carol]) assert.deepEqual(r.open(msg), { text: 'group secret', security: 'e2e' });
  assert.equal(dave.open(msg).security, 'failed', 'no key box for dave');
  // A key box changed in transit: fails for that member only.
  const bad = { ...msg, body: { ...msg.body, keys: { ...msg.body.keys, bob: flip(msg.body.keys.bob) } } };
  assert.equal(bob.open(bad).security, 'failed');
  assert.equal(carol.open(bad).security, 'e2e');
});

test('group: a member who knows K cannot forge a different text under the sender\'s name', () => {
  const g = { '#g': ['alice', 'bob', 'carol'] };
  const { alice, bob, carol } = people(['alice', 'bob', 'carol'], g);
  const msg = sealed(alice, '#g', 'the meeting is at 5');
  // bob opens his key box (he is a member) and learns K ...
  const nonce = E2E.fromB64(msg.body.nonce);
  const sealedKey = nacl.box.open(E2E.fromB64(msg.body.keys.bob), nonce, bob.keyOf('alice'), bob.secret);
  const K = sealedKey.subarray(0, 32);
  // ... and makes a new ciphertext under K, keeping alice's key boxes (helped by the server).
  const inner = JSON.stringify({ from: 'alice', to: '#g', id: msg.id, ts: msg.ts, text: 'the meeting is cancelled' });
  const forgedBox = nacl.secretbox(new TextEncoder().encode(inner), nonce, K);
  const forged = { ...msg, body: { ...msg.body, box: E2E.toB64(forgedBox) } };
  // carol's key box names the hash of alice's ciphertext, so the forgery is detected.
  assert.equal(carol.open(forged).security, 'failed');
  assert.equal(carol.open(msg).text, 'the meeting is at 5');
});

test('seal refuses (and sends nothing) without a usable key: NO_KEY, KEY_CHANGED, NOT_MEMBER', () => {
  const alice = ring('alice');
  const code = fn => { try { fn(); return null; } catch (e) { assert.ok(e instanceof E2E.E2EError); return e.code; } };
  assert.equal(code(() => sealed(alice, 'bob', 'x')), 'NO_KEY');
  assert.equal(code(() => sealed(alice, '#nope', 'x')), 'NOT_MEMBER');
  alice.setMembers('#g', ['alice', 'bob'], 5);
  assert.equal(code(() => sealed(alice, '#g', 'x')), 'NO_KEY');
  alice.learn('bob', E2E.newIdentity().publicKey);
  alice.learn('bob', E2E.newIdentity().publicKey); // the server now claims another key
  assert.equal(code(() => sealed(alice, 'bob', 'x')), 'KEY_CHANGED');
  assert.equal(code(() => sealed(alice, '#g', 'x')), 'KEY_CHANGED');
  // Older GROUP events never overwrite a newer member list; leaving removes the group.
  alice.setMembers('#g', ['alice'], 4);
  assert.deepEqual(alice.groups.get('#g').members, ['alice', 'bob']);
  alice.setMembers('#g', ['bob'], 6);
  assert.equal(alice.groups.has('#g'), false);
});

test('TOFU: the first key is pinned; a different one is held back until trust(); pins are saved', () => {
  const bob1 = E2E.newIdentity(), bob2 = E2E.newIdentity();
  const saved = [];
  const r = new E2E.KeyRing({ me: 'alice', identity: E2E.newIdentity(), onPin: pins => saved.push({ ...pins }) });
  assert.equal(r.status('bob'), 'unknown');
  assert.equal(r.learn('bob', bob1.publicKey), 'new');
  assert.equal(r.learn('bob', bob1.publicKey), 'same');
  assert.deepEqual(saved, [{ bob: bob1.publicKey }]);
  assert.deepEqual(r.learnAll({ bob: bob2.publicKey }), ['bob']);
  assert.equal(r.status('bob'), 'changed');
  assert.equal(E2E.toB64(r.keyOf('bob')), bob1.publicKey, 'the pinned key stays in use');
  assert.notEqual(r.safetyNumber('bob', true), r.safetyNumber('bob'), 'the new key has a different safety number');
  assert.equal(r.trust('bob'), true);
  assert.equal(r.status('bob'), 'pinned');
  assert.equal(E2E.toB64(r.keyOf('bob')), bob2.publicKey);
  assert.deepEqual(saved.at(-1), { bob: bob2.publicKey });
  // A pinned ring from an earlier page load: the same key is fine.
  const again = new E2E.KeyRing({ me: 'alice', identity: E2E.newIdentity(), pins: saved.at(-1) });
  assert.equal(again.learn('bob', bob2.publicKey), 'same');
});

test('safety number: 6 groups of 5 digits, the same on both phones, different for another key', () => {
  const { alice, bob } = people(['alice', 'bob']);
  const n = alice.safetyNumber('bob');
  assert.match(n, /^\d{5}( \d{5}){5}$/);
  assert.equal(bob.safetyNumber('alice'), n);
  const mitm = E2E.newIdentity().publicKey;
  assert.notEqual(E2E.safetyNumber('alice', alice.publicKey, 'bob', mitm), n);
});

test('worst case still fits in one frame: 2000 three-byte chars to a full group of 50 with long names', () => {
  const names = Array.from({ length: ChatProto.MAX_GROUP_MEMBERS }, (_, i) => `member_${String(i).padStart(2, '0')}_xxxxxxxxx`.slice(0, 20));
  const members = Object.fromEntries(names.map(n => [n, E2E.newIdentity()]));
  const r = new E2E.KeyRing({ me: names[0], identity: members[names[0]] });
  for (const n of names.slice(1)) r.learn(n, members[n].publicKey);
  r.setMembers('#g', names, 1);
  const msg = sealed(r, '#g', '€'.repeat(ChatProto.MAX_BODY_CHARS));
  const frame = ChatProto.encode(msg);
  assert.ok(Buffer.byteLength(frame) < ChatProto.MAX_FRAME_BYTES, `${Buffer.byteLength(frame)} bytes`);
  assert.equal(ChatProto.decode(frame, ChatProto.SERVER_TYPES).ok, true);
});

// ---------------------------------------------------------------------------------------------
// Unit tests: the ChatProto additions
// ---------------------------------------------------------------------------------------------

test('ChatProto: sealed body shapes, HELLO.key, WELCOME/USERS.keys, PRESENCE.key', () => {
  const key = E2E.newIdentity().publicKey;
  const nonce = E2E.toB64(nacl.randomBytes(24));
  const box = E2E.toB64(nacl.randomBytes(40));
  const keybox = E2E.toB64(nacl.randomBytes(80));
  const msg = (to, body) => decodeObj(ChatProto.make(TYPES.MSG, { to, body }));
  const code = r => (r.ok ? 'ok' : r.error.code);

  assert.equal(code(msg('bob', { nonce, box })), 'ok');
  assert.equal(code(msg('#g', { nonce, box, keys: { alice: keybox, bob: keybox } })), 'ok');
  assert.equal(code(msg('bob', { nonce, box, keys: { bob: keybox } })), ERRORS.BAD_FIELD, '1-to-1 has no key boxes');
  assert.equal(code(msg('#g', { nonce, box })), ERRORS.BAD_FIELD, 'group needs key boxes');
  assert.equal(code(msg('#g', { nonce, box, keys: {} })), ERRORS.BAD_FIELD);
  assert.equal(code(msg('#g', { nonce, box, keys: { 'not a name': keybox } })), ERRORS.BAD_FIELD);
  assert.equal(code(msg('#g', { nonce, box, keys: { bob: key } })), ERRORS.BAD_FIELD, 'a key box is 80 bytes');
  assert.equal(code(msg('bob', { nonce: nonce.slice(4), box })), ERRORS.BAD_FIELD);
  assert.equal(code(msg('bob', { nonce, box: 'AAAA' })), ERRORS.BAD_FIELD, 'shorter than the 16-byte tag');
  assert.equal(code(msg('bob', { nonce, box: box + '*' })), ERRORS.BAD_FIELD);
  assert.equal(code(msg('bob', { nonce, box, text: 'leak' })), ERRORS.BAD_FIELD, 'no extra fields next to the ciphertext');
  assert.equal(code(msg('bob', {})), ERRORS.BAD_FIELD);
  assert.equal(code(msg('bob', 'plain text still works')), 'ok');

  const helloOf = body => decodeObj(ChatProto.make(TYPES.HELLO, { body: { username: 'alice', token: tokenOf('alice'), ...body } }));
  assert.equal(code(helloOf({ key })), 'ok');
  assert.equal(code(helloOf({})), 'ok', 'key is optional (older clients, the Step 3-7 tests)');
  assert.equal(code(helloOf({ key: 'short' })), ERRORS.BAD_FIELD);
  const S = ChatProto.SERVER_TYPES;
  assert.equal(code(decodeObj(ChatProto.make(TYPES.WELCOME, { body: { users: [], keys: { bob: key } } }), S)), 'ok');
  assert.equal(code(decodeObj(ChatProto.make(TYPES.USERS, { body: { users: [], keys: { bob: 'x' } } }), S)), ERRORS.BAD_FIELD);
  assert.equal(code(decodeObj(ChatProto.make(TYPES.PRESENCE, { body: { username: 'bob', status: 'online', key } }), S)), 'ok');
  assert.equal(code(decodeObj(ChatProto.make(TYPES.PRESENCE, { body: { username: 'bob', status: 'online', key: 7 } }), S)), ERRORS.BAD_FIELD);
});

// ---------------------------------------------------------------------------------------------
// Integration: real server, ChatClients with key rings, WebSocket and MQTT
// ---------------------------------------------------------------------------------------------

const fast = { ackTimeoutMs: 150, maxMissedAcks: 2, backoffMinMs: 30, backoffMaxMs: 200 };

// A ChatClient with its own key ring, joined (synced) over 'ws' or 'mqtt'.
// identity: reuse a key pair (the same device after a reload).
async function client(urls, name, proto, { identity = E2E.newIdentity(), lastSeq = 0 } = {}) {
  const e2e = new E2E.KeyRing({ me: name, identity });
  const c = new ChatClient({
    url: urls.ws, WebSocket, token: tokenOf(name), e2e, ...fast,
    ...(proto === 'mqtt' && { openSocket: () => new MqttSocket(urls.mqtt) }),
  });
  c.identity = identity;
  c.lastSeq = lastSeq;
  c.received = [];
  c.wire = [];      // every frame text in and out: exactly what the server sees of this client
  c.acks = [];
  c.errors = [];
  c.keychanges = [];
  c.on('message', m => c.received.push(m));
  c.on('frame', (dir, text) => c.wire.push(text));
  c.on('ack', b => c.acks.push(b));
  c.on('serverError', b => c.errors.push(b));
  c.on('keychange', u => c.keychanges.push(u));
  const synced = once(c, 'synced');
  c.join(name);
  c.connect();
  await synced;
  return c;
}
const urlsOf = server => ({ ws: `ws://127.0.0.1:${server.port}/ws`, mqtt: `ws://127.0.0.1:${server.port}${MqttBinding.PATH}` });

// Wait until `c` has pinned the keys of `names`. A user who joins after c is announced by PRESENCE
// (with the key), which can arrive just after that user's own SYNCED.
const knows = (c, ...names) => until(() => names.every(n => c.e2e.status(n) === 'pinned'));

// Create a group through ChatClient and wait until every client's key ring knows its members.
async function makeGroup(owner, name, others) {
  owner.changeGroup('create', name, others.map(c => c.username));
  await until(() => [owner, ...others].every(c => c.e2e.groups.has(name)));
}

test('the server, the frames and the database FILE never contain the plaintext (both protocols, 1-to-1 and group)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-e2e-'));
  const dbPath = path.join(dir, 'e2e.db');
  const logs = [];
  const server = await startServer({ host: '127.0.0.1', port: 0, dbPath, log: line => logs.push(String(line)) });
  const urls = urlsOf(server);
  const SECRET_DM = 'TopSecretDirect-7731';
  const SECRET_GROUP = 'TopSecretGroup-4410';
  let clients = [];
  try {
    const alice = await client(urls, 'p_alice', 'ws');
    const bob = await client(urls, 'p_bob', 'mqtt');
    const carol = await client(urls, 'p_carol', 'ws');
    clients = [alice, bob, carol];
    await knows(alice, 'p_bob', 'p_carol');
    await knows(bob, 'p_carol');

    alice.send('p_bob', SECRET_DM);
    await until(() => bob.received.length === 1 && alice.pending.size === 0);
    assert.deepEqual([bob.received[0].body, bob.received[0].security, bob.received[0].from], [SECRET_DM, 'e2e', 'p_alice']);

    await makeGroup(bob, '#p_g', [alice, carol]);
    bob.send('#p_g', SECRET_GROUP); // from the MQTT client
    await until(() => alice.received.length === 1 && carol.received.length === 1 && bob.pending.size === 0);
    for (const c of [alice, carol]) assert.deepEqual([c.received[0].body, c.received[0].security], [SECRET_GROUP, 'e2e']);

    // What the server stores: the sealed object, flagged e2e = 1, nothing readable.
    const rows = server.store.db.prepare("SELECT body, e2e FROM messages WHERE kind = 'text' ORDER BY seq").all();
    assert.equal(rows.length, 2);
    for (const r of rows) {
      assert.equal(r.e2e, 1);
      assert.ok(ChatProto.isSealed(JSON.parse(r.body)));
    }
    assert.deepEqual(Object.keys(JSON.parse(rows[1].body).keys).sort(), ['p_alice', 'p_bob', 'p_carol']);
    assert.equal(server.store.counts().encrypted, 2);
    // Everything that crossed the network (as the clients wrote and read it), and the server log.
    const everything = [...alice.wire, ...bob.wire, ...carol.wire, ...logs].join('\n');
    for (const secret of [SECRET_DM, SECRET_GROUP]) assert.ok(!everything.includes(secret), `${secret} went over the wire`);
    assert.ok(alice.wire.some(t => t.includes('"box"')), 'and the ciphertext did');
  } finally {
    for (const c of clients) c.close();
    await server.close();
  }
  // The database files on disk (main file + write-ahead log), byte for byte.
  for (const file of fs.readdirSync(dir)) {
    const bytes = fs.readFileSync(path.join(dir, file));
    for (const secret of ['TopSecretDirect-7731', 'TopSecretGroup-4410']) {
      assert.equal(bytes.indexOf(Buffer.from(secret)), -1, `${secret} found in ${file}`);
    }
  }
  fs.rmSync(dir, { recursive: true, force: true });
});

test.describe('end-to-end encryption over both protocols', () => {
  let server, urls;
  test.before(async () => {
    server = await startServer({ ...quiet, port: 0, dbPath: ':memory:' });
    urls = urlsOf(server);
  });
  test.after(() => server.close());

  test('HELLO registers and pins the key; WELCOME / USERS / PRESENCE hand it out; another key = KEY_MISMATCH', async () => {
    const k1 = E2E.newIdentity().publicKey;
    const a = await joinAs(urls.ws, 'k_alice', { key: k1 });
    const b = await joinMqttAs(urls.mqtt, 'k_bob', { key: E2E.newIdentity().publicKey });
    const presence = await a.next(TYPES.PRESENCE);
    assert.equal(presence.body.username, 'k_bob');
    assert.match(presence.body.key, ChatProto.KEY_RE, 'a user who registers later can be written to at once');
    b.send(ChatProto.make(TYPES.LIST));
    assert.equal((await b.next(TYPES.USERS)).body.keys.k_alice, k1);
    assert.equal(server.store.publicKey('k_alice'), k1);

    // Same name, same token, different key: refused (the server will not swap keys on request).
    const again = await rawClient(urls.ws);
    again.send(hello('k_alice', { key: E2E.newIdentity().publicKey }));
    const err = await again.next(TYPES.ERROR);
    assert.equal(err.body.code, ERRORS.KEY_MISMATCH);
    assert.equal(server.store.publicKey('k_alice'), k1, 'still the first key');
    // A HELLO without a key (an old client) is still accepted and keeps the key.
    again.send(hello('k_alice'));
    assert.equal((await again.next(TYPES.WELCOME)).body.keys.k_alice, k1);
    await Promise.all([a.close(), b.close(), again.close()]);
  });

  test('1-to-1 WS -> MQTT -> WS, offline sync, and the sender reading her own history after a "reload"', async () => {
    const alice = await client(urls, 's_alice', 'ws');
    const bob = await client(urls, 's_bob', 'mqtt');
    await knows(alice, 's_bob');
    alice.send('s_bob', 'hello bob');
    await until(() => bob.received.length === 1);
    bob.send('s_alice', 'hi alice <script>x</script>');
    await until(() => alice.received.length === 1);
    assert.deepEqual([alice.received[0].body, alice.received[0].security], ['hi alice <script>x</script>', 'e2e']);

    // carol is registered but offline: the sealed message waits in SQLite and opens after sync.
    const carolId = E2E.newIdentity();
    (await client(urls, 's_carol', 'mqtt', { identity: carolId })).close();
    // alice learns carol's key from her PRESENCE online
    await knows(alice, 's_carol');
    await until(() => !server.hub.users.has('s_carol')); // her disconnect has reached the server
    alice.send('s_carol', 'for later');
    await until(() => alice.acks.some(a => a.status === 'stored'));
    const carol = await client(urls, 's_carol', 'mqtt', { identity: carolId });
    assert.deepEqual(carol.received.map(m => [m.body, m.security]), [['for later', 'e2e']]);

    // A fresh page for alice (same device keys, lastSeq 0): her whole history replays and opens,
    // her own sent messages included.
    alice.close();
    const alice2 = await client(urls, 's_alice', 'ws', { identity: alice.identity });
    assert.deepEqual(alice2.received.map(m => [m.from, m.body, m.security]), [
      ['s_alice', 'hello bob', 'e2e'],
      ['s_bob', 'hi alice <script>x</script>', 'e2e'],
      ['s_alice', 'for later', 'e2e'],
    ]);
    for (const c of [alice2, bob, carol]) c.close();
  });

  test('a retry is the identical ciphertext: ACKed as duplicate with the same seq, delivered once', async () => {
    const alice = await client(urls, 'r_alice', 'mqtt');
    const bob = await client(urls, 'r_bob', 'ws');
    await knows(alice, 'r_bob');
    const frame = alice.send('r_bob', 'once only');
    await until(() => alice.acks.length === 1);
    alice.sendFrame(frame); // the exact same frame again (the debug panel's "re-send last")
    await until(() => alice.acks.length === 2);
    assert.deepEqual(alice.acks.map(a => a.status), ['delivered', 'duplicate']);
    assert.equal(alice.acks[0].seq, alice.acks[1].seq);
    await until(() => bob.received.length === 1);
    assert.equal(bob.received[0].body, 'once only');
    alice.close();
    bob.close();
  });

  test('group across protocols: every member reads it; someone who left cannot read later messages', async () => {
    const alice = await client(urls, 'g_alice', 'ws');
    const bob = await client(urls, 'g_bob', 'mqtt');
    const carol = await client(urls, 'g_carol', 'mqtt');
    await knows(alice, 'g_bob', 'g_carol');
    await knows(bob, 'g_carol');
    await makeGroup(alice, '#g_e2e', [bob, carol]);
    alice.send('#g_e2e', 'all three');
    await until(() => bob.received.length === 1 && carol.received.length === 1);
    assert.deepEqual([bob.received[0].body, carol.received[0].body], ['all three', 'all three']);

    carol.changeGroup('leave', '#g_e2e');
    await until(() => alice.e2e.groups.get('#g_e2e').members.length === 2);
    const after = alice.send('#g_e2e', 'carol is gone');
    assert.deepEqual(Object.keys(after.body.keys).sort(), ['g_alice', 'g_bob'], 'no key box for carol any more');
    await until(() => bob.received.length === 2);
    assert.equal(bob.received[1].body, 'carol is gone');
    // Even if the server handed carol this frame, she could not open it.
    assert.equal(carol.e2e.open({ ...after, from: 'g_alice' }).security, 'failed');
    for (const c of [alice, bob, carol]) c.close();
  });

  test('STALE_MEMBERS: a message sealed for the old member list is refused before storing, re-sealed and delivered', async () => {
    const alice = await client(urls, 'm_alice', 'mqtt');
    const bob = await client(urls, 'm_bob', 'ws');
    const dave = await client(urls, 'm_dave', 'ws');
    await knows(alice, 'm_bob', 'm_dave');
    await makeGroup(alice, '#m_g', [bob]);

    // Raw: key boxes that do not match the members are refused, and nothing is stored.
    const raw = await joinAs(urls.ws, 'm_bob'); // replaces bob's session for a moment
    const before = server.store.counts().messages;
    const bad = ChatProto.make(TYPES.MSG, { to: '#m_g' });
    bad.body = { nonce: E2E.toB64(nacl.randomBytes(24)), box: E2E.toB64(nacl.randomBytes(40)), keys: { m_bob: E2E.toB64(nacl.randomBytes(80)) } };
    raw.send(bad);
    const err = await raw.next(TYPES.ERROR);
    assert.deepEqual([err.body.code, err.body.ref], [ERRORS.STALE_MEMBERS, bad.id]);
    assert.equal(server.store.counts().messages, before);
    await raw.close();

    // bob adds dave. Alice's message is sealed for the OLD list (alice, bob), as if she typed it
    // while the add was on its way: the server refuses it, ChatClient re-seals for the new list.
    const bob2 = await client(urls, 'm_bob', 'ws', { identity: bob.identity });
    bob2.changeGroup('add', '#m_g', ['m_dave']);
    await until(() => alice.e2e.groups.get('#m_g').members.length === 3 && dave.e2e.groups.has('#m_g'));
    const current = alice.e2e.groups.get('#m_g');
    alice.e2e.groups.set('#m_g', { members: ['m_alice', 'm_bob'], seq: current.seq });
    const resealed = once(alice, 'resealed');
    const sent = alice.send('#m_g', 'welcome dave');
    alice.e2e.groups.set('#m_g', current); // the GROUP event "arrives" before the ERROR
    const [again] = await resealed;
    assert.equal(again.id, sent.id, 'same id: the refused one was never stored');
    assert.deepEqual(Object.keys(again.body.keys).sort(), ['m_alice', 'm_bob', 'm_dave']);
    await until(() => dave.received.length === 1 && alice.pending.size === 0);
    assert.deepEqual([dave.received[0].body, dave.received[0].security, dave.received[0].id], ['welcome dave', 'e2e', sent.id]);
    assert.equal(alice.errors.length, 0, 'the page never saw the STALE_MEMBERS error');
    for (const c of [alice, bob, bob2, dave]) c.close();
  });

  test('a key swapped by a (malicious) server is detected: keychange, sending blocked, safety number differs', async () => {
    const alice = await client(urls, 't_alice', 'ws');
    const bob = await client(urls, 't_bob', 'mqtt');
    await knows(alice, 't_bob');
    const honest = alice.e2e.safetyNumber('t_bob');
    assert.equal(bob.e2e.safetyNumber('t_alice'), honest, 'both phones show the same 30 digits');

    // The server operator replaces bob's key with their own in the database.
    const mitm = E2E.newIdentity();
    server.store.db.prepare('UPDATE users SET public_key = ? WHERE username = ?').run(mitm.publicKey, 't_bob');
    alice.sendFrame(ChatProto.make(TYPES.LIST));
    await until(() => alice.keychanges.length === 1);
    assert.deepEqual(alice.keychanges, ['t_bob']);
    assert.throws(() => alice.send('t_bob', 'secret'), e => e.code === 'KEY_CHANGED');
    assert.equal(alice.pending.size, 0, 'nothing was queued');
    assert.notEqual(alice.e2e.safetyNumber('t_bob', true), honest, 'comparing numbers would reveal the attack');
    // Messages still go to the real (pinned) key: bob can read, the attacker cannot.
    server.store.db.prepare('UPDATE users SET public_key = ? WHERE username = ?').run(bob.e2e.publicKey, 't_bob');
    alice.sendFrame(ChatProto.make(TYPES.LIST));
    await until(() => alice.e2e.status('t_bob') === 'pinned');
    alice.send('t_bob', 'still safe');
    await until(() => bob.received.length === 1);
    assert.equal(bob.received[0].body, 'still safe');
    alice.close();
    bob.close();
  });

  test('metrics count sealed messages per protocol, never their content', async () => {
    const was = server.metrics.snapshot().protocols;
    const alice = await client(urls, 'x_alice', 'mqtt');
    const bob = await client(urls, 'x_bob', 'ws');
    await knows(alice, 'x_bob');
    alice.send('x_bob', 'counted');
    bob.sendFrame(ChatProto.make(TYPES.MSG, { to: 'x_alice', body: 'plain' }));
    await until(() => alice.received.length === 1 && bob.received.length === 1);
    assert.equal(alice.received[0].security, 'plain', 'the page marks it as not encrypted');
    const now = server.metrics.snapshot().protocols;
    assert.equal(now.mqtt.messages.e2e - was.mqtt.messages.e2e, 1);
    assert.equal(now.ws.messages.e2e - was.ws.messages.e2e, 0);
    assert.equal(now.ws.messages.sent - was.ws.messages.sent, 1);
    alice.close();
    bob.close();
  });
});
