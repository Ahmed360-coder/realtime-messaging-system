// Unit tests for server/core/store.js (SQLite storage), no network involved.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Store } = require('../server/core/store');

const TOKEN_A = 'token-alice-0123456789';
const TOKEN_B = 'token-bob-0123456789';

function freshStore() {
  const s = new Store(':memory:');
  s.claimUser('alice', TOKEN_A);
  s.claimUser('bob', TOKEN_B);
  return s;
}
let n = 0;
const msg = (from, to, body) => ({ id: `00000000-0000-4000-8000-${String(++n).padStart(12, '0')}`, from, to, body, ts: Date.now() });

function tempDbPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-store-'));
  return { file: path.join(dir, 'test.db'), cleanup: () => fs.rmSync(dir, { recursive: true, force: true }) };
}

test('claimUser registers a name and then only accepts the same token', () => {
  const s = new Store(':memory:');
  assert.equal(s.claimUser('alice', TOKEN_A), true);  // registers
  assert.equal(s.claimUser('alice', TOKEN_A), true);  // same device again
  assert.equal(s.claimUser('alice', TOKEN_B), false); // someone else
  assert.deepEqual(s.knownUsers(), ['alice']);
  // Only a hash is stored, never the token itself.
  const row = s.db.prepare('SELECT token_hash FROM users WHERE username = ?').get('alice');
  assert.notEqual(row.token_hash, TOKEN_A);
  assert.match(row.token_hash, /^[0-9a-f]{64}$/);
  s.close();
});

test('saveMessage assigns increasing seq numbers', () => {
  const s = freshStore();
  const seqs = [1, 2, 3].map(i => s.saveMessage(msg('alice', 'bob', `m${i}`)).seq);
  assert.ok(seqs[0] < seqs[1] && seqs[1] < seqs[2]);
  s.close();
});

test('saving the same id twice is a duplicate: same seq, nothing new stored', () => {
  const s = freshStore();
  const m = msg('alice', 'bob', 'once');
  const first = s.saveMessage(m);
  const again = s.saveMessage(m);
  assert.equal(first.duplicate, false);
  assert.equal(again.duplicate, true);
  assert.equal(again.seq, first.seq);
  assert.equal(again.from, 'alice');
  assert.equal(s.messagesFor('bob').length, 1);
  // A duplicate must not use up a seq number: the next message gets the very next one.
  assert.equal(s.saveMessage(msg('alice', 'bob', 'next')).seq, first.seq + 1);
  s.close();
});

test('messagesFor returns messages to/from a user after a seq, in order', () => {
  const s = freshStore();
  s.claimUser('carol', 'token-carol-0123456789');
  const a1 = s.saveMessage(msg('alice', 'bob', 'a->b 1')).seq;
  s.saveMessage(msg('alice', 'carol', 'a->c')); // not bob's business
  const b1 = s.saveMessage(msg('bob', 'alice', 'b->a')).seq;
  const a2 = s.saveMessage(msg('alice', 'bob', 'a->b 2')).seq;

  assert.deepEqual(s.messagesFor('bob').map(m => m.seq), [a1, b1, a2]);
  assert.deepEqual(s.messagesFor('bob', a1).map(m => m.body), ['b->a', 'a->b 2']);
  assert.deepEqual(s.messagesFor('bob', a2), []);
  const [m] = s.messagesFor('bob', b1);
  assert.deepEqual(Object.keys(m).sort(), ['body', 'from', 'id', 'kind', 'seq', 'to', 'ts']);
  s.close();
});

test('foreign keys: a message to a never-registered user is rejected', () => {
  const s = freshStore();
  assert.throws(() => s.saveMessage(msg('alice', 'nobody', 'hi')), /FOREIGN KEY/);
  s.close();
});

test('data and seq survive closing and reopening the file (server restart)', () => {
  const { file, cleanup } = tempDbPath();
  try {
    let s = new Store(file);
    s.claimUser('alice', TOKEN_A);
    s.claimUser('bob', TOKEN_B);
    const first = s.saveMessage(msg('alice', 'bob', 'before restart')).seq;
    s.close();

    s = new Store(file);
    assert.equal(s.claimUser('alice', TOKEN_A), true);
    assert.equal(s.claimUser('alice', TOKEN_B), false);
    assert.deepEqual(s.messagesFor('bob').map(m => m.body), ['before restart']);
    assert.ok(s.saveMessage(msg('bob', 'alice', 'after restart')).seq > first);
    s.close();
  } finally {
    cleanup();
  }
});

test('AUTOINCREMENT never reuses a seq, even after the newest row is deleted', () => {
  const s = freshStore();
  const last = s.saveMessage(msg('alice', 'bob', 'x')).seq;
  s.db.prepare('DELETE FROM messages WHERE seq = ?').run(last);
  assert.ok(s.saveMessage(msg('alice', 'bob', 'y')).seq > last);
  s.close();
});

test('transaction() rolls back every write if anything throws', () => {
  const s = freshStore();
  assert.throws(() => s.transaction(() => {
    s.saveMessage(msg('alice', 'bob', 'will be rolled back'));
    throw new Error('boom');
  }), /boom/);
  assert.equal(s.messagesFor('bob').length, 0);
  s.close();
});

// ---- Step 7: groups ----

const { DatabaseSync } = require('node:sqlite');
const { SCHEMA_VERSION } = require('../server/core/store');

// A database file exactly as Steps 3-6 created it (schema version 0).
function oldSchemaFile(file) {
  const db = new DatabaseSync(file);
  db.exec(`
    CREATE TABLE users (username TEXT PRIMARY KEY, token_hash TEXT NOT NULL, created_at INTEGER NOT NULL, last_seen INTEGER NOT NULL);
    CREATE TABLE messages (
      seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
      sender TEXT NOT NULL REFERENCES users(username), recipient TEXT NOT NULL REFERENCES users(username),
      body TEXT NOT NULL, ts INTEGER NOT NULL, stored_at INTEGER NOT NULL);
    CREATE INDEX messages_by_recipient ON messages(recipient, seq);
    CREATE INDEX messages_by_sender ON messages(sender, seq);`);
  db.close();
}

test('migration: an old chat.db keeps its users, messages and seq counter', () => {
  const { file, cleanup } = tempDbPath();
  try {
    oldSchemaFile(file);
    // Fill it through the old columns, then delete the newest row (the counter must not go back).
    const old = new DatabaseSync(file);
    old.exec("INSERT INTO users VALUES ('alice', 'h', 1, 1), ('bob', 'h', 1, 1)");
    for (let i = 1; i <= 3; i++) {
      old.prepare('INSERT INTO messages (id, sender, recipient, body, ts, stored_at) VALUES (?, ?, ?, ?, ?, ?)')
        .run(`old-${i}`, 'alice', 'bob', `old ${i}`, i, i);
    }
    old.exec('DELETE FROM messages WHERE seq = 3');
    assert.equal(old.prepare('PRAGMA user_version').get().user_version, 0);
    old.close();

    const s = new Store(file);
    assert.equal(s.db.prepare('PRAGMA user_version').get().user_version, SCHEMA_VERSION);
    assert.deepEqual(s.messagesFor('bob').map(m => [m.seq, m.kind, m.body]), [[1, 'text', 'old 1'], [2, 'text', 'old 2']]);
    assert.ok(s.saveMessage(msg('bob', 'alice', 'new')).seq > 3, 'seq 3 was handed out before: never again');
    // The new constraints are in place: a group message to a group that does not exist is refused.
    assert.throws(() => s.saveMessage(msg('alice', '#nope', 'x')), /FOREIGN KEY/);
    s.close();
    // Opening it again does not migrate twice.
    const again = new Store(file);
    assert.equal(again.messagesFor('bob').length, 3);
    again.close();
  } finally {
    cleanup();
  }
});

test('groups: create, members with joined_seq, one stored row per group message', () => {
  const s = freshStore();
  s.claimUser('carol', 'token-carol-0123456789');
  s.createGroup('#g', 'alice');
  const created = s.saveMessage({ ...msg('alice', '#g', '{"users":["bob"],"members":["alice","bob"]}'), kind: 'create' }).seq;
  s.addMembers('#g', ['alice', 'bob'], created);
  assert.deepEqual(s.getGroup('#g'), { name: '#g', createdBy: 'alice' });
  assert.equal(s.getGroup('#other'), null);
  assert.deepEqual(s.groupMembers('#g'), ['alice', 'bob']);
  assert.deepEqual(s.groupsOf('bob'), ['#g']);

  const hello = s.saveMessage(msg('bob', '#g', 'hello group')).seq;
  assert.equal(s.db.prepare("SELECT COUNT(*) AS n FROM messages WHERE group_name = '#g' AND kind = 'text'").get().n, 1);
  // A message must have exactly one address.
  assert.throws(() => s.db.prepare("INSERT INTO messages (id, sender, recipient, group_name, body, ts, stored_at) VALUES ('x', 'alice', 'bob', '#g', 'b', 1, 1)").run(), /CHECK/);
  assert.equal(s.counts().groups, 1);

  // carol joins later: she sees the group from the event that added her, not before.
  const added = s.saveMessage({ ...msg('bob', '#g', '{"users":["carol"],"members":["alice","bob","carol"]}'), kind: 'add' }).seq;
  s.addMembers('#g', ['carol'], added);
  const after = s.saveMessage(msg('alice', '#g', 'welcome carol')).seq;
  assert.deepEqual(s.messagesFor('carol').map(m => m.seq), [added, after]);
  assert.deepEqual(s.messagesFor('bob').map(m => m.seq), [created, hello, added, after]);
  assert.deepEqual(s.messagesFor('bob', hello).map(m => m.kind), ['add', 'text']);
  assert.equal(s.messagesFor('carol')[0].to, '#g');

  // After leaving, the group is no longer replayed (and a fresh device does not see it).
  s.removeMember('#g', 'bob');
  assert.deepEqual(s.messagesFor('bob'), []);
  s.close();
});

test('sync merges 1-to-1 and group messages into one seq order', () => {
  const s = freshStore();
  s.createGroup('#g', 'alice');
  const c = s.saveMessage({ ...msg('alice', '#g', '{"users":["bob"],"members":["alice","bob"]}'), kind: 'create' }).seq;
  s.addMembers('#g', ['alice', 'bob'], c);
  const a = s.saveMessage(msg('alice', 'bob', 'dm 1')).seq;
  const b = s.saveMessage(msg('bob', '#g', 'group 1')).seq;
  const d = s.saveMessage(msg('bob', 'alice', 'dm 2')).seq;
  const e = s.saveMessage(msg('alice', '#g', 'group 2')).seq;
  assert.deepEqual(s.messagesFor('bob').map(m => m.seq), [c, a, b, d, e]);
  assert.deepEqual(s.messagesFor('bob', b).map(m => m.body), ['dm 2', 'group 2']);
  s.close();
});
