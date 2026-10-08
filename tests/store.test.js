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
  assert.deepEqual(Object.keys(m).sort(), ['body', 'from', 'id', 'seq', 'to', 'ts']);
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
