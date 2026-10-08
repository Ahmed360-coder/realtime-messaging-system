// Step 4: the chat page's state (client/conversations.js) – ordering, ticks, unread counts.
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const Conversations = require('../client/conversations');

let n = 0;
// A MSG frame as the server would send it (seq given) or as we create it (seq null).
const msg = (from, to, body, seq = null) =>
  ({ v: 1, type: 'MSG', id: `id-${++n}`, ts: 1000 + n, from, to, body, ...(seq != null && { seq }) });
const bodies = list => list.map(e => e.body);

test('messages are grouped per contact, both directions', () => {
  const c = new Conversations('alice');
  c.addReceived(msg('bob', 'alice', 'hi', 1));
  c.addSent(msg('alice', 'bob', 'hey'));
  c.addReceived(msg('carol', 'alice', 'yo', 2));
  assert.deepEqual(bodies(c.messages('bob')), ['hi', 'hey']);
  assert.deepEqual(bodies(c.messages('carol')), ['yo']);
  assert.equal(c.messages('bob')[1].mine, true);
  assert.equal(c.messages('bob')[1].status, 'waiting');
});

test('order follows the server seq, unACKed messages last', () => {
  const c = new Conversations('alice');
  const mine = c.addSent(msg('alice', 'bob', 'mine, no ACK yet'));
  c.addReceived(msg('bob', 'alice', 'seq 5', 5));
  c.addReceived(msg('bob', 'alice', 'seq 3 (arrived late)', 3));
  assert.deepEqual(bodies(c.messages('bob')), ['seq 3 (arrived late)', 'seq 5', 'mine, no ACK yet']);
  // The ACK gives it seq 4: it moves to its place in the global order.
  c.ack({ ref: mine.id, seq: 4, status: 'delivered' });
  assert.deepEqual(bodies(c.messages('bob')), ['seq 3 (arrived late)', 'mine, no ACK yet', 'seq 5']);
});

test('ACK status becomes the tick, and ticks never go backwards', () => {
  const c = new Conversations('alice');
  const a = c.addSent(msg('alice', 'bob', 'to online bob'));
  const b = c.addSent(msg('alice', 'dave', 'to offline dave'));
  c.ack({ ref: a.id, seq: 1, status: 'delivered' });
  c.ack({ ref: b.id, seq: 2, status: 'stored' });
  assert.equal(a.status, 'delivered');
  assert.equal(b.status, 'stored');
  // A retry answered with 'duplicate' only proves "stored": it must not downgrade ✓✓.
  assert.equal(c.ack({ ref: a.id, seq: 1, status: 'duplicate' }), false);
  assert.equal(a.status, 'delivered');
});

test('a server ERROR marks a waiting message failed, but not one already ACKed', () => {
  const c = new Conversations('alice');
  const a = c.addSent(msg('alice', 'ghost', 'nobody'));
  c.setStatus(a.id, 'failed', null, 'unknown user');
  assert.equal(a.status, 'failed');
  assert.equal(a.error, 'unknown user');
  const b = c.addSent(msg('alice', 'bob', 'ok'));
  c.ack({ ref: b.id, seq: 9, status: 'stored' });
  assert.equal(c.setStatus(b.id, 'failed'), false);
  assert.equal(b.status, 'stored');
});

test('the same id never makes two bubbles (outbox restore + history replay)', () => {
  const c = new Conversations('alice');
  // After a reload: the saved outbox is restored first (no seq, waiting) ...
  const sent = msg('alice', 'bob', 'sent just before the reload');
  c.addSent(sent);
  // ... then the sync replays it, because the server had stored it after all.
  c.addReceived({ ...sent, seq: 7 });
  assert.equal(c.messages('bob').length, 1);
  assert.equal(c.messages('bob')[0].seq, 7);
  assert.equal(c.messages('bob')[0].status, 'stored');
});

test('our own messages replayed from history show as stored (✓)', () => {
  const c = new Conversations('alice');
  const e = c.addReceived(msg('alice', 'bob', 'from an earlier session', 3));
  assert.equal(e.mine, true);
  assert.equal(e.status, 'stored');
  assert.equal(c.unread('bob'), 0); // our own messages are never unread
});

test('unread counts use a read cursor that survives a reload', () => {
  const c = new Conversations('alice');
  c.addReceived(msg('bob', 'alice', 'one', 1));
  c.addReceived(msg('bob', 'alice', 'two', 2));
  c.addReceived(msg('carol', 'alice', 'three', 3));
  assert.equal(c.unread('bob'), 2);
  assert.equal(c.totalUnread(), 3);
  assert.equal(c.markRead('bob'), true);
  assert.equal(c.markRead('bob'), false); // nothing new
  assert.equal(c.unread('bob'), 0);
  assert.deepEqual(c.readUpTo, { bob: 2 });

  // Reload: the whole history is replayed, but the saved cursor keeps it read.
  const again = new Conversations('alice', c.readUpTo);
  again.addReceived(msg('bob', 'alice', 'one', 1));
  again.addReceived(msg('bob', 'alice', 'two', 2));
  again.addReceived(msg('bob', 'alice', 'new while away', 4));
  assert.equal(again.unread('bob'), 1);
});

test('contact list: everyone but me, most recent chat first, then by name', () => {
  const c = new Conversations('alice');
  c.addReceived(msg('dave', 'alice', 'older', 1));
  c.addReceived(msg('bob', 'alice', 'newer', 2));
  const rows = c.contacts(['alice', 'bob', 'carol', 'dave', 'erin']);
  assert.deepEqual(rows.map(r => r.name), ['bob', 'dave', 'carol', 'erin']);
  assert.equal(rows[0].last.body, 'newer');
  assert.equal(rows[0].unread, 1);
  assert.equal(rows[2].last, null);
});
