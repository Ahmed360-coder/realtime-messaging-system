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

// ---- Step 7: groups ----

// A GROUP event as the server sends it.
const groupEvent = (from, to, op, users, members, seq) =>
  ({ v: 1, type: 'GROUP', id: `id-${++n}`, ts: 1000 + n, from, to, seq, body: { op, users, members } });

test('a group is one conversation; a GROUP event is a system line and sets the members', () => {
  const c = new Conversations('me');
  c.applyGroup(groupEvent('alice', '#g', 'create', ['me', 'bob'], ['alice', 'bob', 'me'], 1));
  assert.deepEqual(c.members('#g'), ['alice', 'bob', 'me']);
  c.addReceived(msg('bob', '#g', 'hi all', 2));
  c.addSent(msg('me', '#g', 'hello'));
  c.addReceived(msg('alice', 'me', 'private', 3)); // a 1-to-1 message stays in its own chat
  const entries = c.messages('#g');
  assert.deepEqual(entries.map(e => e.event ? e.event.op : e.body), ['create', 'hi all', 'hello']);
  assert.equal(entries[0].mine, false);
  // System lines are never unread; only bob's message is.
  assert.equal(c.unread('#g'), 1);
  assert.equal(c.unread('alice'), 1);

  c.applyGroup(groupEvent('bob', '#g', 'add', ['carol'], ['alice', 'bob', 'carol', 'me'], 4));
  assert.deepEqual(c.members('#g'), ['alice', 'bob', 'carol', 'me']);
  // An older event (should never come after a newer one) does not overwrite the member list.
  c.applyGroup(groupEvent('alice', '#g', 'add', ['zed'], ['alice', 'me', 'zed'], 3));
  assert.deepEqual(c.members('#g'), ['alice', 'bob', 'carol', 'me']);
});

test('group ACK counts are kept for the tick ("delivered to 2 of 3")', () => {
  const c = new Conversations('me');
  c.applyGroup(groupEvent('me', '#g', 'create', ['a', 'b', 'c'], ['a', 'b', 'c', 'me'], 1));
  const m = msg('me', '#g', 'hi');
  c.addSent(m);
  c.ack({ ref: m.id, seq: 2, status: 'stored', recipients: 3, delivered: 2 });
  const e = c.messages('#g').find(x => x.id === m.id);
  assert.equal(e.status, 'stored');
  assert.deepEqual([e.delivered, e.recipients, e.seq], [2, 3, 2]);
  // A later duplicate ACK (a retry) does not change the counts.
  c.ack({ ref: m.id, seq: 2, status: 'duplicate' });
  assert.deepEqual([e.delivered, e.recipients], [2, 3]);
});

test('leaving removes the group from the list; being added again shows it again', () => {
  const c = new Conversations('me');
  const create = groupEvent('alice', '#g', 'create', ['me'], ['alice', 'me'], 1);
  c.applyGroup(create);
  c.addReceived(msg('alice', '#g', 'one', 2));
  c.markRead('#g');
  assert.deepEqual(c.contacts(['alice', 'me']).map(r => r.name), ['#g', 'alice']);
  assert.equal(c.contacts(['alice', 'me'])[0].group, true);

  c.applyGroup(groupEvent('me', '#g', 'leave', [], ['alice'], 3));
  assert.equal(c.members('#g'), null);
  assert.deepEqual(c.messages('#g'), []);
  assert.deepEqual(c.contacts(['alice', 'me']).map(r => r.name), ['alice']);
  assert.equal(c.readUpTo['#g'], undefined);

  c.applyGroup(groupEvent('alice', '#g', 'add', ['me'], ['alice', 'me'], 4));
  assert.deepEqual(c.contacts(['alice', 'me']).map(r => r.name), ['#g', 'alice']);
  assert.equal(c.messages('#g').length, 1); // only the "added you" line: history before it is not ours
});
