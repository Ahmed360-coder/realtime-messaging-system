// Conversations – the chat page's STATE (what to show), without any DOM code.
// The page (app.js) feeds it ChatClient events and draws the screen from it; the Node tests
// use it directly, so ordering, delivery ticks and unread counts are tested, not just clicked.
//
// One conversation per contact (the other user). Each entry is
//   { id, from, to, body, ts, seq, mine, status, error }
// where `status` only matters for our own messages:
//   waiting   = sent, no ACK yet (or still in the outbox while offline)     🕓
//   stored    = server stored it; recipient was offline (gets it at next     ✓
//               sync), or delivery is unknown (duplicate ACK, history replay)
//   delivered = server stored it AND pushed it to the recipient's socket    ✓✓
//   failed    = server refused it with an ERROR (re-sending cannot help)    !

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.Conversations = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  // Ticks only move forward: a late 'duplicate' ACK must not turn ✓✓ back into ✓.
  const RANK = { waiting: 0, stored: 1, delivered: 2 };

  // ACK.body.status -> our tick. 'duplicate' means "the server already had it", which
  // proves it is stored but not whether it was delivered.
  const ACK_STATUS = { delivered: 'delivered', stored: 'stored', duplicate: 'stored' };

  // Order inside a conversation: by server seq (one global order for everyone);
  // messages without a seq yet (waiting for their ACK) go last, oldest first.
  function before(a, b) {
    if (a.seq != null && b.seq != null) return a.seq < b.seq;
    if (a.seq != null) return true;
    if (b.seq != null) return false;
    return a.ts < b.ts;
  }

  class Conversations {
    /** me: our username. readUpTo: { contact: seq } saved by an earlier page load. */
    constructor(me, readUpTo = {}) {
      this.me = me;
      this.byContact = new Map(); // contact -> entries, kept sorted (see before())
      this.byId = new Map();      // message id -> entry (dedup + fast status updates)
      // "Read up to seq N" per contact: received messages with a higher seq are unread.
      // One number per chat, the same idea as lastSeq being a cumulative ACK.
      this.readUpTo = { ...readUpTo };
    }

    contactOf(msg) { return msg.from === this.me ? msg.to : msg.from; }

    /** Our own message, just handed to ChatClient.send() (or restored from the outbox). */
    addSent(msg, status = 'waiting') {
      return this.add({ ...msg, from: this.me }, status);
    }

    /**
     * A MSG from the server. In a history replay this can be one of OUR messages (from me):
     * it has a seq, so it is stored; whether it was delivered back then is not known -> ✓.
     */
    addReceived(msg) {
      return this.add(msg, msg.from === this.me ? 'stored' : null);
    }

    add(msg, status) {
      const old = this.byId.get(msg.id);
      if (old) {
        // Already shown (e.g. restored from the outbox, then replayed by the sync): just
        // learn its seq. Never add a second bubble for the same id.
        if (old.seq == null && msg.seq != null) this.setStatus(old.id, status || old.status, msg.seq);
        return old;
      }
      const entry = {
        id: msg.id, from: msg.from, to: msg.to, body: msg.body, ts: msg.ts,
        seq: msg.seq ?? null, mine: msg.from === this.me, status, error: null,
      };
      this.byId.set(entry.id, entry);
      this.insert(this.contactOf(entry), entry);
      return entry;
    }

    // Insert keeping the order. New messages almost always belong at the end, so walk
    // backwards from the end: usually 0 steps instead of re-sorting the whole list.
    insert(contact, entry) {
      if (!this.byContact.has(contact)) this.byContact.set(contact, []);
      const list = this.byContact.get(contact);
      let i = list.length;
      while (i > 0 && before(entry, list[i - 1])) i--;
      list.splice(i, 0, entry);
    }

    /** Update a tick (from an ACK or ERROR). Returns false if nothing changed. */
    setStatus(id, status, seq = null, error = null) {
      const e = this.byId.get(id);
      if (!e || !e.mine) return false;
      let changed = false;
      if (status === 'failed') {
        if (e.status !== 'waiting') return false; // already ACKed: the server has it
        e.status = 'failed';
        e.error = error;
        changed = true;
      } else if (status in RANK && (e.status === 'failed' || RANK[status] > RANK[e.status])) {
        e.status = status;
        changed = true;
      }
      if (seq != null && e.seq !== seq) {
        // It now has its place in the global order: move it there.
        const list = this.byContact.get(this.contactOf(e));
        list.splice(list.indexOf(e), 1);
        e.seq = seq;
        this.insert(this.contactOf(e), e);
        changed = true;
      }
      return changed;
    }

    /** Apply an ACK frame's body ({ ref, seq, status }). */
    ack(body) {
      return this.setStatus(body.ref, ACK_STATUS[body.status], body.seq);
    }

    messages(contact) { return this.byContact.get(contact) || []; }

    last(contact) {
      const list = this.messages(contact);
      return list[list.length - 1] || null;
    }

    /** Received (not ours) messages in this chat with seq above the read cursor. */
    unread(contact) {
      const read = this.readUpTo[contact] || 0;
      let n = 0;
      for (const e of this.messages(contact)) if (!e.mine && e.seq > read) n++;
      return n;
    }

    totalUnread() {
      let n = 0;
      for (const contact of this.byContact.keys()) n += this.unread(contact);
      return n;
    }

    /** Opened the chat: everything in it is read now. Returns true if the cursor moved. */
    markRead(contact) {
      let max = this.readUpTo[contact] || 0;
      for (const e of this.messages(contact)) if (!e.mine && e.seq > max) max = e.seq;
      if (max === (this.readUpTo[contact] || 0)) return false;
      this.readUpTo[contact] = max;
      return true;
    }

    /**
     * Rows for the contact list: every known user except us. Chats with messages come
     * first, most recent activity on top (like any messenger); the rest alphabetically.
     */
    contacts(known) {
      const names = new Set(known);
      for (const c of this.byContact.keys()) names.add(c);
      names.delete(this.me);
      const time = e => (e ? e.ts : -1);
      return [...names]
        .map(name => ({ name, last: this.last(name), unread: this.unread(name) }))
        .sort((a, b) => time(b.last) - time(a.last) || a.name.localeCompare(b.name));
    }
  }

  Conversations.ACK_STATUS = ACK_STATUS;
  return Conversations;
});
