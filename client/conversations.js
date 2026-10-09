// Conversations – the chat page's STATE (what to show), without any DOM code.
// The page (app.js) feeds it ChatClient events and draws the screen from it; the Node tests
// use it directly, so ordering, delivery ticks and unread counts are tested, not just clicked.
//
// One conversation per contact: the other user, or a group ('#study', Step 7). Each entry is
//   { id, from, to, body, ts, seq, mine, status, error, recipients, delivered, event, security, sealed }
// Step 8: `body` is always the decrypted text. `security` says how it arrived: 'e2e' (sealed and
// authenticated), 'plain' (not encrypted) or 'failed' (sealed, but it did not open); null when
// encryption is not in use. `sealed` keeps the ciphertext of a 'failed' one, to try again later.
// where `status` only matters for our own messages:
//   waiting   = sent, no ACK yet (or still in the outbox while offline)     🕓
//   stored    = server stored it; recipient was offline (gets it at next     ✓
//               sync), or delivery is unknown (duplicate ACK, history replay)
//   delivered = server stored it AND pushed it to the recipient's socket    ✓✓
//               (group: to EVERY other member; recipients/delivered say "2 of 3")
//   failed    = server refused it with an ERROR (re-sending cannot help)    !
// `event` is set for a membership change shown as a system line: { op, users } (by `from`).

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
      this.groups = new Map();    // '#name' -> { members, seq }: the groups we are in (Step 7)
      // "Read up to seq N" per contact: received messages with a higher seq are unread.
      // One number per chat, the same idea as lastSeq being a cumulative ACK.
      this.readUpTo = { ...readUpTo };
    }

    // The conversation a message belongs to: the group, or the other user.
    contactOf(msg) {
      if (isGroup(msg.to)) return msg.to;
      return msg.from === this.me ? msg.to : msg.from;
    }

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
        recipients: null, delivered: null, event: null,
        security: msg.security ?? null, sealed: msg.security === 'failed' ? msg.sealed : null,
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

    /**
     * Step 8: try to open the messages that did not decrypt again (e.g. after the user accepted
     * a contact's new key). reveal(msg) -> { body, security }. Returns how many opened now.
     */
    reopenFailed(reveal) {
      let n = 0;
      for (const e of this.byId.values()) {
        if (e.security !== 'failed' || !e.sealed) continue;
        const r = reveal({ id: e.id, from: e.from, to: e.to, ts: e.ts, seq: e.seq, body: e.sealed });
        if (r.security !== 'e2e') continue;
        Object.assign(e, { body: r.body, security: 'e2e', sealed: null });
        n++;
      }
      return n;
    }

    /** Apply an ACK frame's body ({ ref, seq, status }, for a group also { recipients, delivered }). */
    ack(body) {
      const e = this.byId.get(body.ref);
      if (e && body.recipients !== undefined && body.status !== 'duplicate') {
        e.recipients = body.recipients;
        e.delivered = body.delivered;
      }
      return this.setStatus(body.ref, ACK_STATUS[body.status], body.seq);
    }

    /**
     * A GROUP event from the server (Step 7): { id, from, to: '#name', seq, ts, body: { op, users, members } }.
     * It becomes a system line in that group's chat and updates the member list. If we are no
     * longer a member (we left), the conversation is removed: the server will not replay it either.
     * Returns true if anything changed.
     */
    applyGroup(msg) {
      const name = msg.to;
      const { op, users = [], members = [] } = msg.body;
      if (!members.includes(this.me)) return this.dropGroup(name);
      const known = this.groups.get(name);
      // Events come in seq order; still, never let an older one overwrite a newer member list.
      if (!known || msg.seq > known.seq) this.groups.set(name, { members: [...members], seq: msg.seq });
      if (this.byId.has(msg.id)) return true;
      const entry = this.add({ id: msg.id, from: msg.from, to: name, body: '', ts: msg.ts, seq: msg.seq }, null);
      entry.mine = false; // a system line, not a bubble: no tick, never unread
      entry.event = { op, users: [...users] };
      return true;
    }

    dropGroup(name) {
      const had = this.groups.has(name) || this.byContact.has(name);
      // Forget its entries too, so if we are added again later they can be shown again.
      for (const e of this.messages(name)) this.byId.delete(e.id);
      this.groups.delete(name);
      this.byContact.delete(name);
      delete this.readUpTo[name];
      return had;
    }

    /** Members of a group we are in (sorted), or null. */
    members(name) {
      const g = this.groups.get(name);
      return g ? g.members : null;
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
      for (const e of this.messages(contact)) if (!e.mine && !e.event && e.seq > read) n++;
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
     * Rows for the contact list: every known user except us, and our groups. Chats with
     * messages come first, most recent activity on top (like any messenger); the rest
     * alphabetically. A group row also has `members`.
     */
    contacts(known) {
      const names = new Set(known);
      for (const c of this.byContact.keys()) names.add(c);
      for (const g of this.groups.keys()) names.add(g);
      names.delete(this.me);
      const time = e => (e ? e.ts : -1);
      return [...names]
        .map(name => ({ name, last: this.last(name), unread: this.unread(name), group: isGroup(name), members: this.members(name) }))
        .sort((a, b) => time(b.last) - time(a.last) || a.name.localeCompare(b.name));
    }
  }

  // Group addresses start with '#' (a username never can). Same rule as ChatProto.isGroup.
  function isGroup(name) { return typeof name === 'string' && name.startsWith('#'); }

  Conversations.ACK_STATUS = ACK_STATUS;
  Conversations.isGroup = isGroup;
  return Conversations;
});
