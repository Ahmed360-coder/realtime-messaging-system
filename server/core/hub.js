// Message core ("hub"): who is online and where messages go.
// It is protocol-agnostic: it never touches a socket or parses a frame. Each transport
// (server/transports: WebSocket, MQTT) wraps a connection in a "session" and calls the hub:
//
//   session = { username: null, protocol: 'ws' | 'mqtt',
//               deliver(msg, onSent) { ...encode + send; onSent() once written... },
//               close(code, reason) { ...close the connection... } }
//
// Messages passed to deliver() use the ChatProto message model (v, type, id, ts, from, to, body, seq);
// each transport turns that object into its own wire format.
//
// Reliability (Step 3): every chat message is written to the store BEFORE the hub answers,
// so an ACK always means "safely on disk". Offline recipients get it later via sync().
//
// Events (Step 6): the hub is an EventEmitter and announces what happened, without knowing who
// listens (server/core/metrics.js counts them). `protocol` is that session's protocol; receivedAt
// is the monotonic time (performance.now()) at which the message's frame reached the server.
//   'message'   { protocol, status, receivedAt,   a chat message was delivered / stored / a duplicate;
//                 group, recipients, delivered,   recipients = copies to deliver (1 for 1-to-1, members - 1
//                 e2e }                           for a group), delivered = how many went out live,
//                                                 e2e = the body was sealed (Step 8)
//   'delivered' { protocol, receivedAt }          one copy of a MSG was written to a recipient's connection
//   'acked'     { protocol, receivedAt }          its ACK was written to the sender's connection (dispatch.js)
//   'synced'    { protocol, count }               offline sync replayed `count` messages
//   'failure'   { protocol, code }                an ERROR reply, or INTERNAL (dispatch.js)
//   'group'     { protocol, op }                  a membership change (create / add / leave) was stored
//
// Groups (Step 7): a group message is stored ONCE, then the hub pushes a copy to every online
// member except the sender (fan-out); offline members get it from sync(). Membership changes are
// stored in the same message log, so they share the seq order, the id dedup and offline sync.
//
// End-to-end encryption (Step 8): the hub never encrypts or decrypts anything. It only
//   - pins each user's public key at join (like the token) and hands the keys out (key directory);
//   - stores and forwards a sealed body exactly like a text body: id, seq, dedup, sync and
//     fan-out never look inside it;
//   - checks that a sealed group message carries a key box for exactly the current members.

const { EventEmitter } = require('node:events');
const { performance } = require('node:perf_hooks');
const ChatProto = require('../../protocols/chatproto');
const { TYPES, ERRORS } = ChatProto;

class Hub extends EventEmitter {
  constructor({ store, log = () => {} }) {
    super();
    this.store = store;
    this.users = new Map(); // username -> session (one live session per username)
    this.log = log;
  }

  // Sorted list of online usernames.
  onlineUsers() {
    return [...this.users.keys()].sort();
  }

  // Everyone who has ever joined (possible recipients, online or not).
  knownUsers() {
    return this.store.knownUsers();
  }

  // Step 8: the key directory, { username: public key } of every user who has one.
  publicKeys() {
    return this.store.publicKeys();
  }

  /**
   * Bind a username to a session. The token proves this is the same device that registered
   * the name. If that user already has a live session (e.g. a phone that lost Wi-Fi and
   * reconnected before the old socket timed out), the new session takes over.
   * Step 8: `key` (optional) is the device's public key. The first one is pinned; a different
   * key later is refused (KEY_MISMATCH), so the server cannot be asked to swap a user's key.
   * Returns { ok: true, users, known, keys } or { ok: false, code, message }.
   */
  join(session, { username, token, key }) {
    if (session.username) return fail(ERRORS.ALREADY_JOINED, 'already joined');
    const claim = this.store.transaction(() => {
      if (!this.store.claimUser(username, token)) return fail(ERRORS.NAME_TAKEN, `"${username}" belongs to another device`);
      if (key && !this.store.pinPublicKey(username, key)) {
        return fail(ERRORS.KEY_MISMATCH, `"${username}" is registered with a different public key`);
      }
      return { ok: true };
    });
    if (!claim.ok) return claim;

    const old = this.users.get(username);
    session.username = username;
    this.users.set(username, session);
    if (old) {
      // Same user, newer connection: drop the old one. Its later leave() is ignored
      // because users.get(username) no longer points at it.
      old.username = null;
      old.close(ChatProto.CLOSE_REPLACED, 'replaced by a newer connection');
      this.log(`~ ${username} reconnected via ${session.protocol} (old session replaced)`);
    } else {
      this.log(`+ ${username} joined via ${session.protocol} (${this.users.size} online)`);
      this.broadcastPresence(username, 'online');
    }
    return { ok: true, users: this.onlineUsers(), known: this.knownUsers(), keys: this.publicKeys() };
  }

  /**
   * Offline sync: deliver every stored message to/from this user with seq > lastSeq, in seq
   * order. Called by the transport right after WELCOME, in the same event-loop turn as join(),
   * so no live message can be delivered in between (no gaps, no reordering).
   * Returns { count, lastSeq } for the SYNCED reply.
   */
  sync(session, lastSeq = 0) {
    const missed = this.store.messagesFor(session.username, lastSeq);
    for (const m of missed) session.deliver(toWire(m));
    this.emit('synced', { protocol: session.protocol, count: missed.length });
    if (missed.length) this.log(`  replayed ${missed.length} message(s) to ${session.username} after seq ${lastSeq}`);
    return { count: missed.length, lastSeq: missed.length ? missed[missed.length - 1].seq : lastSeq };
  }

  /** Called when a session's connection closes. Safe to call more than once. */
  leave(session) {
    const name = session.username;
    // Only remove the entry if it still points at THIS session.
    if (!name || this.users.get(name) !== session) return;
    this.users.delete(name);
    this.store.touchUser(name);
    this.log(`- ${name} left (${this.users.size} online)`);
    this.broadcastPresence(name, 'offline');
  }

  /**
   * Route a 1-to-1 chat message: store first, then push to the recipient if online.
   * `from` is set by the server, so a client cannot pretend to be someone else.
   * A retried id is not stored or delivered again; it gets the original seq back.
   * Returns { ok: true, seq, status: 'delivered'|'stored'|'duplicate' } or { ok: false, code, message }.
   */
  sendDirect(session, msg, receivedAt = performance.now()) {
    if (!this.store.userExists(msg.to)) return fail(ERRORS.UNKNOWN_USER, `"${msg.to}" has never joined`);

    const stored = { id: msg.id, from: session.username, to: msg.to, body: msg.body, ts: msg.ts };
    const res = this.store.saveMessage(stored);
    if (res.duplicate) {
      // Same id from someone else is not a retry: refuse instead of acknowledging their message.
      if (res.from !== session.username) return fail(ERRORS.BAD_FIELD, 'id already used by another message');
      this.log(`  duplicate ${msg.id.slice(0, 8)} from ${session.username} (seq ${res.seq}), not stored again`);
      this.emit('message', { protocol: session.protocol, status: 'duplicate', receivedAt });
      return { ok: true, seq: res.seq, status: 'duplicate' };
    }

    const target = this.users.get(msg.to);
    const status = target ? 'delivered' : 'stored';
    this.emit('message', { protocol: session.protocol, status, receivedAt, group: false, recipients: 1,
      delivered: target ? 1 : 0, e2e: ChatProto.isSealed(msg.body) });
    // The callback runs once the transport has written the MSG to the recipient's connection.
    if (target) {
      target.deliver(toWire({ ...stored, seq: res.seq }),
        () => this.emit('delivered', { protocol: target.protocol, receivedAt }));
    }
    return { ok: true, seq: res.seq, status };
  }

  /**
   * Route a group chat message (msg.to = '#name'). The same guarantees as sendDirect: stored
   * once (one row, one seq) BEFORE the ACK, retries recognised by id, offline members get it
   * from sync. Only members may send.
   * Returns { ok: true, seq, status, recipients, delivered } or { ok: false, code, message }.
   *   recipients = the other members; delivered = how many of them got it live.
   *   status = 'delivered' only if ALL of them did (✓✓), else 'stored' (✓).
   */
  sendGroup(session, msg, receivedAt = performance.now()) {
    const me = session.username;
    // Check-and-store is one transaction: membership cannot change between the check and the insert.
    const res = this.store.transaction(() => {
      // A retry first: it must get its ACK back even if the sender has left the group since.
      const dup = this.retryOf(session, msg);
      if (dup) return dup;
      if (!this.store.getGroup(msg.to)) return fail(ERRORS.UNKNOWN_GROUP, `no group ${msg.to}`);
      const members = this.store.groupMembers(msg.to);
      if (!members.includes(me)) return fail(ERRORS.NOT_MEMBER, `you are not a member of ${msg.to}`);
      // Step 8: a sealed message must have a key box for exactly the current members (sender
      // included, so they can read their own history). Otherwise a member just added could not
      // read it, or one who just left could. Refused BEFORE storing: the id stays unused, and
      // the client re-seals for the new member list and sends it again with the same id.
      if (ChatProto.isSealed(msg.body) && !sameMembers(Object.keys(msg.body.keys), members)) {
        return fail(ERRORS.STALE_MEMBERS, `the members of ${msg.to} have changed: ${members.join(', ')}`);
      }
      const stored = { id: msg.id, from: me, to: msg.to, body: msg.body, ts: msg.ts };
      const { seq } = this.store.saveMessage(stored);
      return { ok: true, seq, stored, members };
    });
    if (!res.ok || res.status === 'duplicate') {
      if (res.ok) this.emit('message', { protocol: session.protocol, status: 'duplicate', receivedAt, group: true });
      return res;
    }

    // Committed: now fan out. Every member gets the same frame with the same seq.
    const others = res.members.filter(u => u !== me);
    const delivered = this.fanOut(others, toWire({ ...res.stored, seq: res.seq }),
      target => this.emit('delivered', { protocol: target.protocol, receivedAt }));
    const status = others.length > 0 && delivered === others.length ? 'delivered' : 'stored';
    this.emit('message', { protocol: session.protocol, status, receivedAt, group: true, recipients: others.length,
      delivered, e2e: ChatProto.isSealed(msg.body) });
    return { ok: true, seq: res.seq, status, recipients: others.length, delivered };
  }

  /**
   * A membership change: msg = GROUP { to: '#name', body: { op, users } }.
   *   create  the name must be free; the members are the creator + body.users
   *   add     only a member may add; body.users are added (those not in it yet)
   *   leave   only a member may leave
   * Every user in body.users must be registered. At most MAX_GROUP_MEMBERS members.
   * The change is stored as a message (kind = op) with a seq; the members table is updated in
   * the same transaction. Then the event (with the member list after the change) is pushed to
   * everyone it concerns: the members before AND after, including the one who made the change.
   * Returns { ok: true, seq, status, recipients, delivered } or { ok: false, code, message }.
   */
  changeGroup(session, msg) {
    const me = session.username;
    const { op } = msg.body;
    const name = msg.to;
    const res = this.store.transaction(() => {
      const dup = this.retryOf(session, msg);
      if (dup) return dup;

      const group = this.store.getGroup(name);
      const before = group ? this.store.groupMembers(name) : [];
      let added = [];
      if (op === 'create') {
        if (group) return fail(ERRORS.GROUP_EXISTS, `${name} already exists`);
        added = unique([me, ...msg.body.users]);
      } else {
        if (!group) return fail(ERRORS.UNKNOWN_GROUP, `no group ${name}`);
        if (!before.includes(me)) return fail(ERRORS.NOT_MEMBER, `you are not a member of ${name}`);
        if (op === 'add') {
          added = unique(msg.body.users).filter(u => !before.includes(u));
          if (added.length === 0) return fail(ERRORS.BAD_FIELD, 'they are already members');
        }
      }
      const unknown = added.find(u => !this.store.userExists(u));
      if (unknown) return fail(ERRORS.UNKNOWN_USER, `"${unknown}" has never joined`);
      const after = op === 'leave' ? before.filter(u => u !== me) : [...before, ...added].sort();
      if (after.length > ChatProto.MAX_GROUP_MEMBERS) {
        return fail(ERRORS.GROUP_FULL, `a group has at most ${ChatProto.MAX_GROUP_MEMBERS} members`);
      }

      // Write: the group row (create), the event in the message log, then the members table.
      if (op === 'create') this.store.createGroup(name, me);
      const users = op === 'create' ? added.filter(u => u !== me) : added;
      const stored = { id: msg.id, kind: op, from: me, to: name, body: JSON.stringify({ users, members: after }), ts: msg.ts };
      const { seq } = this.store.saveMessage(stored);
      if (added.length) this.store.addMembers(name, added, seq);
      if (op === 'leave') this.store.removeMember(name, me);
      return { ok: true, seq, stored, concerned: unique([...before, ...after]) };
    });
    if (!res.ok || res.status === 'duplicate') return res;

    this.log(`  ${me} ${op} ${name}${op === 'leave' ? '' : ` ${JSON.parse(res.stored.body).users.join(', ')}`}`);
    this.emit('group', { protocol: session.protocol, op });
    const frame = toWire({ ...res.stored, seq: res.seq });
    const others = res.concerned.filter(u => u !== me);
    const delivered = this.fanOut(others, frame);
    this.fanOut([me], frame); // the actor learns the new member list the same way as everyone else
    const status = others.length > 0 && delivered === others.length ? 'delivered' : 'stored';
    return { ok: true, seq: res.seq, status, recipients: others.length, delivered };
  }

  /**
   * If msg.id is already stored, this is a retry (its ACK was lost): answer it like the first
   * time, with the original seq and status 'duplicate', and do nothing else.
   * The same id from someone else is not a retry: refuse it.
   */
  retryOf(session, msg) {
    const old = this.store.findMessage(msg.id);
    if (!old) return null;
    if (old.from !== session.username) return fail(ERRORS.BAD_FIELD, 'id already used by another message');
    this.log(`  duplicate ${msg.id.slice(0, 8)} from ${session.username} (seq ${old.seq}), not stored again`);
    return { ok: true, seq: old.seq, status: 'duplicate' };
  }

  /**
   * Fan-out: push one frame to each of `usernames` who is online. Offline ones are skipped;
   * they get it from sync() when they come back. Returns how many were online.
   * Cost: one deliver() (one WebSocket frame or one MQTT PUBLISH) per online member.
   */
  fanOut(usernames, frame, onSent = null) {
    let delivered = 0;
    for (const name of usernames) {
      const target = this.users.get(name);
      if (!target) continue;
      delivered++;
      target.deliver(frame, onSent && (() => onSent(target)));
    }
    return delivered;
  }

  // Tell everyone except the user themself that they came online / went offline.
  // Step 8: 'online' also carries their public key, so a user who registered after our
  // WELCOME can be written to at once.
  broadcastPresence(username, status) {
    const key = status === 'online' ? this.store.publicKey(username) : null;
    const event = ChatProto.make(TYPES.PRESENCE, { body: key ? { username, status, key } : { username, status } });
    for (const [name, s] of this.users) {
      if (name !== username) s.deliver(event);
    }
  }
}

// A stored message as a ChatProto frame (keeps the sender's id so receivers can dedup):
// chat text -> MSG, a membership change -> GROUP { op, users, members }.
function toWire({ seq, id, kind = 'text', from, to, body, ts }) {
  if (kind === 'text') return { v: ChatProto.VERSION, type: TYPES.MSG, id, ts, from, to, body, seq };
  return { v: ChatProto.VERSION, type: TYPES.GROUP, id, ts, from, to, body: { op: kind, ...JSON.parse(body) }, seq };
}

const fail = (code, message) => ({ ok: false, code, message });
const unique = list => [...new Set(list)];
// Same usernames, ignoring order (both lists are free of repeats: object keys / a primary key).
const sameMembers = (a, b) => a.length === b.length && a.every(u => b.includes(u));

module.exports = { Hub };
