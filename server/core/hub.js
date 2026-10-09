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
//   'message'   { protocol, status, receivedAt }  a chat message was delivered / stored / a duplicate
//   'delivered' { protocol, receivedAt }          its MSG was written to the recipient's connection
//   'acked'     { protocol, receivedAt }          its ACK was written to the sender's connection (dispatch.js)
//   'synced'    { protocol, count }               offline sync replayed `count` messages
//   'failure'   { protocol, code }                an ERROR reply, or INTERNAL (dispatch.js)

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

  /**
   * Bind a username to a session. The token proves this is the same device that registered
   * the name. If that user already has a live session (e.g. a phone that lost Wi-Fi and
   * reconnected before the old socket timed out), the new session takes over.
   * Returns { ok: true, users, known } or { ok: false, code, message }.
   */
  join(session, { username, token }) {
    if (session.username) return fail(ERRORS.ALREADY_JOINED, 'already joined');
    if (!this.store.claimUser(username, token)) {
      return fail(ERRORS.NAME_TAKEN, `"${username}" belongs to another device`);
    }

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
    return { ok: true, users: this.onlineUsers(), known: this.knownUsers() };
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
    this.emit('message', { protocol: session.protocol, status, receivedAt });
    // The callback runs once the transport has written the MSG to the recipient's connection.
    if (target) {
      target.deliver(toWire({ ...stored, seq: res.seq }),
        () => this.emit('delivered', { protocol: target.protocol, receivedAt }));
    }
    return { ok: true, seq: res.seq, status };
  }

  // Tell everyone except the user themself that they came online / went offline.
  broadcastPresence(username, status) {
    const event = ChatProto.make(TYPES.PRESENCE, { body: { username, status } });
    for (const [name, s] of this.users) {
      if (name !== username) s.deliver(event);
    }
  }
}

// A stored message as a ChatProto MSG (keeps the sender's id so receivers can dedup).
function toWire({ seq, id, from, to, body, ts }) {
  return { v: ChatProto.VERSION, type: TYPES.MSG, id, ts, from, to, body, seq };
}

const fail = (code, message) => ({ ok: false, code, message });

module.exports = { Hub };
