// Message core ("hub"): who is online and where messages go.
// It is protocol-agnostic: it never touches a socket or parses a frame. Each transport
// (WebSocket now, MQTT in Step 5) wraps a connection in a "session" and calls the hub:
//
//   session = { username: null, protocol: 'ws', deliver(msg) { ...encode + send... } }
//
// Messages passed to deliver() use the ChatProto message model (v, type, id, ts, from, to, body);
// each transport turns that object into its own wire format.

const ChatProto = require('../../protocols/chatproto');
const { TYPES, ERRORS } = ChatProto;

class Hub {
  constructor({ log = () => {} } = {}) {
    this.users = new Map(); // username -> session (one live session per username)
    this.log = log;
  }

  // Sorted list of online usernames.
  onlineUsers() {
    return [...this.users.keys()].sort();
  }

  /** Bind a username to a session. Returns { ok: true, users } or { ok: false, code }. */
  join(session, username) {
    if (session.username) return { ok: false, code: ERRORS.ALREADY_JOINED };
    if (this.users.has(username)) return { ok: false, code: ERRORS.NAME_TAKEN };

    session.username = username;
    this.users.set(username, session);
    this.log(`+ ${username} joined via ${session.protocol} (${this.users.size} online)`);
    this.broadcastPresence(username, 'online');
    return { ok: true, users: this.onlineUsers() };
  }

  /** Called when a session's connection closes. Safe to call more than once. */
  leave(session) {
    const name = session.username;
    // Only remove the entry if it still points at THIS session.
    if (!name || this.users.get(name) !== session) return;
    this.users.delete(name);
    this.log(`- ${name} left (${this.users.size} online)`);
    this.broadcastPresence(name, 'offline');
  }

  /**
   * Route a 1-to-1 chat message. The forwarded copy keeps the sender's id (Step 3 uses it
   * for dedup) but `from` is set by the server, so a client cannot pretend to be someone else.
   * Returns { ok: true, status: 'delivered' } or { ok: false, code }.
   */
  sendDirect(session, msg) {
    const target = this.users.get(msg.to);
    if (!target) return { ok: false, code: ERRORS.USER_OFFLINE };

    target.deliver({
      v: ChatProto.VERSION,
      type: TYPES.MSG,
      id: msg.id,
      ts: msg.ts,
      from: session.username,
      to: msg.to,
      body: msg.body,
    });
    return { ok: true, status: 'delivered' };
  }

  // Tell everyone except the user themself that they came online / went offline.
  broadcastPresence(username, status) {
    const event = ChatProto.make(TYPES.PRESENCE, { body: { username, status } });
    for (const [name, s] of this.users) {
      if (name !== username) s.deliver(event);
    }
  }
}

module.exports = { Hub };
