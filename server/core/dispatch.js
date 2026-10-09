// The ChatProto state machine for one session, shared by every transport (Step 5).
// A transport (WebSocket, MQTT) only moves text in and out; it hands each received frame to
// handleFrame(), and replies go back through session.deliver(). So both protocols run exactly
// the same join / sync / send / list logic and give exactly the same answers and errors.

const ChatProto = require('../../protocols/chatproto');
const { TYPES, ERRORS } = ChatProto;

/**
 * Decode, validate and handle one frame of text from a client.
 * Malformed frames get an ERROR reply; the connection stays open.
 */
function handleFrame(hub, session, text, log = () => {}) {
  const who = session.username || session.label;
  const result = ChatProto.decode(text, ChatProto.CLIENT_TYPES);
  if (!result.ok) {
    log(`${session.protocol} bad frame from ${who}: ${result.error.code} ${result.error.message}`);
    session.deliver(ChatProto.error(result.error.code, result.error.message, result.error.ref));
    return;
  }
  try {
    handleMessage(hub, session, result.msg);
  } catch (err) {
    // E.g. the database could not write. Send no ACK: the client keeps the message
    // pending and retries, which is exactly right when storing failed.
    log(`${session.protocol} error handling ${result.msg.type} from ${who}: ${err.message}`);
  }
}

// One valid message from one client.
function handleMessage(hub, session, msg) {
  const reply = m => session.deliver(m);

  // Before HELLO only HELLO is allowed.
  if (!session.username && msg.type !== TYPES.HELLO) {
    return reply(ChatProto.error(ERRORS.NOT_JOINED, 'send HELLO first', msg.id));
  }

  switch (msg.type) {
    case TYPES.HELLO: {
      const res = hub.join(session, msg.body);
      if (!res.ok) return reply(ChatProto.error(res.code, res.message, msg.id));
      reply(ChatProto.make(TYPES.WELCOME, { body: { username: session.username, users: res.users, known: res.known } }));
      // Offline sync: replay what this client missed, then tell it the replay is complete.
      // Same synchronous turn as join(), so nothing else can be delivered in between.
      const synced = hub.sync(session, msg.body.lastSeq || 0);
      return reply(ChatProto.make(TYPES.SYNCED, { body: synced }));
    }
    case TYPES.MSG: {
      // The hub stores the message (or recognises a retry) BEFORE we send the ACK.
      const res = hub.sendDirect(session, msg);
      if (!res.ok) return reply(ChatProto.error(res.code, res.message, msg.id));
      return reply(ChatProto.make(TYPES.ACK, { body: { ref: msg.id, seq: res.seq, status: res.status } }));
    }
    case TYPES.LIST:
      return reply(ChatProto.make(TYPES.USERS, { body: { users: hub.onlineUsers(), known: hub.knownUsers() } }));
  }
}

module.exports = { handleFrame };
