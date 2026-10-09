// The ChatProto state machine for one session, shared by every transport (Step 5).
// A transport (WebSocket, MQTT) only moves text in and out; it hands each received frame to
// handleFrame(), and replies go back through session.deliver(). So both protocols run exactly
// the same join / sync / send / list logic and give exactly the same answers and errors.
// Step 6: it also announces 'acked' and 'failure' on the hub (see hub.js) for the metrics.
// Step 7: MSG to a '#group' goes to hub.sendGroup; GROUP (create / add / leave) to hub.changeGroup.

const { performance } = require('node:perf_hooks');
const ChatProto = require('../../protocols/chatproto');
const { TYPES, ERRORS } = ChatProto;

/**
 * Decode, validate and handle one frame of text from a client.
 * Malformed frames get an ERROR reply; the connection stays open.
 */
function handleFrame(hub, session, text, log = () => {}) {
  // Monotonic clock: the start of the server-side latency of this message.
  const receivedAt = performance.now();
  const who = session.username || session.label;
  const result = ChatProto.decode(text, ChatProto.CLIENT_TYPES);
  if (!result.ok) {
    log(`${session.protocol} bad frame from ${who}: ${result.error.code} ${result.error.message}`);
    refuse(hub, session, result.error.code, result.error.message, result.error.ref);
    return;
  }
  try {
    handleMessage(hub, session, result.msg, receivedAt);
  } catch (err) {
    // E.g. the database could not write. Send no ACK: the client keeps the message
    // pending and retries, which is exactly right when storing failed.
    log(`${session.protocol} error handling ${result.msg.type} from ${who}: ${err.message}`);
    hub.emit('failure', { protocol: session.protocol, code: 'INTERNAL' });
  }
}

// One valid message from one client.
function handleMessage(hub, session, msg, receivedAt) {
  const reply = m => session.deliver(m);
  const fail = (code, message) => refuse(hub, session, code, message, msg.id);

  // Before HELLO only HELLO is allowed.
  if (!session.username && msg.type !== TYPES.HELLO) {
    return fail(ERRORS.NOT_JOINED, 'send HELLO first');
  }

  switch (msg.type) {
    case TYPES.HELLO: {
      const res = hub.join(session, msg.body);
      if (!res.ok) return fail(res.code, res.message);
      reply(ChatProto.make(TYPES.WELCOME, { body: { username: session.username, users: res.users, known: res.known } }));
      // Offline sync: replay what this client missed, then tell it the replay is complete.
      // Same synchronous turn as join(), so nothing else can be delivered in between.
      const synced = hub.sync(session, msg.body.lastSeq || 0);
      return reply(ChatProto.make(TYPES.SYNCED, { body: synced }));
    }
    case TYPES.MSG: {
      // The hub stores the message (or recognises a retry) BEFORE we send the ACK.
      // The address decides the route: '#name' = group (Step 7), otherwise 1-to-1.
      const res = ChatProto.isGroup(msg.to) ? hub.sendGroup(session, msg, receivedAt) : hub.sendDirect(session, msg, receivedAt);
      if (!res.ok) return fail(res.code, res.message);
      // The callback runs once the transport has written the ACK to the sender's connection.
      return session.deliver(ackFor(msg, res), () => hub.emit('acked', { protocol: session.protocol, receivedAt }));
    }
    case TYPES.GROUP: {
      // Step 7: create / add / leave. Stored (with a seq) before the ACK, like a MSG.
      const res = hub.changeGroup(session, msg);
      if (!res.ok) return fail(res.code, res.message);
      return reply(ackFor(msg, res));
    }
    case TYPES.LIST:
      return reply(ChatProto.make(TYPES.USERS, { body: { users: hub.onlineUsers(), known: hub.knownUsers() } }));
  }
}

// ACK { ref, seq, status }; for a group also { recipients, delivered } ("delivered to 2 of 3").
function ackFor(msg, res) {
  const body = { ref: msg.id, seq: res.seq, status: res.status };
  if (res.recipients !== undefined) Object.assign(body, { recipients: res.recipients, delivered: res.delivered });
  return ChatProto.make(TYPES.ACK, { body });
}

// Every ERROR reply goes through here, so each one is also announced as a 'failure'.
function refuse(hub, session, code, message, ref) {
  hub.emit('failure', { protocol: session.protocol, code });
  session.deliver(ChatProto.error(code, message, ref));
}

module.exports = { handleFrame };
