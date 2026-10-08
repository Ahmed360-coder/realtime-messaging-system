// WebSocket transport: speaks ChatProto v1 over WebSocket frames and hands
// valid messages to the hub. Framing/validation lives in protocols/chatproto.js,
// routing lives in server/core/hub.js; this file only connects the two.

const { WebSocketServer, WebSocket } = require('ws');
const ChatProto = require('../../protocols/chatproto');
const { TYPES, ERRORS } = ChatProto;

const HEARTBEAT_MS = 30_000; // how often we ping to detect dead connections

/**
 * Attach a WebSocket endpoint to an existing HTTP server.
 * Browsers connect with new WebSocket('ws://<host>:<port>/ws'); the ws library
 * answers the HTTP Upgrade request with "101 Switching Protocols".
 */
function attachWebSocket(httpServer, hub, { path = '/ws', log = () => {} } = {}) {
  const wss = new WebSocketServer({
    server: httpServer,
    path,
    // Frames bigger than this are refused and the socket is closed with code 1009 (too big).
    maxPayload: ChatProto.MAX_FRAME_BYTES,
  });

  wss.on('connection', (ws, req) => {
    const remote = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
    log(`ws connected ${remote}`);

    // The hub only sees this session object, never the socket itself.
    const session = {
      username: null,
      protocol: 'ws',
      deliver: msg => send(ws, msg),
    };

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        send(ws, ChatProto.error(ERRORS.BAD_JSON, 'binary frames are not supported, send JSON text'));
        return;
      }
      const result = ChatProto.decode(data.toString('utf8'), ChatProto.CLIENT_TYPES);
      if (!result.ok) {
        log(`ws bad frame from ${session.username || remote}: ${result.error.code} ${result.error.message}`);
        send(ws, ChatProto.error(result.error.code, result.error.message, result.error.ref));
        return;
      }
      handle(ws, session, result.msg, hub);
    });

    // Fires once however the connection ends (close frame, timeout, crash). Only this
    // client's session is removed; every other socket keeps working.
    ws.on('close', (code) => {
      log(`ws closed ${session.username || remote} (code ${code})`);
      hub.leave(session);
    });

    ws.on('error', (err) => log(`ws error ${session.username || remote}: ${err.message}`));
  });

  // Heartbeat: a phone that loses Wi-Fi never sends a close frame. Ping everyone;
  // anyone who did not answer the previous ping is considered dead and dropped.
  const timer = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) { ws.terminate(); continue; }
      ws.isAlive = false;
      ws.ping();
    }
  }, HEARTBEAT_MS);
  wss.on('close', () => clearInterval(timer));

  return wss;
}

// Protocol state machine for one valid message from one client.
function handle(ws, session, msg, hub) {
  // Before HELLO only HELLO is allowed.
  if (!session.username && msg.type !== TYPES.HELLO) {
    return send(ws, ChatProto.error(ERRORS.NOT_JOINED, 'send HELLO first', msg.id));
  }

  switch (msg.type) {
    case TYPES.HELLO: {
      const res = hub.join(session, msg.body.username);
      if (!res.ok) return send(ws, ChatProto.error(res.code, `cannot join as "${msg.body.username}"`, msg.id));
      return send(ws, ChatProto.make(TYPES.WELCOME, { body: { username: session.username, users: res.users } }));
    }
    case TYPES.MSG: {
      const res = hub.sendDirect(session, msg);
      if (!res.ok) return send(ws, ChatProto.error(res.code, `"${msg.to}" is not online`, msg.id));
      return send(ws, ChatProto.make(TYPES.ACK, { body: { ref: msg.id, status: res.status } }));
    }
    case TYPES.LIST:
      return send(ws, ChatProto.make(TYPES.USERS, { body: { users: hub.onlineUsers() } }));
  }
}

// Encode and send, but only if the socket is still open (it may be closing).
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(ChatProto.encode(msg));
}

module.exports = { attachWebSocket };
