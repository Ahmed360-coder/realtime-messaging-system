// WebSocket transport: speaks ChatProto v1 over WebSocket frames and hands
// valid messages to the hub. Framing/validation lives in protocols/chatproto.js,
// the ChatProto state machine in server/core/dispatch.js (shared with MQTT), routing in
// server/core/hub.js, storage in server/core/store.js; this file only connects them.

const { WebSocketServer, WebSocket } = require('ws');
const ChatProto = require('../../protocols/chatproto');
const { handleFrame } = require('../core/dispatch');
const { ERRORS } = ChatProto;

const HEARTBEAT_MS = 30_000; // how often we ping to detect dead connections

/**
 * Create the ChatProto WebSocket endpoint. Browsers connect with
 * new WebSocket('ws://<host>:<port>/ws'); server/index.js passes us the HTTP Upgrade
 * requests for that path and the ws library answers "101 Switching Protocols".
 */
function createWebSocketEndpoint(hub, { log = () => {} } = {}) {
  const wss = new WebSocketServer({
    // noServer: index.js decides which endpoint gets an upgrade (/ws here, /mqtt for MQTT).
    noServer: true,
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
      label: remote,
      deliver: msg => send(ws, msg),
      close: (code, reason) => ws.close(code, reason),
    };

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        send(ws, ChatProto.error(ERRORS.BAD_JSON, 'binary frames are not supported, send JSON text'));
        return;
      }
      handleFrame(hub, session, data.toString('utf8'), log);
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

// Encode and send, but only if the socket is still open (it may be closing).
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(ChatProto.encode(msg));
}

module.exports = { createWebSocketEndpoint };
