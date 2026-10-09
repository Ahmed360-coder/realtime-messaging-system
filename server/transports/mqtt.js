// MQTT transport (Step 5): an MQTT broker (Aedes) embedded in our server, reached by browsers
// as MQTT over WebSocket on /mqtt. Every MQTT connection becomes one hub session, exactly like
// a ChatProto WebSocket, so both protocols share routing, SQLite storage, seq, ACKs, dedup and
// offline sync, and their users can chat with each other.
//
// Topics (protocols/mqtt-binding.js):  chat/<clientId>/up    client -> server
//                                      chat/<clientId>/down  server -> client
// Payloads are ChatProto JSON objects. The broker's hooks are our access rules:
//   authenticate        CONNECT: the client id must be well-formed and not in use
//   authorizeSubscribe  SUBSCRIBE: only your own down topic (no wildcards) -> else SUBACK 128
//   authorizePublish    PUBLISH: only your own up topic -> handled by the ChatProto state machine

const { WebSocketServer, createWebSocketStream } = require('ws');
const { Aedes } = require('aedes');
const ChatProto = require('../../protocols/chatproto');
const MqttBinding = require('../../protocols/mqtt-binding');
const { handleFrame } = require('../core/dispatch');

const BYE_GRACE_MS = 2000; // after BYE the client should disconnect itself; then we force it

// An error whose returnCode Aedes puts into the CONNACK packet (2 = identifier rejected).
function connackError(returnCode, message) {
  const err = new Error(message);
  err.returnCode = returnCode;
  return err;
}

/** Create the broker and the WebSocket endpoint that carries it. Returns { wss, close() }. */
async function createMqttEndpoint(hub, { log = () => {} } = {}) {
  const sessions = new Map(); // Aedes client -> hub session

  const broker = await Aedes.createBroker({
    // CONNECT. The client id is fresh per connection and names the topics, so it must not
    // contain topic characters. We refuse an id that is already connected: MQTT's rule would be
    // "the newer connection takes over", which would let anyone who learns an id steal it.
    // Login happens later, in the ChatProto HELLO (same code and errors as WebSocket).
    authenticate(client, username, password, done) {
      if (!MqttBinding.CLIENT_ID_RE.test(client.id)) {
        return done(connackError(2, 'client id must be c- followed by 32 hex digits'), false);
      }
      if (broker.clients[client.id]) return done(connackError(2, 'client id already connected'), false);
      done(null, true);
    },

    // SUBSCRIBE. Only "chat/<own id>/down". Anything else (another connection's topic,
    // chat/+/down, #) is answered with SUBACK 128 = refused. (We publish at QoS 1, and MQTT
    // delivers at the lower of publish and subscription QoS, so the client gets QoS 1 at most.)
    authorizeSubscribe(client, sub, done) {
      if (sub.topic !== MqttBinding.downTopic(client.id)) {
        log(`mqtt refused SUBSCRIBE ${sub.topic} from ${client.id}`);
        return done(null, null);
      }
      done(null, sub);
    },

    // PUBLISH from a client. Aedes calls this BEFORE it sends the PUBACK, so the hub has stored
    // the message in SQLite before the client hears anything back. An error here makes Aedes
    // close the connection (MQTT 3.1.1 has no other way to refuse a PUBLISH).
    authorizePublish(client, packet, done) {
      if (packet === client.will) return done(new Error('last will not used'));
      if (packet.topic !== MqttBinding.upTopic(client.id)) {
        log(`mqtt refused PUBLISH to ${packet.topic} from ${client.id}`);
        return done(new Error('publish only to your own up topic'));
      }
      if (packet.retain) return done(new Error('retained messages not used'));
      handleFrame(hub, sessionOf(client), packet.payload.toString('utf8'), log);
      // Allowed: the broker now forwards it to subscribers of the up topic, but there are none
      // (authorizeSubscribe refuses them), so it goes nowhere else.
      done(null);
    },
  });

  // The hub only sees this session object, never the MQTT client.
  function sessionOf(client) {
    let session = sessions.get(client);
    if (session) return session;
    const topic = MqttBinding.downTopic(client.id);
    let closing = null;
    session = {
      username: null,
      protocol: 'mqtt',
      label: client.id,
      // One ChatProto message = one PUBLISH on this connection's down topic, QoS 1.
      deliver(msg) {
        if (closing || client.closed) return;
        broker.publish({
          cmd: 'publish', topic, qos: MqttBinding.QOS, retain: false,
          payload: Buffer.from(ChatProto.encode(msg), 'utf8'),
        }, () => {});
      },
      // E.g. replaced by a newer connection of the same user: say why (BYE), let the client
      // disconnect, and cut the connection ourselves if it does not.
      close(code, reason) {
        if (closing || client.closed) return;
        session.deliver(ChatProto.make(ChatProto.TYPES.BYE, { body: { code, reason } }));
        closing = setTimeout(() => client.close(), BYE_GRACE_MS);
        closing.unref();
      },
    };
    sessions.set(client, session);
    return session;
  }

  broker.on('client', client => log(`mqtt connected ${client.id}`));
  broker.on('keepaliveTimeout', client => log(`mqtt keepalive timeout ${client.id} (no packet for 1.5 x keepalive)`));
  broker.on('clientError', (client, err) => log(`mqtt error ${client.id}: ${err.message}`));
  // Fires once however the connection ends: DISCONNECT packet, socket closed, keepalive timeout.
  // This is why we need no Last Will: the broker IS our server, so the hub hears of it directly.
  broker.on('clientDisconnect', client => {
    const session = sessions.get(client);
    sessions.delete(client);
    log(`mqtt closed ${(session && session.username) || client.id}`);
    if (session) hub.leave(session);
  });

  // MQTT over WebSocket: each MQTT packet travels in binary WebSocket frames. The browser asks
  // for the "mqtt" subprotocol; Aedes reads and writes a plain byte stream over that socket.
  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: ChatProto.MAX_FRAME_BYTES + 1024, // one ChatProto frame + topic + MQTT header
    handleProtocols: protocols => (protocols.has('mqtt') ? 'mqtt' : false),
  });
  wss.on('connection', (ws, req) => {
    const stream = createWebSocketStream(ws);
    stream.on('error', () => {}); // Aedes logs and closes on its own
    broker.handle(stream, req);
  });

  return {
    wss,
    close: async () => {
      // Closes every MQTT client; each fires clientDisconnect -> hub.leave before this resolves.
      await new Promise(done => broker.close(done));
      for (const ws of wss.clients) ws.terminate(); // sockets that never finished CONNECT
      wss.close();
    },
  };
}

module.exports = { createMqttEndpoint };
