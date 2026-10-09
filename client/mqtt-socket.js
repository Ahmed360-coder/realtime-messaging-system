// MqttSocket – makes an MQTT connection look like a WebSocket to ChatClient (Step 5).
// ChatClient only uses: readyState, send(text), close(code, reason), onopen, onmessage, onclose.
// This adapter gives mqtt.js (the MQTT client library) exactly that shape, so every reliability
// feature of ChatClient (outbox + retry, dedup, lastSeq sync, reconnect with backoff) works over
// MQTT without a single change:
//
//   WebSocket                    MQTT (protocols/mqtt-binding.js)
//   new WebSocket(url)           CONNECT (fresh client id, clean session, keepalive 30 s)
//   'open'                       CONNACK accepted + SUBACK for chat/<id>/down
//   send(text)                   PUBLISH chat/<id>/up, QoS 1
//   'message'                    PUBLISH received on chat/<id>/down
//   close(code)                  DISCONNECT
//   'close' with code            connection lost (1006), or the code from a BYE frame
//
// Used by the browser (<script src="/vendor/mqtt.min.js"> first) and by the Node tests.

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory(require('mqtt'), require('../protocols/chatproto'), require('../protocols/mqtt-binding'));
  } else {
    root.MqttSocket = factory(root.mqtt, root.ChatProto, root.MqttBinding);
  }
})(typeof self !== 'undefined' ? self : this, function (mqtt, ChatProto, MqttBinding) {
  'use strict';

  // The same numbers as WebSocket.CONNECTING / OPEN / CLOSING / CLOSED.
  const CONNECTING = 0, OPEN = 1, CLOSING = 2, CLOSED = 3;
  const CLOSE_NORMAL = 1000, CLOSE_PROTOCOL_ERROR = 1002, CLOSE_ABNORMAL = 1006; // WebSocket close codes
  const SUBACK_FAILURE = 128;

  class MqttSocket {
    /** url: ws://host:port/mqtt */
    constructor(url) {
      this.readyState = CONNECTING;
      this.onopen = this.onmessage = this.onclose = this.onerror = null;
      this.traceFn = null;
      this.earlyTrace = []; // CONNECT is sent inside this constructor, before ontrace can be set
      this.closeCode = null;

      // A new client id for every connection: our topics belong to this connection only.
      this.clientId = MqttBinding.newClientId(ChatProto.uuid);
      this.upTopic = MqttBinding.upTopic(this.clientId);
      this.downTopic = MqttBinding.downTopic(this.clientId);

      const client = mqtt.connect(url, {
        protocolVersion: 4,                   // MQTT 3.1.1
        clientId: this.clientId,
        clean: true,                          // no broker-side session: SQLite + lastSeq do offline sync
        keepalive: MqttBinding.KEEPALIVE_S,   // PINGREQ when idle; the broker drops us after 1.5 x this
        reconnectPeriod: 0,                   // ChatClient reconnects (with backoff), not mqtt.js
        connectTimeout: 10_000,
        manualConnect: true,                  // connect() below, after 'packetsend' is listened to
      });
      this.client = client;

      client.on('packetsend', p => this.trace('out', p));
      client.on('packetreceive', p => this.trace('in', p));

      // CONNACK accepted. With a clean session nothing is subscribed yet, and the server's replies
      // would be dropped, so subscribe to our down topic BEFORE telling ChatClient we are open.
      client.on('connect', () => {
        client.subscribe(this.downTopic, { qos: MqttBinding.QOS }, (err, granted) => {
          if (this.readyState !== CONNECTING) return;
          if (err || !granted || !granted.length || granted[0].qos === SUBACK_FAILURE) {
            return this.close(CLOSE_PROTOCOL_ERROR, 'subscription refused');
          }
          this.readyState = OPEN;
          if (this.onopen) this.onopen();
        });
      });

      client.on('message', (topic, payload) => {
        if (topic !== this.downTopic || this.readyState !== OPEN) return;
        const text = payload.toString();
        if (this.onmessage) this.onmessage({ data: text });
        // BYE = the server is closing us and says why (e.g. 4001 replaced): close with that code,
        // as a WebSocket close frame would.
        const r = ChatProto.decode(text, ChatProto.SERVER_TYPES);
        if (r.ok && r.msg.type === ChatProto.TYPES.BYE) this.close(r.msg.body.code, r.msg.body.reason);
      });

      // Network gone, broker closed us, keepalive timeout, CONNACK refused ... or our own end().
      client.on('close', () => this.finish(CLOSE_ABNORMAL));
      client.on('error', err => { if (this.onerror) this.onerror(err); }); // a 'close' follows
      client.connect(); // opens the WebSocket, then sends CONNECT
    }

    /** Publish one ChatProto frame to our up topic (QoS 1: mqtt.js re-sends until PUBACK). */
    send(text) {
      if (this.readyState !== OPEN) throw new Error('MqttSocket is not open');
      this.client.publish(this.upTopic, text, { qos: MqttBinding.QOS });
    }

    /** Send DISCONNECT and close. The code is what onclose reports (like a WebSocket close frame). */
    close(code = CLOSE_NORMAL, reason = '') {
      if (this.readyState === CLOSING || this.readyState === CLOSED) return;
      this.readyState = CLOSING;
      this.closeCode = code;
      // force = true: do not wait for in-flight PUBACKs, ChatClient re-sends anything unACKed.
      this.client.end(true, () => this.finish(code));
    }

    finish(code) {
      if (this.readyState === CLOSED) return;
      this.readyState = CLOSED;
      this.client.end(true); // no-op if already ended
      if (this.onclose) this.onclose({ code: this.closeCode == null ? code : this.closeCode });
    }

    /** Optional (dir, text) callback for every MQTT packet, for the debug log. */
    get ontrace() { return this.traceFn; }
    set ontrace(fn) {
      this.traceFn = fn;
      if (fn) for (const [dir, text] of this.earlyTrace.splice(0)) fn(dir, text);
    }

    // One line per MQTT packet for the debug log, e.g. "MQTT PUBACK id=3".
    trace(dir, p) {
      let text = `MQTT ${p.cmd.toUpperCase()}`;
      switch (p.cmd) {
        case 'connect': text += ` clientId=${p.clientId} clean=${p.clean} keepalive=${p.keepalive}s`; break;
        case 'connack': text += ` returnCode=${p.returnCode}`; break;
        case 'subscribe': text += ' ' + p.subscriptions.map(s => `${s.topic} qos${s.qos}`).join(', '); break;
        case 'suback': text += ` granted=[${p.granted}]`; break;
        case 'publish': text += ` ${p.topic} qos${p.qos}${p.messageId != null ? ` id=${p.messageId}` : ''}`; break;
        case 'puback': text += ` id=${p.messageId}`; break;
      }
      if (this.traceFn) this.traceFn(dir, text);
      else if (this.earlyTrace.length < 10) this.earlyTrace.push([dir, text]);
    }
  }

  Object.assign(MqttSocket, { CONNECTING, OPEN, CLOSING, CLOSED });
  return MqttSocket;
});
