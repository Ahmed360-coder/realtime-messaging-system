// ChatProto over MQTT (Step 5): how ChatProto messages travel on an MQTT broker.
// Like chatproto.js this file only describes the format (topic names, QoS, keepalive), so the
// server (require) and the browser (<script src="/protocols/mqtt-binding.js">) share it.
//
// Each MQTT connection gets a fresh random client id and exactly two topics:
//
//   chat/<clientId>/up     client -> server   payload: ChatProto HELLO / MSG / LIST
//   chat/<clientId>/down   server -> client   payload: ChatProto WELCOME / SYNCED / MSG / ACK /
//                                                      PRESENCE / USERS / ERROR / BYE
//
// The payload of every PUBLISH is one ChatProto JSON object, exactly what one WebSocket text
// frame carries on /ws. Clients never publish to each other: the server (the hub) is the only
// subscriber of the up topics and the only publisher on the down topics, because it must store
// each message and give it a seq before anyone receives it. The broker's access rules (see
// server/transports/mqtt.js) allow a connection to use only its own two topics.

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.MqttBinding = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const PATH = '/mqtt';     // MQTT over WebSocket: ws://<laptop-ip>:<port>/mqtt
  const QOS = 1;            // at least once on every hop; ChatProto ids remove the duplicates
  const KEEPALIVE_S = 30;   // client sends PINGREQ when idle; broker drops it after 1.5 x 30 s
  // "c-" + 32 hex digits. No "/", "+" or "#", so a client id can never change a topic's meaning.
  const CLIENT_ID_RE = /^c-[0-9a-f]{32}$/;

  // A new random client id for every connection (never reused, see server/transports/mqtt.js).
  function newClientId(uuid) {
    return 'c-' + uuid().replace(/-/g, '');
  }

  const upTopic = clientId => `chat/${clientId}/up`;
  const downTopic = clientId => `chat/${clientId}/down`;

  return { PATH, QOS, KEEPALIVE_S, CLIENT_ID_RE, newClientId, upTopic, downTopic };
});
