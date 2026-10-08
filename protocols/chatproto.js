// ChatProto v1 – our custom application-layer protocol.
// This file only knows the MESSAGE FORMAT: how to build, encode, decode and validate
// a ChatProto message. It knows nothing about sockets or routing, so the same file is
// used by the Node server (require) and by the browser (<script src="/protocols/chatproto.js">).
//
// Wire format: one JSON object per WebSocket text frame, for example
//   { "v": 1, "type": "MSG", "id": "<uuid>", "ts": 1760000000000,
//     "from": "alice", "to": "bob", "body": "hi", "seq": 42 }
// `id` is chosen by the sender (used for dedup); `seq` is assigned by the server when it
// stores the message (used for ordering and offline sync).

(function (root, factory) {
  // Works in Node (CommonJS) and in the browser (global ChatProto).
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.ChatProto = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const VERSION = 1;

  // Message types and who is allowed to send them.
  const TYPES = {
    // client -> server
    HELLO: 'HELLO',       // join the chat:          body = { username, token, lastSeq }
    MSG: 'MSG',           // 1-to-1 chat message:    to, body = text (server adds from)
    LIST: 'LIST',         // ask for online users
    // server -> client
    WELCOME: 'WELCOME',   // join accepted:          body = { username, users, known }
    SYNCED: 'SYNCED',     // offline replay done:    body = { count, lastSeq }
    ACK: 'ACK',           // server STORED a MSG:    body = { ref, seq, status: 'delivered'|'stored'|'duplicate' }
    PRESENCE: 'PRESENCE', // someone joined/left:    body = { username, status: 'online'|'offline' }
    USERS: 'USERS',       // online + known users:   body = { users, known }
    ERROR: 'ERROR',       // something was wrong:    body = { code, message, ref }
  };
  const CLIENT_TYPES = new Set([TYPES.HELLO, TYPES.MSG, TYPES.LIST]);
  const SERVER_TYPES = new Set([TYPES.WELCOME, TYPES.SYNCED, TYPES.MSG, TYPES.ACK, TYPES.PRESENCE, TYPES.USERS, TYPES.ERROR]);

  // Error codes carried in ERROR.body.code.
  const ERRORS = {
    BAD_JSON: 'BAD_JSON',         // frame is not valid JSON / not an object
    BAD_VERSION: 'BAD_VERSION',   // v is not 1
    BAD_TYPE: 'BAD_TYPE',         // unknown type, or a type this side may not send
    BAD_FIELD: 'BAD_FIELD',       // a required field is missing or has the wrong shape
    TOO_LARGE: 'TOO_LARGE',       // frame bigger than MAX_FRAME_BYTES
    NOT_JOINED: 'NOT_JOINED',     // sent something other than HELLO before joining
    ALREADY_JOINED: 'ALREADY_JOINED',
    NAME_TAKEN: 'NAME_TAKEN',     // username is registered to a different token
    UNKNOWN_USER: 'UNKNOWN_USER', // recipient never joined (offline users are fine: stored)
  };

  // WebSocket close code (4000-4999 = application-defined) sent to an old connection when
  // the same user reconnects on a new one. Clients must NOT auto-reconnect after it.
  const CLOSE_REPLACED = 4001;

  const MAX_FRAME_BYTES = 16 * 1024; // whole encoded frame
  const MAX_BODY_CHARS = 2000;       // chat text
  const USERNAME_RE = /^[A-Za-z0-9_]{1,20}$/;
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const TOKEN_RE = /^[A-Za-z0-9_-]{16,128}$/; // random secret: "same device as last time"
  const isSeq = x => Number.isSafeInteger(x) && x >= 0;

  // A random (version 4) UUID. crypto.randomUUID() only exists in "secure contexts"
  // (HTTPS or localhost); phones open http://192.168.x.x, so fall back to getRandomValues.
  function uuid() {
    const c = globalThis.crypto;
    if (c && typeof c.randomUUID === 'function') {
      try { return c.randomUUID(); } catch (e) { /* not a secure context */ }
    }
    const b = c.getRandomValues(new Uint8Array(16));
    b[6] = (b[6] & 0x0f) | 0x40; // version 4
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    const h = Array.from(b, x => x.toString(16).padStart(2, '0')).join('');
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
  }

  // Build a message with the common header fields filled in.
  function make(type, fields) {
    return Object.assign({ v: VERSION, type, id: uuid(), ts: Date.now() }, fields);
  }

  // Encapsulation: message object -> text that goes inside one WebSocket frame.
  function encode(msg) {
    return JSON.stringify(msg);
  }

  const isPlainObject = x => x !== null && typeof x === 'object' && !Array.isArray(x);
  const fail = (code, message, ref = null) => ({ ok: false, error: { code, message, ref } });

  /**
   * Decapsulation + validation: raw frame text -> { ok: true, msg } or { ok: false, error }.
   * `allowed` is the set of types the sender is permitted to send
   * (the server passes CLIENT_TYPES, the browser passes SERVER_TYPES).
   */
  function decode(raw, allowed = CLIENT_TYPES) {
    if (typeof raw !== 'string') return fail(ERRORS.BAD_JSON, 'frame must be text');
    // Cheap length check in characters; the ws library also enforces MAX_FRAME_BYTES in bytes.
    if (raw.length > MAX_FRAME_BYTES) return fail(ERRORS.TOO_LARGE, `frame exceeds ${MAX_FRAME_BYTES} characters`);

    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return fail(ERRORS.BAD_JSON, 'frame is not valid JSON'); }
    if (!isPlainObject(msg)) return fail(ERRORS.BAD_JSON, 'frame must be a JSON object');

    // If the frame has a usable id, echo it back in the ERROR so the sender knows which message failed.
    const ref = typeof msg.id === 'string' && UUID_RE.test(msg.id) ? msg.id : null;

    // Header fields, common to every type.
    if (msg.v !== VERSION) return fail(ERRORS.BAD_VERSION, `unsupported version ${JSON.stringify(msg.v)}, expected ${VERSION}`, ref);
    // hasOwn, not TYPES[msg.type]: a type like "toString" must not count as known.
    if (typeof msg.type !== 'string' || !Object.hasOwn(TYPES, msg.type)) return fail(ERRORS.BAD_TYPE, `unknown type ${JSON.stringify(msg.type)}`, ref);
    if (!allowed.has(msg.type)) return fail(ERRORS.BAD_TYPE, `type ${msg.type} may not be sent by this side`, ref);
    if (!ref) return fail(ERRORS.BAD_FIELD, 'id must be a UUID string');
    if (!Number.isFinite(msg.ts)) return fail(ERRORS.BAD_FIELD, 'ts must be a number (ms since epoch)', ref);

    // Type-specific fields.
    const problem = checkFields(msg);
    if (problem) return fail(ERRORS.BAD_FIELD, problem, ref);
    return { ok: true, msg };
  }

  // Returns a description of what is wrong, or null if the type-specific fields are fine.
  function checkFields(msg) {
    switch (msg.type) {
      case TYPES.HELLO:
        if (!isPlainObject(msg.body) || typeof msg.body.username !== 'string' || !USERNAME_RE.test(msg.body.username))
          return 'HELLO body.username must be 1-20 letters, digits or _';
        if (typeof msg.body.token !== 'string' || !TOKEN_RE.test(msg.body.token))
          return 'HELLO body.token must be 16-128 letters, digits, _ or -';
        if (msg.body.lastSeq !== undefined && !isSeq(msg.body.lastSeq))
          return 'HELLO body.lastSeq must be an integer >= 0';
        return null;
      case TYPES.MSG:
        if (typeof msg.to !== 'string' || !USERNAME_RE.test(msg.to)) return 'MSG to must be a valid username';
        if (typeof msg.body !== 'string' || msg.body.trim() === '') return 'MSG body must be non-empty text';
        if (msg.body.length > MAX_BODY_CHARS) return `MSG body longer than ${MAX_BODY_CHARS} characters`;
        // Only the server assigns seq (a client's seq is ignored), but it must be well-formed.
        if (msg.seq !== undefined && !isSeq(msg.seq)) return 'MSG seq must be an integer >= 0';
        return null;
      case TYPES.LIST:
        return null;
      case TYPES.WELCOME:
      case TYPES.USERS:
        if (!isPlainObject(msg.body) || !Array.isArray(msg.body.users)) return `${msg.type} body.users must be an array`;
        return null;
      case TYPES.SYNCED:
        if (!isPlainObject(msg.body) || !isSeq(msg.body.count) || !isSeq(msg.body.lastSeq))
          return 'SYNCED body needs integer count and lastSeq';
        return null;
      case TYPES.ACK:
        if (!isPlainObject(msg.body) || typeof msg.body.ref !== 'string') return 'ACK body.ref must be the acknowledged id';
        if (!isSeq(msg.body.seq)) return 'ACK body.seq must be the stored sequence number';
        return null;
      case TYPES.PRESENCE:
        if (!isPlainObject(msg.body) || typeof msg.body.username !== 'string' ||
            !['online', 'offline'].includes(msg.body.status)) return 'PRESENCE body needs username and status online|offline';
        return null;
      case TYPES.ERROR:
        if (!isPlainObject(msg.body) || typeof msg.body.code !== 'string') return 'ERROR body.code must be a string';
        return null;
      default:
        return 'unhandled type';
    }
  }

  // Convenience builder for ERROR replies. `ref` is the id of the offending message, if known.
  function error(code, message, ref) {
    return make(TYPES.ERROR, { body: { code, message, ref: ref || null } });
  }

  return {
    VERSION, TYPES, CLIENT_TYPES, SERVER_TYPES, ERRORS, CLOSE_REPLACED,
    MAX_FRAME_BYTES, MAX_BODY_CHARS, USERNAME_RE, TOKEN_RE,
    uuid, make, encode, decode, error,
  };
});
