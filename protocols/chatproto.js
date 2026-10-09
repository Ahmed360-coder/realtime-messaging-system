// ChatProto v1 – our custom application-layer protocol.
// This file only knows the MESSAGE FORMAT: how to build, encode, decode and validate
// a ChatProto message. It knows nothing about sockets or routing, so the same file is
// used by the Node server (require) and by the browser (<script src="/protocols/chatproto.js">).
//
// Wire format: one JSON object per WebSocket text frame (or per MQTT PUBLISH payload, see
// protocols/mqtt-binding.js), for example
//   { "v": 1, "type": "MSG", "id": "<uuid>", "ts": 1760000000000,
//     "from": "alice", "to": "bob", "body": "hi", "seq": 42 }
// `id` is chosen by the sender (used for dedup); `seq` is assigned by the server when it
// stores the message (used for ordering and offline sync).
//
// Addresses (Step 7): `to` is either a username ("bob", 1-to-1) or a group ("#study").
// A username can never contain '#', so the address alone says which kind it is.
//
// End-to-end encryption (Step 8): a MSG body is either plain text (a string) or a SEALED body,
// an object the server cannot read (made and opened by client/e2e.js, never by the server):
//   1-to-1: { nonce, box }          box = nacl.box of the message, to the other user
//   group:  { nonce, box, keys }    box = nacl.secretbox under a fresh key K; keys = { member: K boxed to them }
// Public keys travel in HELLO.body.key, WELCOME/USERS.body.keys and PRESENCE.body.key.

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
    HELLO: 'HELLO',       // join the chat:          body = { username, token, lastSeq, key (Step 8) }
    MSG: 'MSG',           // chat message:           to = user or #group, body = text or sealed (server adds from)
    LIST: 'LIST',         // ask for online users
    // both directions (Step 7)
    GROUP: 'GROUP',       // membership change:      to = #group, body = { op: create|add|leave, users }
                          //   server -> members: + from (who did it), seq, body.members (after the change)
    // server -> client
    WELCOME: 'WELCOME',   // join accepted:          body = { username, users, known, keys (Step 8) }
    SYNCED: 'SYNCED',     // offline replay done:    body = { count, lastSeq }
    ACK: 'ACK',           // server STORED a MSG:    body = { ref, seq, status: 'delivered'|'stored'|'duplicate' }
    PRESENCE: 'PRESENCE', // someone joined/left:    body = { username, status: 'online'|'offline', key (Step 8) }
    USERS: 'USERS',       // online + known users:   body = { users, known, keys (Step 8) }
    ERROR: 'ERROR',       // something was wrong:    body = { code, message, ref }
    BYE: 'BYE',           // server closes us (MQTT): body = { code, reason } (see CLOSE_REPLACED)
  };
  const CLIENT_TYPES = new Set([TYPES.HELLO, TYPES.MSG, TYPES.LIST, TYPES.GROUP]);
  const SERVER_TYPES = new Set([TYPES.WELCOME, TYPES.SYNCED, TYPES.MSG, TYPES.ACK, TYPES.PRESENCE, TYPES.USERS, TYPES.ERROR, TYPES.BYE, TYPES.GROUP]);

  // Membership operations carried in GROUP.body.op (Step 7). Groups are invite-only:
  //   create  anyone; the creator + body.users become the members
  //   add     any member may add registered users
  //   leave   any member may leave (there is no kick and no admin)
  const GROUP_OPS = ['create', 'add', 'leave'];
  const MAX_GROUP_MEMBERS = 50; // caps the fan-out: one group message = at most 49 copies

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
    // Step 7: groups
    UNKNOWN_GROUP: 'UNKNOWN_GROUP', // no group with that name
    NOT_MEMBER: 'NOT_MEMBER',       // only members may send to / add to / leave a group
    GROUP_EXISTS: 'GROUP_EXISTS',   // create: the name is taken
    GROUP_FULL: 'GROUP_FULL',       // the group would have more than MAX_GROUP_MEMBERS members
    // Step 8: end-to-end encryption
    KEY_MISMATCH: 'KEY_MISMATCH',   // HELLO.key differs from the public key registered for that username
    STALE_MEMBERS: 'STALE_MEMBERS', // sealed group MSG: body.keys is not exactly the current members
  };

  // WebSocket close code (4000-4999 = application-defined) sent to an old connection when
  // the same user reconnects on a new one. Clients must NOT auto-reconnect after it.
  const CLOSE_REPLACED = 4001;
  // A WebSocket close frame carries such a code, but an MQTT 3.1.1 broker cannot tell a client
  // why it is being disconnected. Over MQTT the server therefore sends BYE { code, reason }
  // just before closing, and the client treats it like a close frame with that code.

  const MAX_FRAME_BYTES = 16 * 1024; // whole encoded frame
  const MAX_BODY_CHARS = 2000;       // chat text
  const USERNAME_RE = /^[A-Za-z0-9_]{1,20}$/;
  const GROUP_RE = /^#[A-Za-z0-9_]{1,20}$/; // a group address: '#' + the same characters
  const isGroup = to => typeof to === 'string' && GROUP_RE.test(to);
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const TOKEN_RE = /^[A-Za-z0-9_-]{16,128}$/; // random secret: "same device as last time"
  const isSeq = x => Number.isSafeInteger(x) && x >= 0;

  // Step 8: binary values travel as base64 text. NaCl sizes are fixed: a Curve25519 public key
  // is 32 bytes (44 chars), a nonce 24 bytes (32 chars), a group key box 80 bytes (108 chars:
  // the 32-byte message key + a 32-byte hash of the ciphertext + the 16-byte Poly1305 tag).
  const KEY_RE = /^[A-Za-z0-9+/]{43}=$/;
  const NONCE_RE = /^[A-Za-z0-9+/]{32}$/;
  const KEYBOX_RE = /^[A-Za-z0-9+/]{107}=$/;
  const BASE64_RE = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
  const MIN_BOX_CHARS = 24; // a box is at least the 16-byte tag + 1 byte of text = 17 bytes = 24 chars

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
        if (msg.body.key !== undefined && !isKey(msg.body.key))
          return 'HELLO body.key must be a base64 Curve25519 public key (32 bytes)';
        return null;
      case TYPES.MSG:
        if (typeof msg.to !== 'string' || !(USERNAME_RE.test(msg.to) || isGroup(msg.to))) return 'MSG to must be a username or #group';
        if (isSealed(msg.body)) {
          const problem = checkSealed(msg.body, isGroup(msg.to));
          if (problem) return problem;
        } else {
          if (typeof msg.body !== 'string' || msg.body.trim() === '') return 'MSG body must be non-empty text or a sealed object';
          if (msg.body.length > MAX_BODY_CHARS) return `MSG body longer than ${MAX_BODY_CHARS} characters`;
        }
        // Only the server assigns seq (a client's seq is ignored), but it must be well-formed.
        if (msg.seq !== undefined && !isSeq(msg.seq)) return 'MSG seq must be an integer >= 0';
        return null;
      case TYPES.LIST:
        return null;
      case TYPES.GROUP: {
        if (!isGroup(msg.to)) return 'GROUP to must be #name (1-20 letters, digits or _)';
        const b = msg.body;
        if (!isPlainObject(b) || !GROUP_OPS.includes(b.op)) return `GROUP body.op must be one of ${GROUP_OPS.join('|')}`;
        if (b.op !== 'leave') {
          if (!Array.isArray(b.users) || !b.users.every(u => typeof u === 'string' && USERNAME_RE.test(u)))
            return 'GROUP body.users must be an array of usernames';
          if (b.users.length > MAX_GROUP_MEMBERS) return `a group has at most ${MAX_GROUP_MEMBERS} members`;
          if (b.op === 'add' && b.users.length === 0) return 'GROUP add needs at least one user';
        }
        // From the server it also lists who is in the group after the change.
        if (b.members !== undefined && (!Array.isArray(b.members) || !b.members.every(u => typeof u === 'string')))
          return 'GROUP body.members must be an array of usernames';
        if (msg.seq !== undefined && !isSeq(msg.seq)) return 'GROUP seq must be an integer >= 0';
        return null;
      }
      case TYPES.WELCOME:
      case TYPES.USERS:
        if (!isPlainObject(msg.body) || !Array.isArray(msg.body.users)) return `${msg.type} body.users must be an array`;
        if (msg.body.keys !== undefined && !isKeyMap(msg.body.keys, KEY_RE)) return `${msg.type} body.keys must map usernames to public keys`;
        return null;
      case TYPES.SYNCED:
        if (!isPlainObject(msg.body) || !isSeq(msg.body.count) || !isSeq(msg.body.lastSeq))
          return 'SYNCED body needs integer count and lastSeq';
        return null;
      case TYPES.ACK:
        if (!isPlainObject(msg.body) || typeof msg.body.ref !== 'string') return 'ACK body.ref must be the acknowledged id';
        if (!isSeq(msg.body.seq)) return 'ACK body.seq must be the stored sequence number';
        // Group messages (Step 7): how many members got it live, out of how many.
        if (msg.body.recipients !== undefined && (!isSeq(msg.body.recipients) || !isSeq(msg.body.delivered)))
          return 'ACK body.recipients and body.delivered must be integers >= 0';
        return null;
      case TYPES.PRESENCE:
        if (!isPlainObject(msg.body) || typeof msg.body.username !== 'string' ||
            !['online', 'offline'].includes(msg.body.status)) return 'PRESENCE body needs username and status online|offline';
        if (msg.body.key !== undefined && !isKey(msg.body.key)) return 'PRESENCE body.key must be a public key';
        return null;
      case TYPES.ERROR:
        if (!isPlainObject(msg.body) || typeof msg.body.code !== 'string') return 'ERROR body.code must be a string';
        return null;
      case TYPES.BYE:
        if (!isPlainObject(msg.body) || !Number.isInteger(msg.body.code)) return 'BYE body.code must be an integer close code';
        return null;
      default:
        return 'unhandled type';
    }
  }

  // ---- Step 8: end-to-end encryption ----

  const isKey = k => typeof k === 'string' && KEY_RE.test(k);

  // A MSG body that is an object is a sealed (encrypted) body; a string is plain text.
  const isSealed = body => isPlainObject(body);

  // { username: base64 }, every value matching `re`.
  function isKeyMap(map, re) {
    return isPlainObject(map) && Object.entries(map).every(([u, k]) => USERNAME_RE.test(u) && typeof k === 'string' && re.test(k));
  }

  // The shape of a sealed MSG body. The server can only check the SHAPE (sizes, base64, who the
  // key boxes are for). Whether the bytes really are ciphertext, only the recipients can tell.
  function checkSealed(b, group) {
    const fields = group ? ['nonce', 'box', 'keys'] : ['nonce', 'box'];
    const extra = Object.keys(b).find(k => !fields.includes(k));
    if (extra) return `sealed MSG body has an unexpected field ${JSON.stringify(extra)}`;
    if (typeof b.nonce !== 'string' || !NONCE_RE.test(b.nonce)) return 'sealed MSG body.nonce must be 24 bytes of base64';
    if (typeof b.box !== 'string' || b.box.length < MIN_BOX_CHARS || !BASE64_RE.test(b.box)) return 'sealed MSG body.box must be base64 ciphertext';
    if (!group) return null;
    if (!isKeyMap(b.keys, KEYBOX_RE)) return 'sealed group MSG body.keys must map each member to an 80-byte key box';
    const n = Object.keys(b.keys).length;
    if (n === 0 || n > MAX_GROUP_MEMBERS) return `sealed group MSG body.keys must have 1-${MAX_GROUP_MEMBERS} entries`;
    return null;
  }

  // Convenience builder for ERROR replies. `ref` is the id of the offending message, if known.
  function error(code, message, ref) {
    return make(TYPES.ERROR, { body: { code, message, ref: ref || null } });
  }

  return {
    VERSION, TYPES, CLIENT_TYPES, SERVER_TYPES, ERRORS, CLOSE_REPLACED,
    MAX_FRAME_BYTES, MAX_BODY_CHARS, USERNAME_RE, TOKEN_RE, GROUP_RE, GROUP_OPS, MAX_GROUP_MEMBERS,
    KEY_RE, NONCE_RE, KEYBOX_RE,
    uuid, make, encode, decode, error, isGroup, isSealed,
  };
});
