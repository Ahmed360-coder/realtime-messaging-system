// End-to-end encryption (Step 8) with TweetNaCl. Runs ONLY on the clients: the server never
// has a secret key, so it stores and forwards ciphertext it cannot read.
// Used by the browser page (<script src="/vendor/nacl.min.js"> and chatproto.js first) and by
// the Node tests (require), like chat-client.js.
//
// Building blocks (TweetNaCl = a JavaScript port of the NaCl library):
//   nacl.box.keyPair()                Curve25519 key pair: 32-byte secret key, 32-byte public key
//   nacl.box(m, nonce, theirPk, mySk) Diffie-Hellman shared key (X25519) + XSalsa20-Poly1305:
//                                     encrypt AND authenticate m. box(alice -> bob) and box(bob -> alice)
//                                     use the same shared key, so the sender can open her own messages.
//   nacl.secretbox(m, nonce, key)     XSalsa20-Poly1305 with a 32-byte symmetric key
//   nacl.hash                         SHA-512;  nacl.randomBytes: the OS's secure random generator
//
// Sealed MSG bodies (the format is checked by protocols/chatproto.js):
//   1-to-1  { nonce, box }        box  = nacl.box(inner, nonce, recipient's public key, my secret key)
//   group   { nonce, box, keys }  box  = nacl.secretbox(inner, nonce, K), K = 32 fresh random bytes
//                                 keys = { member: nacl.box(K + first 32 bytes of SHA-512(box), nonce, member's key, my key) }
//                                 for EVERY current member, the sender included.
//   inner = JSON { from, to, id, ts, text }: the envelope is copied inside the sealed part, and the
//   receiver checks it, so the server cannot move a message to another chat, change who it is
//   from, or replay it under a new id or time.
//
// Nonces: 24 random bytes per message. A nonce must never be used twice WITH THE SAME KEY. In a
// group message the one nonce is used with K and with each member's shared key: all different keys.
// A retry re-sends the identical frame (same id, same nonce, same ciphertext): the same message, not a reuse.
//
// Trust: public keys come from the server (WELCOME / USERS / PRESENCE). The first key seen for a
// user is pinned (TOFU, trust on first use). If the server later presents a different one, we
// keep the pinned key, refuse to encrypt to the new one and warn, until the user compares the
// safety number and accepts it.

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('tweetnacl'), require('../protocols/chatproto'));
  else root.E2E = factory(root.nacl, root.ChatProto);
})(typeof self !== 'undefined' ? self : this, function (nacl, ChatProto) {
  'use strict';

  const utf8 = new TextEncoder();
  const fromUtf8 = new TextDecoder('utf-8', { fatal: true }); // invalid UTF-8 throws, never guesses

  // ---- base64 (bytes <-> text): binary data travels inside JSON as base64 ----
  function toB64(bytes) {
    let s = '';
    for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
    return btoa(s);
  }
  function fromB64(text) {
    const s = atob(text);
    const bytes = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) bytes[i] = s.charCodeAt(i);
    return bytes;
  }
  function concat(a, b) {
    const out = new Uint8Array(a.length + b.length);
    out.set(a);
    out.set(b, a.length);
    return out;
  }
  // The first 32 bytes of SHA-512: binds a group's key box to exactly this ciphertext.
  const digest = bytes => nacl.hash(bytes).subarray(0, 32);

  // ---- Identity: this device's key pair ----

  /** A new Curve25519 key pair, as base64 text (ready for localStorage). */
  function newIdentity() {
    const kp = nacl.box.keyPair();
    return { publicKey: toB64(kp.publicKey), secretKey: toB64(kp.secretKey) };
  }

  /** Is `x` a usable stored identity (e.g. read back from localStorage)? */
  function isIdentity(x) {
    if (!x || typeof x.publicKey !== 'string' || typeof x.secretKey !== 'string') return false;
    if (!ChatProto.KEY_RE.test(x.publicKey) || !ChatProto.KEY_RE.test(x.secretKey)) return false;
    // The public key must belong to the secret key.
    const kp = nacl.box.keyPair.fromSecretKey(fromB64(x.secretKey));
    return toB64(kp.publicKey) === x.publicKey;
  }

  /**
   * Safety number of two users: 30 digits from SHA-512 over both usernames and public keys,
   * sorted by name so both sides compute the same digits. Equal on both phones = nobody (not
   * even the server) swapped a key in between. Shown in 6 groups of 5 digits.
   */
  function safetyNumber(nameA, keyA, nameB, keyB) {
    const pair = [[nameA, keyA], [nameB, keyB]].sort((x, y) => (x[0] < y[0] ? -1 : 1));
    const h = nacl.hash(utf8.encode(JSON.stringify(pair)));
    const groups = [];
    for (let i = 0; i < 6; i++) {
      let n = 0;
      for (let j = 0; j < 5; j++) n = n * 256 + h[i * 5 + j]; // 5 bytes = 40 bits, exact in a JS number
      groups.push(String(n % 100000).padStart(5, '0'));
    }
    return groups.join(' ');
  }

  /** Refused to encrypt: code NO_KEY (no public key known), KEY_CHANGED (not accepted yet), NOT_MEMBER. */
  class E2EError extends Error {
    constructor(code, message) {
      super(message);
      this.code = code;
    }
  }

  // ---- Seal / open: pure functions on bytes ----

  // The part that is encrypted: the text plus a copy of the envelope (see "inner" above).
  const innerOf = (msg, text) => utf8.encode(JSON.stringify({ from: msg.from, to: msg.to, id: msg.id, ts: msg.ts, text }));

  // Decode `bytes` as inner JSON and check it matches the envelope of `msg`; the text, or null.
  function checkInner(bytes, msg) {
    if (!bytes) return null; // the Poly1305 tag did not verify: wrong key or modified bytes
    let inner;
    try { inner = JSON.parse(fromUtf8.decode(bytes)); } catch (e) { return null; }
    if (!inner || typeof inner.text !== 'string') return null;
    if (inner.from !== msg.from || inner.to !== msg.to || inner.id !== msg.id || inner.ts !== msg.ts) return null;
    return inner.text;
  }

  /** 1-to-1: { nonce, box } for msg (from, to, id, ts). theirKey / mySecret are bytes. */
  function sealDirect(msg, text, theirKey, mySecret) {
    const nonce = nacl.randomBytes(nacl.box.nonceLength);
    const box = nacl.box(innerOf(msg, text), nonce, theirKey, mySecret);
    return { nonce: toB64(nonce), box: toB64(box) };
  }

  /** peerKey = the OTHER party's public key (the recipient's if we sent it). The text, or null. */
  function openDirect(msg, peerKey, mySecret) {
    const opened = nacl.box.open(fromB64(msg.body.box), fromB64(msg.body.nonce), peerKey, mySecret);
    return checkInner(opened, msg);
  }

  /** Group: memberKeys = { username: public key bytes } of every member, the sender included. */
  function sealGroup(msg, text, memberKeys, mySecret) {
    const nonce = nacl.randomBytes(nacl.secretbox.nonceLength);
    const key = nacl.randomBytes(nacl.secretbox.keyLength); // K: used for this one message only
    const box = nacl.secretbox(innerOf(msg, text), nonce, key);
    const sealedKey = concat(key, digest(box));             // K + "for exactly this ciphertext"
    const keys = {};
    for (const [user, pk] of Object.entries(memberKeys)) keys[user] = toB64(nacl.box(sealedKey, nonce, pk, mySecret));
    return { nonce: toB64(nonce), box: toB64(box), keys };
  }

  /** senderKey = the sender's public key (ours if we sent it). The text, or null. */
  function openGroup(msg, me, senderKey, mySecret) {
    const mine = msg.body.keys[me];
    if (!mine) return null; // not encrypted to us
    const nonce = fromB64(msg.body.nonce);
    const sealedKey = nacl.box.open(fromB64(mine), nonce, senderKey, mySecret);
    if (!sealedKey || sealedKey.length !== 64) return null;
    const box = fromB64(msg.body.box);
    // Only the sender can make our key box, and it names this exact ciphertext: another member,
    // who also knows K, cannot swap in a different text under the sender's name.
    if (!nacl.verify(digest(box), sealedKey.subarray(32))) return null;
    return checkInner(nacl.secretbox.open(box, nonce, sealedKey.subarray(0, 32)), msg);
  }

  // ---- KeyRing: one user's keys, the public keys of everybody else, and the group members ----

  class KeyRing {
    /**
     * me:       our username
     * identity: { publicKey, secretKey } (base64), from newIdentity()
     * pins:     { username: public key } pinned by an earlier page load (TOFU)
     * onPin:    called with the pins whenever they change, so the page can save them
     */
    constructor({ me, identity, pins = {}, onPin = () => {} }) {
      this.me = me;
      this.publicKey = identity.publicKey;
      this.secret = fromB64(identity.secretKey);
      this.pins = { ...pins };
      this.onPin = onPin;
      this.changed = new Map(); // username -> the different key the server offered (not trusted)
      this.groups = new Map();  // '#name' -> { members, seq }, from GROUP events
    }

    /**
     * The server says `user` has public key `key`. Returns 'new' (pinned now), 'same', or
     * 'changed' (differs from the pinned key: the pinned one is kept until trust(user)).
     */
    learn(user, key) {
      if (!key || user === this.me) return 'same';
      const pinned = this.pins[user];
      if (!pinned) {
        this.pins[user] = key;
        this.onPin(this.pins);
        return 'new';
      }
      if (pinned === key) {
        this.changed.delete(user);
        return 'same';
      }
      this.changed.set(user, key);
      return 'changed';
    }

    /** learn() every entry of { username: key }. Returns the users whose key changed. */
    learnAll(keys = {}) {
      return Object.entries(keys).filter(([user, key]) => this.learn(user, key) === 'changed').map(([user]) => user);
    }

    /** The user compared the new safety number and accepts the new key. */
    trust(user) {
      const key = this.changed.get(user);
      if (!key) return false;
      this.pins[user] = key;
      this.changed.delete(user);
      this.onPin(this.pins);
      return true;
    }

    /** 'self' | 'pinned' | 'changed' | 'unknown' */
    status(user) {
      if (user === this.me) return 'self';
      if (this.changed.has(user)) return 'changed';
      return this.pins[user] ? 'pinned' : 'unknown';
    }

    /** The trusted public key of `user` (bytes), or null. */
    keyOf(user) {
      if (user === this.me) return fromB64(this.publicKey);
      return this.pins[user] ? fromB64(this.pins[user]) : null;
    }

    /** Safety number with `user`, for the key we trust (or for the new key, if `offered`). */
    safetyNumber(user, offered = false) {
      const key = offered ? this.changed.get(user) : this.pins[user];
      return key ? safetyNumber(this.me, this.publicKey, user, key) : null;
    }

    /** A GROUP event: the member list after a change. Older events never overwrite newer ones. */
    setMembers(group, members, seq) {
      const known = this.groups.get(group);
      if (known && !(seq > known.seq)) return;
      if (members.includes(this.me)) this.groups.set(group, { members: [...members], seq });
      else this.groups.delete(group);
    }

    // The trusted key of `user`, or an E2EError saying why we must not encrypt to them.
    requireKey(user) {
      if (this.changed.has(user)) {
        throw new E2EError('KEY_CHANGED', `${user}'s security key has changed. Compare the safety number before sending.`);
      }
      const key = this.keyOf(user);
      if (!key) throw new E2EError('NO_KEY', `${user} has no encryption key yet (they must open the app once).`);
      return key;
    }

    /**
     * Seal the text of an outgoing MSG (msg: to, id, ts). Returns the sealed body.
     * Throws E2EError if it cannot be encrypted to every recipient: then nothing is sent.
     */
    seal(msg, text) {
      const env = { from: this.me, to: msg.to, id: msg.id, ts: msg.ts };
      if (!ChatProto.isGroup(msg.to)) return sealDirect(env, text, this.requireKey(msg.to), this.secret);
      const group = this.groups.get(msg.to);
      if (!group) throw new E2EError('NOT_MEMBER', `you are not a member of ${msg.to}`);
      const keys = {};
      for (const user of group.members) keys[user] = this.requireKey(user);
      return sealGroup(env, text, keys, this.secret);
    }

    /**
     * Open a MSG (received, replayed, or our own). Returns { text, security }:
     *   security 'e2e'    sealed, decrypted and authenticated
     *            'plain'  sent as plain text (not encrypted: the sender is not verified either)
     *            'failed' sealed, but it does not open for us (wrong key, modified, not for us)
     */
    open(msg) {
      if (!ChatProto.isSealed(msg.body)) return { text: String(msg.body), security: 'plain' };
      let text = null;
      try {
        if (ChatProto.isGroup(msg.to)) {
          const senderKey = this.keyOf(msg.from);
          if (senderKey) text = openGroup(msg, this.me, senderKey, this.secret);
        } else {
          const peerKey = this.keyOf(msg.from === this.me ? msg.to : msg.from);
          if (peerKey) text = openDirect(msg, peerKey, this.secret);
        }
      } catch (e) {
        text = null; // malformed base64 and the like: it simply does not open
      }
      return text === null ? { text: '', security: 'failed' } : { text, security: 'e2e' };
    }

    /** The message as the page shows it: body = the text, plus security and the sealed original. */
    reveal(msg) {
      const { text, security } = this.open(msg);
      return { ...msg, body: text, security, sealed: ChatProto.isSealed(msg.body) ? msg.body : null };
    }

    /**
     * One of OUR group messages was refused with STALE_MEMBERS: open it (we have a key box for
     * ourselves) and seal it again for the current members. Same id, ts and text.
     */
    reseal(msg) {
      const { text, security } = this.open({ ...msg, from: this.me });
      if (security !== 'e2e') throw new E2EError('NO_KEY', 'cannot re-open our own message');
      return this.seal(msg, text);
    }
  }

  return { KeyRing, E2EError, newIdentity, isIdentity, safetyNumber, sealDirect, openDirect, sealGroup, openGroup, toB64, fromB64 };
});
