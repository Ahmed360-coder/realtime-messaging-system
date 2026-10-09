// ChatClient – the client side of ChatProto reliability, without any UI.
// Used by the browser page (<script src="/chat-client.js">, after chatproto.js) and by the
// Node tests (require), so the retry / dedup / reconnect logic is tested, not just clicked.
//
// What it guarantees on top of TCP:
//  - no loss:       every MSG stays in `pending` (the outbox) until its ACK arrives; it is re-sent
//                   after a timeout and after every reconnect (same id, so the server can dedup).
//                   The page saves the outbox ('outbox' event) and restore()s it after a reload.
//  - no duplicates: ids already seen are ignored.
//  - offline sync:  HELLO carries `lastSeq`, the highest server seq received; the server
//                   replays everything after it.
//  - reconnect:     automatic, with exponential backoff + jitter.

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('../protocols/chatproto'));
  else root.ChatClient = factory(root.ChatProto);
})(typeof self !== 'undefined' ? self : this, function (ChatProto) {
  'use strict';

  const { TYPES } = ChatProto;
  const OPEN = 1; // WebSocket.OPEN, the same value in browsers and in the ws library
  const CLOSE_ACK_TIMEOUT = 4000; // our own close code: "gave up waiting for ACKs"

  class ChatClient {
    /**
     * url:           ws://host:port/ws
     * token:         random secret that ties our username to this device
     * WebSocket:     constructor to use (the browser's, or require('ws') in Node)
     * openSocket:    optional () => socket, for another protocol (Step 5: () => new MqttSocket(url)).
     *                The socket must look like a WebSocket: readyState, send, close, onopen,
     *                onmessage, onclose. Default: new WebSocket(url).
     * ackTimeoutMs:  how long to wait for an ACK before re-sending
     * maxMissedAcks: after this many unanswered sends, assume the connection is dead and reconnect
     * backoffMinMs / backoffMaxMs: reconnect delay grows 0.5 s, 1 s, 2 s ... up to the max
     */
    constructor({ url, token, WebSocket = globalThis.WebSocket, openSocket = null,
                  ackTimeoutMs = 3000, maxMissedAcks = 3, backoffMinMs = 500, backoffMaxMs = 10000 }) {
      Object.assign(this, { url, token, WebSocket, ackTimeoutMs, maxMissedAcks, backoffMinMs, backoffMaxMs });
      this.openSocket = openSocket || (() => new this.WebSocket(this.url));
      this.ws = null;
      this.username = null;    // wanted username; remembered so reconnects re-join automatically
      this.helloId = null;     // id of our last HELLO, to recognise an ERROR reply to it
      this.joined = false;     // true between WELCOME and the connection closing
      this.lastSeq = 0;        // highest server seq we have received (our sync cursor)
      this.seen = new Set();   // message ids we already have (dedup)
      this.pending = new Map();// id -> { msg, attempts, timer }: sent but not yet ACKed
      this.reconnects = 0;     // failed attempts since the last WELCOME (drives the backoff)
      this.reconnectTimer = null;
      this.stopped = false;    // true after close() or after being replaced: no auto-reconnect
      this.handlers = {};
    }

    // Tiny event emitter: client.on('message', fn) etc.
    on(event, fn) { (this.handlers[event] ||= []).push(fn); return this; }
    emit(event, ...args) { for (const fn of this.handlers[event] || []) fn(...args); }

    connect() {
      this.stopped = false;
      clearTimeout(this.reconnectTimer);
      const ws = this.openSocket();
      this.ws = ws;
      this.emit('status', 'connecting');
      // MQTT only: its control packets (CONNECT, SUBACK, PUBACK ...) go to the frame log too.
      if ('ontrace' in ws) ws.ontrace = (dir, text) => { if (ws === this.ws) this.emit('frame', dir, text); };
      ws.onopen = () => {
        if (ws !== this.ws) return;
        this.emit('status', 'open');
        if (this.username) this.sendHello(); // auto re-join after a reconnect
      };
      ws.onmessage = (event) => { if (ws === this.ws) this.onFrame(String(event.data)); };
      // Ignore events from an old socket we already gave up on (see dropConnection).
      ws.onclose = (event) => { if (ws === this.ws) this.onClosed(event.code); };
      ws.onerror = () => {}; // a 'close' event always follows an error
    }

    /** Join (or re-join) as `username`. Sent now if connected, otherwise on the next open. */
    join(username) {
      this.username = username;
      if (this.ws && this.ws.readyState === OPEN) this.sendHello();
    }

    sendHello() {
      const hello = ChatProto.make(TYPES.HELLO, { body: { username: this.username, token: this.token, lastSeq: this.lastSeq } });
      this.helloId = hello.id;
      this.sendFrame(hello);
    }

    /**
     * Send a chat message reliably. `to` is a username or a group ('#study').
     * Returns the message (its id identifies it in later events).
     */
    send(to, body) {
      const msg = ChatProto.make(TYPES.MSG, { to, body });
      this.seen.add(msg.id); // if the server ever replays our own message to us, ignore it
      return this.enqueue(msg);
    }

    /**
     * Step 7: change a group, op = 'create' | 'add' | 'leave'. It goes through the same outbox as
     * a MSG (retried until ACKed, saved across a reload, dedup by id on the server). Its id is NOT
     * marked as seen: the server sends the change back to us as a GROUP event (with the new member
     * list), and that event is how the page learns the result, the same way as every other member.
     */
    changeGroup(op, group, users = []) {
      const body = op === 'leave' ? { op } : { op, users };
      return this.enqueue(ChatProto.make(TYPES.GROUP, { to: group, body }));
    }

    enqueue(msg) {
      this.pending.set(msg.id, { msg, attempts: 0, timer: null });
      this.emit('outbox', this.outbox());
      this.transmit(msg.id);
      return msg;
    }

    /** Messages sent but not yet ACKed. The page saves them so a reload cannot lose them. */
    outbox() {
      return [...this.pending.values()].map(p => p.msg);
    }

    /**
     * Put messages saved by an earlier page load back into the outbox. They keep their
     * original ids, so if the server already stored one, the re-send is just a 'duplicate'.
     * They are sent after the next SYNCED.
     */
    restore(msgs) {
      for (const msg of msgs) {
        if (this.pending.has(msg.id)) continue;
        if (msg.type === TYPES.MSG) this.seen.add(msg.id); // a GROUP change must still be applied when it comes back
        this.pending.set(msg.id, { msg, attempts: 0, timer: null });
      }
    }

    // (Re)send one pending message and arm its retry timer.
    transmit(id) {
      const p = this.pending.get(id);
      if (!p) return; // already ACKed
      clearTimeout(p.timer);
      if (!this.joined) return; // offline: it is re-sent after the next SYNCED
      if (p.attempts >= this.maxMissedAcks) {
        // TCP says the socket is open but nothing comes back: probably a half-open connection
        // (e.g. Wi-Fi changed). Stop trusting it and reconnect; pending messages survive.
        this.dropConnection('no ACK after ' + p.attempts + ' tries');
        return;
      }
      p.attempts++;
      if (p.attempts > 1) this.emit('retry', p.msg, p.attempts);
      this.sendFrame(p.msg);
      p.timer = setTimeout(() => this.transmit(id), this.ackTimeoutMs);
    }

    // Re-send everything still pending, in the order it was first sent (Map keeps insertion order).
    flushPending() {
      for (const [id, p] of this.pending) {
        p.attempts = 0;
        this.transmit(id);
      }
    }

    /** Encode and write one frame if the socket is open. Also used by the demo "send duplicate" button. */
    sendFrame(msg) {
      if (!this.ws || this.ws.readyState !== OPEN) return false;
      const text = typeof msg === 'string' ? msg : ChatProto.encode(msg);
      this.ws.send(text);
      this.emit('frame', 'out', text);
      return true;
    }

    onFrame(text) {
      this.emit('frame', 'in', text);
      const r = ChatProto.decode(text, ChatProto.SERVER_TYPES);
      if (!r.ok) return this.emit('protocolError', r.error);
      const msg = r.msg;

      switch (msg.type) {
        case TYPES.WELCOME:
          this.joined = true;
          this.reconnects = 0; // connection is healthy again: reset the backoff
          return this.emit('welcome', msg.body);

        case TYPES.SYNCED:
          // Missed messages have been replayed; now re-send our own unACKed ones.
          this.emit('synced', msg.body);
          return this.flushPending();

        case TYPES.MSG:
          // Advance the cursor even for a duplicate: having seen it means we have it.
          if (msg.seq > this.lastSeq) this.lastSeq = msg.seq;
          if (this.seen.has(msg.id)) return this.emit('duplicate', msg);
          this.seen.add(msg.id);
          return this.emit('message', msg);

        case TYPES.GROUP:
          // A membership change (Step 7): part of the same seq stream as MSG, so it moves the
          // sync cursor and is deduplicated the same way. A replayed one adds nothing new.
          if (msg.seq > this.lastSeq) this.lastSeq = msg.seq;
          if (this.seen.has(msg.id)) return;
          this.seen.add(msg.id);
          return this.emit('group', msg);

        case TYPES.ACK: {
          const p = this.pending.get(msg.body.ref);
          if (p) {
            clearTimeout(p.timer);
            this.pending.delete(msg.body.ref);
            this.emit('outbox', this.outbox());
          }
          return this.emit('ack', msg.body, p ? p.msg : null);
        }

        case TYPES.ERROR: {
          const ref = msg.body.ref;
          if (ref && ref === this.helloId) this.username = null; // join refused: don't retry it on reconnect
          // The server refused this message (e.g. UNKNOWN_USER): re-sending cannot help.
          const p = ref && this.pending.get(ref);
          if (p) {
            clearTimeout(p.timer);
            this.pending.delete(ref);
            this.emit('outbox', this.outbox());
          }
          return this.emit('serverError', msg.body, p ? p.msg : null);
        }

        case TYPES.PRESENCE: return this.emit('presence', msg.body);
        case TYPES.USERS: return this.emit('users', msg.body);
        // BYE (MQTT only): the socket adapter closes with body.code right after this frame,
        // so it arrives here as a normal close (see onClosed).
        case TYPES.BYE: return;
      }
    }

    onClosed(code) {
      this.joined = false;
      for (const p of this.pending.values()) clearTimeout(p.timer); // keep the messages, stop the timers
      this.emit('status', 'closed', code);
      if (code === ChatProto.CLOSE_REPLACED) {
        // We opened a newer connection elsewhere (e.g. another tab). Reconnecting would
        // kick that one out, which would kick us out again, forever.
        this.stopped = true;
        return this.emit('replaced');
      }
      if (!this.stopped) this.scheduleReconnect();
    }

    // Exponential backoff with jitter: base * 2^n, capped, then +-25% random so many clients
    // that lost the same server do not all reconnect at the same instant.
    scheduleReconnect() {
      const base = Math.min(this.backoffMaxMs, this.backoffMinMs * 2 ** this.reconnects);
      const delay = Math.round(base * (0.75 + Math.random() * 0.5));
      this.reconnects++;
      this.emit('reconnecting', delay, this.reconnects);
      this.reconnectTimer = setTimeout(() => this.connect(), delay);
    }

    /** Abandon the current socket now (don't wait for a close handshake that may never finish). */
    dropConnection(reason = 'dropped') {
      const ws = this.ws;
      if (!ws) return;
      this.ws = null; // from now on, events from this socket are ignored
      try { ws.close(CLOSE_ACK_TIMEOUT, reason); } catch (e) { /* already closing */ }
      this.onClosed(CLOSE_ACK_TIMEOUT);
    }

    /** Stop for good: no reconnect. */
    close() {
      this.stopped = true;
      clearTimeout(this.reconnectTimer);
      for (const p of this.pending.values()) clearTimeout(p.timer);
      if (this.ws) this.ws.close(1000, 'bye');
    }
  }

  ChatClient.CLOSE_ACK_TIMEOUT = CLOSE_ACK_TIMEOUT;
  return ChatClient;
});
