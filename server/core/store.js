// Persistent storage: users and messages in one SQLite file (Node's built-in node:sqlite).
// It knows nothing about sockets or ChatProto frames; the hub calls it, and any transport
// (WebSocket, MQTT) gets persistence for free.
//
// DatabaseSync is synchronous: when run() returns, the row is committed to disk. The hub
// relies on that: it stores a message and only THEN lets the transport send the ACK.

const { DatabaseSync } = require('node:sqlite');
const crypto = require('node:crypto');

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS users (
    username   TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,             -- SHA-256 of the client's secret token, never the token
    created_at INTEGER NOT NULL,          -- ms since epoch
    last_seen  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS messages (
    -- seq: server-assigned order. AUTOINCREMENT = strictly increasing, never reused,
    -- even after a delete or a server restart.
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    id        TEXT NOT NULL UNIQUE,       -- sender's UUID; UNIQUE is what makes retries safe (dedup)
    sender    TEXT NOT NULL REFERENCES users(username),
    recipient TEXT NOT NULL REFERENCES users(username),
    body      TEXT NOT NULL,
    ts        INTEGER NOT NULL,           -- sender's clock (used later for latency)
    stored_at INTEGER NOT NULL            -- server's clock
  );

  -- Offline sync asks "messages to/from X with seq > N"; these indexes make that a range scan.
  CREATE INDEX IF NOT EXISTS messages_by_recipient ON messages(recipient, seq);
  CREATE INDEX IF NOT EXISTS messages_by_sender    ON messages(sender, seq);
`;

const sha256 = text => crypto.createHash('sha256').update(text).digest('hex');

class Store {
  /** `file` is a path such as "chat.db", or ":memory:" for a throw-away database (tests). */
  constructor(file = ':memory:') {
    this.db = new DatabaseSync(file);
    // WAL (write-ahead log): a commit is appended to a log file. synchronous=FULL makes SQLite
    // fsync that log on every commit, so a committed (= ACKed) message survives a power cut.
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA synchronous = FULL');
    this.db.exec('PRAGMA foreign_keys = ON');
    this.db.exec(SCHEMA);

    // Prepared statements: SQL is compiled once, values are bound to the ? placeholders.
    // User text is never pasted into SQL, so SQL injection is impossible.
    this.sql = {
      getUser: this.db.prepare('SELECT username, token_hash FROM users WHERE username = ?'),
      addUser: this.db.prepare('INSERT INTO users (username, token_hash, created_at, last_seen) VALUES (?, ?, ?, ?)'),
      touchUser: this.db.prepare('UPDATE users SET last_seen = ? WHERE username = ?'),
      allUsers: this.db.prepare('SELECT username FROM users ORDER BY username'),
      addMessage: this.db.prepare(
        'INSERT INTO messages (id, sender, recipient, body, ts, stored_at) VALUES (?, ?, ?, ?, ?, ?)'),
      messageById: this.db.prepare('SELECT seq, id, sender, recipient, body, ts FROM messages WHERE id = ?'),
      messagesAfter: this.db.prepare(`
        SELECT seq, id, sender, recipient, body, ts FROM messages
        WHERE (recipient = ? OR sender = ?) AND seq > ?
        ORDER BY seq`),
      counts: this.db.prepare(`
        SELECT (SELECT COUNT(*) FROM messages) AS messages, (SELECT COUNT(*) FROM users) AS users,
               (SELECT COALESCE(MAX(seq), 0) FROM messages) AS lastSeq`),
    };
  }

  // Run fn inside one transaction: either all of its writes are committed or none are.
  // If a transaction is already open, fn simply becomes part of it (SQLite cannot nest BEGIN).
  transaction(fn) {
    if (this.db.isTransaction) return fn();
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  /**
   * First HELLO for a username registers it with this token; later HELLOs must present the
   * same token. Returns true if `token` may use `username`.
   * Check-then-insert is one transaction so two writers could never both "register" a name.
   */
  claimUser(username, token, now = Date.now()) {
    const hash = sha256(token);
    return this.transaction(() => {
      const row = this.sql.getUser.get(username);
      if (!row) {
        this.sql.addUser.run(username, hash, now, now);
        return true;
      }
      // Constant-time compare so response timing does not leak how much of the hash matched.
      if (!crypto.timingSafeEqual(Buffer.from(row.token_hash, 'hex'), Buffer.from(hash, 'hex'))) return false;
      this.sql.touchUser.run(now, username);
      return true;
    });
  }

  touchUser(username, now = Date.now()) {
    this.sql.touchUser.run(now, username);
  }

  userExists(username) {
    return this.sql.getUser.get(username) !== undefined;
  }

  /** Every username that has ever joined, sorted. */
  knownUsers() {
    return this.sql.allUsers.all().map(r => r.username);
  }

  /**
   * Store a chat message: { id, from, to, body, ts }.
   * Returns { duplicate: false, seq } for a new id, or { duplicate: true, seq, from } if a
   * message with this id is already stored (a retry); nothing is written in that case.
   * Look-up and insert are one transaction. We look up first (instead of INSERT ... ON CONFLICT
   * DO NOTHING) because a refused INSERT still uses up an AUTOINCREMENT number, leaving gaps.
   * The UNIQUE constraint on id stays as a safety net.
   */
  saveMessage({ id, from, to, body, ts }, now = Date.now()) {
    return this.transaction(() => {
      const existing = this.sql.messageById.get(id);
      if (existing) return { duplicate: true, seq: existing.seq, from: existing.sender };
      const res = this.sql.addMessage.run(id, from, to, body, ts, now);
      return { duplicate: false, seq: Number(res.lastInsertRowid) };
    });
  }

  /** Messages sent to or by `username` with seq > afterSeq, oldest first (offline sync). */
  messagesFor(username, afterSeq = 0) {
    return this.sql.messagesAfter.all(username, username, afterSeq).map(r => ({
      seq: r.seq, id: r.id, from: r.sender, to: r.recipient, body: r.body, ts: r.ts,
    }));
  }

  /** Totals for the metrics dashboard: { messages, users, lastSeq }. Unlike counters, they survive restarts. */
  counts() {
    const { messages, users, lastSeq } = this.sql.counts.get();
    return { messages, users, lastSeq };
  }

  close() {
    if (this.db.isOpen) this.db.close();
  }
}

module.exports = { Store };
