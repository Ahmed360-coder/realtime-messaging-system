// Persistent storage: users, messages and groups in one SQLite file (Node's built-in node:sqlite).
// It knows nothing about sockets or ChatProto frames; the hub calls it, and any transport
// (WebSocket, MQTT) gets persistence for free.
//
// DatabaseSync is synchronous: when run() returns, the row is committed to disk. The hub
// relies on that: it stores a message and only THEN lets the transport send the ACK.
//
// Step 7 (groups): a group message is ONE row (group_name set, recipient NULL), however many
// members the group has (fan-out on read). Membership changes (create / add / leave) are rows
// in the same table (kind != 'text'), so they get a seq and are replayed by offline sync too.

const { DatabaseSync } = require('node:sqlite');
const crypto = require('node:crypto');

// PRAGMA user_version: a free integer in the file header, used as our schema version.
//   0 = Steps 3-6 (messages.recipient NOT NULL, no groups)   1 = Step 7 (groups)
const SCHEMA_VERSION = 1;

// The messages table, as a function of its name: the migration builds it as messages_new first.
const messagesTable = name => `
  CREATE TABLE IF NOT EXISTS ${name} (
    -- seq: server-assigned order. AUTOINCREMENT = strictly increasing, never reused,
    -- even after a delete or a server restart. ONE counter for 1-to-1 and every group.
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    id         TEXT NOT NULL UNIQUE,      -- sender's UUID; UNIQUE is what makes retries safe (dedup)
    kind       TEXT NOT NULL DEFAULT 'text'
               CHECK (kind IN ('text', 'create', 'add', 'leave')), -- chat text or a membership change
    sender     TEXT NOT NULL REFERENCES users(username),
    recipient  TEXT REFERENCES users(username),  -- 1-to-1 message: the other user
    group_name TEXT REFERENCES groups(name),     -- group message or membership change: the group
    body       TEXT NOT NULL,             -- the text; for a membership change, JSON { users, members }
    ts         INTEGER NOT NULL,          -- sender's clock (used later for latency)
    stored_at  INTEGER NOT NULL,          -- server's clock
    CHECK ((recipient IS NULL) <> (group_name IS NULL)), -- exactly one of the two addresses
    CHECK (kind = 'text' OR group_name IS NOT NULL)      -- membership changes belong to a group
  )`;

// Every table except messages (the migration needs them to exist before it rebuilds messages).
const OTHER_TABLES = `
  CREATE TABLE IF NOT EXISTS users (
    username   TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL,             -- SHA-256 of the client's secret token, never the token
    created_at INTEGER NOT NULL,          -- ms since epoch
    last_seen  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS groups (
    name       TEXT PRIMARY KEY,          -- the address, e.g. '#study' (unique: PRIMARY KEY)
    created_by TEXT NOT NULL REFERENCES users(username),
    created_at INTEGER NOT NULL
  );

  -- Current members only: leaving deletes the row.
  CREATE TABLE IF NOT EXISTS group_members (
    group_name TEXT NOT NULL REFERENCES groups(name),
    username   TEXT NOT NULL REFERENCES users(username),
    joined_seq INTEGER NOT NULL,          -- seq of the event that added them: they see the group from here on
    PRIMARY KEY (group_name, username)
  );
`;

const SCHEMA = `
  ${OTHER_TABLES}
  ${messagesTable('messages')};

  -- Offline sync asks "messages to/from X with seq > N" and "messages of group G with seq > N";
  -- these indexes make each of them a range scan.
  CREATE INDEX IF NOT EXISTS messages_by_recipient ON messages(recipient, seq);
  CREATE INDEX IF NOT EXISTS messages_by_sender    ON messages(sender, seq);
  CREATE INDEX IF NOT EXISTS messages_by_group     ON messages(group_name, seq);
  CREATE INDEX IF NOT EXISTS members_by_user       ON group_members(username);
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
    this.migrate(); // before foreign_keys = ON: the table rebuild must not trigger FK checks
    this.db.exec('PRAGMA foreign_keys = ON');

    // Prepared statements: SQL is compiled once, values are bound to the ? placeholders.
    // User text is never pasted into SQL, so SQL injection is impossible.
    this.sql = {
      getUser: this.db.prepare('SELECT username, token_hash FROM users WHERE username = ?'),
      addUser: this.db.prepare('INSERT INTO users (username, token_hash, created_at, last_seen) VALUES (?, ?, ?, ?)'),
      touchUser: this.db.prepare('UPDATE users SET last_seen = ? WHERE username = ?'),
      allUsers: this.db.prepare('SELECT username FROM users ORDER BY username'),
      addMessage: this.db.prepare(
        'INSERT INTO messages (id, kind, sender, recipient, group_name, body, ts, stored_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'),
      messageById: this.db.prepare('SELECT seq, sender FROM messages WHERE id = ?'),
      // Offline sync: three indexed range scans, merged and sorted by seq.
      //   1. 1-to-1 messages to me   2. 1-to-1 messages from me
      //   3. everything in the groups I am in now, from the moment I joined each of them
      // (UNION, not UNION ALL, so a message to myself is not listed twice.)
      messagesAfter: this.db.prepare(`
        SELECT seq, id, kind, sender, recipient, group_name, body, ts FROM messages
          WHERE recipient = :me AND seq > :after
        UNION
        SELECT seq, id, kind, sender, recipient, group_name, body, ts FROM messages
          WHERE sender = :me AND group_name IS NULL AND seq > :after
        UNION
        SELECT m.seq, m.id, m.kind, m.sender, m.recipient, m.group_name, m.body, m.ts
          FROM group_members g JOIN messages m ON m.group_name = g.group_name
          WHERE g.username = :me AND m.seq >= g.joined_seq AND m.seq > :after
        ORDER BY seq`),
      getGroup: this.db.prepare('SELECT name, created_by FROM groups WHERE name = ?'),
      addGroup: this.db.prepare('INSERT INTO groups (name, created_by, created_at) VALUES (?, ?, ?)'),
      members: this.db.prepare('SELECT username FROM group_members WHERE group_name = ? ORDER BY username'),
      addMember: this.db.prepare('INSERT INTO group_members (group_name, username, joined_seq) VALUES (?, ?, ?)'),
      removeMember: this.db.prepare('DELETE FROM group_members WHERE group_name = ? AND username = ?'),
      groupsOf: this.db.prepare('SELECT group_name FROM group_members WHERE username = ? ORDER BY group_name'),
      counts: this.db.prepare(`
        SELECT (SELECT COUNT(*) FROM messages) AS messages, (SELECT COUNT(*) FROM users) AS users,
               (SELECT COUNT(*) FROM groups) AS groups,
               (SELECT COALESCE(MAX(seq), 0) FROM messages) AS lastSeq`),
    };
  }

  /**
   * Bring an older chat.db up to SCHEMA_VERSION, once, at startup.
   * Version 0 -> 1: messages.recipient was NOT NULL REFERENCES users, and SQLite's ALTER TABLE
   * cannot drop a constraint. So the table is rebuilt, the way the SQLite manual describes it:
   * create the new table, copy every row (keeping its seq), drop the old one, rename the new one.
   * All in one transaction: if anything fails, the old file is left exactly as it was.
   */
  migrate() {
    const version = this.db.prepare('PRAGMA user_version').get().user_version;
    if (version >= SCHEMA_VERSION) return this.db.exec(SCHEMA); // up to date (CREATE IF NOT EXISTS: no-op)
    const hasOld = this.db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'messages'").get();
    this.transaction(() => {
      // First the new tables: RENAME below checks that every table messages refers to exists.
      this.db.exec(OTHER_TABLES);
      if (hasOld) {
        const counter = this.db.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'messages'").get();
        this.db.exec(`
          ${messagesTable('messages_new')};
          INSERT INTO messages_new (seq, id, kind, sender, recipient, group_name, body, ts, stored_at)
            SELECT seq, id, 'text', sender, recipient, NULL, body, ts, stored_at FROM messages;
          DROP TABLE messages;
          ALTER TABLE messages_new RENAME TO messages;`);
        // The copy set the AUTOINCREMENT counter to MAX(seq); keep the old one if it was higher
        // (rows deleted at the end), so a seq is never handed out twice.
        if (counter) this.db.prepare("UPDATE sqlite_sequence SET seq = MAX(seq, ?) WHERE name = 'messages'").run(counter.seq);
      }
      this.db.exec(SCHEMA); // messages (if this is a new file) and the indexes
      this.db.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
    });
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

  /** The stored message with this id: { seq, from }, or null. Used to recognise a retry. */
  findMessage(id) {
    const row = this.sql.messageById.get(id);
    return row ? { seq: row.seq, from: row.sender } : null;
  }

  /**
   * Store a message: { id, from, to, body, ts, kind }. `to` is a username (1-to-1) or a group
   * address ('#study'); `kind` is 'text' (default) or a membership change ('create' | 'add' | 'leave').
   * Returns { duplicate: false, seq } for a new id, or { duplicate: true, seq, from } if a
   * message with this id is already stored (a retry); nothing is written in that case.
   * Look-up and insert are one transaction. We look up first (instead of INSERT ... ON CONFLICT
   * DO NOTHING) because a refused INSERT still uses up an AUTOINCREMENT number, leaving gaps.
   * The UNIQUE constraint on id stays as a safety net.
   */
  saveMessage({ id, from, to, body, ts, kind = 'text' }, now = Date.now()) {
    const group = to.startsWith('#');
    return this.transaction(() => {
      const existing = this.findMessage(id);
      if (existing) return { duplicate: true, seq: existing.seq, from: existing.from };
      const res = this.sql.addMessage.run(id, kind, from, group ? null : to, group ? to : null, body, ts, now);
      return { duplicate: false, seq: Number(res.lastInsertRowid) };
    });
  }

  /**
   * Messages for `username` with seq > afterSeq, oldest first (offline sync): 1-to-1 messages
   * to or from them, and everything in their groups since they joined each one.
   * Each is { seq, id, kind, from, to, body, ts }; `to` is a username or a group address.
   */
  messagesFor(username, afterSeq = 0) {
    return this.sql.messagesAfter.all({ me: username, after: afterSeq }).map(r => ({
      seq: r.seq, id: r.id, kind: r.kind, from: r.sender, to: r.recipient ?? r.group_name, body: r.body, ts: r.ts,
    }));
  }

  // ---- Groups (Step 7) ----

  /** { name, createdBy } or null. */
  getGroup(name) {
    const row = this.sql.getGroup.get(name);
    return row ? { name: row.name, createdBy: row.created_by } : null;
  }

  createGroup(name, createdBy, now = Date.now()) {
    this.sql.addGroup.run(name, createdBy, now);
  }

  /** Current members, sorted. */
  groupMembers(name) {
    return this.sql.members.all(name).map(r => r.username);
  }

  /** `joinedSeq` = seq of the membership change that adds them; they see the group from there on. */
  addMembers(name, usernames, joinedSeq) {
    this.transaction(() => { for (const u of usernames) this.sql.addMember.run(name, u, joinedSeq); });
  }

  removeMember(name, username) {
    this.sql.removeMember.run(name, username);
  }

  /** The groups `username` is in now, sorted. */
  groupsOf(username) {
    return this.sql.groupsOf.all(username).map(r => r.group_name);
  }

  /** Totals for the metrics dashboard: { messages, users, groups, lastSeq }. Unlike counters, they survive restarts. */
  counts() {
    const { messages, users, groups, lastSeq } = this.sql.counts.get();
    return { messages, users, groups, lastSeq };
  }

  close() {
    if (this.db.isOpen) this.db.close();
  }
}

module.exports = { Store, SCHEMA_VERSION };
