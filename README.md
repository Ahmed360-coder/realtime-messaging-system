# Real-Time Client-Server Messaging System

CSEN 503 – Computer Networks, Winter 2026 (Instructor: Amr Saber)

> Status: **planning**. This README will grow into the 3-page design document.

## Team

| Member | Responsibility |
|---|---|
| _TBD_ | Server core + WebSocket protocol |
| _TBD_ | HTTP long-polling protocol + reliability (ACK, dedup, sync) |
| _TBD_ | Mobile web client UI |
| _TBD_ | Database, group chat, metrics + benchmarks |

## Overview

- One laptop runs the server; each member's phone is a client (mobile web page).
- All devices are on the same local Wi-Fi network.
- Messages are delivered in real time, with no loss, no duplication, and
  automatic synchronization of missed messages when a client reconnects.

## Planned Design

### Network / Link Layer
- Server binds to `0.0.0.0` so it is reachable on the laptop's Wi-Fi IP.
- Phones open `http://<laptop-ip>:<port>` in the browser.

### Transport Layer
- TCP (via WebSocket and HTTP) for reliable, ordered byte streams.
- Server handles many simultaneous connections; one client closing does not affect others.

### Application Layer – two protocols
1. **Custom JSON protocol over WebSocket**: message types such as `MSG`, `ACK`, `SYNC`.
2. **HTTP long-polling**: same chat, implemented with plain HTTP requests.

### ChatProto v1 (Step 2, extended for reliability in Step 3)
One JSON object per WebSocket text frame on `ws://<laptop-ip>:3000/ws`
(codec: `protocols/chatproto.js`, shared by server and browser).

```json
{ "v": 1, "type": "MSG", "id": "<uuid>", "ts": 1760000000000,
  "from": "alice", "to": "bob", "body": "hi", "seq": 42 }
```

`id` is chosen by the sender and identifies the message for deduplication; `seq` is
assigned by the server when it stores the message (ordering and offline sync).

| Type | Direction | Fields |
|---|---|---|
| `HELLO` | client → server | `body.username` (1–20 of `A-Z a-z 0-9 _`), `body.token` (16–128 of `A-Z a-z 0-9 _ -`), `body.lastSeq` (optional integer ≥ 0, default 0) |
| `MSG` | both | `to`, `body` (text ≤ 2000 chars); server sets `from` and adds `seq` |
| `LIST` | client → server | – |
| `WELCOME` | server → client | `body.username`, `body.users` (online), `body.known` (all registered) |
| `SYNCED` | server → client | `body.count` (messages replayed), `body.lastSeq` |
| `ACK` | server → client | `body.ref` (acked id), `body.seq`, `body.status` (`delivered` / `stored` / `duplicate`) |
| `PRESENCE` | server → client | `body.username`, `body.status` (`online`/`offline`) |
| `USERS` | server → client | `body.users` (online), `body.known` (all registered) |
| `ERROR` | server → client | `body.code`, `body.message`, `body.ref` |

Error codes: `BAD_JSON`, `BAD_VERSION`, `BAD_TYPE`, `BAD_FIELD`, `TOO_LARGE`, `NOT_JOINED`,
`ALREADY_JOINED`, `NAME_TAKEN` (name registered to another token), `UNKNOWN_USER`
(recipient never joined). Malformed frames get an `ERROR` and the connection stays open;
frames over 16 KB close the socket with code 1009. The server pings every 30 s and drops
clients that do not answer. Close code `4001` = "replaced by a newer connection of the
same user"; the client must not auto-reconnect after it.

**Join and sync sequence:** `HELLO{lastSeq}` → `WELCOME` → every stored `MSG` to/from the
user with `seq > lastSeq`, oldest first → `SYNCED`. Join and replay run in the same
event-loop turn, so no live message can arrive in the middle of the replay.

**Why still `v: 1`:** Step 3 adds types and fields and makes `HELLO.token` required. The
version field lets *independently deployed* peers detect incompatibility. Our server serves
the client page and codec from the same origin, so a client of another version cannot
exist, and v1 had not been frozen or released. Bumping to v2 would add a compatibility
branch with no client to use it.

Code layout: `protocols/chatproto.js` (format) → `server/transports/websocket.js`
(socket ↔ protocol) → `server/core/hub.js` (protocol-agnostic routing, presence, sync) →
`server/core/store.js` (SQLite). Client side: `client/chat-client.js` (retry, dedup,
reconnect; no UI) → `client/index.html` (UI).

### Reliability (implemented in Step 3)
TCP only guarantees delivery between two kernels while one connection lives; it cannot
know whether our server saved a message or whether a phone that lost Wi-Fi got it. So the
application adds its own end-to-end checks:

- **Store before ACK (no loss at the server).** The hub writes the message to SQLite
  (`node:sqlite`, WAL journal, `synchronous=FULL`) and only then is the `ACK` sent. ACK =
  "on disk", even if the server crashes right after.
- **Client retries until ACKed (at-least-once).** Unacknowledged messages stay in an outbox
  and are re-sent every 3 s with the *same id*. After 3 unanswered sends the client assumes
  a half-open connection and reconnects. The browser saves the outbox in `localStorage`, so
  a page reload cannot lose unsent messages.
- **Deduplication by id (exactly-once effect).** `messages.id` is `UNIQUE`; a retried id is
  not stored or delivered again, it is re-ACKed with its original `seq` and status
  `duplicate`. Receivers also ignore ids they have already seen.
- **Sequence numbers.** `seq INTEGER PRIMARY KEY AUTOINCREMENT`: strictly increasing, never
  reused, survives restarts. One server, one thread, so one global order.
- **Offline delivery (store-and-forward).** A message to an offline (but registered) user is
  stored and ACKed with `stored`; it is delivered by the next sync.
- **Offline sync.** The client remembers `lastSeq`, the highest `seq` it has received, and sends it
  in `HELLO`. The server replays everything after it. That makes `lastSeq` a *cumulative
  acknowledgement*, so receivers need no per-message receipts. A fresh page sends 0 and gets
  its full history.
- **Auto-reconnect with exponential backoff + jitter:** 0.5 s, 1 s, 2 s … max 10 s, ±25 %.
- **Identity on reconnect.** Each browser keeps a random token in `localStorage`. The first
  `HELLO` registers the username to that token (the server stores only its SHA-256 hash).
  Later, the same token is required. The same token while an old "zombie" session still exists
  takes it over (`4001`), so a phone that changed networks can reconnect immediately.

Database (`chat.db`, git-ignored; `DB_PATH` overrides): `users(username PK, token_hash,
created_at, last_seen)`, `messages(seq PK AUTOINCREMENT, id UNIQUE, sender, recipient,
body, ts, stored_at)` with indexes on `(recipient, seq)` and `(sender, seq)`.

### Additional Features (planned)
- Group chat.

### Monitoring
- `/stats` page: active connections, messages per second, totals.

## Performance Evaluation (planned)
- Latency (average sender → recipient time).
- Protocol comparison: WebSocket vs HTTP long-polling.
- Scalability or Wi-Fi distance (near vs far).

## Repository Structure

```
server/      server code (connections, routing, storage, metrics)
client/      mobile web client (HTML/CSS/JS)
protocols/   message format definitions for each protocol
benchmarks/  performance test scripts and results
docs/        design document and diagrams
```

## Progress

- [x] Step 1 – Server skeleton: phones reach the laptop over Wi-Fi (HTTP, LAN IP, QR code)
- [x] Step 2 – ChatProto v1: custom protocol over WebSocket
- [x] Step 3 – Reliability: SQLite storage, ACKs, deduplication, offline sync
- [ ] Step 4 – Mobile chat UI
- [ ] Step 5 – MQTT: second application-layer protocol
- [ ] Step 6 – Live metrics dashboard
- [ ] Step 7 – Group chat
- [ ] Step 8 – End-to-end encryption
- [ ] Step 9 – Benchmarks and charts
- [ ] Step 10 – Documentation, demo rehearsal, oral defense prep

## Running

Requires Node.js 24+ (uses the built-in `node:sqlite`).

```bash
npm install
npm start
npm test        # unit + integration tests
```

The server prints its Wi-Fi address and a QR code. Open that address on a phone connected
to the same Wi-Fi. If the phone cannot connect, allow Node.js through Windows Firewall
(Private networks).
