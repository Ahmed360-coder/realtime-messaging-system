# Real-Time Client-Server Messaging System

CSEN 503 – Computer Networks, Winter 2026 (Instructor: Amr Saber)

> Status: **planning**. This README will grow into the 3-page design document.

## Team

| Member | Responsibility |
|---|---|
| _TBD_ | Server core + WebSocket protocol |
| _TBD_ | MQTT protocol + reliability (ACK, dedup, sync) |
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
1. **ChatProto v1 over WebSocket** (`/ws`): our own JSON protocol, request/response style.
2. **MQTT 3.1.1 over WebSocket** (`/mqtt`, Step 5): publish/subscribe through a broker
   (Aedes) embedded in the server. Same hub, same database: users of the two protocols chat
   with each other.

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
| `BYE` | server → client (MQTT only) | `body.code` (e.g. `4001`), `body.reason`: "I am closing you, this is why" |

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

Code layout: `protocols/chatproto.js` (format) and `protocols/mqtt-binding.js` (MQTT topics) →
`server/transports/websocket.js` / `server/transports/mqtt.js` (bytes ↔ ChatProto text) →
`server/core/dispatch.js` (the ChatProto state machine, shared by both transports) →
`server/core/hub.js` (protocol-agnostic routing, presence, sync) → `server/core/store.js`
(SQLite). `server/index.js` routes each WebSocket upgrade by path (`/ws` or `/mqtt`).
Client side: `client/mqtt-socket.js` (an MQTT connection that looks like a WebSocket) →
`client/chat-client.js` (retry, dedup, reconnect; no UI) → `client/conversations.js`
(page state; no UI) → `client/app.js` + `index.html` + `style.css` (the phone UI).

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

### Mobile chat UI (Step 4)
Plain HTML/CSS/JS served from `client/` (no framework, no build step), designed for
360–430px phone screens first:

- **Screens:** join → chat list (online dot, last message, unread badge) → one chat
  (bubbles, time, delivery ticks). Opening a chat pushes a browser-history entry, so the
  phone's back gesture returns to the list.
- **Delivery ticks come from ChatProto:** 🕓 waiting (in the outbox, no `ACK` yet) ·
  ✓ stored (`ACK stored`/`duplicate`, or replayed from history) · ✓✓ delivered
  (`ACK delivered`) · ! refused (`ERROR` with `ref`).
- **Unread counts:** a "read up to seq N" cursor per chat, saved in `localStorage`, so a
  reload (which replays the full history) does not mark everything unread again.
- **Connection banner:** connecting / offline + reconnect countdown + messages waiting /
  "opened in another tab" (close `4001`, with a *Use here* button) / "N messages synced".
- **State → view:** ChatClient events only update the state (`client/conversations.js`,
  unit-tested in Node); `render()` redraws the screen from it.
- **XSS safety:** user text reaches the page only through `textContent`; a test fails if
  client code ever uses `innerHTML`, `outerHTML`, `insertAdjacentHTML` or `document.write`.
- **Phone details:** viewport meta, `100dvh`, safe-area insets, 16px gutter, 48px tap
  targets, 16px input font (no iOS zoom), light/dark colours, `aria-live` banner.
- **Debug panel** (`{ }` button, for the demo): raw frame log, `lastSeq`, outbox size,
  *Drop connection*, *Re-send last (same id)*, raw-frame input.

### MQTT – the second protocol (Step 5)
MQTT is a publish/subscribe protocol: clients never address each other, they **publish** to a
named **topic**, and a **broker** forwards the message to every client that **subscribed** to it.
It is binary (2–5 byte fixed header) with these packets: `CONNECT`/`CONNACK`,
`SUBSCRIBE`/`SUBACK`, `PUBLISH`/`PUBACK`, `PINGREQ`/`PINGRESP`, `DISCONNECT`.

**Stack.** Browser: [mqtt.js](https://github.com/mqttjs/MQTT.js), served by our own server at
`/vendor/mqtt.min.js` (phones need no internet). Server: the [Aedes](https://github.com/moscajs/aedes)
broker, embedded in the same Node process. Browsers cannot open raw TCP sockets, so MQTT runs
**over WebSocket** (binary frames, subprotocol `mqtt`): TCP → HTTP Upgrade → WebSocket → MQTT →
ChatProto JSON payload.

**Topics** (`protocols/mqtt-binding.js`). Each connection uses a fresh random client id
`c-<32 hex>` and exactly two topics:

| Topic | Direction | Payload |
|---|---|---|
| `chat/<clientId>/up` | client → server | ChatProto `HELLO` / `MSG` / `LIST` |
| `chat/<clientId>/down` | server → client | ChatProto `WELCOME` / `SYNCED` / `MSG` / `ACK` / `PRESENCE` / `USERS` / `ERROR` / `BYE` |

The payload of each PUBLISH is exactly one ChatProto JSON object, the same text a WebSocket
frame carries. Clients never publish to each other's topics: the hub must store the message,
give it a `seq` and check the sender before anyone receives it, and a WebSocket user could not
receive it otherwise. So the server is the only reader of `up` topics and the only writer of
`down` topics. Topics are per **connection**, not per user, so two tabs of one user never see
each other's traffic.

**Access rules** (Aedes hooks in `server/transports/mqtt.js`):
- `authenticate` (CONNECT): the client id must match `c-<32 hex>` and must not already be
  connected, otherwise CONNACK return code 2. (MQTT would let a newer connection with the same
  id take over the old one; with per-connection topics that would be a hijack.)
- `authorizeSubscribe`: only your own `down` topic. Another connection's topic or a
  wildcard (`chat/+/down`, `chat/#`, `#`) gets SUBACK `128` (refused).
- `authorizePublish`: only your own `up` topic, not retained; it is handed to the ChatProto
  state machine. Anything else closes the connection (MQTT 3.1.1 cannot refuse a single
  PUBLISH). Aedes calls this hook **before** it sends the PUBACK, so the message is in SQLite first.

**Login** stays the ChatProto `HELLO` (sent as the first PUBLISH after SUBACK). Both protocols
share one join path and get the same errors (`NAME_TAKEN` …). MQTT's username/password fields
are not used.

**MQTT reliability features – what we use and why**

| Feature | Used? | Why |
|---|---|---|
| QoS 0 (at most once) | no | can lose messages |
| **QoS 1** (at least once, PUBACK) | **yes, both directions** | duplicates are removed by our ChatProto `id` dedup, end to end |
| QoS 2 (exactly once, 4 packets) | no | only per hop and per connection: after a reconnect our client re-sends in a *new* MQTT session, which QoS 2 cannot recognise. Twice the packets for nothing |
| PUBACK as delivery receipt | no | it only means "broker received". Our `ACK` says "stored in SQLite, seq 42, delivered/stored" |
| Retained messages | no | a topic keeps only its **last** message (history would be lost) and it is re-sent on every subscribe. Presence comes from the hub |
| **Clean session** (`clean=true`) | **yes** | a persistent session would be a second offline queue (in Aedes memory, lost on restart, MQTT only). SQLite + `lastSeq` sync already does this for both protocols |
| Last Will (LWT) | no | the broker *is* our server: Aedes' `clientDisconnect` event tells the hub about every disconnect (DISCONNECT, socket closed, keepalive timeout) |
| **Keepalive** 30 s | **yes** | the client sends `PINGREQ` when idle; the broker drops a client silent for 1.5 × 30 s. It replaces our own WebSocket ping heartbeat |

**Closing with a reason (`BYE`).** A WebSocket close frame carries a code (`4001` = replaced),
but an MQTT 3.1.1 broker cannot tell a client why it disconnects it. So before closing a replaced
MQTT session the server publishes `BYE {code: 4001}`. The client then closes with that code
itself (the server cuts the connection after 2 s if it does not). ChatClient sees an ordinary
close with code 4001 and does not reconnect.

**Client side.** `client/mqtt-socket.js` makes an mqtt.js connection look like a WebSocket:
open = CONNACK + SUBACK, `send(text)` = PUBLISH QoS 1 to `up`, message = PUBLISH on `down`,
close = DISCONNECT. ChatClient takes an `openSocket` option and is otherwise unchanged, so the
outbox, retries, dedup, `lastSeq` sync and backoff reconnect are the same code for both
protocols. mqtt.js' own auto-reconnect is switched off (one reconnect policy only). On the join
screen the user picks **WebSocket** or **MQTT**, remembered per tab. The debug panel also lists
every MQTT packet (`CONNECT`, `SUBACK`, `PUBLISH … id=7`, `PUBACK id=7` …).

**Preparing the Step 9 comparison** (cost of one chat message, per direction):

| | ChatProto over WebSocket | ChatProto over MQTT over WebSocket |
|---|---|---|
| Connection setup | TCP + HTTP Upgrade, then `HELLO` → `WELCOME` | TCP + HTTP Upgrade + `CONNECT`/`CONNACK` + `SUBSCRIBE`/`SUBACK`, then `HELLO` → `WELCOME` (2 extra round trips) |
| Per message on the wire | WS header (2–8 B) + JSON | WS header + MQTT header (1 B + 2 B length) + topic (2 + 42–44 B) + packet id (2 B) + JSON: ≈ 50 B more |
| Per-hop receipts | none (TCP only) | `PUBACK` (4 B + WS header) for every PUBLISH, both directions |
| End-to-end receipt | ChatProto `ACK` | ChatProto `ACK` (the same) |
| Dead-peer detection | server ping every 30 s | client `PINGREQ` after 30 s idle, broker timeout 45 s |

### Additional Features (planned)
- Group chat.

### Monitoring
- `/stats` page: active connections, messages per second, totals.

## Performance Evaluation (planned)
- Latency (average sender → recipient time).
- Protocol comparison: ChatProto over WebSocket vs MQTT over WebSocket (see the table in Step 5).
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
- [x] Step 4 – Mobile chat UI
- [x] Step 5 – MQTT: second application-layer protocol
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

`PORT` and `DB_PATH` run a second copy without touching the real database, e.g.
`PORT=3105 DB_PATH=/tmp/test.db npm start` (bash) or
`$env:PORT=3105; $env:DB_PATH="$env:TEMP\test.db"; npm start` (PowerShell).

The server prints its Wi-Fi address and a QR code. Open that address on a phone connected
to the same Wi-Fi. If the phone cannot connect, allow Node.js through Windows Firewall
(Private networks).
