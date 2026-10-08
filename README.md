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

### ChatProto v1 (implemented in Step 2)
One JSON object per WebSocket text frame on `ws://<laptop-ip>:3000/ws`
(codec: `protocols/chatproto.js`, shared by server and browser).

```json
{ "v": 1, "type": "MSG", "id": "<uuid>", "ts": 1760000000000,
  "from": "alice", "to": "bob", "body": "hi" }
```

| Type | Direction | Fields |
|---|---|---|
| `HELLO` | client → server | `body.username` (1–20 of `A-Z a-z 0-9 _`) |
| `MSG` | both | `to`, `body` (text ≤ 2000 chars); server sets `from` |
| `LIST` | client → server | – |
| `WELCOME` | server → client | `body.username`, `body.users` |
| `ACK` | server → client | `body.ref` (acked id), `body.status` |
| `PRESENCE` | server → client | `body.username`, `body.status` (`online`/`offline`) |
| `USERS` | server → client | `body.users` |
| `ERROR` | server → client | `body.code`, `body.message`, `body.ref` |

Error codes: `BAD_JSON`, `BAD_VERSION`, `BAD_TYPE`, `BAD_FIELD`, `TOO_LARGE`, `NOT_JOINED`,
`ALREADY_JOINED`, `NAME_TAKEN`, `USER_OFFLINE`. Malformed frames get an `ERROR` and the
connection stays open; frames over 16 KB close the socket with code 1009. The server
pings every 30 s and drops clients that do not answer.

Code layout: `protocols/chatproto.js` (format) → `server/transports/websocket.js`
(socket ↔ protocol) → `server/core/hub.js` (protocol-agnostic routing and presence).

### Reliability
- Every message has a unique ID and a server sequence number.
- Server stores a message before acknowledging it; the client retries until it gets the `ACK` (no loss).
- Receivers ignore message IDs they have already seen (no duplication).
- On reconnect, the client sends its last sequence number; the server replays everything after it (offline sync).

### Additional Features (planned)
- User registration + persistent storage (SQLite) for full message history.
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
- [ ] Step 3 – Reliability: SQLite storage, ACKs, deduplication, offline sync
- [ ] Step 4 – Mobile chat UI
- [ ] Step 5 – MQTT: second application-layer protocol
- [ ] Step 6 – Live metrics dashboard
- [ ] Step 7 – Group chat
- [ ] Step 8 – End-to-end encryption
- [ ] Step 9 – Benchmarks and charts
- [ ] Step 10 – Documentation, demo rehearsal, oral defense prep

## Running

Requires Node.js 22+.

```bash
npm install
npm start
npm test        # unit + integration tests
```

The server prints its Wi-Fi address and a QR code. Open that address on a phone connected
to the same Wi-Fi. If the phone cannot connect, allow Node.js through Windows Firewall
(Private networks).
