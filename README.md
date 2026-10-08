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

## Running (to be added)
