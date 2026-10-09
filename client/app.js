// Realtime Chat – the phone UI (Step 4).
//
// Three layers, each in its own file:
//   chat-client.js    protocol + reliability (retry, dedup, reconnect, sync)   – no DOM
//   mqtt-socket.js    MQTT connection that looks like a WebSocket (Step 5)      – no DOM
//   conversations.js  what to show (messages per contact, ticks, unread)       – no DOM
//   app.js (this)     wires ChatClient events into the state, then render()s the DOM
//
// Rule for this file: every event only CHANGES STATE and calls render(); render() draws the
// screen from the state. And user text only ever reaches the page through textContent,
// never innerHTML, so a message like <img onerror=...> is shown as text, not run (XSS).

(function () {
  'use strict';

  const $ = id => document.getElementById(id);

  // ---------------------------------------------------------------------------------------
  // Browser storage. Every access is wrapped: storage can be blocked (private mode), and the
  // chat must still work then, just without surviving a reload.
  // getText/setText store plain strings (token, username: same format as the Step 3 page,
  // so existing devices keep their identity); get/set store JSON (outbox, read cursors).
  const store = {
    getText(area, key) {
      try { return area.getItem(key); } catch (e) { return null; }
    },
    setText(area, key, value) {
      try { if (value == null) area.removeItem(key); else area.setItem(key, value); } catch (e) { /* storage blocked */ }
    },
    get(area, key, fallback) {
      try { const v = area.getItem(key); return v == null ? fallback : JSON.parse(v); } catch (e) { return fallback; }
    },
    set(area, key, value) {
      try { area.setItem(key, JSON.stringify(value)); } catch (e) { /* storage blocked */ }
    },
  };
  const local = (() => { try { return window.localStorage; } catch (e) { return null; } })();
  const session = (() => { try { return window.sessionStorage; } catch (e) { return null; } })();

  // Device token (Step 3): random, generated once per browser, proves "same device" on reconnect.
  function deviceToken() {
    let t = store.getText(local, 'chat.token');
    if (!t) { t = ChatProto.uuid(); store.setText(local, 'chat.token', t); }
    return t;
  }

  // Outbox per user in localStorage, so closing the tab while offline cannot lose messages.
  function loadOutbox(name) {
    const saved = store.get(local, `chat.outbox.${name}`, []);
    if (!Array.isArray(saved)) return [];
    // Only keep entries that are still valid ChatProto MSG frames.
    return saved.filter(m => m && m.type === ChatProto.TYPES.MSG && ChatProto.decode(ChatProto.encode(m)).ok);
  }

  // ---------------------------------------------------------------------------------------
  // STATE – everything the screen shows is derived from these values.
  const state = {
    conn: 'idle',        // idle (before Join) | connecting | open (socket up, not joined) | joined | offline | replaced
    protocol: store.getText(session, 'chat.protocol') === 'mqtt' ? 'mqtt' : 'ws', // chosen on the join screen
    retryIn: 0,         // ms until the next reconnect attempt (when offline)
    joining: null,       // username we asked for, until WELCOME or ERROR
    joinError: '',
    me: null,            // our username after WELCOME
    online: new Set(),   // usernames online right now
    known: [],           // every registered username
    conv: null,          // Conversations (created at the first WELCOME)
    open: history.state && history.state.chat || null, // contact whose chat is open
    note: '',            // short-lived good news in the banner ("synced 3 messages")
    debug: store.get(local, 'chat.debug', false),
  };
  let lastSent = null;   // for the "re-send last (same id)" demo button
  let noteTimer = null;
  let synced = false;    // false between WELCOME and SYNCED (the offline replay)
  let replayedNew = 0;   // messages in the current replay that we did not have yet

  // The two protocols (Step 5). The page only chooses which socket ChatClient opens; everything
  // else (join, retry, dedup, sync, reconnect) is the same ChatClient code for both:
  //   ws    ChatProto JSON in WebSocket text frames on /ws
  //   mqtt  the same ChatProto JSON as MQTT PUBLISH payloads, MQTT over WebSocket on /mqtt
  //         (client/mqtt-socket.js makes mqtt.js look like a WebSocket)
  const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
  const PROTOCOLS = {
    ws: { label: 'WebSocket', open: () => new WebSocket(`${scheme}://${location.host}/ws`) },
    mqtt: { label: 'MQTT', open: () => new MqttSocket(`${scheme}://${location.host}${MqttBinding.PATH}`) },
  };

  let client = null;     // created when the user presses Join (it depends on the chosen protocol)
  let clientProtocol = null; // the protocol `client` speaks
  const handlers = [];   // [event, fn] pairs, attached to every ChatClient we create
  const on = (event, fn) => handlers.push([event, fn]);

  function startClient(protocol) {
    if (client) client.close();
    const c = new ChatClient({ token: deviceToken(), openSocket: PROTOCOLS[protocol].open });
    // Events from a client we already replaced (the user switched protocol) are ignored.
    for (const [event, fn] of handlers) c.on(event, (...args) => { if (c === client) fn(...args); });
    client = c;
    clientProtocol = protocol;
    return c;
  }

  // ---------------------------------------------------------------------------------------
  // ChatClient events -> state changes. All protocol logic stays inside ChatClient.

  on('status', (s, code) => {
    if (s === 'connecting' && state.conn === 'offline') state.retryIn = 0; // attempt running now
    if (s === 'open') state.conn = 'open';
    if (s === 'closed') {
      if (state.conn !== 'replaced') state.conn = 'offline';
      state.online = new Set(); // disconnected: we no longer know who is online
      logNote(`socket closed (code ${code})`);
    }
    render();
  });

  on('reconnecting', (delay, attempt) => {
    state.conn = 'offline';
    state.retryIn = delay;
    logNote(`reconnecting in ${(delay / 1000).toFixed(1)} s (attempt ${attempt})`);
    render();
  });

  on('replaced', () => { state.conn = 'replaced'; render(); });

  on('welcome', body => {
    if (!state.conv || state.me !== body.username) {
      state.conv = new Conversations(body.username, store.get(local, `chat.read.${body.username}`, {}));
    }
    state.me = body.username;
    synced = false;
    state.joining = null;
    state.joinError = '';
    state.conn = 'joined';
    state.online = new Set(body.users);
    state.known = body.known;
    store.setText(session, 'chat.username', state.me);
    // Messages an earlier page load could not get ACKed go back into the outbox; ChatClient
    // re-sends them after SYNCED. Done before the replay, so their ids count as already seen.
    const saved = loadOutbox(state.me).filter(m => !client.pending.has(m.id));
    client.restore(saved);
    for (const m of saved) state.conv.addSent(m);
    if (state.open && !state.known.includes(state.open)) state.open = null;
    render({ scroll: true });
  });

  on('synced', body => {
    logNote(`offline sync: ${body.count} message(s) replayed, up to seq ${body.lastSeq}`);
    // body.count also includes replays we already had (ignored as duplicates); tell the
    // user only about the ones that were new to this page.
    const fresh = replayedNew;
    replayedNew = 0;
    synced = true;
    if (fresh) showNote(`Up to date · ${fresh} message${fresh === 1 ? '' : 's'} synced from the server`);
    if (state.open) markRead(state.open);
    render();
  });

  on('message', msg => {
    if (!synced) replayedNew++; // between WELCOME and SYNCED every MSG is part of the replay
    state.conv.addReceived(msg);
    if (msg.from === state.open && document.visibilityState === 'visible') markRead(state.open);
    render();
  });

  // Already have this id. If it is one of ours restored from the outbox, the replay tells us
  // its seq (the server stored it before the reload).
  on('duplicate', msg => {
    logNote(`duplicate ${msg.id.slice(0, 8)} (seq ${msg.seq}) ignored`);
    if (state.conv && state.conv.addReceived(msg)) render();
  });

  on('retry', (msg, n) => logNote(`no ACK for ${msg.id.slice(0, 8)} yet, sending again (try ${n})`));

  on('ack', body => {
    if (state.conv) state.conv.ack(body);
    render();
  });

  on('serverError', (body, msg) => {
    if (msg && state.conv) {
      state.conv.setStatus(msg.id, 'failed', null, body.message);
    } else if (!state.me && state.joining) {
      // Our HELLO was refused (NAME_TAKEN, bad name ...): back to the join form.
      state.joinError = body.message;
      state.joining = null;
      store.setText(session, 'chat.username', null);
    } else {
      logNote(`server ERROR ${body.code}: ${body.message}`);
    }
    render();
  });

  on('presence', body => {
    if (body.status === 'online') {
      state.online.add(body.username);
      if (!state.known.includes(body.username)) state.known = [...state.known, body.username].sort();
    } else {
      state.online.delete(body.username);
    }
    render();
  });

  on('users', body => { state.online = new Set(body.users); state.known = body.known; render(); });

  on('outbox', msgs => { if (state.me) store.set(local, `chat.outbox.${state.me}`, msgs); });

  on('frame', (dir, text) => logLine(dir, `${dir === 'out' ? '↑' : '↓'} ${text}`));

  // ---------------------------------------------------------------------------------------
  // User actions -> state changes.

  $('joinForm').addEventListener('submit', e => {
    e.preventDefault();
    const name = $('username').value.trim();
    if (!ChatProto.USERNAME_RE.test(name)) {
      state.joinError = 'Use 1–20 letters, digits or _ (no spaces).';
      return render();
    }
    state.joining = name;
    state.joinError = '';
    if (clientProtocol !== state.protocol) {
      startClient(state.protocol);
      client.join(name);  // sent as soon as the connection is open
      client.connect();
    } else {
      client.join(name);  // same connection (e.g. after NAME_TAKEN): sent now
    }
    render();
  });

  // Protocol choice on the join screen, remembered per tab (so two tabs can use different ones).
  $('joinForm').addEventListener('change', e => {
    if (e.target.name !== 'protocol') return;
    state.protocol = e.target.value;
    store.setText(session, 'chat.protocol', state.protocol);
    render();
  });

  $('contactList').addEventListener('click', e => {
    const btn = e.target.closest('button[data-contact]');
    if (btn) openChat(btn.dataset.contact);
  });

  function openChat(name) {
    state.open = name;
    // A history entry per opened chat: the phone's back button/gesture returns to the list.
    history.pushState({ chat: name }, '');
    markRead(name);
    render({ scroll: true });
  }

  $('backBtn').addEventListener('click', () => {
    if (history.state && history.state.chat) history.back(); // -> popstate below
    else { state.open = null; render(); }
  });

  window.addEventListener('popstate', () => {
    state.open = history.state && history.state.chat || null;
    if (state.open) markRead(state.open);
    render({ scroll: true });
  });

  $('sendForm').addEventListener('submit', e => {
    e.preventDefault();
    const input = $('text');
    const body = input.value;
    if (!state.open || !body.trim()) return;
    lastSent = client.send(state.open, body); // ChatClient retries until ACKed
    state.conv.addSent(lastSent);             // shows at once with the 🕓 tick
    input.value = '';
    render({ scroll: true });
  });

  // Coming back to the tab: messages that arrived while it was hidden are read now.
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && state.open && markRead(state.open)) render();
  });

  $('bannerAction').addEventListener('click', () => client && client.connect()); // "Use here" after 4001

  // Debug panel (oral demo).
  for (const btn of document.querySelectorAll('.debug-toggle')) {
    btn.addEventListener('click', () => {
      state.debug = !state.debug;
      store.set(local, 'chat.debug', state.debug);
      render();
      // A hidden list cannot scroll, so jump to the newest frame now that it is visible.
      if (state.debug) $('frames').scrollTop = $('frames').scrollHeight;
    });
  }
  $('dropBtn').addEventListener('click', () => client && client.dropConnection('demo: dropped by user'));
  // Sends the exact same frame again: the server must ACK it as "duplicate", not deliver it twice.
  $('dupBtn').addEventListener('click', () => {
    if (lastSent && client) client.sendFrame(lastSent);
    else logNote('send a message first');
  });
  $('clearLogBtn').addEventListener('click', () => $('frames').replaceChildren());
  $('rawForm').addEventListener('submit', e => {
    e.preventDefault();
    if (client) client.sendFrame($('raw').value); // malformed on purpose -> the server answers with ERROR
  });

  function markRead(contact) {
    if (!state.conv || !state.conv.markRead(contact)) return false;
    store.set(local, `chat.read.${state.me}`, state.conv.readUpTo);
    return true;
  }

  function showNote(text) {
    state.note = text;
    clearTimeout(noteTimer);
    noteTimer = setTimeout(() => { state.note = ''; render(); }, 4000);
  }

  // ---------------------------------------------------------------------------------------
  // VIEW – draw the screen from `state`. Called after every change.

  function render({ scroll = false } = {}) {
    const screen = !state.me ? 'join' : state.open ? 'chat' : 'list';
    $('joinView').hidden = screen !== 'join';
    $('listView').hidden = screen !== 'list';
    $('chatView').hidden = screen !== 'chat';
    renderBanner();
    if (screen === 'join') renderJoin();
    if (screen === 'list') renderList();
    if (screen === 'chat') renderChat(scroll);
    renderDebug();
    // Unread count in the tab title, e.g. "(3) Realtime Chat".
    const unread = state.conv ? state.conv.totalUnread() : 0;
    document.title = (unread ? `(${unread}) ` : '') + 'Realtime Chat';
  }

  function renderBanner() {
    const waiting = client ? client.pending.size : 0;
    const queued = waiting ? ` · ${waiting} message${waiting === 1 ? '' : 's'} waiting to send` : '';
    let text = '';
    let good = false;
    let action = false;
    if (state.conn === 'replaced') {
      text = 'This chat was opened in another tab or device.';
      action = true;
    } else if (state.conn === 'offline') {
      const when = state.retryIn ? `in ${Math.max(1, Math.round(state.retryIn / 1000))} s` : 'now…';
      text = `Offline – reconnecting ${when}${queued}`;
    } else if (state.conn === 'connecting') {
      text = 'Connecting to the server…';
    } else if (state.conn === 'open' && (state.joining || state.me)) {
      text = `${state.me ? 'Re-joining' : 'Joining'} as ${state.joining || state.me}…`;
    } else if (state.note) {
      text = state.note;
      good = true;
    }
    $('banner').hidden = !text;
    $('banner').classList.toggle('good', good);
    $('bannerText').textContent = text;
    $('bannerAction').hidden = !action;
  }

  function renderJoin() {
    $('joinError').textContent = state.joinError;
    $('joinBtn').disabled = !!state.joining;
    $('joinBtn').textContent = state.joining ? 'Joining…' : 'Join';
    for (const radio of $('joinForm').elements.protocol) {
      radio.checked = radio.value === state.protocol;
      radio.disabled = !!state.joining;
    }
  }

  function renderList() {
    $('meName').textContent = state.me;
    $('meProtocol').textContent = PROTOCOLS[clientProtocol].label;
    const rows = state.conv.contacts(state.known);
    $('noContacts').hidden = rows.length > 0;
    $('contactList').replaceChildren(...rows.map(contactRow));
  }

  // One contact: avatar with presence dot, name, time, last message, unread badge.
  function contactRow({ name, last, unread }) {
    const online = state.online.has(name);
    const btn = el('button', 'contact');
    btn.type = 'button';
    btn.dataset.contact = name;
    // Screen readers get one clear sentence instead of the separate pieces.
    btn.setAttribute('aria-label',
      `${name}, ${online ? 'online' : 'offline'}${unread ? `, ${unread} unread` : ''}`);

    const avatar = el('span', 'avatar', name[0]);
    avatar.append(el('span', `dot ${online ? 'online' : 'offline'}`));

    const top = el('span', 'contact-top');
    top.append(el('span', 'contact-name', name));
    if (last) top.append(el('span', 'contact-time', timeLabel(last.ts)));

    const bottom = el('span', 'contact-bottom');
    const preview = last ? (last.mine ? 'You: ' : '') + last.body
                         : online ? 'online · say hi' : 'offline · messages wait on the server';
    bottom.append(el('span', 'preview', preview));
    if (unread) bottom.append(el('span', 'badge', unread > 99 ? '99+' : String(unread)));

    const main = el('span', 'contact-main');
    main.append(top, bottom);
    btn.append(avatar, main);
    const li = el('li');
    li.append(btn);
    return li;
  }

  function renderChat(forceScroll) {
    const name = state.open;
    const online = state.online.has(name);
    $('chatName').textContent = name;
    $('chatPresence').textContent = online ? 'online' : 'offline · messages wait on the server';

    const list = $('messageList');
    // Auto-scroll only if the user was already at (or near) the bottom: if they scrolled
    // up to read older messages, a new message must not yank them down.
    const nearBottom = list.scrollHeight - list.scrollTop - list.clientHeight < 80;
    const items = state.conv.messages(name).map(bubble);
    if (!items.length) items.push(el('li', 'empty', 'No messages yet. Say hi!'));
    list.replaceChildren(...items);
    if (forceScroll || nearBottom) list.scrollTop = list.scrollHeight;
  }

  // Delivery tick for our own messages: text for the eye, a full sentence for screen readers.
  const TICKS = {
    waiting: ['🕓', 'waiting for the server'],
    stored: ['✓', 'stored on the server'],
    delivered: ['✓✓', 'delivered'],
    failed: ['!', 'not sent'],
  };

  function bubble(m) {
    const li = el('li', `bubble${m.mine ? ' mine' : ''}${m.status === 'failed' ? ' failed' : ''}`);
    li.append(document.createTextNode(m.body)); // textContent-style: never parsed as HTML
    const meta = el('span', 'meta');
    meta.append(el('time', '', timeLabel(m.ts)));
    if (m.mine) {
      const [symbol, label] = TICKS[m.status] || TICKS.waiting;
      const tick = el('span', `tick ${m.status}`, symbol);
      tick.title = label + (m.seq != null ? ` (seq ${m.seq})` : '');
      tick.setAttribute('aria-label', label);
      meta.append(tick);
    }
    li.append(meta);
    if (m.error) li.append(el('span', 'fail-reason', `Not sent: ${m.error}`));
    return li;
  }

  function renderDebug() {
    $('debugPanel').hidden = !state.debug;
    for (const btn of document.querySelectorAll('.debug-toggle')) {
      btn.setAttribute('aria-pressed', String(state.debug));
      btn.setAttribute('aria-label', state.debug ? 'Hide debug panel' : 'Show debug panel');
    }
    $('dbgConn').textContent = `${clientProtocol ? PROTOCOLS[clientProtocol].label + ' · ' : ''}${state.conn}${state.me ? ` as ${state.me}` : ''}`;
    $('dbgSeq').textContent = client ? client.lastSeq : 0;
    $('dbgPending').textContent = client ? client.pending.size : 0;
  }

  // Raw frame log. Capped, so a long demo does not grow the page forever.
  const MAX_LOG = 300;
  function logLine(cls, text) {
    const list = $('frames');
    const nearBottom = list.scrollHeight - list.scrollTop - list.clientHeight < 40;
    list.append(el('li', cls, text));
    while (list.childElementCount > MAX_LOG) list.firstElementChild.remove();
    if (nearBottom) list.scrollTop = list.scrollHeight;
  }
  function logNote(text) { logLine('note', `• ${text}`); }

  // Create an element; text goes in with textContent, so it can never become HTML.
  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text;
    return node;
  }

  // "14:05" for today, "Oct 7, 14:05" for older messages.
  function timeLabel(ts) {
    const d = new Date(ts);
    const time = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    if (d.toDateString() === new Date().toDateString()) return time;
    return `${d.toLocaleDateString([], { month: 'short', day: 'numeric' })}, ${time}`;
  }

  // ---------------------------------------------------------------------------------------
  // Start: re-join automatically after a reload of this tab (username and protocol kept per tab).
  const savedName = store.getText(session, 'chat.username');
  if (savedName) {
    state.joining = savedName;
    $('username').value = savedName;
    startClient(state.protocol);
    client.join(savedName);
    client.connect();
  }
  render();
})();
