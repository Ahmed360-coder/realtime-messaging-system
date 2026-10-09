// Realtime Chat – the phone UI (Step 4).
//
// Three layers, each in its own file:
//   chat-client.js    protocol + reliability (retry, dedup, reconnect, sync)   – no DOM
//   mqtt-socket.js    MQTT connection that looks like a WebSocket (Step 5)      – no DOM
//   conversations.js  what to show (messages per contact, ticks, unread)       – no DOM
//   app.js (this)     wires ChatClient events into the state, then render()s the DOM
//
// Step 7 adds groups: '#name' chats in the list, a new-group / add-people form, a Leave button,
// sender names on received group bubbles and membership changes as system lines.
//
// Step 8 adds end-to-end encryption (client/e2e.js): a key pair per username in localStorage,
// a lock bar in each chat with the safety number, a warning when a contact's key changes, and
// a mark on messages that were not encrypted or could not be decrypted.
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

  // Step 8: the key pair of `name` on this device, made at its first join and kept next to the
  // device token. The secret key never leaves this browser. Also the keys we pinned for others.
  function keyRingFor(name) {
    let identity = store.get(local, `chat.keys.${name}`, null);
    if (!E2E.isIdentity(identity)) {
      identity = E2E.newIdentity();
      store.set(local, `chat.keys.${name}`, identity);
    }
    return new E2E.KeyRing({
      me: name,
      identity,
      pins: store.get(local, `chat.pins.${name}`, {}),
      onPin: pins => store.set(local, `chat.pins.${name}`, pins),
    });
  }

  // Join as `name`, with that name's keys (a tab can only be one user at a time).
  function joinAs(name) {
    if (!client.e2e || client.e2e.me !== name) client.e2e = keyRingFor(name);
    client.join(name);
  }

  // Outbox per user in localStorage, so closing the tab while offline cannot lose messages.
  function loadOutbox(name) {
    const saved = store.get(local, `chat.outbox.${name}`, []);
    if (!Array.isArray(saved)) return [];
    // Only keep entries that are still valid ChatProto MSG (or, Step 7, GROUP change) frames.
    const kinds = [ChatProto.TYPES.MSG, ChatProto.TYPES.GROUP];
    return saved.filter(m => m && kinds.includes(m.type) && ChatProto.decode(ChatProto.encode(m)).ok);
  }

  const isGroup = ChatProto.isGroup;

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
    open: history.state && history.state.chat || null, // contact (or '#group') whose chat is open
    // Step 7: the new-group / add-people form, when it is open:
    //   { mode: 'create' | 'add', group, picked: Set of usernames, error, pending: id of our GROUP request }
    form: null,
    safetyOpen: false,   // Step 8: the safety-number panel of the open chat is shown
    sendError: '',       // Step 8: why the last message could not be encrypted (and was not sent)
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
    // e2e: the key ring of the user we are (still) joining as; joinAs() sets it.
    const c = new ChatClient({ token: deviceToken(), openSocket: PROTOCOLS[protocol].open, e2e: client ? client.e2e : null });
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
    // They were saved sealed; we can open our own messages (Step 8) to show them.
    for (const m of saved) if (m.type === ChatProto.TYPES.MSG) state.conv.addSent(client.e2e.reveal({ ...m, from: state.me }));
    // A group chat is checked after SYNCED instead: our groups are only known once the replay is in.
    if (state.open && !isGroup(state.open) && !state.known.includes(state.open)) state.open = null;
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
    closeGoneGroup();
    if (state.open) markRead(state.open);
    render();
  });

  on('message', msg => {
    if (!synced) replayedNew++; // between WELCOME and SYNCED every MSG is part of the replay
    state.conv.addReceived(msg);
    if (state.conv.contactOf(msg) === state.open && document.visibilityState === 'visible') markRead(state.open);
    render();
  });

  // Step 7: a membership change (create / add / leave), live or replayed by the sync.
  on('group', msg => {
    if (!synced) replayedNew++;
    state.conv.applyGroup(msg);
    const form = state.form;
    if (form && form.pending === msg.id) {
      // Our own request came back as an event: done. A new group opens straight away.
      state.form = null;
      if (form.mode === 'create') {
        state.open = msg.to;
        history.replaceState({ chat: msg.to }, ''); // the form's history entry becomes the chat
        markRead(msg.to);
        render({ scroll: true });
        return;
      }
      history.back(); // add: back to the group chat (popstate re-renders)
    }
    closeGoneGroup();
    if (msg.to === state.open && document.visibilityState === 'visible') markRead(state.open);
    render();
  });

  // We left the group that is open (or it is not ours after a reload): back to the list.
  function closeGoneGroup() {
    if (!state.open || !isGroup(state.open) || !synced || state.conv.members(state.open)) return;
    state.open = null;
    state.form = null;
    history.replaceState(null, '');
  }

  // Already have this id. If it is one of ours restored from the outbox, the replay tells us
  // its seq (the server stored it before the reload).
  on('duplicate', msg => {
    logNote(`duplicate ${msg.id.slice(0, 8)} (seq ${msg.seq}) ignored`);
    if (state.conv && state.conv.addReceived(msg)) render();
  });

  on('retry', (msg, n) => logNote(`no ACK for ${msg.id.slice(0, 8)} yet, sending again (try ${n})`));

  // Step 8: the group changed while our message was on its way; ChatClient sealed it again.
  on('resealed', msg => logNote(`${msg.to} members changed: ${msg.id.slice(0, 8)} re-encrypted for ${Object.keys(msg.body.keys).length} members`));

  // Step 8: the server presents a different public key than the one we pinned for `user`.
  // Either they really have a new device, or someone (the server?) is in the middle.
  on('keychange', user => {
    logNote(`public key of ${user} CHANGED – not used until accepted`);
    showNote(`⚠ ${user}'s security key changed. Open the chat to compare the safety number.`);
    render();
  });

  on('ack', body => {
    if (state.conv) state.conv.ack(body);
    render();
  });

  on('serverError', (body, msg) => {
    if (msg && msg.type === ChatProto.TYPES.GROUP) {
      // A refused group change (GROUP_EXISTS, UNKNOWN_USER ...): show it in the form if open.
      if (state.form && state.form.pending === msg.id) {
        state.form.error = body.message;
        state.form.pending = null;
      } else {
        showNote(`Group change refused: ${body.message}`);
      }
    } else if (msg && state.conv) {
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
      joinAs(name);  // sent as soon as the connection is open
      client.connect();
    } else {
      joinAs(name);  // same connection (e.g. after NAME_TAKEN): sent now
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
    state.safetyOpen = false;
    state.sendError = '';
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
    const open = history.state && history.state.chat || null;
    if (open !== state.open) { state.safetyOpen = false; state.sendError = ''; }
    state.open = open;
    if (!(history.state && history.state.form)) state.form = null; // back out of the group form
    if (state.open) markRead(state.open);
    render({ scroll: true });
  });

  // ---- Step 7: groups ----

  // Open the form: 'create' (from the chat list) or 'add' (people to the open group).
  function openForm(mode) {
    state.form = { mode, group: mode === 'add' ? state.open : null, picked: new Set(), error: '', pending: null };
    history.pushState({ form: mode, chat: state.open }, ''); // the back gesture cancels the form
    $('groupName').value = '';
    render();
    if (mode === 'create') $('groupName').focus();
  }

  $('newGroupBtn').addEventListener('click', () => openForm('create'));
  $('addMembersBtn').addEventListener('click', () => openForm('add'));
  $('groupBackBtn').addEventListener('click', () => history.back());

  $('pickList').addEventListener('change', e => {
    if (!state.form || e.target.type !== 'checkbox') return;
    if (e.target.checked) state.form.picked.add(e.target.value);
    else state.form.picked.delete(e.target.value);
    render();
  });

  $('groupForm').addEventListener('submit', e => {
    e.preventDefault();
    const form = state.form;
    if (!form || form.pending) return;
    const users = [...form.picked].sort();
    if (form.mode === 'create') {
      const name = '#' + $('groupName').value.trim();
      if (!ChatProto.GROUP_RE.test(name)) {
        form.error = 'Use 1–20 letters, digits or _ (no spaces).';
        return render();
      }
      // Goes through ChatClient's outbox: retried until ACKed, like a message.
      form.pending = client.changeGroup('create', name, users).id;
    } else {
      if (!users.length) { form.error = 'Pick at least one person.'; return render(); }
      form.pending = client.changeGroup('add', form.group, users).id;
    }
    form.error = '';
    render();
  });

  $('leaveBtn').addEventListener('click', () => {
    const name = state.open;
    if (!isGroup(name) || !window.confirm(`Leave ${name}? You will no longer get its messages.`)) return;
    // The chat closes when the server's GROUP event (members without us) comes back.
    client.changeGroup('leave', name);
    showNote(`Leaving ${name}…`);
    render();
  });

  $('sendForm').addEventListener('submit', e => {
    e.preventDefault();
    const input = $('text');
    const body = input.value;
    if (!state.open || !body.trim()) return;
    try {
      lastSent = client.send(state.open, body); // sealed (Step 8), then retried until ACKed
    } catch (err) {
      if (!(err instanceof E2E.E2EError)) throw err;
      // Not encrypted = not sent. The text stays in the input.
      state.sendError = err.message;
      return render();
    }
    state.sendError = '';
    // Shows at once with the 🕓 tick; the bubble shows our text, the frame carries ciphertext.
    state.conv.addSent({ ...lastSent, body, security: 'e2e' });
    input.value = '';
    render({ scroll: true });
  });

  // ---- Step 8: end-to-end encryption ----

  $('secureBar').addEventListener('click', () => {
    state.safetyOpen = !state.safetyOpen;
    render();
  });

  // The user compared the NEW safety number with the contact (in person) and accepts the new key.
  $('trustBtn').addEventListener('click', () => {
    const user = state.open;
    if (!client.e2e.trust(user)) return;
    // Messages that came with the new key and did not open can be read now.
    const opened = state.conv.reopenFailed(m => client.e2e.reveal(m));
    showNote(`New key of ${user} accepted${opened ? ` · ${opened} message${opened === 1 ? '' : 's'} decrypted` : ''}`);
    state.sendError = '';
    render();
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
    const screen = !state.me ? 'join' : state.form ? 'group' : state.open ? 'chat' : 'list';
    $('joinView').hidden = screen !== 'join';
    $('listView').hidden = screen !== 'list';
    $('chatView').hidden = screen !== 'chat';
    $('groupView').hidden = screen !== 'group';
    renderBanner();
    if (screen === 'join') renderJoin();
    if (screen === 'list') renderList();
    if (screen === 'chat') renderChat(scroll);
    if (screen === 'group') renderGroupForm();
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

  // One contact or group: avatar (with presence dot for a person), name, time, last message, unread badge.
  function contactRow({ name, last, unread, group, members }) {
    const online = state.online.has(name);
    const btn = el('button', 'contact');
    btn.type = 'button';
    btn.dataset.contact = name;
    // Screen readers get one clear sentence instead of the separate pieces.
    const status = group ? `group, ${members.length} members` : online ? 'online' : 'offline';
    btn.setAttribute('aria-label', `${name}, ${status}${unread ? `, ${unread} unread` : ''}`);

    const avatar = el('span', group ? 'avatar group' : 'avatar', group ? '#' : name[0]);
    if (!group) avatar.append(el('span', `dot ${online ? 'online' : 'offline'}`));

    const top = el('span', 'contact-top');
    top.append(el('span', 'contact-name', name));
    if (last) top.append(el('span', 'contact-time', timeLabel(last.ts)));

    const bottom = el('span', 'contact-bottom');
    let preview;
    if (last && last.event) preview = eventText(last);
    else if (last) preview = (last.mine ? 'You: ' : group ? `${last.from}: ` : '') + (last.security === 'failed' ? '🔒 encrypted message' : last.body);
    else if (group) preview = `${members.length} members`;
    else preview = online ? 'online · say hi' : 'offline · messages wait on the server';
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
    const members = state.conv.members(name); // null for a 1-to-1 chat (or a group not synced yet)
    const group = isGroup(name);
    $('chatName').textContent = name;
    if (group) {
      // Presence stays global (PRESENCE frames); a group just counts its members who are online.
      const list = members || [];
      const online = list.filter(u => u === state.me || state.online.has(u)).length;
      const names = list.map(u => (u === state.me ? 'you' : u)).join(', ');
      $('chatPresence').textContent = `${list.length} members · ${online} online · ${names}`;
    } else {
      $('chatPresence').textContent = state.online.has(name) ? 'online' : 'offline · messages wait on the server';
    }
    $('addMembersBtn').hidden = !members;
    $('leaveBtn').hidden = !members;
    renderSecurity(name, members);
    $('sendError').hidden = !state.sendError;
    $('sendError').textContent = state.sendError;

    const list = $('messageList');
    // Auto-scroll only if the user was already at (or near) the bottom: if they scrolled
    // up to read older messages, a new message must not yank them down.
    const nearBottom = list.scrollHeight - list.scrollTop - list.clientHeight < 80;
    const items = state.conv.messages(name).map(bubble);
    if (!items.length) items.push(el('li', 'empty', 'No messages yet. Say hi!'));
    list.replaceChildren(...items);
    if (forceScroll || nearBottom) list.scrollTop = list.scrollHeight;
  }

  // Step 8: the lock bar under the chat header and, when tapped, the safety-number panel.
  function renderSecurity(name, members) {
    const ring = client.e2e;
    const bar = $('secureBar');
    let text, warn = false, intro, number = '', trust = false;
    if (isGroup(name)) {
      // In a group nobody can be "changed" without a warning in their 1-to-1 chat: we list them here too.
      const list = members || [];
      const changed = list.filter(u => ring.status(u) === 'changed');
      const missing = list.filter(u => ring.status(u) === 'unknown');
      warn = changed.length > 0;
      text = warn ? `⚠ Key changed: ${changed.join(', ')} · tap for details`
        : `🔒 End-to-end encrypted for ${list.length} members`;
      intro = 'Every message gets a new random key, which is encrypted separately for each member ' +
        '(you included). The server only stores and forwards ciphertext. Verify each member by ' +
        'comparing safety numbers in your 1-to-1 chat with them.' +
        (warn ? ` Sending is blocked until you accept the new key of ${changed.join(', ')} in their chat.` : '') +
        (missing.length ? ` No key yet for: ${missing.join(', ')}.` : '');
    } else {
      const status = ring.status(name);
      warn = status === 'changed';
      text = warn ? `⚠ ${name}'s security key changed · tap to verify`
        : status === 'pinned' ? '🔒 End-to-end encrypted · tap for safety number'
        : `🔓 No key for ${name} yet · cannot send`;
      if (warn) {
        intro = `The server now gives a different key for ${name} than before. Either ${name} ` +
          'has a new device, or someone (even the server) is trying to read your messages. ' +
          `Compare this NEW safety number with ${name}'s phone before you accept it. Until then, sending is blocked.`;
        number = ring.safetyNumber(name, true);
        trust = true;
      } else if (status === 'pinned') {
        intro = `Messages to ${name} are encrypted on this phone and can only be opened on ${name}'s. ` +
          `Compare these 30 digits with ${name}'s screen: if they match, nobody (not even the server) ` +
          'swapped a key in between.';
        number = ring.safetyNumber(name);
      } else {
        intro = `${name} has not opened the app since encryption was added, so there is no public key to encrypt to.`;
      }
    }
    bar.textContent = text;
    bar.classList.toggle('warn', warn);
    bar.setAttribute('aria-expanded', String(state.safetyOpen));
    $('safetyPanel').hidden = !state.safetyOpen;
    $('safetyText').textContent = intro;
    $('safetyNumber').textContent = number;
    $('safetyNumber').hidden = !number;
    $('trustBtn').hidden = !trust;
  }

  // Delivery tick for our own messages: text for the eye, a full sentence for screen readers.
  const TICKS = {
    waiting: ['🕓', 'waiting for the server'],
    stored: ['✓', 'stored on the server'],
    delivered: ['✓✓', 'delivered'],
    failed: ['!', 'not sent'],
  };

  function bubble(m) {
    if (m.event) return el('li', 'system', eventText(m)); // membership change: a centred grey line
    const li = el('li', `bubble${m.mine ? ' mine' : ''}${m.status === 'failed' ? ' failed' : ''}`);
    // In a group, say who wrote it (textContent, like the message itself).
    if (!m.mine && isGroup(m.to)) li.append(el('span', 'sender', m.from));
    // Step 8: a message that did not decrypt (wrong key, modified, not for us) shows no text.
    if (m.security === 'failed') li.append(el('span', 'undecryptable', '🔒 Could not decrypt this message'));
    else li.append(document.createTextNode(m.body)); // textContent-style: never parsed as HTML
    const meta = el('span', 'meta');
    // Plain text has no proof of who wrote it: anyone, the server included, could have.
    if (m.security === 'plain' && !m.mine) meta.append(el('span', 'plain-mark', '⚠ not encrypted'));
    meta.append(el('time', '', timeLabel(m.ts)));
    if (m.mine) {
      const [symbol, label] = TICKS[m.status] || TICKS.waiting;
      const tick = el('span', `tick ${m.status}`, symbol);
      // Group: ✓✓ only when every other member got it live; the tooltip says how many did.
      const counts = m.recipients != null ? ` · live to ${m.delivered} of ${m.recipients} members` : '';
      tick.title = label + counts + (m.seq != null ? ` (seq ${m.seq})` : '');
      tick.setAttribute('aria-label', label);
      meta.append(tick);
    }
    li.append(meta);
    if (m.error) li.append(el('span', 'fail-reason', `Not sent: ${m.error}`));
    return li;
  }

  // "alice added bob and you", "You created the group with bob", "carol left".
  function eventText(e) {
    const who = e.from === state.me ? 'You' : e.from;
    const names = e.event.users.map(u => (u === state.me ? 'you' : u));
    const list = names.length > 1 ? `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}` : names[0];
    if (e.event.op === 'create') return `${who} created the group${list ? ` with ${list}` : ''}`;
    if (e.event.op === 'add') return `${who} added ${list}`;
    return `${who} left`;
  }

  // The new-group / add-people form (Step 7).
  function renderGroupForm() {
    const form = state.form;
    const create = form.mode === 'create';
    const members = create ? [state.me] : state.conv.members(form.group) || [];
    $('groupTitle').textContent = create ? 'New group' : `Add to ${form.group}`;
    $('groupSubtitle').textContent = create ? 'You are added automatically' : `${members.length} members now`;
    $('groupNameRow').hidden = !create;
    $('groupName').disabled = !!form.pending;
    $('pickLegend').textContent = create ? 'Add people (optional)' : 'Pick people to add';

    // Everyone registered who is not in the group yet. The list is only rebuilt when it changes,
    // so a tap on a checkbox does not lose the keyboard focus.
    const candidates = state.known.filter(u => !members.includes(u));
    for (const u of [...form.picked]) if (!candidates.includes(u)) form.picked.delete(u);
    const list = $('pickList');
    const key = candidates.join(',');
    if (list.dataset.key !== key) {
      list.dataset.key = key;
      list.replaceChildren(...candidates.map(u => {
        const box = el('input');
        box.type = 'checkbox';
        box.value = u;
        const label = el('label');
        label.append(box, el('span', 'pick-name', u), el('span', 'pick-state'));
        const li = el('li');
        li.append(label);
        return li;
      }));
    }
    for (const label of list.querySelectorAll('label')) {
      const box = label.querySelector('input');
      box.checked = form.picked.has(box.value);
      box.disabled = !!form.pending;
      const online = state.online.has(box.value);
      const st = label.querySelector('.pick-state');
      st.textContent = online ? 'online' : 'offline';
      st.className = `pick-state${online ? ' online' : ''}`;
    }
    $('pickEmpty').hidden = candidates.length > 0;

    $('groupError').textContent = form.error;
    const n = form.picked.size;
    $('groupSubmit').disabled = !!form.pending;
    $('groupSubmit').textContent = form.pending ? (create ? 'Creating…' : 'Adding…')
      : create ? `Create group${n ? ` with ${n}` : ''}` : `Add ${n || ''}`.trim();
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
    joinAs(savedName);
    client.connect();
  }
  render();
})();
