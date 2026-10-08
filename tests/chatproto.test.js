// Unit tests for the ChatProto v1 codec (no network involved).
// Run with: npm test

const test = require('node:test');
const assert = require('node:assert/strict');
const ChatProto = require('../protocols/chatproto');
const { TYPES, ERRORS } = ChatProto;

const decodeObj = (obj, allowed) => ChatProto.decode(JSON.stringify(obj), allowed);
const validMsg = () => ChatProto.make(TYPES.MSG, { to: 'bob', body: 'hi' });

test('make() fills the header fields', () => {
  const m = ChatProto.make(TYPES.HELLO, { body: { username: 'alice' } });
  assert.equal(m.v, 1);
  assert.equal(m.type, 'HELLO');
  assert.match(m.id, /^[0-9a-f-]{36}$/);
  assert.ok(Number.isFinite(m.ts));
});

test('uuid() produces distinct v4 UUIDs', () => {
  const ids = new Set(Array.from({ length: 1000 }, ChatProto.uuid));
  assert.equal(ids.size, 1000);
  for (const id of ids) assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
});

test('encode -> decode round trip', () => {
  const m = validMsg();
  const r = ChatProto.decode(ChatProto.encode(m));
  assert.equal(r.ok, true);
  assert.deepEqual(r.msg, m);
});

test('rejects non-JSON and non-objects', () => {
  assert.equal(ChatProto.decode('hello').error.code, ERRORS.BAD_JSON);
  assert.equal(ChatProto.decode('[1,2]').error.code, ERRORS.BAD_JSON);
  assert.equal(ChatProto.decode('null').error.code, ERRORS.BAD_JSON);
  assert.equal(ChatProto.decode(42).error.code, ERRORS.BAD_JSON);
});

test('rejects oversized frames', () => {
  const r = ChatProto.decode('x'.repeat(ChatProto.MAX_FRAME_BYTES + 1));
  assert.equal(r.error.code, ERRORS.TOO_LARGE);
});

test('rejects wrong version and echoes the id as ref', () => {
  const m = { ...validMsg(), v: 2 };
  const r = decodeObj(m);
  assert.equal(r.error.code, ERRORS.BAD_VERSION);
  assert.equal(r.error.ref, m.id);
});

test('rejects unknown types and types the client may not send', () => {
  assert.equal(decodeObj({ ...validMsg(), type: 'NOPE' }).error.code, ERRORS.BAD_TYPE);
  assert.equal(decodeObj({ ...validMsg(), type: 'toString' }).error.code, ERRORS.BAD_TYPE);
  // WELCOME is server -> client only.
  assert.equal(decodeObj({ ...validMsg(), type: 'WELCOME' }, ChatProto.CLIENT_TYPES).error.code, ERRORS.BAD_TYPE);
});

test('rejects bad header fields', () => {
  assert.equal(decodeObj({ ...validMsg(), id: 'abc' }).error.code, ERRORS.BAD_FIELD);
  assert.equal(decodeObj({ ...validMsg(), id: undefined }).error.code, ERRORS.BAD_FIELD);
  assert.equal(decodeObj({ ...validMsg(), ts: 'now' }).error.code, ERRORS.BAD_FIELD);
});

const TOKEN = 'test-token-0123456789';

test('validates HELLO usernames', () => {
  const hello = username => decodeObj(ChatProto.make(TYPES.HELLO, { body: { username, token: TOKEN } }));
  assert.equal(hello('alice_2').ok, true);
  assert.equal(hello('').error.code, ERRORS.BAD_FIELD);
  assert.equal(hello('has space').error.code, ERRORS.BAD_FIELD);
  assert.equal(hello('x'.repeat(21)).error.code, ERRORS.BAD_FIELD);
  assert.equal(decodeObj(ChatProto.make(TYPES.HELLO, {})).error.code, ERRORS.BAD_FIELD);
});

test('validates HELLO token and lastSeq', () => {
  const hello = body => decodeObj(ChatProto.make(TYPES.HELLO, { body: { username: 'alice', ...body } }));
  assert.equal(hello({ token: TOKEN }).ok, true);
  assert.equal(hello({ token: TOKEN, lastSeq: 0 }).ok, true);
  assert.equal(hello({ token: TOKEN, lastSeq: 42 }).ok, true);
  assert.equal(hello({}).error.code, ERRORS.BAD_FIELD);                  // token required
  assert.equal(hello({ token: 'short' }).error.code, ERRORS.BAD_FIELD);
  assert.equal(hello({ token: TOKEN, lastSeq: -1 }).error.code, ERRORS.BAD_FIELD);
  assert.equal(hello({ token: TOKEN, lastSeq: 1.5 }).error.code, ERRORS.BAD_FIELD);
  assert.equal(hello({ token: TOKEN, lastSeq: '3' }).error.code, ERRORS.BAD_FIELD);
});

test('validates ACK seq and SYNCED', () => {
  const ack = body => decodeObj(ChatProto.make(TYPES.ACK, { body }), ChatProto.SERVER_TYPES);
  assert.equal(ack({ ref: 'x', seq: 7, status: 'stored' }).ok, true);
  assert.equal(ack({ ref: 'x', status: 'stored' }).error.code, ERRORS.BAD_FIELD);
  const synced = body => decodeObj(ChatProto.make(TYPES.SYNCED, { body }), ChatProto.SERVER_TYPES);
  assert.equal(synced({ count: 3, lastSeq: 9 }).ok, true);
  assert.equal(synced({ count: 3 }).error.code, ERRORS.BAD_FIELD);
  // SYNCED is server -> client only.
  assert.equal(decodeObj(ChatProto.make(TYPES.SYNCED, { body: { count: 0, lastSeq: 0 } })).error.code, ERRORS.BAD_TYPE);
});

test('validates MSG to/body', () => {
  assert.equal(decodeObj({ ...validMsg(), to: undefined }).error.code, ERRORS.BAD_FIELD);
  assert.equal(decodeObj({ ...validMsg(), body: '   ' }).error.code, ERRORS.BAD_FIELD);
  assert.equal(decodeObj({ ...validMsg(), body: { text: 'hi' } }).error.code, ERRORS.BAD_FIELD);
  assert.equal(decodeObj({ ...validMsg(), body: 'x'.repeat(ChatProto.MAX_BODY_CHARS + 1) }).error.code, ERRORS.BAD_FIELD);
  assert.equal(decodeObj({ ...validMsg(), seq: 5 }, ChatProto.SERVER_TYPES).ok, true);
  assert.equal(decodeObj({ ...validMsg(), seq: -5 }, ChatProto.SERVER_TYPES).error.code, ERRORS.BAD_FIELD);
});

test('decodes server messages with SERVER_TYPES', () => {
  const welcome = ChatProto.make(TYPES.WELCOME, { body: { username: 'a', users: ['a'] } });
  assert.equal(decodeObj(welcome, ChatProto.SERVER_TYPES).ok, true);
  const err = ChatProto.error(ERRORS.NAME_TAKEN, 'taken', null);
  assert.equal(decodeObj(err, ChatProto.SERVER_TYPES).ok, true);
});
