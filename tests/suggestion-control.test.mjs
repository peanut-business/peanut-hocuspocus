import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHmac } from 'node:crypto';
import { Readable } from 'node:stream';
import { pathToFileURL } from 'node:url';
import * as Y from 'yjs';

const root = process.env.COLLAB_TEST_MODULE_ROOT ?? new URL('../dist/src/', import.meta.url).pathname;
const moduleAt = (name) => import(pathToFileURL(`${root}/${name}.js`).href);
const { CollaborationRuntime } = await moduleAt('runtime');
const { PhpClient } = await moduleAt('php-client');
const { initializeOnce, project, suggestionNode, applySuggestionNode, plainText } = await moduleAt('projector');
const { sha, phpJson } = await moduleAt('digests');
const { Control, controlPath } = await moduleAt('business-control');

const content = (text) => ({ schemaVersion: 'peanut.richtext/1', editorModel: 'tiptap-prosemirror',
  content: { type: 'doc', content: [{ type: 'paragraph', content: [{ type: 'text', text }] }] }, annotations: [] });
const binding = { instance_id: 'test', edition: 'multi-tenant', tenant_id: 1, document_id: 3,
  owner: { module_key: 'content', object_type: 'story', object_id: '7', slot: 'body' } };
const config = { baseUrl: 'http://127.0.0.1:1/internal/', routes: { authorize: 'authorize' },
  instanceId: 'test', serviceInstanceId: 'native-test', keyId: 'test-key', secret: 's'.repeat(32), timeoutMs: 1000 };

function fixture() {
  const php = new PhpClient(config);
  php.call = async () => { throw new Error('UNEXPECTED_PHP_CALL'); };
  const runtime = new CollaborationRuntime(php, '127.0.0.1', 1234);
  const original = initializeOnce(content('原文😀'));
  const bytes = Y.encodeStateAsUpdate(original);
  const baseline = project(original);
  original.destroy();
  const drain = { protocol: 2, action: 'drain', operation_id: 'suggestion:1', input_sha256: sha('input'),
    room_key: sha('room'), holder_key: php.holderKey, lease_epoch: 2 };
  const apply = { ...drain, action: 'suggestion.accept', suggestion: { binding, actor_member_id: 4, base_state_seq: 9,
    baseline_binary_sha256: sha(bytes), baseline_binary_base64: Buffer.from(bytes).toString('base64'),
    baseline_structured_content: baseline.value, baseline_projection_sha256: baseline.sha256,
    structured_content: content('人审后的建议😀') } };
  return { runtime, php, drain, apply };
}

async function dispose(runtime) {
  for (const document of runtime.server.hocuspocus.documents.values()) document.destroy();
  runtime.server.hocuspocus.documents.clear();
}

test('offline signed suggestion loads existing CRDT into native custody and freezes one capture', async () => {
  const { runtime, drain, apply } = fixture();
  try {
    assert.equal((await runtime.controlRoom(drain)).phase, 'offline');
    const capture = await runtime.controlRoom(apply);
    const document = runtime.server.hocuspocus.documents.get('test:multi-tenant:1:3');
    assert.ok(document);
    assert.equal(capture.phase, 'suggestion_applied');
    assert.equal(capture.base_state_seq, 9);
    assert.equal(capture.actor_member_id, 4);
    assert.deepEqual(capture.structured_content, apply.suggestion.structured_content);
    assert.equal(capture.binary_sha256, sha(Buffer.from(capture.binary_base64, 'base64')));
    const originalVector = Y.decodeStateVector(Y.encodeStateVectorFromUpdate(Buffer.from(apply.suggestion.baseline_binary_base64, 'base64')));
    const appliedVector = Y.decodeStateVector(Y.encodeStateVector(document));
    for (const [client, clock] of originalVector) assert.ok(appliedVector.get(client) >= clock);
    const recovered = new Y.Doc({ gc: false });
    Y.applyUpdate(recovered, Buffer.from(capture.binary_base64, 'base64'));
    assert.equal(project(recovered).sha256, capture.projection_sha256);
    assert.ok(Y.equalSnapshots(Y.snapshot(recovered), Y.decodeSnapshot(Buffer.from(capture.snapshot_base64, 'base64'))));
    recovered.destroy();
    assert.deepEqual(await runtime.controlRoom(structuredClone(apply)), capture);
    assert.equal(runtime.server.hocuspocus.documents.get(document.name), document);
    await assert.rejects(runtime.controlRoom({ ...apply, suggestion: { ...apply.suggestion, structured_content: content('另一候选') } }), /ORIGINAL_SUGGESTION_REQUIRED/);
    const state = runtime.states.get(document.name);
    assert.equal(state.changes.length, 0);
    assert.equal(state.blocked, false);
    await runtime.persist(state, document); // A late native autosave does not send a human store.
    assert.equal((await runtime.controlRoom({ ...drain, action: 'release', result_epoch: 3, status: 'active' })).phase, 'released');
    assert.equal(runtime.server.hocuspocus.documents.has(document.name), false);
  } finally { await dispose(runtime); }
});

test('baseline drift rejects before editing and original barrier stays closed', async () => {
  const { runtime, drain, apply } = fixture();
  try {
    await runtime.controlRoom(drain);
    await assert.rejects(runtime.controlRoom({ ...apply, suggestion: { ...apply.suggestion,
      baseline_projection_sha256: sha('wrong baseline') } }), /SUGGESTION_BASELINE_INVALID/);
    assert.equal(runtime.fences.get(drain.room_key).active, true);
  } finally { await dispose(runtime); }
});

test('invalid candidate confirms pre-mutation rejection and can release unchanged room', async () => {
  const { runtime, drain, apply } = fixture();
  try {
    await runtime.controlRoom(drain);
    const invalid = { ...apply, suggestion: { ...apply.suggestion,
      structured_content: { ...apply.suggestion.structured_content, annotations: [{ id: 'forbidden' }] } } };
    await assert.rejects(runtime.controlRoom(invalid), /SUGGESTION_CONTENT_INVALID/);
    assert.equal(runtime.server.hocuspocus.documents.size, 0);
    assert.equal(runtime.fences.get(drain.room_key).active, true);
    assert.equal((await runtime.controlRoom({ ...drain, action: 'release', result_epoch: 3, status: 'active' })).phase, 'released');
  } finally { await dispose(runtime); }
});

test('native frame journal fixes each authenticated submitter and rejects unowned local writes', async () => {
  const { runtime, php, drain, apply } = fixture();
  const auth = { protocol: 2, binding, document_id: 3, room_key: drain.room_key, holder_key: php.holderKey,
    lease_epoch: 2, mode: 'write', member_id: 4, nickname: 'tester', connection_key: sha('connection'),
    authorization_revision: sha('grant'), lease_expires_at: Date.now() + 60000 };
  const state = { auth, document: null, stateSeq: 9, baselineSha: apply.suggestion.baseline_binary_sha256,
    progress: 0, stored: null, capture: null, sources: new Map(), references: new Set(), requests: new Map(),
    blocked: false, dirty: false, pendingBytes: 0, changes: [], changeBytes: 0, reservedChangeBytes: 0 };
  runtime.states.set('test:multi-tenant:1:3', state);
  php.call = async (endpoint) => {
    assert.equal(endpoint, 'load');
    return { binding, holder_key: php.holderKey, document_id: 3, lease_epoch: 2, state_seq: 9,
      state_sha256: apply.suggestion.baseline_binary_sha256, binary_base64: apply.suggestion.baseline_binary_base64,
      structured_content: apply.suggestion.baseline_structured_content, initialization_required: false, store_id: 'store:baseline' };
  };
  try {
    const context = { auth, checkedAt: Date.now(), nextRenewal: 0, renewing: false,
      verifiedUpdate: { connection_key: auth.connection_key, authorization_revision: auth.authorization_revision, authorized_at_ms: Date.now() } };
    const document = await runtime.server.hocuspocus.createDocument('test:multi-tenant:1:3', new Request('http://internal.invalid/'),
      'test', { readOnly: false, isAuthenticated: true }, context);
    const connection = { context, document, readOnly: false, send: () => {}, sendStateless: () => {},
      close: () => document.removeConnection(connection), waitForPendingMessages: async () => {} };
    document.addConnection(connection);
    applySuggestionNode(document, suggestionNode(content('第一帧')), { source: 'connection', connection });
    context.verifiedUpdate = { ...context.verifiedUpdate, authorized_at_ms: Date.now() + 1 };
    applySuggestionNode(document, suggestionNode(content('第二帧😀')), { source: 'connection', connection });
    assert.equal(state.changes.length, 2);
    assert.equal(state.changes[0].before_text, '原文😀');
    assert.equal(state.changes[0].after_text, '第一帧');
    assert.equal(state.changes[1].before_text, '第一帧');
    assert.equal(state.changes[1].after_text, '第二帧😀');
    assert.equal(state.changes[0].after_projection_sha256, state.changes[1].before_projection_sha256);
    assert.equal(state.changes[1].connection_key, auth.connection_key);
    const capture = runtime.capture(state, document);
    assert.deepEqual(capture.tuple.changes, state.changes);
    php.call = async (endpoint, input) => {
      assert.equal(endpoint, 'store');
      return { document_id: input.document_id, store_id: input.store_id, lease_epoch: input.lease_epoch,
        state_seq: input.base_state_seq + 1, binary_sha256: input.binary_sha256, projection_sha256: input.projection_sha256,
        persisted_at: new Date().toISOString(), target_progress: input.target_progress };
    };
    assert.equal(state.changeBytes, state.changes.reduce((bytes, change) => bytes + Buffer.byteLength(JSON.stringify(change)) + 1, 0));
    // Simulate a nearly-full serialized ledger without allocating a large body.
    // The next frame stays unapplied until the existing native flush has a receipt.
    state.changeBytes = 16 * 1024 * 1024 - 1;
    const pending = new Y.Doc({ gc: false });
    Y.applyUpdate(pending, Y.encodeStateAsUpdate(document));
    applySuggestionNode(pending, suggestionNode(content('下一帧\n\u0000')), { source: 'test' });
    const update = Y.encodeStateAsUpdate(pending, Y.encodeStateVector(document));
    const beforeFlush = project(document).sha256;
    const payload = { connection, document, documentName: document.name, context, type: 2, payload: update };
    await runtime.server.hocuspocus.configuration.beforeSync(payload);
    assert.equal(state.stateSeq, 10);
    assert.equal(state.dirty, false);
    assert.equal(state.capture, null);
    assert.equal(state.changeBytes, 0);
    assert.ok(state.reservedChangeBytes > 0);
    assert.equal(project(document).sha256, beforeFlush);
    await runtime.server.hocuspocus.configuration.afterHandleMessage(payload);
    assert.equal(state.reservedChangeBytes, 0);
    // The SDK awaits beforeSync before it applies a frame. Hold three completed
    // hooks without applying them; a fourth must not overbook the journal budget.
    const queued = Array.from({ length: 4 }, () => {
      const queuedContext = { ...context };
      const queuedConnection = { ...connection, context: queuedContext };
      document.addConnection(queuedConnection);
      return { ...payload, connection: queuedConnection, context: queuedContext };
    });
    const admissions = await Promise.allSettled(queued.map((frame) => runtime.server.hocuspocus.configuration.beforeSync(frame)));
    assert.equal(admissions.filter((result) => result.status === 'fulfilled').length, 3);
    assert.equal(admissions.filter((result) => result.status === 'rejected').length, 1);
    for (const frame of queued) {
      await runtime.server.hocuspocus.configuration.afterHandleMessage(frame);
      document.removeConnection(frame.connection);
    }
    assert.equal(state.reservedChangeBytes, 0);
    pending.destroy();
    const baseline = project(document);
    const baselineBytes = Y.encodeStateAsUpdate(document);
    assert.equal((await runtime.controlRoom(drain)).phase, 'drained');
    const accepted = await runtime.controlRoom({ ...apply, suggestion: { ...apply.suggestion,
      base_state_seq: state.stateSeq, baseline_binary_sha256: sha(baselineBytes),
      baseline_binary_base64: Buffer.from(baselineBytes).toString('base64'), baseline_structured_content: baseline.value,
      baseline_projection_sha256: baseline.sha256 } });
    assert.equal(accepted.base_state_seq, 10);
    assert.equal(runtime.server.hocuspocus.documents.get(document.name), document);
    assert.equal(state.changes.length, 0);
    assert.equal(state.blocked, false);
    applySuggestionNode(document, suggestionNode(content('无来源')), { source: 'local', skipStoreHooks: true });
    assert.equal(state.blockedCode, 'UPDATE_SOURCE_OR_PROJECTION_INVALID');
    await assert.rejects(runtime.persist(state, document), /UPDATE_SOURCE_OR_PROJECTION_INVALID/);
  } finally { await dispose(runtime); }
});

test('plain-text profile retains block, table, media and Unicode boundaries', () => {
  assert.equal(plainText({ type: 'doc', content: [
    { type: 'paragraph', content: [{ type: 'text', text: '中文😀e\u0301' }, { type: 'hardBreak' }] },
    { type: 'table', content: [{ type: 'tableRow', content: [
      { type: 'tableCell', content: [{ type: 'paragraph', content: [{ type: 'text', text: 'A' }] }] },
      { type: 'tableCell', content: [{ type: 'paragraph' }] },
    ] }] }, { type: 'media' },
  ] }), '中文😀e\u0301\n\nA\t\n\uFFFC');
});

test('suggestion command uses original control HMAC and response authentication', async () => {
  const { php, runtime, apply } = fixture();
  let calls = 0;
  const control = new Control(php, async () => { calls++; return { phase: 'suggestion_applied' }; }, async () => { throw new Error('unexpected'); });
  const raw = JSON.stringify(apply), nonce = sha('nonce'), timestamp = String(Math.floor(Date.now() / 1000));
  const signature = createHmac('sha256', config.secret).update(['peanut.collaboration.control.request.v2',
    config.instanceId, config.serviceInstanceId, config.keyId, php.holderKey, timestamp, nonce, 'POST', controlPath, sha(raw)].join('\n')).digest('hex');
  const request = () => Object.assign(Readable.from([Buffer.from(raw)]), { url: controlPath, method: 'POST', headers: {
    'content-type': 'application/json', 'x-collaboration-protocol': '2', 'x-collaboration-service-instance': config.serviceInstanceId,
    'x-collaboration-key-id': config.keyId, 'x-collaboration-holder-key': php.holderKey, 'x-collaboration-nonce': nonce,
    'x-collaboration-timestamp': timestamp, 'x-collaboration-signature': signature } });
  let status, headers, body;
  const response = { writeHead: (s, h) => { status = s; headers = h; }, end: (value) => { body = value; } };
  await control.handle(request(), response);
  assert.equal(status, 200);
  assert.equal(calls, 1);
  const expected = createHmac('sha256', config.secret).update(['peanut.collaboration.control.response.v2',
    config.instanceId, config.serviceInstanceId, config.keyId, php.holderKey, nonce, 'POST', controlPath, sha(raw),
    headers['X-Collaboration-Timestamp'], '200', sha(body)].join('\n')).digest('hex');
  assert.equal(headers['X-Collaboration-Signature'], expected);
  const invalid = request();
  invalid.headers['x-collaboration-signature'] = '0'.repeat(64);
  await control.handle(invalid, response);
  assert.equal(status, 403);
  assert.equal(calls, 1);
  await dispose(runtime);
});
