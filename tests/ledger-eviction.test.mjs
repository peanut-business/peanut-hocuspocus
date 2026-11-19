import test from 'node:test';
import assert from 'node:assert/strict';
import { CollaborationRuntime } from '../dist/src/runtime.js';

const fakePhp = {
  config: { instanceId: 'inst-1', baseUrl: 'http://127.0.0.1:1', timeoutMs: 1000, secret: 's'.repeat(32) },
  holderKey: 'holder-1',
  call: async () => {
    throw new Error('UNEXPECTED_PHP_CALL');
  },
};

test('pruneExpiredLedgers evicts settled ledgers after TTL to prevent permanent backpressure', () => {
  const runtime = new CollaborationRuntime(fakePhp, '127.0.0.1', 12345);
  const now = 1_000_000;
  const expired = now - 16 * 60 * 1000;

  runtime.targetRecords.set('doc-old', new Map([['r1', { phase: 'completed', settledAt: expired }]]));
  runtime.targetRecordUnloadedAt.set('doc-old', expired);
  runtime.recoveries.set('room:1', { result: { room_key: 'room', lease_epoch: 1, binding: {} }, settledAt: expired });
  runtime.controlOperations.set('room:op1', { active: false, releasedAt: expired });
  runtime.invalidatedGrants.set('tuple-1', expired);
  runtime.invalidationFacts.set('fact-1', 'f');
  runtime.invalidationFactTimes.set('fact-1', expired);
  runtime.invalidationOperations.set('op-1', 'd');
  runtime.invalidationOperationTimes.set('op-1', expired);
  runtime.invalidationResults.set('res-1', { protocol_version: 1 });
  runtime.invalidationResultTimes.set('res-1', expired);

  // Active entries must not be evicted
  runtime.controlOperations.set('room:active', { active: true });
  runtime.invalidatedGrants.set('tuple-fresh', now - 1000);

  runtime.pruneExpiredLedgers(now);

  assert.equal(runtime.targetRecords.has('doc-old'), false);
  assert.equal(runtime.recoveries.has('room:1'), false);
  assert.equal(runtime.controlOperations.has('room:op1'), false);
  assert.equal(runtime.controlOperations.has('room:active'), true);
  assert.equal(runtime.invalidatedGrants.has('tuple-1'), false);
  assert.equal(runtime.invalidatedGrants.has('tuple-fresh'), true);
  assert.equal(runtime.invalidationFacts.has('fact-1'), false);
  assert.equal(runtime.invalidationOperations.has('op-1'), false);
  assert.equal(runtime.invalidationResults.has('res-1'), false);
});
