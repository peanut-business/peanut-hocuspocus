import { randomUUID } from 'node:crypto';
import { Server, type Connection, type Document } from '@hocuspocus/server';
import * as Y from 'yjs';
import {
  assertAuth,
  assertBinding,
  invalidationJson,
  type AccessInvalidationInput,
  type AccessInvalidationTarget,
  type AccessInvalidationAck,
  binary,
  digest,
  requestId,
  type ConnectionAuth,
  type Load,
  type StoreInput,
  type Receipt,
  type Participant,
  type Committed,
  type Binding,
  type Recovery,
  type AcceptedChange,
  roomName,
} from './contracts.js';
import { PhpClient, PhpFailure } from './php-client.js';
import { phpJson, sha } from './digests.js';
import {
  initializeOnce,
  project,
  projectorVersion,
  type Structured,
  plainText,
  suggestionNode,
  applySuggestionNode,
} from './projector.js';
import {
  Control,
  controlPath,
  accessPath,
  type ControlInput,
} from './business-control.js';

interface Context {
  auth: ConnectionAuth;
  checkedAt: number;
  nextRenewal: number;
  renewing: boolean;
  verifiedUpdate?: Participant;
  changeReservation?: number;
}
interface Capture {
  tuple: StoreInput;
  hash: string;
  snapshot: Y.Snapshot;
  attempted: boolean;
  failures: number;
  pendingBytes: number;
}
interface FixedContent {
  bytes: Uint8Array;
  snapshot: Y.Snapshot;
  projection: ReturnType<typeof project>;
  progress: number;
}
interface TargetRecord {
  fixedClick: FixedContent;
  actor: {
    memberId: number;
    tenantId: number;
    documentId: number;
    epoch: number;
    roomKey: string;
  };
  phase:
    | 'preparing'
    | 'saving'
    | 'confirming'
    | 'completed'
    | 'unconfirmed'
    | 'canceled';
  prepared: boolean;
  confirmation: Record<string, unknown> | null;
  committed: Committed | null;
  inFlight?: Promise<Committed>;
  settledAt?: number;
}
interface Stored {
  hasReceipt: boolean;
  proof: Committed;
  snapshot: Y.Snapshot;
  tuple: StoreInput | null;
  bytes: Uint8Array;
  value: Structured;
  referenceDigest: string;
  progress: number;
}
interface State {
  auth: ConnectionAuth;
  document: Document | null;
  stateSeq: number;
  baselineSha: string;
  progress: number;
  stored: Stored | null;
  capture: Capture | null;
  sources: Map<string, { participant: Participant; progress: number }>;
  references: Set<string>;
  requests: Map<string, TargetRecord>;
  blocked: boolean;
  blockedCode?: string;
  dirty: boolean;
  pendingBytes: number;
  changes: AcceptedChange[];
  changeBytes: number;
  reservedChangeBytes: number;
  lastProjection?: ReturnType<typeof project>;
}
interface Fence {
  input: ControlInput;
  drain?: Promise<Record<string, unknown>>;
  drained?: Record<string, unknown>;
  active: boolean;
  releasedAt?: number;
  releaseInput?: string;
  release?: Promise<Record<string, unknown>>;
  released?: Record<string, unknown>;
  suggestionInput?: string;
  suggestion?: Promise<Record<string, unknown>>;
  applied?: Record<string, unknown>;
}
interface RecoveryAttempt {
  requestId: string;
  roomKey: string;
  epoch: number;
  binding: Binding;
  result?: Pick<Recovery, 'room_key' | 'lease_epoch' | 'binding'>;
  settledAt?: number;
  inFlight?: Promise<Recovery>;
}
// Both plain-text JSON strings are bounded by their 2 MiB full projections;
// the fixed connection/digest/progress fields occupy less than 1 KiB.
const maxChangeBytes = 2 * 2097152 + 1024;
const ledgerTtlMs = 15 * 60 * 1000;

/** Native Hocuspocus owns documents, connections, message order, broadcasts and saveMutex. */
export class CollaborationRuntime {
  readonly server: Server<Context>;
  private readonly states = new Map<string, State>();
  private readonly targetRecords = new Map<string, Map<string, TargetRecord>>();
  private readonly actions = new Set<Promise<unknown>>();
  private readonly fences = new Map<string, Fence>();
  private readonly controlOperations = new Map<string, Fence>();
  private readonly recoveries = new Map<string, RecoveryAttempt>();
  private readonly control: Control;
  private readonly suggestionOrigins = new WeakMap<object, Fence>();
  private readonly businessLoads = new Map<string, NonNullable<ControlInput['suggestion']>>();
  // Negative tuples and idempotency ledgers expire after ledgerTtlMs so long-lived
  // processes cannot hit permanent backpressure DoS once historical entries age out.
  private readonly invalidatedGrants = new Map<string, number>();
  private readonly invalidationFacts = new Map<string, string>();
  private readonly invalidationFactTimes = new Map<string, number>();
  private readonly invalidationOperations = new Map<string, string>();
  private readonly invalidationOperationTimes = new Map<string, number>();
  private readonly invalidationResults = new Map<string, AccessInvalidationAck>();
  private readonly invalidationResultTimes = new Map<string, number>();
  private readonly targetRecordUnloadedAt = new Map<string, number>();
  private maintenance: ReturnType<typeof setInterval> | undefined;
  private stopping = false;
  private readonly sockets = new Set<import('node:net').Socket>();
  private upgradeWindow = 0;
  private upgradeCount = 0;

  constructor(
    readonly php: PhpClient,
    readonly address: string,
    readonly port: number
  ) {
    if (!address || !Number.isSafeInteger(port) || port < 1 || port > 65535)
      throw new Error('LISTENER_CONFIG_REQUIRED');
    this.control = new Control(
      php,
      (input) => this.controlRoom(input),
      async (input) => this.invalidateAccess(input)
    );
    this.server = new Server<Context>({
      address,
      port,
      stopOnSignals: false,
      quiet: true,
      debounce: 2000,
      maxDebounce: 10000,
      timeout: 30000,
      maxUnauthenticatedQueueSize: 1048576,
      maxUnauthenticatedQueueMessages: 128,
      maxPendingDocuments: 8,
      unloadImmediately: false,
      yDocOptions: { gc: false, gcFilter: () => true },
      websocketOptions: { maxPayload: 4200000 },
      onUpgrade: async ({ socket }) => {
        const now = Date.now();
        if (now - this.upgradeWindow >= 1000) {
          this.upgradeWindow = now;
          this.upgradeCount = 0;
        }
        if (this.stopping || this.sockets.size >= 1000 || ++this.upgradeCount > 100) {
          // Hocuspocus's async upgrade listener rethrows truthy hook errors.
          // Reject only this socket; its empty sentinel stops the upgrade safely.
          socket.destroy();
          throw undefined;
        }
        this.sockets.add(socket);
        socket.once('close', () => this.sockets.delete(socket));
      },
      onConnect: async () => {
        if (this.stopping) throw new Error('SERVICE_STOPPING');
      },
      onAuthenticate: async (payload) => {
        if (
          this.stopping ||
          !payload.token ||
          payload.token.length > 8192 ||
          (this.states.size >= 1000 && !this.states.has(payload.documentName))
        )
          throw new Error('CONNECTION_UNAVAILABLE');
        let auth: ConnectionAuth;
        try {
          auth = await this.authorize(payload.token, payload.documentName);
        } catch (error) {
          throw new Error(this.code(error));
        }
        if (this.stopping) throw new Error('SERVICE_STOPPING');
        assertAuth(
          auth,
          payload.documentName,
          php.config.instanceId,
          php.holderKey
        );
        this.assertGrantNotInvalidated(auth);
        if (this.fences.get(auth.room_key)?.active)
          throw new Error('DOCUMENT_BARRIER');
        let state = this.states.get(payload.documentName);
        if (!state) {
          if (this.states.size >= 1000)
            throw new Error('CONNECTION_UNAVAILABLE');
          this.targetRecordUnloadedAt.delete(payload.documentName);
          state = {
            auth,
            document: null,
            stateSeq: 0,
            baselineSha: '',
            progress: 0,
            stored: null,
            capture: null,
            sources: new Map(),
            references: new Set(),
            requests: this.targetRecords.get(payload.documentName) ?? new Map(),
            blocked: false,
            dirty: false,
            pendingBytes: 0,
            changes: [],
            changeBytes: 0,
            reservedChangeBytes: 0,
          };
          this.states.set(payload.documentName, state);
        }
        this.sameRoom(state, auth);
        if (state.blocked) throw new Error('ROOM_RECOVERY_REQUIRED');
        payload.connectionConfig.readOnly = auth.mode === 'read';
        return {
          auth,
          checkedAt: Date.now(),
          nextRenewal: this.renewalAt(auth),
          renewing: false,
        } satisfies Context;
      },
      onTokenSync: async () => {
        throw new Error('FRESH_CONNECTION_REQUIRED');
      },
      onLoadDocument: async (payload) => {
        const state = this.state(payload.documentName);
        try {
          const business = this.businessLoads.get(payload.documentName);
          if (business) this.loadBusinessBaseline(state, payload.document, business);
          else await this.load(state, payload.document, payload.context);
          state.lastProjection = project(payload.document);
          // Native update events run synchronously, before another connection can mutate
          // the document or replace verifiedUpdate. Async onChange hooks cannot journal frames.
          payload.document.on('update', (update: Uint8Array, origin: unknown) =>
            this.recordChange(state, payload.document, update, origin));
        } catch (error) {
          if (!state.capture && !state.dirty) {
            this.states.delete(payload.documentName);
            payload.document.destroy();
          } else state.blocked = true;
          throw new Error(this.code(error));
        }
      },
      connected: async (payload) => {
        this.assertConnection(payload.connection, false);
        this.roster(this.state(payload.documentName));
      },
      onDisconnect: async (payload) => {
        const state = this.states.get(payload.documentName);
        if (state) this.roster(state);
      },
      beforeHandleAwareness: async (payload) => {
        if (!payload.connection) throw new Error('AWARENESS_SOURCE_INVALID');
        this.assertConnection(payload.connection, false);
        // Native awareness is transport data; no client identity reaches the roster.
        payload.states.clear();
      },
      beforeSync: async (payload) => {
        try {
          const state = this.assertConnection(payload.connection, false);
          if (payload.type === 0) return;
          if (payload.type !== 1 && payload.type !== 2)
            throw new Error('SYNC_TYPE_INVALID');
          if (payload.payload.length > 4194304)
            throw new Error('UPDATE_TOO_LARGE');
          if (
            Y.snapshotContainsUpdate(
              Y.snapshot(payload.document),
              payload.payload
            )
          )
            return;
          this.assertConnection(payload.connection, true);
          if (
            state.pendingBytes + payload.payload.length > 16 * 1024 * 1024 ||
            state.sources.size >= 1000
          ) {
            this.block(state, 'ROOM_BACKPRESSURE');
            throw new Error('ROOM_BACKPRESSURE');
          }
          await this.validateReferences(
            state,
            payload.connection,
            payload.payload
          );
          this.assertConnection(payload.connection, true);
          if (state.changes.length + state.reservedChangeBytes / maxChangeBytes >= 10000
            || state.changeBytes + state.reservedChangeBytes + maxChangeBytes + 2 > 16 * 1024 * 1024) {
            await this.flushDocument(payload.document);
            this.assertConnection(payload.connection, true);
          }
          if (state.changes.length + state.reservedChangeBytes / maxChangeBytes >= 10000
            || state.changeBytes + state.reservedChangeBytes + maxChangeBytes + 2 > 16 * 1024 * 1024)
            throw new Error('ROOM_BACKPRESSURE');
          // Reserve across the SDK's async beforeSync hook chain. Other connections
          // can pass their hooks before this frame reaches native Y.applyUpdate.
          payload.connection.context.changeReservation = maxChangeBytes;
          state.reservedChangeBytes += maxChangeBytes;
          const auth = payload.connection.context.auth;
          // Native Connection processes one complete frame at a time. Copy the grant
          // after the final await so renewal cannot rewrite this applied frame's source.
          payload.connection.context.verifiedUpdate = {
            connection_key: auth.connection_key,
            authorization_revision: auth.authorization_revision,
            authorized_at_ms: Date.now(),
          };
        } catch (error) {
          // PM schema failures can include document text. SDK logging receives
          // only a bounded code; never forward the original exception or body.
          throw new Error(this.code(error));
        }
      },
      afterHandleMessage: async (payload) => {
        this.releaseChangeReservation(this.state(payload.documentName), payload.context);
        delete payload.context.verifiedUpdate;
      },
      onStoreDocument: async (payload) => {
        try {
          await this.persist(
            this.state(payload.documentName),
            payload.document
          );
        } catch (error) {
          throw new Error(this.code(error));
        }
      },
      beforeUnloadDocument: async (payload) => {
        const state = this.state(payload.documentName);
        const fence = this.fences.get(state.auth.room_key);
        if (fence?.active && fence.releaseInput === undefined)
          throw new Error('BUSINESS_BARRIER_DOCUMENT_RETAINED');
        if (state.capture || state.dirty)
          throw new Error('UNCOMMITTED_DOCUMENT_RETAINED');
      },
      afterUnloadDocument: async (payload) => {
        this.states.delete(payload.documentName);
        const records = this.targetRecords.get(payload.documentName);
        if (records) {
          const allSettled = [...records.values()].every(
            (record) => record.phase === 'completed' || record.phase === 'canceled'
          );
          if (allSettled) {
            this.targetRecordUnloadedAt.set(payload.documentName, Date.now());
          }
        }
      },
      onStateless: async (payload) => {
        try {
          const state = this.assertConnection(payload.connection, false);
          const value: unknown = JSON.parse(payload.payload);
          if (!value || typeof value !== 'object' || Array.isArray(value))
            throw new Error('STATELESS_INVALID');
          const message = value as Record<string, unknown>;
          if (
            Object.keys(message).sort().join(',') !== 'requestId,type' ||
            !requestId(message.requestId)
          )
            throw new Error('STATELESS_INVALID');
          if (message.type === 'progress') {
            if (!state.stored) throw new Error('COMMITTED_BASELINE_MISSING');
            payload.connection.sendStateless(
              JSON.stringify({
                ...state.stored.proof,
                requestId: message.requestId,
              })
            );
            return;
          }
          if (message.type !== 'save_target')
            throw new Error('STATELESS_TYPE_INVALID');
          this.assertConnection(payload.connection, true);
          // MessageReceiver does not await onStateless. Fix the target synchronously
          // here, before new native updates can interleave with persistence awaits.
          const id = message.requestId;
          const auth = payload.connection.context.auth as ConnectionAuth;
          let record = state.requests.get(id);
          if (!record) {
            const now = Date.now();
            if (state.requests.size >= 1000) {
              for (const [reqKey, existingRecord] of state.requests) {
                if (
                  (existingRecord.phase === 'completed' || existingRecord.phase === 'canceled') &&
                  existingRecord.settledAt !== undefined &&
                  now - existingRecord.settledAt >= ledgerTtlMs
                ) {
                  state.requests.delete(reqKey);
                }
              }
            }
            let activeForMember = 0;
            for (const existingRecord of state.requests.values()) {
              if (
                existingRecord.actor.memberId === auth.member_id &&
                existingRecord.phase !== 'completed' &&
                existingRecord.phase !== 'canceled'
              ) {
                activeForMember += 1;
              }
            }
            if (state.requests.size >= 1000 || activeForMember >= 16)
              throw new Error('SAVE_TARGET_BACKPRESSURE');
            if (
              !this.targetRecords.has(payload.documentName) &&
              this.targetRecords.size >= 1000
            ) {
              this.pruneExpiredLedgers(now);
              if (this.targetRecords.size >= 1000) {
                this.evictOldestSettledTargetRecord();
              }
              if (this.targetRecords.size >= 1000)
                throw new Error('SAVE_TARGET_BACKPRESSURE');
            }
            record = {
              fixedClick: this.fixedContent(payload.document, state.progress),
              actor: {
                memberId: auth.member_id,
                tenantId: auth.binding.tenant_id,
                documentId: auth.document_id,
                epoch: auth.lease_epoch,
                roomKey: auth.room_key,
              },
              phase: 'preparing',
              prepared: false,
              confirmation: null,
              committed: null,
            };
            state.requests.set(id, record);
            this.targetRecords.set(payload.documentName, state.requests);
          }
          if (
            record.actor.memberId !== auth.member_id ||
            record.actor.tenantId !== auth.binding.tenant_id ||
            record.actor.documentId !== auth.document_id ||
            record.actor.epoch !== auth.lease_epoch ||
            record.actor.roomKey !== auth.room_key
          )
            throw new Error('SAVE_TARGET_ACTOR_OR_EPOCH_CHANGED');
          if (record.phase === 'canceled')
            throw new Error('DOCUMENT_TARGET_CANCELED');
          let pending = record.inFlight;
          if (!pending) {
            const original = record;
            // Completed targets still check the original registry tuple so a later
            // explicit cancellation cannot be hidden by this process's cache.
            pending = this.saveTarget(state, payload.connection, id, record);
            record.inFlight = pending;
            const attempt = pending;
            void pending.then(
              () => {
                if (original.inFlight === attempt) delete original.inFlight;
              },
              (error: unknown) => {
                original.phase =
                  this.code(error) === 'DOCUMENT_TARGET_CANCELED'
                    ? 'canceled'
                    : 'unconfirmed';
                if (original.inFlight === attempt) delete original.inFlight;
              }
            );
            this.track(pending);
          }
          void pending
            .then((proof) => {
              if (payload.document.hasConnection(payload.connection))
                payload.connection.sendStateless(
                  JSON.stringify({ ...proof, requestId: id })
                );
            })
            .catch((error: unknown) =>
              this.connectionError(payload.connection, this.code(error), id)
            );
        } catch (error) {
          this.connectionError(payload.connection, this.code(error));
        }
      },
      onRequest: async (payload) => {
        if (['/live', '/ready'].includes(payload.request.url ?? '') &&
          payload.request.method === 'GET') {
          const dependency = this.php.dependencyState();
          const ready = !this.stopping && dependency === 'available';
          payload.response.writeHead(
            payload.request.url === '/live' ? (this.stopping ? 503 : 200) : (ready ? 200 : 503),
            { 'Content-Type': 'application/json' }
          );
          payload.response.end(JSON.stringify({
            listening: !this.stopping, ready, backend: dependency, native_protocol: 2,
          }));
          throw undefined;
        }
        if ([controlPath, accessPath].includes(payload.request.url ?? '')) {
          await this.control.handle(payload.request, payload.response);
          throw undefined;
        }
        payload.response.writeHead(404, { 'Content-Type': 'application/json' });
        payload.response.end(
          JSON.stringify({ error: 'CONTROL_CONTRACT_NOT_CONFIGURED' })
        );
        throw undefined;
      },
    });
  }

  private code(error: unknown): string {
    const value = error instanceof Error ? error.message : '';
    return /^[A-Z][A-Z0-9_]{0,127}$/.test(value)
      ? value
      : 'COLLABORATION_UNCONFIRMED';
  }
  private state(name: string): State {
    const state = this.states.get(name);
    if (!state) throw new Error('ROOM_AUTHORIZATION_REQUIRED');
    return state;
  }
  private async authorize(
    ticket: string,
    name: string
  ): Promise<ConnectionAuth> {
    try {
      return await this.php.call<ConnectionAuth>('authorize', { token: ticket, document_name: name });
    } catch (error) {
      if (
        !(error instanceof PhpFailure) ||
        error.code !== 'RECOVERY_REQUIRED' ||
        error.status !== 409
      )
        throw new Error(this.code(error));
      const details = error.details;
      if (
        Object.keys(details).sort().join(',') !==
          'binding,error_code,lease_epoch,room_key' ||
        details.error_code !== 'RECOVERY_REQUIRED' ||
        !digest(details.room_key) ||
        !Number.isSafeInteger(details.lease_epoch) ||
        Number(details.lease_epoch) < 1 ||
        Number(details.lease_epoch) >= Number.MAX_SAFE_INTEGER
      )
        throw new Error('PHP_RECOVERY_DETAILS_INVALID');
      const binding = details.binding as Binding;
      assertBinding(binding, name, this.php.config.instanceId);
      if (
        this.stopping ||
        this.states.has(name) ||
        this.fences.get(details.room_key)?.active
      )
        throw new Error('ROOM_RECOVERY_REQUIRES_FRESH_PROCESS');
      const key = `${details.room_key}:${details.lease_epoch}`;
      let attempt = this.recoveries.get(key);
      if (!attempt) {
        if (this.recoveries.size >= 1000) {
          this.pruneExpiredLedgers(Date.now());
          if (this.recoveries.size >= 1000) {
            for (const [k, existing] of this.recoveries) {
              if (existing.result !== undefined && !existing.inFlight) {
                this.recoveries.delete(k);
                break;
              }
            }
          }
          if (this.recoveries.size >= 1000)
            throw new Error('RECOVERY_BACKPRESSURE');
        }
        attempt = {
          requestId: randomUUID(),
          roomKey: details.room_key,
          epoch: Number(details.lease_epoch),
          binding: structuredClone(binding),
        };
        this.recoveries.set(key, attempt);
      } else if (phpJson(attempt.binding) !== phpJson(binding))
        throw new Error('ORIGINAL_RECOVERY_BINDING_REQUIRED');
      let recovered = attempt.result;
      if (!recovered) {
        if (!attempt.inFlight) {
          const original = attempt;
          const pending = this.recoverRoom(original, name);
          original.inFlight = pending;
          void pending.then(
            (result) => {
              original.result = {
                room_key: result.room_key,
                lease_epoch: result.lease_epoch,
                binding: result.binding,
              };
              original.settledAt = Date.now();
              if (original.inFlight === pending) delete original.inFlight;
            },
            () => {
              if (original.inFlight === pending) delete original.inFlight;
            }
          );
          this.track(pending);
        }
        recovered = await attempt.inFlight;
      }
      if (!recovered) throw new Error('PHP_RECOVERY_RESULT_INVALID');
      if (this.stopping) throw new Error('SERVICE_DRAINING');
      // Exactly one reauthorization of the original, still-unconsumed ticket.
      // A further recovery requirement is returned to the caller, never looped.
      const auth = await this.php.call<ConnectionAuth>('authorize', { token: ticket, document_name: name });
      assertAuth(auth, name, this.php.config.instanceId, this.php.holderKey);
      if (
        auth.room_key !== recovered.room_key ||
        auth.lease_epoch !== recovered.lease_epoch ||
        phpJson(auth.binding) !== phpJson(recovered.binding)
      )
        throw new Error('PHP_RECOVERED_AUTH_INVALID');
      return auth;
    }
  }
  private async recoverRoom(
    attempt: RecoveryAttempt,
    name: string
  ): Promise<Recovery> {
    const result = await this.php.call<Recovery>('recovery', {
      protocol: 2,
      room_key: attempt.roomKey,
      lease_epoch: attempt.epoch,
      request_id: attempt.requestId,
    });
    assertBinding(result.binding, name, this.php.config.instanceId);
    const baseline = result.baseline;
    if (
      Object.keys(result).sort().join(',') !==
        'baseline,binding,holder_key,lease_epoch,protocol,request_id,room_key' ||
      result.protocol !== 2 ||
      result.request_id !== attempt.requestId ||
      result.room_key !== attempt.roomKey ||
      result.holder_key !== this.php.holderKey ||
      result.lease_epoch !== attempt.epoch + 1 ||
      phpJson(result.binding) !== phpJson(attempt.binding) ||
      !baseline ||
      Object.keys(baseline).sort().join(',') !==
        'binary_base64,document_id,initialization_required,lease_epoch,state_seq,state_sha256,status,store_id,structured_content' ||
      baseline.document_id !== attempt.binding.document_id ||
      baseline.lease_epoch !== result.lease_epoch ||
      baseline.status !== 'active' ||
      !Number.isSafeInteger(baseline.state_seq) ||
      baseline.state_seq < 0 ||
      !digest(baseline.state_sha256) ||
      typeof baseline.initialization_required !== 'boolean' ||
      baseline.initialization_required !== (baseline.binary_base64 === null) ||
      (baseline.store_id !== null &&
        (typeof baseline.store_id !== 'string' ||
          !baseline.store_id ||
          baseline.store_id.length > 200)) ||
      !baseline.structured_content ||
      Object.keys(baseline.structured_content).sort().join(',') !==
        'annotations,content,editorModel,schemaVersion' ||
      baseline.structured_content.schemaVersion !== 'peanut.richtext/1' ||
      baseline.structured_content.editorModel !== 'tiptap-prosemirror' ||
      !Array.isArray(baseline.structured_content.annotations) ||
      baseline.structured_content.annotations.length !== 0 ||
      !baseline.structured_content.content ||
      typeof baseline.structured_content.content !== 'object' ||
      Array.isArray(baseline.structured_content.content)
    )
      throw new Error('PHP_RECOVERY_RESULT_INVALID');
    if (
      baseline.binary_base64 !== null &&
      (typeof baseline.binary_base64 !== 'string' ||
        sha(binary(baseline.binary_base64)) !== baseline.state_sha256)
    )
      throw new Error('PHP_RECOVERY_BASELINE_INVALID');
    // The response is verified but never applied to an existing Y.Doc. Native
    // onLoadDocument remains the only path loading the current persisted baseline.
    return result;
  }
  private sameRoom(state: State, auth: ConnectionAuth): void {
    if (
      state.auth.room_key !== auth.room_key ||
      auth.holder_key !== this.php.holderKey ||
      state.auth.holder_key !== auth.holder_key ||
      state.auth.lease_epoch !== auth.lease_epoch ||
      phpJson(state.auth.binding) !== phpJson(auth.binding)
    )
      throw new Error('ROOM_BINDING_CHANGED');
  }
  private assertConnection(
    connection: Connection<Context>,
    write: boolean
  ): State {
    const state = this.state(connection.document.name);
    const auth = connection.context?.auth;
    if (!auth) throw new Error('CONNECTION_AUTHORIZATION_REQUIRED');
    this.assertGrantNotInvalidated(auth, connection);
    this.sameRoom(state, auth);
    if (
      !connection.document.hasConnection(connection) ||
      auth.lease_expires_at <= Date.now()
    ) {
      this.connectionError(connection, 'CONNECTION_AUTHORIZATION_EXPIRED');
      throw new Error('CONNECTION_AUTHORIZATION_EXPIRED');
    }
    if (
      write &&
      (this.stopping ||
        state.blocked ||
        this.fences.get(auth.room_key)?.active ||
        connection.readOnly ||
        auth.mode !== 'write')
    )
      throw new Error('DOCUMENT_READONLY');
    return state;
  }
  private connectionError(
    connection: Connection,
    code: string,
    id?: string
  ): void {
    connection.readOnly = true;
    connection.sendStateless(
      JSON.stringify({ type: 'error', code, ...(id ? { requestId: id } : {}) })
    );
    connection.close({ code: 1008, reason: code });
  }
  private block(state: State, code: string): void {
    state.blocked = true;
    if (state.blockedCode === undefined || state.blockedCode === 'STORE_UNCONFIRMED')
      state.blockedCode = code;
    state.document
      ?.getConnections()
      .forEach((connection) => this.connectionError(connection, code));
  }
  private track<T>(promise: Promise<T>): Promise<T> {
    this.actions.add(promise);
    void promise.finally(() => this.actions.delete(promise)).catch(() => {});
    return promise;
  }
  private renewalAt(auth: ConnectionAuth): number {
    return (
      Date.now() +
      Math.min(
        15000,
        Math.max(1000, Math.floor((auth.lease_expires_at - Date.now()) / 2))
      )
    );
  }
  private roster(state: State): void {
    const members = new Map<number, { memberId: number; nickname: string }>();
    const connections = state.document?.getConnections() ?? [];
    connections.forEach((connection: Connection<Context>) => {
      const auth = connection.context.auth;
      if (auth.lease_expires_at > Date.now())
        members.set(auth.member_id, {
          memberId: auth.member_id,
          nickname: auth.nickname,
        });
    });
    const payload = JSON.stringify({
      type: 'members',
      documentId: state.auth.document_id,
      roomEpoch: state.auth.lease_epoch,
      members: [...members.values()].sort((a, b) => a.memberId - b.memberId),
    });
    connections.forEach((connection) => connection.sendStateless(payload));
  }

  private async load(
    state: State,
    document: Document,
    context: Context
  ): Promise<void> {
    const auth = context.auth;
    const loaded = await this.php.call<Load>('load', {
      connection_key: auth.connection_key,
      room_key: auth.room_key,
      lease_epoch: auth.lease_epoch,
    });
    // An invalidation may arrive while PHP's load or the SDK's authentication is pending.
    this.assertGrantNotInvalidated(auth);
    assertBinding(loaded.binding, document.name, this.php.config.instanceId);
    if (
      phpJson(loaded.binding) !== phpJson(auth.binding) ||
      loaded.document_id !== auth.document_id ||
      loaded.lease_epoch !== auth.lease_epoch ||
      loaded.holder_key !== this.php.holderKey ||
      loaded.holder_key !== auth.holder_key ||
      !Number.isSafeInteger(loaded.state_seq) ||
      loaded.state_seq < 0 ||
      !digest(loaded.state_sha256) ||
      typeof loaded.initialization_required !== 'boolean' ||
      (loaded.store_id !== null &&
        (typeof loaded.store_id !== 'string' ||
          !loaded.store_id ||
          loaded.store_id.length > 200))
    )
      throw new Error('PHP_LOAD_INVALID');
    state.document = document;
    state.stateSeq = loaded.state_seq;
    state.baselineSha = loaded.state_sha256;
    document.gc = false;
    if (loaded.binary_base64 === null) {
      if (!loaded.initialization_required || auth.mode !== 'write')
        throw new Error('INITIALIZATION_WRITE_REQUIRED');
      const initial = initializeOnce(loaded.structured_content);
      try {
        Y.applyUpdate(document, Y.encodeStateAsUpdate(initial));
      } finally {
        initial.destroy();
      }
      state.progress = 1;
      state.dirty = true;
      state.sources.set(auth.connection_key, {
        participant: {
          connection_key: auth.connection_key,
          authorization_revision: auth.authorization_revision,
          authorized_at_ms: Date.now(),
        },
        progress: 1,
      });
      state.references = new Set(project(document).fileKeys);
      // The native room is not opened until its one-time CRDT initialization really commits.
      await document.saveMutex.runExclusive(() =>
        this.persist(state, document)
      );
      this.assertGrantNotInvalidated(auth);
      return;
    }
    if (
      loaded.initialization_required ||
      typeof loaded.binary_base64 !== 'string'
    )
      throw new Error('PHP_LOAD_INVALID');
    const bytes = binary(loaded.binary_base64);
    if (sha(bytes) !== loaded.state_sha256)
      throw new Error('PHP_LOAD_DIGEST_INVALID');
    Y.applyUpdate(document, bytes);
    const projection = project(document);
    if (projection.json !== phpJson(loaded.structured_content, true))
      throw new Error('PHP_LOAD_PROJECTION_INVALID');
    const snapshot = Y.snapshot(document);
    state.references = new Set(projection.fileKeys);
    state.stored = {
      hasReceipt: loaded.store_id !== null,
      proof: {
        type: 'committed',
        documentId: auth.document_id,
        roomEpoch: auth.lease_epoch,
        stateSeq: loaded.state_seq,
        captureId: loaded.store_id ?? `baseline:${loaded.state_sha256}`,
        binarySha256: loaded.state_sha256,
        snapshotBase64: Buffer.from(Y.encodeSnapshot(snapshot)).toString(
          'base64'
        ),
      },
      snapshot,
      tuple: null,
      bytes,
      value: projection.value,
      referenceDigest: projection.referenceDigest,
      progress: 0,
    };
  }

  private releaseChangeReservation(state: State, context: Context): void {
    if (context.changeReservation === undefined) return;
    state.reservedChangeBytes -= context.changeReservation;
    delete context.changeReservation;
  }

  private recordChange(state: State, document: Document, update: Uint8Array, origin: unknown): void {
    try {
      const suggestion = typeof origin === 'object' && origin !== null ? this.suggestionOrigins.get(origin) : undefined;
      if (suggestion) {
        if (!suggestion.active || this.fences.get(state.auth.room_key) !== suggestion || !suggestion.drained)
          throw new Error('SUGGESTION_BARRIER_INVALID');
        return;
      }
      const source = origin as { source?: string; connection?: Connection<Context> } | null;
      const connection = source?.source === 'connection' ? source.connection : undefined;
      if (!connection) throw new Error('UPDATE_HUMAN_SOURCE_REQUIRED');
      const context = connection.context;
      this.releaseChangeReservation(state, context);
      this.sameRoom(state, context.auth);
      const participant = context.verifiedUpdate;
      if (!participant || !state.lastProjection) throw new Error('UPDATE_VERIFIED_SOURCE_REQUIRED');
      const before = state.lastProjection;
      const after = project(document);
      const change: AcceptedChange = {
        ...participant,
        progress: ++state.progress,
        update_sha256: sha(update),
        before_projection_sha256: before.sha256,
        after_projection_sha256: after.sha256,
        before_text: plainText(before.value.content),
        after_text: plainText(after.value.content),
      };
      state.lastProjection = after;
      state.changes.push(change);
      state.changeBytes += Buffer.byteLength(JSON.stringify(change)) + 1;
      state.dirty = true;
      state.pendingBytes += update.length;
      state.sources.set(`${participant.connection_key}:${participant.authorization_revision}`, {
        participant: { ...participant }, progress: state.progress,
      });
      state.references = new Set(after.fileKeys);
    } catch (error) {
      console.warn('HOCUSPOCUS_RECORD_CHANGE_BLOCKED', {
        room: document.name,
        error: error instanceof Error ? error.message : String(error),
      });
      this.block(state, 'UPDATE_SOURCE_OR_PROJECTION_INVALID');
    }
  }

  private loadBusinessBaseline(state: State, document: Document, input: NonNullable<ControlInput['suggestion']>): void {
    assertBinding(input.binding, document.name, this.php.config.instanceId);
    const bytes = binary(input.baseline_binary_base64);
    if (sha(bytes) !== input.baseline_binary_sha256) throw new Error('SUGGESTION_BASELINE_INVALID');
    Y.applyUpdate(document, bytes);
    const projection = project(document);
    if (projection.sha256 !== input.baseline_projection_sha256
      || projection.json !== phpJson(input.baseline_structured_content, true)) throw new Error('SUGGESTION_BASELINE_INVALID');
    const snapshot = Y.snapshot(document);
    state.document = document;
    state.stateSeq = input.base_state_seq;
    state.baselineSha = input.baseline_binary_sha256;
    state.references = new Set(projection.fileKeys);
    state.stored = {
      hasReceipt: false,
      proof: { type: 'committed', documentId: input.binding.document_id, roomEpoch: state.auth.lease_epoch,
        stateSeq: input.base_state_seq, captureId: `baseline:${input.baseline_binary_sha256}`,
        binarySha256: input.baseline_binary_sha256, snapshotBase64: Buffer.from(Y.encodeSnapshot(snapshot)).toString('base64') },
      snapshot, tuple: null, bytes, value: projection.value, referenceDigest: projection.referenceDigest, progress: 0,
    };
  }

  private async validateReferences(
    state: State,
    connection: Connection<Context>,
    update: Uint8Array
  ): Promise<void> {
    const candidate = new Y.Doc({ gc: false });
    let keys: string[];
    let nextChangeBytes = 0;
    try {
      Y.applyUpdate(candidate, Y.encodeStateAsUpdate(connection.document));
      Y.applyUpdate(candidate, update);
      // Pending unresolved CRDT dependencies are not accepted as invisible future writes.
      if (candidate.store.pendingStructs || candidate.store.pendingDs)
        throw new Error('UPDATE_DEPENDENCIES_MISSING');
      const projection = project(candidate);
      const before = state.lastProjection ?? project(connection.document);
      const nextChange = { connection_key: '0'.repeat(64), authorization_revision: '0'.repeat(64), authorized_at_ms: Date.now(),
        progress: state.progress + 1, update_sha256: '0'.repeat(64), before_projection_sha256: before.sha256,
        after_projection_sha256: projection.sha256, before_text: plainText(before.value.content), after_text: plainText(projection.value.content) };
      nextChangeBytes = Buffer.byteLength(JSON.stringify(nextChange)) + 1;
      if (nextChangeBytes + 2 > 16 * 1024 * 1024) throw new Error('UPDATE_TOO_LARGE');
      keys = projection.fileKeys.filter(
        (key) => !state.references.has(key)
      );
    } finally {
      candidate.destroy();
    }
    if (state.changes.length >= 10000 || state.changeBytes + nextChangeBytes + 2 > 16 * 1024 * 1024) {
      await this.flushDocument(connection.document);
      if (state.dirty || state.capture || state.changes.length >= 10000
        || state.changeBytes + nextChangeBytes + 2 > 16 * 1024 * 1024) throw new Error('CONTROL_STORE_UNCONFIRMED');
    }
    if (!keys.length) return;
    const auth = connection.context.auth;
    const result = await this.php.call<{
      document_id: number;
      room_key: string;
      lease_epoch: number;
      file_keys: string[];
      authorization_revision: string;
      authorized_at_ms: number;
    }>('references', {
      connection_key: auth.connection_key,
      room_key: auth.room_key,
      lease_epoch: auth.lease_epoch,
      file_keys: keys,
      request_id: randomUUID(),
    });
    if (
      result.document_id !== auth.document_id ||
      result.room_key !== auth.room_key ||
      result.lease_epoch !== auth.lease_epoch ||
      phpJson(result.file_keys) !== phpJson(keys) ||
      result.authorization_revision !== auth.authorization_revision ||
      !Number.isSafeInteger(result.authorized_at_ms) ||
      result.authorized_at_ms > Date.now() ||
      result.authorized_at_ms < connection.context.checkedAt
    )
      throw new Error('REFERENCE_AUTHORIZATION_INVALID');
  }

  private fixedContent(document: Document, progress: number): FixedContent {
    const bytes = Y.encodeStateAsUpdate(document);
    if (bytes.length > 4194304) throw new Error('STATE_BINARY_TOO_LARGE');
    const fixed = new Y.Doc({ gc: false });
    try {
      Y.applyUpdate(fixed, bytes);
      if (fixed.store.pendingStructs || fixed.store.pendingDs)
        throw new Error('STATE_DEPENDENCIES_MISSING');
      return {
        bytes,
        snapshot: Y.snapshot(fixed),
        projection: project(fixed),
        progress,
      };
    } finally {
      fixed.destroy();
    }
  }

  private capture(state: State, document: Document): Capture {
    const bytes = Y.encodeStateAsUpdate(document);
    if (bytes.length > 4194304) throw new Error('STATE_BINARY_TOO_LARGE');
    const fixed = new Y.Doc({ gc: false });
    try {
      Y.applyUpdate(fixed, bytes);
      const projection = project(fixed);
      const tuple: StoreInput = {
        protocol: 2,
        ...state.auth.binding,
        room_key: state.auth.room_key,
        service_instance_id: this.php.config.serviceInstanceId,
        lease_epoch: state.auth.lease_epoch,
        base_state_seq: state.stateSeq,
        store_id: `store:${randomUUID()}`,
        baseline_sha256: state.baselineSha,
        binary_sha256: sha(bytes),
        projection_sha256: projection.sha256,
        reference_digest: projection.referenceDigest,
        projector_version: projectorVersion,
        binary_base64: Buffer.from(bytes).toString('base64'),
        structured_content: projection.value,
        participants: [...state.sources.values()].map((source) => ({
          ...source.participant,
        })),
        changes: structuredClone(state.changes),
        target_progress: state.progress,
      };
      const immutable = structuredClone(tuple);
      return {
        tuple: immutable,
        hash: sha(phpJson(immutable)),
        snapshot: Y.snapshot(fixed),
        attempted: false,
        failures: 0,
        pendingBytes: state.pendingBytes,
      };
    } finally {
      fixed.destroy();
    }
  }

  private validateReceipt(capture: Capture, receipt: Receipt): void {
    const tuple = capture.tuple;
    if (
      !receipt ||
      !Number.isSafeInteger(receipt.state_seq) ||
      receipt.document_id !== tuple.document_id ||
      receipt.store_id !== tuple.store_id ||
      receipt.lease_epoch !== tuple.lease_epoch ||
      receipt.state_seq !== tuple.base_state_seq + 1 ||
      receipt.binary_sha256 !== tuple.binary_sha256 ||
      receipt.projection_sha256 !== tuple.projection_sha256 ||
      receipt.target_progress !== tuple.target_progress ||
      typeof receipt.persisted_at !== 'string' ||
      !receipt.persisted_at ||
      !Number.isFinite(Date.parse(receipt.persisted_at))
    )
      throw new Error('PHP_COMMITTED_RECEIPT_INVALID');
  }
  private async receipt(capture: Capture): Promise<Receipt | null> {
    const result = await this.php.call<{ found: boolean; receipt?: Receipt }>(
      'receipt',
      {
        room_key: capture.tuple.room_key,
        lease_epoch: capture.tuple.lease_epoch,
        store_id: capture.tuple.store_id,
        capture_sha256: capture.hash,
      }
    );
    if (result.found === false && Object.keys(result).join(',') === 'found')
      return null;
    if (
      result.found !== true ||
      !result.receipt ||
      Object.keys(result).sort().join(',') !== 'found,receipt'
    )
      throw new Error('PHP_COMMITTED_RECEIPT_INVALID');
    this.validateReceipt(capture, result.receipt);
    return result.receipt;
  }
  private async persist(state: State, document: Document): Promise<Stored> {
    if (state.blockedCode === 'UPDATE_SOURCE_OR_PROJECTION_INVALID') throw new Error('UPDATE_SOURCE_OR_PROJECTION_INVALID');
    // The PHP business transaction owns this fixed signed capture. A pending SDK
    // autosave must never turn it into an ordinary connection-attributed store.
    if (this.fences.get(state.auth.room_key)?.applied) {
      if (!state.stored) throw new Error('SUGGESTION_BASELINE_INVALID');
      return state.stored;
    }
    if (
      !state.capture &&
      state.stored &&
      Y.snapshotContainsUpdate(
        state.stored.snapshot,
        Y.encodeStateAsUpdate(document)
      )
    ) {
      state.dirty = false;
      state.pendingBytes = 0;
      state.sources.clear();
      state.changes = [];
      state.changeBytes = 0;
      return state.stored;
    }
    // Fixed bytes, projection, participants and original base/store identity precede every await.
    const capture = (state.capture ??= this.capture(state, document));
    let receipt: Receipt | null = null;
    // An exhausted store budget may still observe its original durable receipt;
    // it never permits another store attempt or a replacement identity.
    if (capture.attempted && capture.failures >= 3) {
      try {
        receipt = await this.receipt(capture);
      } catch (error) {
        if (this.code(error) === 'PHP_COMMITTED_RECEIPT_INVALID')
          this.block(state, 'COMMITTED_RECEIPT_INVALID');
        throw error;
      }
    }
    while (!receipt && capture.failures < 3) {
      try {
        if (capture.attempted) receipt = await this.receipt(capture);
        if (!receipt) {
          capture.attempted = true;
          receipt = await this.php.call<Receipt>('store', capture.tuple);
          this.validateReceipt(capture, receipt);
        }
      } catch (error) {
        if (this.code(error) === 'PHP_COMMITTED_RECEIPT_INVALID') {
          this.block(state, 'COMMITTED_RECEIPT_INVALID');
          throw error;
        }
        capture.failures += 1;
        if (capture.failures >= 3) {
          this.block(state, 'STORE_UNCONFIRMED');
          throw new Error('STORE_UNCONFIRMED');
        }
        await new Promise<void>((resolve) =>
          setTimeout(resolve, 500 * 2 ** capture.failures)
        );
      }
    }
    if (!receipt) throw new Error('STORE_UNCONFIRMED');
    const tuple = capture.tuple;
    const stored: Stored = {
      hasReceipt: true,
      proof: {
        type: 'committed',
        documentId: tuple.document_id,
        roomEpoch: tuple.lease_epoch,
        stateSeq: receipt.state_seq,
        captureId: tuple.store_id,
        binarySha256: tuple.binary_sha256,
        snapshotBase64: Buffer.from(
          Y.encodeSnapshot(capture.snapshot)
        ).toString('base64'),
      },
      snapshot: capture.snapshot,
      tuple,
      bytes: binary(tuple.binary_base64),
      value: tuple.structured_content,
      referenceDigest: tuple.reference_digest,
      progress: tuple.target_progress,
    };
    state.stored = stored;
    state.stateSeq = receipt.state_seq;
    state.baselineSha = tuple.binary_sha256;
    state.capture = null;
    state.pendingBytes = Math.max(0, state.pendingBytes - capture.pendingBytes);
    state.sources.forEach((source, key) => {
      if (source.progress <= tuple.target_progress) state.sources.delete(key);
    });
    state.changes = state.changes.filter((change) => change.progress > tuple.target_progress);
    state.changeBytes = state.changes.reduce((size, change) => size + Buffer.byteLength(JSON.stringify(change)) + 1, 0);
    state.dirty = !Y.snapshotContainsUpdate(
      stored.snapshot,
      Y.encodeStateAsUpdate(document)
    );
    if (!state.dirty) state.pendingBytes = 0;
    document
      .getConnections()
      .forEach((connection) =>
        connection.sendStateless(JSON.stringify(stored.proof))
      );
    return stored;
  }

  private async saveTarget(
    state: State,
    connection: Connection<Context>,
    id: string,
    record: TargetRecord
  ): Promise<Committed> {
    const target = record.fixedClick;
    const document = connection.document;
    if (!record.confirmation)
      await document.saveMutex.runExclusive(async () => {
        this.assertConnection(connection, true);
        if (!record.prepared) {
          record.phase = 'preparing';
          const prepared = await this.php.call<{
            document_id: number;
            request_id: string;
            target_binary_sha256: string;
            target_projection_sha256: string;
            target_progress: number;
          }>('target/prepare', {
            protocol: 2,
            connection_key: connection.context.auth.connection_key,
            request_id: id,
            target_binary_sha256: sha(target.bytes),
            target_projection_sha256: target.projection.sha256,
            binary_base64: Buffer.from(target.bytes).toString('base64'),
            structured_content: target.projection.value,
            projector_version: projectorVersion,
            reference_digest: target.projection.referenceDigest,
            target_progress: target.progress,
          });
          if (
            Object.keys(prepared).sort().join(',') !==
              'document_id,request_id,target_binary_sha256,target_progress,target_projection_sha256' ||
            prepared.document_id !== state.auth.document_id ||
            prepared.request_id !== id ||
            prepared.target_binary_sha256 !== sha(target.bytes) ||
            prepared.target_projection_sha256 !== target.projection.sha256 ||
            prepared.target_progress !== target.progress
          )
            throw new Error('PHP_SAVE_TARGET_PREPARE_INVALID');
          record.prepared = true;
        }
        record.phase = 'saving';
        let current = state.stored;
        if (
          !current ||
          !Y.snapshotContainsUpdate(current.snapshot, target.bytes)
        ) {
          current = await this.persist(state, document);
          if (!Y.snapshotContainsUpdate(current.snapshot, target.bytes))
            current = await this.persist(state, document);
        }
        // Yjs 13.6.32 checks both struct clocks and deletion coverage for this full update.
        if (!Y.snapshotContainsUpdate(current.snapshot, target.bytes))
          throw new Error('SAVE_TARGET_NOT_COMMITTED');
        if (!current.hasReceipt) throw new Error('BASELINE_RECEIPT_REQUIRED');
        record.committed = { ...current.proof };
        // Select the actual committed capture once. A retry never substitutes a newer
        // store ID, progress, snapshot or click projection for this original tuple.
        record.confirmation = {
          protocol: 2,
          request_id: id,
          store_id: current.proof.captureId,
          state_seq: current.proof.stateSeq,
          committed_binary_sha256: current.proof.binarySha256,
          target_binary_sha256: sha(target.bytes),
          target_projection_sha256: target.projection.sha256,
          binary_base64: Buffer.from(target.bytes).toString('base64'),
          structured_content: target.projection.value,
          projector_version: projectorVersion,
          reference_digest: target.projection.referenceDigest,
          target_progress: target.progress,
          coverage: {
            algorithm: 'yjs.snapshotContainsUpdate/13.6.32',
            covers_target: true,
            committed_snapshot_base64: current.proof.snapshotBase64,
            target_snapshot_base64: Buffer.from(
              Y.encodeSnapshot(target.snapshot)
            ).toString('base64'),
          },
        };
      });
    this.assertConnection(connection, true);
    const committed = record.committed;
    if (!committed || !record.confirmation)
      throw new Error('SAVE_TARGET_CAPTURE_UNCONFIRMED');
    record.phase = 'confirming';
    const result = await this.php.call<{
      document_id: number;
      request_id: string;
      store_id: string;
      state_seq: number;
      committed_binary_sha256: string;
      target_binary_sha256: string;
      target_projection_sha256: string;
      target_progress: number;
    }>('target', {
      ...record.confirmation,
      connection_key: connection.context.auth.connection_key,
    });
    if (
      result.document_id !== state.auth.document_id ||
      result.request_id !== id ||
      result.store_id !== committed.captureId ||
      result.state_seq !== committed.stateSeq ||
      result.committed_binary_sha256 !== committed.binarySha256 ||
      result.target_binary_sha256 !== sha(target.bytes) ||
      result.target_projection_sha256 !== target.projection.sha256 ||
      result.target_progress !== target.progress
    )
      throw new Error('PHP_SAVE_TARGET_INVALID');
    record.phase = 'completed';
    record.settledAt = Date.now();
    return committed;
  }

  startMaintenance(): void {
    if (this.maintenance) throw new Error('MAINTENANCE_ALREADY_STARTED');
    this.maintenance = setInterval(() => {
      const now = Date.now();
      this.pruneExpiredLedgers(now);
      this.states.forEach((state) => {
        state.document
          ?.getConnections()
          .forEach((connection: Connection<Context>) => {
            const context = connection.context;
            if (context.auth.lease_expires_at <= now) {
              this.connectionError(
                connection,
                'CONNECTION_AUTHORIZATION_EXPIRED'
              );
              return;
            }
            if (!context.renewing && context.nextRenewal <= now) {
              context.renewing = true;
              this.track(this.renew(state, connection)).catch((error) => {
                console.error(
                  '[peanut-hocuspocus] connection renewal failed:',
                  error instanceof Error ? error.message : String(error)
                );
              });
            }
          });
      });
    }, 1000);
    this.maintenance.unref();
  }

  pruneExpiredLedgers(now: number = Date.now()): void {
    for (const [docName, unloadedAt] of this.targetRecordUnloadedAt) {
      if (this.states.has(docName)) {
        this.targetRecordUnloadedAt.delete(docName);
        continue;
      }
      if (now - unloadedAt >= ledgerTtlMs) {
        this.targetRecords.delete(docName);
        this.targetRecordUnloadedAt.delete(docName);
      }
    }
    for (const [key, attempt] of this.recoveries) {
      if (
        attempt.result !== undefined &&
        !attempt.inFlight &&
        attempt.settledAt !== undefined &&
        now - attempt.settledAt >= ledgerTtlMs
      ) {
        this.recoveries.delete(key);
      }
    }
    for (const [key, fence] of this.controlOperations) {
      if (
        !fence.active &&
        fence.releasedAt !== undefined &&
        now - fence.releasedAt >= ledgerTtlMs
      ) {
        this.controlOperations.delete(key);
      }
    }
    for (const [tuple, recordedAt] of this.invalidatedGrants) {
      if (now - recordedAt >= ledgerTtlMs) {
        this.invalidatedGrants.delete(tuple);
      }
    }
    for (const [factId, recordedAt] of this.invalidationFactTimes) {
      if (now - recordedAt >= ledgerTtlMs) {
        this.invalidationFacts.delete(factId);
        this.invalidationFactTimes.delete(factId);
      }
    }
    for (const [opId, recordedAt] of this.invalidationOperationTimes) {
      if (now - recordedAt >= ledgerTtlMs) {
        this.invalidationOperations.delete(opId);
        this.invalidationOperationTimes.delete(opId);
      }
    }
    for (const [resultKey, recordedAt] of this.invalidationResultTimes) {
      if (now - recordedAt >= ledgerTtlMs) {
        this.invalidationResults.delete(resultKey);
        this.invalidationResultTimes.delete(resultKey);
      }
    }
  }

  private evictOldestSettledTargetRecord(): void {
    for (const [docName] of this.targetRecordUnloadedAt) {
      if (!this.states.has(docName)) {
        this.targetRecords.delete(docName);
        this.targetRecordUnloadedAt.delete(docName);
        return;
      }
    }
  }
  private async renew(
    state: State,
    connection: Connection<Context>
  ): Promise<void> {
    const context = connection.context;
    const original = context.auth;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      this.assertGrantNotInvalidated(original);
      const checked = await Promise.race([
        this.php.call<ConnectionAuth>('check', {
          connection_key: original.connection_key,
          // Presence renewal is not new write admission. PHP still revalidates
          // edit permission for the original write connection; frames use the
          // native fence and their own write check, including during drain.
          mode: 'read',
        }),
        new Promise<never>((_, reject) => {
          deadline = setTimeout(
            () => reject(new Error('AUTHORIZATION_RENEWAL_TIMEOUT')),
            15000
          );
          deadline.unref();
        }),
      ]);
      assertAuth(
        checked,
        connection.document.name,
        this.php.config.instanceId,
        this.php.holderKey
      );
      this.sameRoom(state, checked);
      if (
        checked.connection_key !== original.connection_key ||
        checked.member_id !== original.member_id ||
        checked.mode !== original.mode
      )
        throw new Error('CONNECTION_AUTHORIZATION_CHANGED');
      if (!connection.document.hasConnection(connection)) return;
      this.assertGrantNotInvalidated(original);
      this.assertGrantNotInvalidated(checked);
      context.auth = checked;
      context.checkedAt = Date.now();
      context.nextRenewal = this.renewalAt(checked);
      this.roster(state);
    } catch (error) {
      this.connectionError(connection, this.code(error));
    } finally {
      if (deadline) clearTimeout(deadline);
      context.renewing = false;
    }
  }

  private invalidationTuple(
    tenantId: number,
    target: AccessInvalidationTarget
  ): string {
    return invalidationJson([
      this.php.config.instanceId, this.php.config.serviceInstanceId, this.php.holderKey,
      tenantId, target.member_id, target.connection_key, target.room_key,
      target.lease_epoch, target.authorization_revision,
    ]);
  }

  private assertGrantNotInvalidated(auth: ConnectionAuth, connection?: Connection<Context>): void {
    if (this.invalidatedGrants.has(this.invalidationTuple(auth.binding.tenant_id, auth))) {
      if (connection) {
        connection.readOnly = true;
        connection.close({ code: 1008, reason: 'CONNECTION_ACCESS_INVALIDATED' });
      }
      throw new Error('CONNECTION_ACCESS_INVALIDATED');
    }
  }

  /** Synchronous mark/close critical section: no await can interleave grant renewal or ACK publication. */
  private invalidateAccess(input: AccessInvalidationInput): AccessInvalidationAck {
    // Control owns the authenticated HTTP envelope and holder validation before this callback.
    const fact = invalidationJson([
      input.tenant_id, input.subject, input.fact_sha256,
      input.negative_action, input.revision_reference_sha256,
    ]);
    const previousFact = this.invalidationFacts.get(input.fact_id);
    if (previousFact !== undefined && previousFact !== fact) {
      throw new Error('ACCESS_INVALIDATION_FACT_CHANGED');
    }
    const operation = this.invalidationOperations.get(input.operation_id);
    if (operation !== undefined && operation !== input.scope_digest) {
      throw new Error('ACCESS_INVALIDATION_OPERATION_CHANGED');
    }
    const resultKey = invalidationJson([
      input.fact_id, input.service_instance_id, input.holder_key, input.scope_digest,
    ]);
    const previous = this.invalidationResults.get(resultKey);
    if (previous) return { ...previous };

    const now = Date.now();
    this.pruneExpiredLedgers(now);
    const targets = new Map(input.targets.map((target) => [target.connection_key, target]));
    const tuples = input.targets.map((target) => this.invalidationTuple(input.tenant_id, target));
    const additions = tuples.filter((tuple) => !this.invalidatedGrants.has(tuple));
    // Reserve all marker/result/identity slots before changing any connection.
    if (this.invalidatedGrants.size + additions.length > 100000
      || (previousFact === undefined && this.invalidationFacts.size >= 10000)
      || (operation === undefined && this.invalidationOperations.size >= 10000)
      || this.invalidationResults.size >= 10000) {
      throw new Error('ACCESS_INVALIDATION_BACKPRESSURE');
    }
    const matches = new Map<string, Connection<Context>[]>();
    this.states.forEach((state) => {
      state.document?.getConnections().forEach((connection: Connection<Context>) => {
        const auth = connection.context?.auth;
        if (!auth) throw new Error('ACCESS_INVALIDATION_CONNECTION_UNKNOWN');
        const target = targets.get(auth.connection_key);
        if (!target) return;
        if (auth.binding.instance_id !== input.instance_id
          || auth.holder_key !== input.holder_key
          || auth.binding.tenant_id !== input.tenant_id
          || auth.member_id !== input.subject.member_id) {
          throw new Error('ACCESS_INVALIDATION_CONNECTION_SCOPE_INVALID');
        }
        if (auth.room_key !== target.room_key || auth.lease_epoch !== target.lease_epoch
          || auth.authorization_revision !== target.authorization_revision) return;
        const connections = matches.get(auth.connection_key) ?? [];
        connections.push(connection);
        matches.set(auth.connection_key, connections);
      });
    });

    this.invalidationFacts.set(input.fact_id, fact);
    this.invalidationFactTimes.set(input.fact_id, now);
    this.invalidationOperations.set(input.operation_id, input.scope_digest);
    this.invalidationOperationTimes.set(input.operation_id, now);
    tuples.forEach((tuple) => this.invalidatedGrants.set(tuple, now));
    // Fence the whole batch before close callbacks, including duplicate SDK instances of one tuple.
    matches.forEach((connections) => connections.forEach((connection) => { connection.readOnly = true; }));
    let failed = false;
    matches.forEach((connections) => connections.forEach((connection) => {
      try {
        connection.close({ code: 1008, reason: 'CONNECTION_ACCESS_INVALIDATED' });
        if (connection.document.hasConnection(connection)) failed = true;
      } catch {
        failed = true;
      }
    }));
    if (failed) throw new Error('ACCESS_INVALIDATION_CLOSE_UNCONFIRMED');
    const ack: AccessInvalidationAck = {
      protocol_version: 1,
      fact_id: input.fact_id,
      fact_sha256: input.fact_sha256,
      operation_id: input.operation_id,
      instance_id: input.instance_id,
      service_instance_id: input.service_instance_id,
      holder_key: input.holder_key,
      scope_digest: input.scope_digest,
      outcome: matches.size ? 'applied' : 'no_matching_connection',
      processed_at: new Date().toISOString(),
      matched_count: matches.size,
      closed_count: matches.size,
    };
    this.invalidationResults.set(resultKey, ack);
    this.invalidationResultTimes.set(resultKey, now);
    return { ...ack };
  }

  private async flushDocument(document: Document): Promise<void> {
    const context = document.getConnections()[0]?.context as
      | Context
      | undefined;
    await this.server.hocuspocus.storeDocumentHooks(
      document,
      {
        clientsCount: document.getConnectionsCount(),
        document,
        documentName: document.name,
        instance: this.server.hocuspocus,
        lastContext: context ?? {
          auth: this.state(document.name).auth,
          checkedAt: Date.now(),
          nextRenewal: 0,
          renewing: false,
        },
        lastTransactionOrigin: undefined,
      },
      true
    );
  }

  private async controlRoom(
    input: ControlInput
  ): Promise<Record<string, unknown>> {
    if (input.holder_key !== this.php.holderKey)
      throw new Error('CONTROL_HOLDER_CHANGED');
    const identity = `${input.room_key}:${input.operation_id}`;
    const existing = this.controlOperations.get(identity);
    if (input.action === 'drain') {
      if (existing) {
        if (phpJson(existing.input) !== phpJson(input))
          throw new Error('ORIGINAL_CONTROL_OPERATION_REQUIRED');
        return this.drainControl(existing);
      }
      if (this.controlOperations.size >= 1000) {
        this.pruneExpiredLedgers(Date.now());
        if (this.controlOperations.size >= 1000) {
          for (const [opKey, oldFence] of this.controlOperations) {
            if (!oldFence.active) {
              this.controlOperations.delete(opKey);
              break;
            }
          }
        }
      }
      if (
        this.stopping ||
        this.controlOperations.size >= 1000 ||
        this.fences.has(input.room_key)
      )
        throw new Error('CONTROL_UNAVAILABLE');
      const fence: Fence = {
        input: structuredClone(input),
        active: true,
      };
      this.fences.set(input.room_key, fence);
      this.controlOperations.set(identity, fence);
      return this.drainControl(fence);
    }
    if (
      !existing ||
      existing.input.operation_id !== input.operation_id ||
      existing.input.input_sha256 !== input.input_sha256 ||
      existing.input.holder_key !== input.holder_key ||
      existing.input.lease_epoch !== input.lease_epoch
    )
      throw new Error('ORIGINAL_CONTROL_OPERATION_REQUIRED');
    if (input.action === 'suggestion.accept') {
      const fixed = phpJson(input);
      if (!existing.active || existing.releaseInput !== undefined
        || existing.suggestionInput !== undefined && existing.suggestionInput !== fixed)
        throw new Error('ORIGINAL_SUGGESTION_REQUIRED');
      if (existing.applied) return existing.applied;
      if (existing.suggestion) return existing.suggestion;
      existing.suggestionInput = fixed;
      const pending = this.applySuggestion(existing, input);
      existing.suggestion = pending;
      return pending;
    }
    const releaseInput = phpJson(input);
    if (
      existing.releaseInput !== undefined &&
      existing.releaseInput !== releaseInput
    )
      throw new Error('ORIGINAL_CONTROL_RELEASE_REQUIRED');
    if (existing.released) return existing.released;
    if (existing.release) {
      if (existing.releaseInput !== releaseInput)
        throw new Error('ORIGINAL_CONTROL_RELEASE_REQUIRED');
      return existing.release;
    }
    existing.releaseInput = releaseInput;
    const pending = this.releaseRoom(existing, input);
    existing.release = pending;
    void pending.then(
      (result) => {
        existing.released = result;
        if (existing.release === pending) delete existing.release;
      },
      (error: unknown) => {
        // Only a confirmed drain with an incomplete native unload can resume.
        if (
          this.code(error) === 'CONTROL_UNLOAD_UNCONFIRMED' &&
          existing.drained &&
          existing.release === pending
        )
          delete existing.release;
      }
    );
    return pending;
  }
  private async applySuggestion(fence: Fence, input: ControlInput): Promise<Record<string, unknown>> {
    if (!fence.drained && fence.drain) await fence.drain;
    if (!fence.drained || !input.suggestion) throw new Error('CONTROL_DRAIN_UNCONFIRMED');
    const s = input.suggestion;
    let candidate: ReturnType<typeof suggestionNode>;
    try {
      candidate = suggestionNode(s.structured_content);
    } catch {
      // This code is reserved for a confirmed pre-mutation rejection. The PHP
      // caller may close the unchanged business operation only after signed ACK.
      throw new Error('SUGGESTION_CONTENT_INVALID');
    }
    const name = roomName(s.binding);
    assertBinding(s.binding, name, this.php.config.instanceId);
    let state = this.states.get(name);
    if (!state) {
      if (fence.drained.phase !== 'offline') throw new Error('CONTROL_LOAD_UNCONFIRMED');
      // This is a signed business load of real persisted CRDT bytes, never a
      // fabricated connection grant or JSON-to-Y.Doc reconstruction.
      const auth: ConnectionAuth = {
        protocol: 2, binding: s.binding, document_id: s.binding.document_id,
        room_key: input.room_key, holder_key: input.holder_key, lease_epoch: input.lease_epoch,
        connection_key: '', authorization_revision: '', mode: 'read', member_id: s.actor_member_id,
        nickname: '', lease_expires_at: 0,
      };
      state = { auth, document: null, stateSeq: s.base_state_seq, baselineSha: s.baseline_binary_sha256,
        progress: 0, stored: null, capture: null, sources: new Map(), references: new Set(), requests: new Map(),
        blocked: false, dirty: false, pendingBytes: 0, changes: [], changeBytes: 0, reservedChangeBytes: 0 };
      this.states.set(name, state);
      this.businessLoads.set(name, s);
      try {
        await this.server.hocuspocus.createDocument(name, new Request('http://internal.invalid/'),
          `business:${input.operation_id}`, { readOnly: true, isAuthenticated: true },
          { auth, checkedAt: Date.now(), nextRenewal: 0, renewing: false });
      } finally {
        this.businessLoads.delete(name);
      }
    }
    const document = state.document;
    if (!document || document.isDestroyed || state.blocked || state.dirty || state.capture
      || state.auth.holder_key !== input.holder_key || state.auth.lease_epoch !== input.lease_epoch
      || phpJson(state.auth.binding) !== phpJson(s.binding) || state.stateSeq !== s.base_state_seq
      || state.baselineSha !== s.baseline_binary_sha256
      || sha(Y.encodeStateAsUpdate(document)) !== s.baseline_binary_sha256
      || project(document).sha256 !== s.baseline_projection_sha256) throw new Error('SUGGESTION_BASELINE_CONFLICT');
    return document.saveMutex.runExclusive(() => {
      const origin = { source: 'local' as const, skipStoreHooks: true };
      this.suggestionOrigins.set(origin, fence);
      applySuggestionNode(document, candidate, origin);
      const projection = project(document);
      const bytes = Y.encodeStateAsUpdate(document);
      if (bytes.length > 4194304 || projection.json !== phpJson(s.structured_content, true)) {
        this.block(state, 'SUGGESTION_CAPTURE_INVALID');
        throw new Error('SUGGESTION_CAPTURE_INVALID');
      }
      const applied: Record<string, unknown> = {
        ...fence.drained, phase: 'suggestion_applied', base_state_seq: s.base_state_seq,
        baseline_binary_sha256: s.baseline_binary_sha256, baseline_projection_sha256: s.baseline_projection_sha256,
        actor_member_id: s.actor_member_id,
        binary_base64: Buffer.from(bytes).toString('base64'), binary_sha256: sha(bytes),
        structured_content: projection.value, projection_sha256: projection.sha256,
        reference_digest: projection.referenceDigest, snapshot_base64: Buffer.from(Y.encodeSnapshot(Y.snapshot(document))).toString('base64'),
      };
      fence.applied = applied;
      return applied;
    });
  }

  private drainControl(fence: Fence): Promise<Record<string, unknown>> {
    if (fence.drained) return Promise.resolve(fence.drained);
    if (fence.drain) return fence.drain;
    const pending = Promise.resolve().then(() => this.drainRoom(fence.input));
    fence.drain = pending;
    void pending.then(
      (result) => {
        fence.drained = result;
        if (fence.drain === pending) delete fence.drain;
      },
      (error: unknown) => {
        const state = [...this.states.values()].find(
          (value) => value.auth.room_key === fence.input.room_key
        );
        // Native store hooks swallow failures. Distinguish exhausted/unknown
        // storage from an invalid receipt or invalid accepted state before retry.
        if (
          this.code(error) === 'CONTROL_STORE_UNCONFIRMED' &&
          state &&
          state.auth.holder_key === fence.input.holder_key &&
          state.auth.lease_epoch === fence.input.lease_epoch &&
          (!state.blocked || state.blockedCode === 'STORE_UNCONFIRMED') &&
          fence.drain === pending
        )
          delete fence.drain;
      }
    );
    return pending;
  }
  private async drainRoom(
    input: ControlInput
  ): Promise<Record<string, unknown>> {
    const state = [...this.states.values()].find(
      (value) => value.auth.room_key === input.room_key
    );
    const identity = {
      protocol: 2,
      operation_id: input.operation_id,
      input_sha256: input.input_sha256,
      room_key: input.room_key,
      holder_key: input.holder_key,
      lease_epoch: input.lease_epoch,
    };
    if (!state) return { ...identity, phase: 'offline' };
    if (state.auth.holder_key !== input.holder_key)
      throw new Error('CONTROL_HOLDER_CHANGED');
    if (state.auth.lease_epoch !== input.lease_epoch)
      throw new Error('CONTROL_EPOCH_CHANGED');
    let document = state.document;
    if (document?.isLoading)
      await this.server.hocuspocus.loadingDocuments.get(document.name);
    document = state.document;
    if (!document || document.isLoading || document.isDestroyed)
      throw new Error('CONTROL_LOAD_UNCONFIRMED');
    const connections = document.getConnections();
    connections.forEach((connection) => {
      connection.readOnly = true;
    });
    // Public SDK join includes awaited beforeSync, Y.applyUpdate and afterHandleMessage.
    // The room fence was installed before this join; later frames cannot add writes.
    await Promise.all(
      connections.map((connection) => connection.waitForPendingMessages())
    );
    await this.flushDocument(document);
    if (state.dirty || state.capture) await this.flushDocument(document);
    const stored = state.stored;
    if (
      !stored ||
      !stored.hasReceipt ||
      state.capture ||
      state.dirty ||
      !Y.snapshotContainsUpdate(
        stored.snapshot,
        Y.encodeStateAsUpdate(document)
      )
    )
      throw new Error('CONTROL_STORE_UNCONFIRMED');
    return {
      ...identity,
      phase: 'drained',
      target_progress: state.progress,
      store_id: stored.proof.captureId,
      state_seq: stored.proof.stateSeq,
      binary_sha256: stored.proof.binarySha256,
      snapshot_base64: stored.proof.snapshotBase64,
    };
  }
  private async releaseRoom(
    fence: Fence,
    input: ControlInput
  ): Promise<Record<string, unknown>> {
    if (!fence.drained) {
      if (!fence.drain) throw new Error('CONTROL_DRAIN_UNCONFIRMED');
      await fence.drain;
    }
    const state = [...this.states.values()].find(
      (value) => value.auth.room_key === input.room_key
    );
    const document = state?.document;
    if (state && state.auth.holder_key !== input.holder_key)
      throw new Error('CONTROL_HOLDER_CHANGED');
    if (state && state.auth.lease_epoch !== input.lease_epoch)
      throw new Error('CONTROL_EPOCH_CHANGED');
    if (state && (state.capture || state.dirty))
      throw new Error('CONTROL_RELEASE_STATE_UNCONFIRMED');
    if (document) {
      document
        .getConnections()
        .forEach((connection) =>
          this.connectionError(connection, 'ROOM_EPOCH_CHANGED')
        );
      if (!fence.applied) await this.flushDocument(document);
      await this.server.hocuspocus.unloadDocument(document);
      if (this.server.hocuspocus.documents.has(document.name))
        throw new Error('CONTROL_UNLOAD_UNCONFIRMED');
    }
    fence.active = false;
    fence.releasedAt = Date.now();
    if (this.fences.get(input.room_key) === fence)
      this.fences.delete(input.room_key);
    return {
      protocol: 2,
      operation_id: input.operation_id,
      input_sha256: input.input_sha256,
      room_key: input.room_key,
      holder_key: input.holder_key,
      lease_epoch: input.lease_epoch,
      result_epoch: input.result_epoch,
      status: input.status,
      phase: 'released',
    };
  }

  async drain(): Promise<void> {
    this.stopping = true;
    if (this.maintenance) clearInterval(this.maintenance);
    this.maintenance = undefined;
    await Promise.allSettled([
      ...this.server.hocuspocus.loadingDocuments.values(),
    ]);
    const documents = [...this.server.hocuspocus.documents.values()];
    documents.forEach((document) =>
      document.getConnections().forEach((connection) => {
        connection.readOnly = true;
      })
    );
    await Promise.all(
      documents.flatMap((document) =>
        document
          .getConnections()
          .map((connection) => connection.waitForPendingMessages())
      )
    );
    await Promise.allSettled([...this.actions]);
    this.server.hocuspocus.flushPendingStores();
    await Promise.all(
      documents.map((document) =>
        document.saveMutex.runExclusive(() =>
          this.persist(this.state(document.name), document)
        )
      )
    );
    if ([...this.states.values()].some((state) => state.capture || state.dirty))
      throw new Error('DRAIN_UNCONFIRMED');
    await this.server.destroy();
  }
}
