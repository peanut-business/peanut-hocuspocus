import { randomUUID } from 'node:crypto';
import { Server, type Connection, type Document } from '@hocuspocus/server';
import * as Y from 'yjs';
import {
  assertAuth,
  assertBinding,
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
} from './contracts.js';
import { PhpClient, PhpFailure } from './php-client.js';
import { phpJson, sha } from './digests.js';
import {
  initializeOnce,
  project,
  projectorVersion,
  type Structured,
} from './projector.js';
import {
  Control,
  controlPath,
  operatorPath,
  type ControlInput,
  type OperatorInput,
} from './business-control.js';

interface Context {
  auth: ConnectionAuth;
  checkedAt: number;
  nextRenewal: number;
  renewing: boolean;
  verifiedUpdate?: Participant;
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
}
interface Fence {
  input: ControlInput;
  drain?: Promise<Record<string, unknown>>;
  drained?: Record<string, unknown>;
  active: boolean;
  releaseInput?: string;
  release?: Promise<Record<string, unknown>>;
  released?: Record<string, unknown>;
}
interface RecoveryAttempt {
  requestId: string;
  roomKey: string;
  epoch: number;
  binding: Binding;
  result?: Pick<Recovery, 'room_key' | 'lease_epoch' | 'binding'>;
  inFlight?: Promise<Recovery>;
}

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
  private maintenance: ReturnType<typeof setInterval> | undefined;
  private stopping = false;
  private admissionOpen = false;
  private readonly probeId = randomUUID();
  private activationId: string | null = null;
  private processLeaseExpires = 0;
  private lifecycleState: Record<string, unknown> | undefined;
  private lifecycleTimer: ReturnType<typeof setInterval> | undefined;
  private lifecyclePending: Promise<Record<string, unknown>> | undefined;
  private instanceDrain:
    | {
        input: OperatorInput;
        pending?: Promise<Record<string, unknown>>;
        result?: Record<string, unknown>;
      }
    | undefined;

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
      (input) => this.operator(input)
    );
    this.server = new Server<Context>({
      address,
      port,
      stopOnSignals: false,
      quiet: true,
      debounce: 2000,
      maxDebounce: 10000,
      timeout: 30000,
      unloadImmediately: false,
      yDocOptions: { gc: false, gcFilter: () => true },
      websocketOptions: { maxPayload: 4200000 },
      onConnect: async () => {
        if (
          this.stopping ||
          !this.admissionOpen ||
          this.processLeaseExpires <= Date.now()
        )
          throw new Error('SERVICE_NOT_ACTIVE');
      },
      onAuthenticate: async (payload) => {
        if (
          this.stopping ||
          !this.admissionOpen ||
          this.processLeaseExpires <= Date.now() ||
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
        if (
          this.stopping ||
          !this.admissionOpen ||
          this.processLeaseExpires <= Date.now()
        )
          throw new Error('SERVICE_NOT_ACTIVE');
        assertAuth(
          auth,
          payload.documentName,
          php.config.instanceId,
          php.holderKey
        );
        if (this.fences.get(auth.room_key)?.active)
          throw new Error('DOCUMENT_BARRIER');
        let state = this.states.get(payload.documentName);
        if (!state) {
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
          await this.load(state, payload.document, payload.context);
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
        delete payload.context.verifiedUpdate;
      },
      onChange: async (payload) => {
        const state = this.state(payload.documentName);
        try {
          if (!payload.connection)
            throw new Error('UPDATE_HUMAN_SOURCE_REQUIRED');
          const context = payload.connection.context as Context;
          this.sameRoom(state, context.auth);
          const participant = context.verifiedUpdate;
          if (!participant) throw new Error('UPDATE_VERIFIED_SOURCE_REQUIRED');
          state.progress += 1;
          state.dirty = true;
          state.pendingBytes += payload.update.length;
          state.sources.set(
            `${participant.connection_key}:${participant.authorization_revision}`,
            {
              participant: { ...participant },
              progress: state.progress,
            }
          );
          state.references = new Set(project(payload.document).fileKeys);
        } catch {
          this.block(state, 'UPDATE_SOURCE_OR_PROJECTION_INVALID');
        }
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
        if (state.capture || state.dirty)
          throw new Error('UNCOMMITTED_DOCUMENT_RETAINED');
      },
      afterUnloadDocument: async (payload) => {
        this.states.delete(payload.documentName);
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
            if (
              state.requests.size >= 1000 ||
              [...state.requests.values()].filter(
                (value) =>
                  value.actor.memberId === auth.member_id &&
                  value.phase !== 'completed' &&
                  value.phase !== 'canceled'
              ).length >= 16
            )
              throw new Error('SAVE_TARGET_BACKPRESSURE');
            if (
              !this.targetRecords.has(payload.documentName) &&
              this.targetRecords.size >= 1000
            )
              throw new Error('SAVE_TARGET_BACKPRESSURE');
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
        if (
          payload.request.url === '/ready' &&
          payload.request.method === 'GET'
        ) {
          const ready =
            this.lifecycleState?.php_ready === true &&
            this.processLeaseExpires > Date.now() &&
            !this.stopping;
          payload.response.writeHead(ready ? 200 : 503, {
            'Content-Type': 'application/json',
          });
          payload.response.end(
            JSON.stringify({
              ready,
              admission_enabled: ready && this.admissionOpen,
              native_protocol: 2,
            })
          );
          throw undefined;
        }
        if ([controlPath, operatorPath].includes(payload.request.url ?? '')) {
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
      return await this.php.call<ConnectionAuth>('authorize', { ticket });
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
        if (this.recoveries.size >= 1000)
          throw new Error('RECOVERY_BACKPRESSURE');
        attempt = {
          requestId: randomUUID(),
          roomKey: details.room_key,
          epoch: Number(details.lease_epoch),
          binding: JSON.parse(JSON.stringify(binding)) as Binding,
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
      const auth = await this.php.call<ConnectionAuth>('authorize', { ticket });
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
        !this.admissionOpen ||
        this.processLeaseExpires <= Date.now() ||
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

  private async validateReferences(
    state: State,
    connection: Connection<Context>,
    update: Uint8Array
  ): Promise<void> {
    const candidate = new Y.Doc({ gc: false });
    let keys: string[];
    try {
      Y.applyUpdate(candidate, Y.encodeStateAsUpdate(connection.document));
      Y.applyUpdate(candidate, update);
      // Pending unresolved CRDT dependencies are not accepted as invisible future writes.
      if (candidate.store.pendingStructs || candidate.store.pendingDs)
        throw new Error('UPDATE_DEPENDENCIES_MISSING');
      keys = project(candidate).fileKeys.filter(
        (key) => !state.references.has(key)
      );
    } finally {
      candidate.destroy();
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
        target_progress: state.progress,
      };
      const immutable = JSON.parse(JSON.stringify(tuple)) as StoreInput;
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
      return state.stored;
    }
    // Fixed bytes, projection, participants and original base/store identity precede every await.
    const capture = (state.capture ??= this.capture(state, document));
    let receipt: Receipt | null = null;
    // An exhausted store budget may still observe its original durable receipt;
    // it never permits another store attempt or a replacement identity.
    if (capture.attempted && capture.failures >= 3)
      receipt = await this.receipt(capture);
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
        if (
          !Y.snapshotContainsUpdate(current.snapshot, target.bytes) ||
          ![...target.snapshot.sv].every(
            ([client, clock]) => (current.snapshot.sv.get(client) ?? 0) >= clock
          ) ||
          !Y.equalDeleteSets(
            current.snapshot.ds,
            Y.mergeDeleteSets([current.snapshot.ds, target.snapshot.ds])
          )
        )
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
    return committed;
  }

  private lifecycleIdentity(): Record<string, unknown> {
    const c = this.php.config;
    if (
      !c.edition ||
      !['engine', 'standalone', 'multi-tenant'].includes(c.edition) ||
      !Number.isSafeInteger(c.configRevision) ||
      Number(c.configRevision) < 1 ||
      !digest(c.configSha256)
    )
      throw new Error('LIFECYCLE_CONFIGURATION_INVALID');
    return {
      instance_id: c.instanceId,
      edition: c.edition,
      service_instance_id: c.serviceInstanceId,
      config_revision: c.configRevision,
      config_sha256: c.configSha256,
      projector_version: projectorVersion,
    };
  }
  private async pairedProbe(full: boolean): Promise<Record<string, unknown>> {
    if (this.lifecyclePending) {
      const previous = await this.lifecyclePending;
      if (!full) return previous;
    }
    const pending = this.php
      .call<Record<string, unknown>>('lifecycle', {
        stage: 'paired_probe',
        probe_id: this.probeId,
        full,
        ...this.lifecycleIdentity(),
      })
      .then((value) => {
        this.validateLifecycle(value);
        this.lifecycleState = value;
        this.processLeaseExpires = Number(value.process_lease_expires_at_ms);
        if (
          value.phase !== 'active' ||
          value.activation_id !== this.activationId
        )
          this.admissionOpen = false;
        return value;
      });
    this.lifecyclePending = pending;
    try {
      return await pending;
    } finally {
      if (this.lifecyclePending === pending) this.lifecyclePending = undefined;
    }
  }
  private validateLifecycle(value: Record<string, unknown>): void {
    if (
      !value ||
      value.native_protocol !== 2 ||
      value.php_ready !== true ||
      value.holder_key !== this.php.holderKey ||
      value.probe_id !== this.probeId ||
      !Number.isSafeInteger(value.process_lease_expires_at_ms) ||
      Number(value.process_lease_expires_at_ms) <= Date.now() ||
      Number(value.process_lease_expires_at_ms) > Date.now() + 120000 ||
      !['prepared', 'active', 'draining', 'durably_drained'].includes(
        String(value.phase)
      ) ||
      Object.entries(this.lifecycleIdentity()).some(
        ([key, expected]) => value[key] !== expected
      )
    )
      throw new Error('PHP_LIFECYCLE_BINDING_INVALID');
  }
  startLifecycle(): void {
    if (this.lifecycleTimer) throw new Error('LIFECYCLE_ALREADY_STARTED');
    const refresh = (full: boolean) => {
      if (this.stopping || this.instanceDrain) return;
      void this.pairedProbe(full).catch(() => {
        if (this.processLeaseExpires <= Date.now()) {
          this.admissionOpen = false;
          this.states.forEach((state) =>
            state.document
              ?.getConnections()
              .forEach((connection) =>
                this.connectionError(connection, 'PROCESS_LEASE_UNCONFIRMED')
              )
          );
        }
      });
    };
    refresh(true);
    this.lifecycleTimer = setInterval(() => refresh(false), 10000);
    this.lifecycleTimer.unref();
  }
  private async operator(
    input: OperatorInput
  ): Promise<Record<string, unknown>> {
    const commonKeys =
      'action,activation_id,config_revision,config_sha256,edition,holder_key,instance_id,probe_id,projector_version,protocol,service_instance_id';
    const expectedKeys =
      input.action === 'drain_instance'
        ? 'action,activation_id,config_revision,config_sha256,drain_id,edition,holder_key,instance_id,owner_container,plan_sha256,probe_id,projector_version,protocol,rooms,service_instance_id'
        : commonKeys;
    if (
      Object.keys(input).sort().join(',') !== expectedKeys ||
      input.protocol !== 2 ||
      input.holder_key !== this.php.holderKey ||
      input.probe_id !== this.probeId ||
      !['probe', 'activate', 'drain_instance'].includes(input.action) ||
      (input.activation_id !== null && !requestId(input.activation_id)) ||
      Object.entries(this.lifecycleIdentity()).some(
        ([key, expected]) => input[key as keyof OperatorInput] !== expected
      )
    )
      throw new Error('OPERATOR_INPUT_INVALID');
    if (input.action === 'drain_instance') return this.drainInstance(input);
    const php =
      input.action === 'probe'
        ? await this.pairedProbe(true)
        : await this.php.call<Record<string, unknown>>('lifecycle', {
            stage: 'activation_state',
            probe_id: this.probeId,
            activation_id: input.activation_id,
          });
    this.validateLifecycle(php);
    this.lifecycleState = php;
    this.processLeaseExpires = Number(php.process_lease_expires_at_ms);
    if (input.action === 'activate') {
      if (
        this.stopping ||
        this.instanceDrain ||
        !input.activation_id ||
        php.phase !== 'active' ||
        php.activation_id !== input.activation_id
      )
        throw new Error('ACTIVATION_UNCONFIRMED');
      this.activationId = input.activation_id;
      this.admissionOpen = true;
    }
    return {
      ...php,
      node_ready: !this.stopping,
      paired_ready: !this.stopping && php.php_ready === true,
      admission_enabled:
        php.phase === 'active' &&
        this.admissionOpen &&
        this.processLeaseExpires > Date.now(),
    };
  }
  private lifecycleHash(value: unknown): string {
    return sha(phpJson(value).replace(/\\\//g, '/'));
  }
  private async drainInstance(
    input: OperatorInput
  ): Promise<Record<string, unknown>> {
    if (
      !requestId(input.drain_id) ||
      !digest(input.plan_sha256) ||
      !input.owner_container ||
      typeof input.owner_container !== 'object' ||
      Array.isArray(input.owner_container) ||
      !Array.isArray(input.rooms) ||
      input.rooms.length > 10000
    )
      throw new Error('INSTANCE_DRAIN_PLAN_INVALID');
    const {
      action: _action,
      protocol: _protocol,
      plan_sha256: _hash,
      ...plan
    } = input;
    if (this.lifecycleHash(plan) !== input.plan_sha256)
      throw new Error('INSTANCE_DRAIN_PLAN_INVALID');
    const keys = new Set<string>();
    for (const room of input.rooms) {
      if (
        !room ||
        Object.keys(room).sort().join(',') !==
          'document_id,holder_key,lease_epoch,room_key' ||
        !digest(room.room_key) ||
        keys.has(room.room_key) ||
        !Number.isSafeInteger(room.document_id) ||
        room.document_id < 1 ||
        !Number.isSafeInteger(room.lease_epoch) ||
        room.lease_epoch < 1 ||
        (room.holder_key !== null &&
          !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
            room.holder_key
          ))
      )
        throw new Error('INSTANCE_DRAIN_PLAN_INVALID');
      keys.add(room.room_key);
    }
    if (
      this.instanceDrain &&
      phpJson(this.instanceDrain.input) !== phpJson(input)
    )
      throw new Error('ORIGINAL_INSTANCE_DRAIN_REQUIRED');
    if (!this.instanceDrain)
      this.instanceDrain = {
        input: JSON.parse(JSON.stringify(input)) as OperatorInput,
      };
    const attempt = this.instanceDrain;
    if (attempt.result) return attempt.result;
    if (attempt.pending) return attempt.pending;
    this.admissionOpen = false;
    this.stopping = true;
    if (this.maintenance) clearInterval(this.maintenance);
    this.maintenance = undefined;
    if (this.lifecycleTimer) clearInterval(this.lifecycleTimer);
    this.lifecycleTimer = undefined;
    const pending = (async () => {
      const php = await this.php.call<Record<string, unknown>>('lifecycle', {
        stage: 'drain_state',
        probe_id: this.probeId,
        drain_id: input.drain_id,
        plan_sha256: input.plan_sha256,
      });
      this.validateLifecycle(php);
      if (
        php.phase !== 'draining' ||
        php.drain_id !== input.drain_id ||
        php.plan_sha256 !== input.plan_sha256 ||
        php.activation_id !== input.activation_id
      )
        throw new Error('INSTANCE_DRAIN_FENCE_UNCONFIRMED');
      await Promise.all([...this.server.hocuspocus.loadingDocuments.values()]);
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
      const receipts: Record<string, unknown>[] = [];
      for (const state of this.states.values()) {
        const expected = input.rooms?.find(
          (room) => room.room_key === state.auth.room_key
        );
        if (
          !expected ||
          expected.document_id !== state.auth.document_id ||
          expected.lease_epoch !== state.auth.lease_epoch ||
          expected.holder_key !== this.php.holderKey
        )
          throw new Error('INSTANCE_DRAIN_SCOPE_CHANGED');
      }
      for (const room of input.rooms ?? []) {
        const state = [...this.states.values()].find(
          (value) => value.auth.room_key === room.room_key
        );
        if (!state) {
          receipts.push({ ...room, source: 'offline' });
          continue;
        }
        const document = state.document;
        if (!document || document.isDestroyed || document.isLoading)
          throw new Error('INSTANCE_DRAIN_LOAD_UNCONFIRMED');
        const captured = this.fixedContent(document, state.progress);
        await this.flushDocument(document);
        if (state.dirty || state.capture) await this.flushDocument(document);
        const stored = state.stored;
        if (
          !stored ||
          !stored.hasReceipt ||
          state.capture ||
          state.dirty ||
          !Y.snapshotContainsUpdate(stored.snapshot, captured.bytes) ||
          !Y.equalDeleteSets(
            stored.snapshot.ds,
            Y.mergeDeleteSets([stored.snapshot.ds, captured.snapshot.ds])
          )
        )
          throw new Error('INSTANCE_DRAIN_STORE_UNCONFIRMED');
        receipts.push({
          ...room,
          source: 'native',
          store_id: stored.proof.captureId,
          state_seq: stored.proof.stateSeq,
          binary_sha256: stored.proof.binarySha256,
          target_progress: stored.progress,
          coverage: {
            algorithm: 'yjs.snapshotContainsUpdate/13.6.32',
            covers_target: true,
            committed_snapshot_sha256: sha(
              new Uint8Array(Buffer.from(stored.proof.snapshotBase64, 'base64'))
            ),
            captured_snapshot_sha256: sha(Y.encodeSnapshot(captured.snapshot)),
          },
        });
      }
      const result = {
        ...this.lifecycleIdentity(),
        native_protocol: 2,
        holder_key: this.php.holderKey,
        probe_id: this.probeId,
        activation_id: input.activation_id,
        drain_id: input.drain_id,
        plan_sha256: input.plan_sha256,
        native_drained: true,
        phase: 'draining',
        admission_enabled: false,
        room_receipts: receipts,
        targets_sha256: this.lifecycleHash(receipts),
      };
      attempt.result = result;
      return result;
    })();
    attempt.pending = pending;
    try {
      return await pending;
    } finally {
      if (attempt.pending === pending) delete attempt.pending;
    }
  }
  startMaintenance(): void {
    if (this.maintenance) throw new Error('MAINTENANCE_ALREADY_STARTED');
    this.maintenance = setInterval(() => {
      this.states.forEach((state) => {
        state.document
          ?.getConnections()
          .forEach((connection: Connection<Context>) => {
            const context = connection.context;
            if (context.auth.lease_expires_at <= Date.now()) {
              this.connectionError(
                connection,
                'CONNECTION_AUTHORIZATION_EXPIRED'
              );
              return;
            }
            if (!context.renewing && context.nextRenewal <= Date.now()) {
              context.renewing = true;
              this.track(this.renew(state, connection)).catch(() => {});
            }
          });
      });
    }, 1000);
    this.maintenance.unref();
  }
  private async renew(
    state: State,
    connection: Connection<Context>
  ): Promise<void> {
    const context = connection.context;
    const original = context.auth;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      const checked = await Promise.race([
        this.php.call<ConnectionAuth>('check', {
          connection_key: original.connection_key,
          mode: original.mode,
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
      if (
        this.stopping ||
        this.controlOperations.size >= 1000 ||
        this.fences.has(input.room_key)
      )
        throw new Error('CONTROL_UNAVAILABLE');
      const fence: Fence = {
        input: JSON.parse(JSON.stringify(input)) as ControlInput,
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
      await this.flushDocument(document);
      await this.server.hocuspocus.unloadDocument(document);
      if (this.server.hocuspocus.documents.has(document.name))
        throw new Error('CONTROL_UNLOAD_UNCONFIRMED');
    }
    fence.active = false;
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
    this.admissionOpen = false;
    if (this.lifecycleTimer) clearInterval(this.lifecycleTimer);
    this.lifecycleTimer = undefined;
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
