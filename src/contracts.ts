import type { Structured } from './projector.js';
import { canonical, sha } from './digests.js';

export interface AccessInvalidationTarget {
  connection_key: string;
  room_key: string;
  lease_epoch: number;
  member_id: number;
  authorization_revision: string;
}
export interface AccessInvalidationInput {
  protocol_version: 1;
  action: 'access.invalidate';
  fact_id: string;
  fact_sha256: string;
  tenant_id: number;
  subject: {
    kind: 'member' | 'role' | 'session' | 'tenant_scope_account';
    id: string;
    member_id: number;
  };
  negative_action: 'invalidate_old_grant';
  revision_reference_sha256: string;
  operation_id: string;
  instance_id: string;
  service_instance_id: string;
  holder_key: string;
  targets: AccessInvalidationTarget[];
  scope_digest: string;
}
export interface AccessInvalidationAck {
  protocol_version: 1;
  fact_id: string;
  fact_sha256: string;
  operation_id: string;
  instance_id: string;
  service_instance_id: string;
  holder_key: string;
  scope_digest: string;
  outcome: 'applied' | 'no_matching_connection';
  processed_at: string;
  matched_count: number;
  closed_count: number;
}

/** PHP canonical JSON_UNESCAPED_SLASHES: Unicode remains escaped, including surrogate pairs. */
export function invalidationJson(value: unknown): string {
  return JSON.stringify(canonical(value)).replace(
    /[\u0080-\uffff]/g,
    (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0')
  );
}

function exactObject(value: unknown, keys: string): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === keys;
}
const positiveId = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0;

export function assertInvalidation(value: unknown): asserts value is AccessInvalidationInput {
  if (!exactObject(value, 'action,fact_id,fact_sha256,holder_key,instance_id,negative_action,operation_id,protocol_version,revision_reference_sha256,scope_digest,service_instance_id,subject,targets,tenant_id')
    || value.protocol_version !== 1 || value.action !== 'access.invalidate'
    || value.negative_action !== 'invalidate_old_grant'
    || typeof value.fact_id !== 'string' || !/^revocation_[0-9a-f]{32}$/.test(value.fact_id)
    || !digest(value.fact_sha256) || !digest(value.revision_reference_sha256) || !digest(value.scope_digest)
    || !positiveId(value.tenant_id)
    || !exactObject(value.subject, 'id,kind,member_id')
    || !positiveId(value.subject.member_id)
    || typeof value.subject.kind !== 'string'
    || !['member', 'role', 'session', 'tenant_scope_account'].includes(value.subject.kind)
    || typeof value.subject.id !== 'string'
    || (value.subject.kind === 'session' ? !digest(value.subject.id) : !/^[1-9][0-9]{0,127}$/.test(value.subject.id))
    || (value.subject.kind === 'member' && value.subject.id !== String(value.subject.member_id))
    || typeof value.operation_id !== 'string' || !/^job_[0-9a-f]{32}:[0-9]{1,10}$/.test(value.operation_id)
    || typeof value.holder_key !== 'string'
    || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value.holder_key)
    || [value.instance_id, value.service_instance_id].some((v) => typeof v !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(v))
    || !Array.isArray(value.targets) || value.targets.length > 100) {
    throw new Error('ACCESS_INVALIDATION_INPUT_INVALID');
  }
  const connections = new Set<string>();
  for (const target of value.targets) {
    if (!exactObject(target, 'authorization_revision,connection_key,lease_epoch,member_id,room_key')
      || !digest(target.connection_key) || !digest(target.room_key) || !digest(target.authorization_revision)
      || !positiveId(target.lease_epoch) || target.member_id !== value.subject.member_id
      || connections.has(target.connection_key)) {
      throw new Error('ACCESS_INVALIDATION_TARGET_INVALID');
    }
    connections.add(target.connection_key);
  }
  const { scope_digest: expected, ...scope } = value;
  if (sha(invalidationJson(scope)) !== expected) {
    throw new Error('ACCESS_INVALIDATION_SCOPE_INVALID');
  }
}

export interface Owner {
  module_key: string;
  object_type: string;
  object_id: string;
  slot: string;
}
export interface Binding {
  instance_id: string;
  edition: 'engine' | 'standalone' | 'multi-tenant';
  tenant_id: number;
  document_id: number;
  owner: Owner;
}
export interface ConnectionAuth {
  protocol: 2;
  connection_key: string;
  room_key: string;
  lease_epoch: number;
  holder_key: string;
  binding: Binding;
  document_id: number;
  mode: 'read' | 'write';
  member_id: number;
  nickname: string;
  authorization_revision: string;
  lease_expires_at: number;
}
export interface Load {
  binding: Binding;
  holder_key: string;
  document_id: number;
  lease_epoch: number;
  state_seq: number;
  state_sha256: string;
  binary_base64: string | null;
  structured_content: Structured;
  initialization_required: boolean;
  store_id: string | null;
}
export interface Participant {
  connection_key: string;
  authorization_revision: string;
  authorized_at_ms: number;
}
export interface Recovery {
  protocol: 2;
  request_id: string;
  room_key: string;
  holder_key: string;
  lease_epoch: number;
  binding: Binding;
  baseline: Omit<Load, 'binding' | 'holder_key'> & { status: 'active' };
}
export interface StoreInput extends Binding {
  protocol: 2;
  room_key: string;
  service_instance_id: string;
  lease_epoch: number;
  base_state_seq: number;
  store_id: string;
  baseline_sha256: string;
  binary_sha256: string;
  projection_sha256: string;
  reference_digest: string;
  projector_version: string;
  binary_base64: string;
  structured_content: Structured;
  participants: Participant[];
  target_progress: number;
}
export interface Receipt {
  document_id: number;
  store_id: string;
  lease_epoch: number;
  state_seq: number;
  binary_sha256: string;
  projection_sha256: string;
  persisted_at: string;
  target_progress: number;
}
export interface Committed {
  type: 'committed';
  documentId: number;
  roomEpoch: number;
  stateSeq: number;
  captureId: string;
  binarySha256: string;
  snapshotBase64: string;
}
export function roomName(binding: Binding): string {
  return [
    binding.instance_id,
    binding.edition,
    binding.tenant_id,
    binding.document_id,
  ].join(':');
}
export function assertBinding(
  binding: Binding,
  name: string,
  instance: string
): void {
  if (
    !binding ||
    Object.keys(binding).sort().join(',') !==
      'document_id,edition,instance_id,owner,tenant_id' ||
    binding.instance_id !== instance ||
    !['engine', 'standalone', 'multi-tenant'].includes(binding.edition) ||
    !Number.isSafeInteger(binding.tenant_id) ||
    binding.tenant_id < 1 ||
    !Number.isSafeInteger(binding.document_id) ||
    binding.document_id < 1 ||
    name !== roomName(binding) ||
    !binding.owner ||
    Object.keys(binding.owner).sort().join(',') !==
      'module_key,object_id,object_type,slot' ||
    Object.values(binding.owner).some(
      (value) => typeof value !== 'string' || !value || value.length > 128
    )
  )
    throw new Error('PHP_ROOM_BINDING_INVALID');
}
export function digest(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
}
export function requestId(value: unknown): value is string {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(value);
}
export function assertAuth(
  value: ConnectionAuth,
  name: string,
  instance: string,
  holderKey: string
): void {
  assertBinding(value?.binding, name, instance);
  if (
    value.protocol !== 2 ||
    value.holder_key !== holderKey ||
    value.document_id !== value.binding.document_id ||
    !digest(value.room_key) ||
    !digest(value.connection_key) ||
    !Number.isSafeInteger(value.lease_epoch) ||
    value.lease_epoch < 1 ||
    !['read', 'write'].includes(value.mode) ||
    !Number.isSafeInteger(value.member_id) ||
    value.member_id < 1 ||
    typeof value.nickname !== 'string' ||
    !value.nickname.trim() ||
    [...value.nickname].length > 160 ||
    !digest(value.authorization_revision) ||
    !Number.isSafeInteger(value.lease_expires_at) ||
    value.lease_expires_at <= Date.now()
  )
    throw new Error('PHP_CONNECTION_BINDING_INVALID');
}
export function binary(base64: string): Uint8Array {
  const bytes = Buffer.from(base64, 'base64');
  if (
    !bytes.length ||
    bytes.length > 4194304 ||
    bytes.toString('base64') !== base64
  )
    throw new Error('STATE_BINARY_INVALID');
  return new Uint8Array(bytes);
}
