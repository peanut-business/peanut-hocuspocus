import { createHmac, timingSafeEqual } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { PhpClient } from './php-client.js';
import {
  digest,
  assertInvalidation,
  type AccessInvalidationInput,
  type AccessInvalidationAck,
} from './contracts.js';
import { sha } from './digests.js';

export const controlPath =
  '/internal/collaboration/control/v1/business-barrier';
export const operatorPath = '/internal/collaboration/operator/v1/lifecycle';
export const accessPath = '/internal/collaboration/access/v1/invalidate';
export interface ControlInput {
  protocol: 2;
  action: 'drain' | 'release';
  operation_id: string;
  input_sha256: string;
  room_key: string;
  holder_key: string;
  lease_epoch: number;
  result_epoch?: number;
  status?: 'active' | 'archived' | 'blocked_recovery';
}
export interface OperatorInput {
  protocol: 2;
  action: 'probe' | 'activate' | 'drain_instance';
  holder_key: string;
  probe_id: string;
  activation_id: string | null;
  instance_id: string;
  edition: string;
  service_instance_id: string;
  config_revision: number;
  config_sha256: string;
  projector_version: string;
  drain_id?: string;
  plan_sha256?: string;
  rooms?: {
    room_key: string;
    document_id: number;
    lease_epoch: number;
    holder_key: string | null;
  }[];
  owner_container?: Record<string, unknown>;
}

/** Existing service HMAC transport, with the native protocol-2 per-room body. */
export class Control {
  private readonly nonces = new Map<string, number>();
  constructor(
    private readonly php: PhpClient,
    private readonly execute: (
      input: ControlInput
    ) => Promise<Record<string, unknown>>,
    private readonly operator: (
      input: OperatorInput
    ) => Promise<Record<string, unknown>>,
    private readonly invalidate: (
      input: AccessInvalidationInput
    ) => Promise<AccessInvalidationAck>
  ) {}

  async handle(
    request: IncomingMessage,
    response: ServerResponse
  ): Promise<void> {
    const config = this.php.config;
    const path = request.url === accessPath ? accessPath
      : request.url === operatorPath ? operatorPath : controlPath;
    const domain = path === accessPath ? 'access-invalidation-v1'
      : path === operatorPath ? 'operator' : 'control';
    const protocol = path === accessPath ? '1' : '2';
    let nonce = '',
      holderKey = '',
      bodyHash = '',
      authenticated = false,
      status = 403;
    let result: Record<string, unknown> = {
      ok: false,
      error_code: 'CONTROL_AUTH_INVALID',
    };
    const sign = (kind: 'request' | 'response', fields: string[]) =>
      createHmac('sha256', config.secret)
        .update(
          [
            `peanut.collaboration.${domain}.${kind}.v${protocol}`,
            config.instanceId,
            config.serviceInstanceId,
            config.keyId,
            holderKey,
            ...fields,
          ].join('\n')
        )
        .digest('hex');
    try {
      if (
        request.url !== path ||
        request.method !== 'POST' ||
        request.headers.cookie ||
        request.headers.authorization ||
        request.headers.origin ||
        request.headers['content-type'] !== 'application/json'
      )
        throw new Error('CONTROL_AUDIENCE_INVALID');
      const header = (name: string): string => {
        const value = request.headers[`x-collaboration-${name}`];
        if (typeof value !== 'string') throw new Error('CONTROL_AUTH_INVALID');
        return value;
      };
      nonce = header('nonce');
      holderKey = header('holder-key');
      const timestamp = header('timestamp'),
        signature = header('signature');
      if (
        header('protocol') !== protocol ||
        header('service-instance') !== config.serviceInstanceId ||
        header('key-id') !== config.keyId ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
          holderKey
        ) ||
        !digest(nonce) ||
        !digest(signature) ||
        !/^[0-9]{10}$/.test(timestamp) ||
        Math.abs(Date.now() / 1000 - Number(timestamp)) > 30
      )
        throw new Error('CONTROL_AUTH_INVALID');
      const chunks: Uint8Array[] = [];
      let length = 0;
      for await (const part of request) {
        const chunk = Buffer.isBuffer(part) ? part : Buffer.from(part);
        length += chunk.length;
        if (length > (domain === 'operator' ? 8388608 : 65536))
          throw new Error('CONTROL_REQUEST_TOO_LARGE');
        chunks.push(new Uint8Array(chunk));
      }
      const bytes = new Uint8Array(Buffer.concat(chunks));
      bodyHash = sha(bytes);
      const expected = sign('request', [
        timestamp,
        nonce,
        'POST',
        path,
        bodyHash,
      ]);
      if (
        !timingSafeEqual(
          new Uint8Array(Buffer.from(signature, 'hex')),
          new Uint8Array(Buffer.from(expected, 'hex'))
        )
      )
        throw new Error('CONTROL_AUTH_INVALID');
      // Access-v1 replay/backpressure is an authenticated rejection, never an unsigned ACK failure.
      if (path === accessPath) authenticated = true;
      this.nonces.forEach((expiry, key) => {
        if (expiry <= Date.now()) this.nonces.delete(key);
      });
      if (this.nonces.has(nonce) || this.nonces.size >= 10000)
        throw new Error('CONTROL_REPLAY_OR_BACKPRESSURE');
      this.nonces.set(nonce, (Number(timestamp) + 31) * 1000);
      authenticated = true;
      const input = JSON.parse(
        new TextDecoder('utf-8', { fatal: true }).decode(bytes)
      );
      if (path === accessPath) {
        assertInvalidation(input);
        if (input.instance_id !== config.instanceId
          || input.service_instance_id !== config.serviceInstanceId
          || input.holder_key !== holderKey || holderKey !== this.php.holderKey) {
          throw new Error('ACCESS_INVALIDATION_HOLDER_CHANGED');
        }
        result = { ok: true, data: await this.invalidate(input) };
        status = 200;
      } else if (domain === 'operator') {
        if (
          !input ||
          input.protocol !== 2 ||
          input.holder_key !== holderKey ||
          holderKey !== this.php.holderKey
        )
          throw new Error('CONTROL_HOLDER_CHANGED');
        result = {
          ok: true,
          data: await this.operator(input as OperatorInput),
        };
        status = 200;
      } else {
        const roomInput = input as ControlInput;
        const expectedKeys =
          roomInput?.action === 'drain'
            ? 'action,holder_key,input_sha256,lease_epoch,operation_id,protocol,room_key'
            : 'action,holder_key,input_sha256,lease_epoch,operation_id,protocol,result_epoch,room_key,status';
        if (
          !input ||
          Object.keys(input).sort().join(',') !== expectedKeys ||
          input.protocol !== 2 ||
          !['drain', 'release'].includes(roomInput.action) ||
          !/^[A-Za-z0-9:_-]{1,128}$/.test(roomInput.operation_id) ||
          !digest(roomInput.input_sha256) ||
          !digest(roomInput.room_key) ||
          !Number.isSafeInteger(roomInput.lease_epoch) ||
          roomInput.lease_epoch < 1 ||
          (roomInput.action === 'release' &&
            (!Number.isSafeInteger(roomInput.result_epoch) ||
              Number(roomInput.result_epoch) <= roomInput.lease_epoch ||
              !['active', 'archived', 'blocked_recovery'].includes(
                String(roomInput.status)
              )))
        )
          throw new Error('CONTROL_INPUT_INVALID');
        if (input.holder_key !== holderKey || holderKey !== this.php.holderKey)
          throw new Error('CONTROL_HOLDER_CHANGED');
        result = { ok: true, data: await this.execute(roomInput) };
        status = 200;
      }
    } catch (error) {
      if (authenticated) {
        status = 409;
        const code =
          error instanceof Error &&
          /^[A-Z][A-Z0-9_]{0,127}$/.test(error.message)
            ? error.message
            : 'CONTROL_UNCONFIRMED';
        result = { ok: false, error_code: code };
      }
    }
    let raw = JSON.stringify(result);
    if (Buffer.byteLength(raw) > 33554432) {
      status = 409;
      raw = JSON.stringify({
        ok: false,
        error_code: 'CONTROL_RESPONSE_TOO_LARGE',
      });
    }
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (authenticated) {
      const timestamp = String(Math.floor(Date.now() / 1000)),
        digest = sha(raw);
      Object.assign(headers, {
        'X-Collaboration-Protocol': protocol,
        'X-Collaboration-Service-Instance': config.serviceInstanceId,
        'X-Collaboration-Key-Id': config.keyId,
        'X-Collaboration-Holder-Key': holderKey,
        'X-Collaboration-Nonce': nonce,
        'X-Collaboration-Timestamp': timestamp,
        'X-Collaboration-Body-Sha256': digest,
        'X-Collaboration-Signature': sign('response', [
          nonce,
          'POST',
          path,
          bodyHash,
          timestamp,
          String(status),
          digest,
        ]),
      });
    }
    response.writeHead(status, headers);
    response.end(raw);
  }
}
