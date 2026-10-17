import {
  createHmac,
  randomBytes,
  randomUUID,
  timingSafeEqual,
} from 'node:crypto';
import { sha } from './digests.js';
export interface ServiceConfig {
  origin: string;
  instanceId: string;
  serviceInstanceId: string;
  keyId: string;
  secret: string;
  timeoutMs: number;
  edition: string;
}
export type Endpoint =
  | 'authorize'
  | 'check'
  | 'load'
  | 'store'
  | 'receipt'
  | 'references'
  | 'recovery'
  | 'target/prepare'
  | 'target';
export class PhpFailure extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
    readonly details: Record<string, unknown> = {}
  ) {
    super(code);
  }
}
export class PhpClient {
  readonly holderKey = randomUUID();
  private lastBackendResponse = 0;
  private backendAvailable = false;

  dependencyState(): 'unconfirmed' | 'available' | 'unavailable' {
    if (!this.lastBackendResponse || Date.now() - this.lastBackendResponse > 60000)
      return 'unconfirmed';
    return this.backendAvailable ? 'available' : 'unavailable';
  }

  constructor(
    readonly config: ServiceConfig,
    private readonly http: typeof fetch = fetch
  ) {
    const url = new URL(config.origin);
    if (
      url.protocol !== 'https:' ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      url.pathname !== '/'
    )
      throw new Error('PHP_ORIGIN_TLS_REQUIRED');
    if (
      [config.instanceId, config.serviceInstanceId, config.keyId].some(
        (s) => !/^[A-Za-z0-9_-]{1,128}$/.test(s)
      ) ||
      Buffer.byteLength(config.secret) < 32 ||
      !['engine', 'standalone', 'multi-tenant'].includes(config.edition) ||
      !Number.isSafeInteger(config.timeoutMs) ||
      config.timeoutMs < 100 ||
      config.timeoutMs > 30000
    )
      throw new Error('SERVICE_CONFIG_INVALID');
  }
  async call<T>(endpoint: Endpoint, input: unknown): Promise<T> {
    try {
      const value = await this.request<T>(endpoint, input);
      this.backendAvailable = true;
      this.lastBackendResponse = Date.now();
      return value;
    } catch (error) {
      // A signed business rejection still proves backend transport availability.
      this.backendAvailable = error instanceof PhpFailure;
      this.lastBackendResponse = Date.now();
      throw error;
    }
  }
  private async request<T>(endpoint: Endpoint, input: unknown): Promise<T> {
    const path = '/adminapi/internal/collaboration/v1/' + endpoint;
    const raw = JSON.stringify(input);
    if (Buffer.byteLength(raw) > 8388608)
      throw new Error('PHP_REQUEST_TOO_LARGE');
    const nonce = randomBytes(32).toString('hex'),
      timestamp = String(Math.floor(Date.now() / 1000));
    const c = this.config,
      bodySha = sha(raw);
    const sign = (s: string): string =>
      createHmac('sha256', c.secret).update(s).digest('hex');
    const response = await this.http(new URL(path, c.origin), {
      method: 'POST',
      redirect: 'error',
      credentials: 'omit',
      signal: AbortSignal.timeout(c.timeoutMs),
      body: raw,
      headers: {
        'Content-Type': 'application/json',
        'X-Collaboration-Protocol': '2',
        'X-Collaboration-Service-Instance': c.serviceInstanceId,
        'X-Collaboration-Key-Id': c.keyId,
        'X-Collaboration-Holder-Key': this.holderKey,
        'X-Collaboration-Timestamp': timestamp,
        'X-Collaboration-Nonce': nonce,
        'X-Collaboration-Signature': sign(
          [
            'peanut.collaboration.request.v2',
            c.instanceId,
            c.serviceInstanceId,
            c.keyId,
            this.holderKey,
            timestamp,
            nonce,
            'POST',
            path,
            bodySha,
          ].join('\n')
        ),
      },
    });
    const reader = response.body?.getReader();
    if (!reader) throw new Error('PHP_RESPONSE_EMPTY');
    const parts: Uint8Array[] = [];
    let length = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        length += value.length;
        if (length > 33554432) throw new Error('PHP_RESPONSE_TOO_LARGE');
        parts.push(value);
      }
    } finally {
      await reader.cancel();
    }
    const bytes = new Uint8Array(Buffer.concat(parts)),
      digest = sha(bytes),
      h = response.headers;
    const receivedTime = h.get('x-collaboration-timestamp') ?? '',
      receivedSign = h.get('x-collaboration-signature') ?? '';
    const expected = sign(
      [
        'peanut.collaboration.response.v2',
        c.instanceId,
        c.serviceInstanceId,
        c.keyId,
        this.holderKey,
        nonce,
        'POST',
        path,
        bodySha,
        receivedTime,
        String(response.status),
        digest,
      ].join('\n')
    );
    if (
      h.get('x-collaboration-protocol') !== '2' ||
      h.get('x-collaboration-service-instance') !== c.serviceInstanceId ||
      h.get('x-collaboration-key-id') !== c.keyId ||
      h.get('x-collaboration-holder-key') !== this.holderKey ||
      h.get('x-collaboration-nonce') !== nonce ||
      h.get('x-collaboration-body-sha256') !== digest ||
      !/^[0-9]{10}$/.test(receivedTime) ||
      Math.abs(Date.now() / 1000 - Number(receivedTime)) > 30 ||
      !/^[0-9a-f]{64}$/.test(receivedSign) ||
      !timingSafeEqual(
        new Uint8Array(Buffer.from(expected, 'hex')),
        new Uint8Array(Buffer.from(receivedSign, 'hex'))
      )
    )
      throw new Error('PHP_RESPONSE_AUTH_INVALID');
    const decoded = JSON.parse(
      new TextDecoder('utf-8', { fatal: true }).decode(bytes)
    ) as { code?: unknown; data?: T; msg?: unknown };
    if (response.status !== 200 || decoded.code !== 20000) {
      const details =
        decoded.data &&
        typeof decoded.data === 'object' &&
        !Array.isArray(decoded.data)
          ? (decoded.data as Record<string, unknown>)
          : {};
      throw new PhpFailure(
        typeof details.error_code === 'string'
          ? details.error_code
          : 'PHP_BUSINESS_REJECTED',
        response.status,
        details
      );
    }
    if (!decoded.data || typeof decoded.data !== 'object')
      throw new Error('PHP_RESPONSE_DATA_INVALID');
    return decoded.data;
  }
}
