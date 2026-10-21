import { lstatSync, readFileSync, realpathSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { sha } from './digests.js';

export const actions = ['authorize', 'check', 'load', 'store', 'receipt',
  'references', 'recovery', 'target/prepare', 'target'] as const;
export type Endpoint = typeof actions[number];
export type ApiRoutes = Readonly<Record<Endpoint, string>>;

export function normalizeBaseUrl(value: string): URL {
  let url: URL;
  try { url = new URL(value); } catch { throw new Error('BASE_URL_INVALID'); }
  if (!['http:', 'https:'].includes(url.protocol) || !url.hostname ||
    url.username || url.password || url.search || url.hash ||
    /[\u0000-\u0020\u007f\\]/.test(value))
    throw new Error('BASE_URL_INVALID');
  url.pathname = url.pathname.replace(/\/+$/, '') + '/';
  return url;
}

export function endpointUrl(base: URL, route: string): URL {
  const relative = route.replace(/^\/+/, '');
  if (route.length > 512 || !/^[A-Za-z0-9_-][A-Za-z0-9._~-]*(?:\/[A-Za-z0-9_-][A-Za-z0-9._~-]*)*$/.test(relative))
    throw new Error('API_PATH_INVALID');
  const url = new URL(relative, base);
  if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname))
    throw new Error('API_PATH_OUTSIDE_BASE');
  return url;
}

function ordinary(path: string, maximum: number): Uint8Array {
  const stat = lstatSync(path);
  if (!stat.isFile() || realpathSync(path) !== path || stat.nlink !== 1 ||
    (stat.mode & 0o022) !== 0 || stat.size < 1 || stat.size > maximum)
    throw new Error('CONFIGURATION_FILE_UNSAFE');
  return new Uint8Array(readFileSync(path));
}

export function readApiRoutes(): ApiRoutes {
  const path = resolve('config/api.json');
  let value: unknown;
  try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(ordinary(path, 65536))); }
  catch { throw new Error('API_CONFIGURATION_INVALID'); }
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
    Object.keys(value).sort().join(',') !== [...actions].sort().join(','))
    throw new Error('API_CONFIGURATION_INVALID');
  const routes = value as Record<Endpoint, unknown>;
  for (const action of actions) {
    if (typeof routes[action] !== 'string') throw new Error('API_CONFIGURATION_INVALID');
    endpointUrl(new URL('https://configuration.invalid/base/'), routes[action]);
  }
  return Object.freeze(routes as Record<Endpoint, string>);
}

export function validateExtraCa(): void {
  const path = process.env.NODE_EXTRA_CA_CERTS;
  const expected = process.env.CA_SHA256;
  if (!path && !expected) return; // Normal HTTPS uses Node's standard trusted CAs.
  if (!path || !expected || !/^[a-f0-9]{64}$/.test(expected) || !path.startsWith('/'))
    throw new Error('CA_CONFIGURATION_INVALID');
  const directory = dirname(path);
  const stat = lstatSync(directory);
  if (!stat.isDirectory() || realpathSync(directory) !== directory || (stat.mode & 0o022) !== 0)
    throw new Error('CA_DIRECTORY_UNSAFE');
  if (sha(ordinary(path, 1048576)) !== expected)
    throw new Error('CA_REFERENCE_CHANGED');
}

export function required(key: string): string {
  const value = process.env[key];
  if (!value) throw new Error('SERVICE_CONFIGURATION_MISSING:' + key);
  return value;
}
