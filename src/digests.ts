import { createHash } from 'node:crypto';
export function sha(bytes: Uint8Array | string): string {
  return createHash('sha256').update(bytes).digest('hex');
}
export function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === 'object')
    return Object.fromEntries(
      Object.entries(value)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, canonical(v)])
    );
  return value;
}
/** PHP JSON_THROW_ON_ERROR canonical tuples differ from unescaped projections. */
export function phpJson(value: unknown, unescaped = false): string {
  let json = JSON.stringify(canonical(value));
  if (typeof json !== 'string') throw new Error('CANONICAL_JSON_INVALID');
  json = json.replace(
    /[\u2028\u2029]/g,
    (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0')
  );
  if (!unescaped)
    json = json
      .replace(/\//g, '\\/')
      .replace(
        /[\u0080-\uffff]/g,
        (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0')
      );
  return json;
}
