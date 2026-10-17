import { CollaborationRuntime } from './runtime.js';
import { PhpClient } from './php-client.js';
import { lstatSync, readFileSync, realpathSync } from 'node:fs';
import { sha } from './digests.js';
const required = (key: string): string => {
  const value = process.env[key];
  if (!value) throw new Error('SERVICE_CONFIGURATION_MISSING:' + key);
  return value;
};
// Standard Compose binds only this repository's certs directory. Validate the
// repository view as well as the second mount, so an external host symlink or
// an unrelated credentials mount cannot silently replace that boundary.
const repositoryCerts = '/srv/peanut-hocuspocus/certs';
const mountedCerts = '/run/peanut-collaboration';
const repositoryDirectory = lstatSync(repositoryCerts);
const mountedDirectory = lstatSync(mountedCerts);
if (
  !repositoryDirectory.isDirectory() || !mountedDirectory.isDirectory() ||
  realpathSync(repositoryCerts) !== repositoryCerts ||
  realpathSync(mountedCerts) !== mountedCerts ||
  repositoryDirectory.dev !== mountedDirectory.dev ||
  repositoryDirectory.ino !== mountedDirectory.ino ||
  (repositoryDirectory.mode & 0o022) !== 0
)
  throw new Error('SERVICE_CERTIFICATE_DIRECTORY_INVALID');
const secretPath = required('COLLABORATION_SECRET_FILE');
const stat = lstatSync(secretPath);
if (
  secretPath !== '/run/peanut-collaboration/service-secret' ||
  realpathSync(secretPath) !== secretPath ||
  !stat.isFile() ||
  stat.nlink !== 1 ||
  (stat.mode & 0o777) !== 0o600 ||
  stat.uid !== process.getuid?.() ||
  stat.size < 32 ||
  stat.size > 4096 ||
  process.env.COLLABORATION_SECRET ||
  process.env.COLLABORATION_TICKET_SECRET ||
  process.env.DB_HOST
)
  throw new Error('SERVICE_SECRET_REFERENCE_INVALID');
const secret = readFileSync(secretPath, 'utf8').trim();
if (secret.length < 32 || /[\r\n\0]/.test(secret))
  throw new Error('SERVICE_SECRET_INVALID');
const caPath = '/run/peanut-collaboration/ca.pem',
  caStat = lstatSync(caPath);
if (
  process.env.NODE_EXTRA_CA_CERTS !== caPath ||
  realpathSync(caPath) !== caPath ||
  !caStat.isFile() ||
  caStat.nlink !== 1 ||
  (caStat.mode & 0o022) !== 0 ||
  caStat.size < 1 ||
  caStat.size > 1048576 ||
  sha(new Uint8Array(readFileSync(caPath))) !==
    required('COLLABORATION_CA_SHA256')
)
  throw new Error('SERVICE_CA_REFERENCE_CHANGED');
// Deployment supplies a registered private listener and TLS ingress. No default resource or TLS bypass.
const runtime = new CollaborationRuntime(
  new PhpClient({
    origin: required('COLLABORATION_PHP_ORIGIN'),
    instanceId: required('COLLABORATION_INSTANCE_ID'),
    serviceInstanceId: required('COLLABORATION_SERVICE_INSTANCE_ID'),
    keyId: required('COLLABORATION_KEY_ID'),
    secret,
    edition: required('COLLABORATION_EDITION'),
    configRevision: Number(required('COLLABORATION_CONFIG_REVISION')),
    configSha256: required('COLLABORATION_CONFIG_SHA256'),
    timeoutMs: Number(required('COLLABORATION_PHP_TIMEOUT_MS')),
  }),
  required('COLLABORATION_LISTEN_ADDRESS'),
  Number(required('COLLABORATION_LISTEN_PORT'))
);
await runtime.server.listen();
runtime.startMaintenance();
runtime.startLifecycle();
let draining = false;
for (const signal of ['SIGTERM', 'SIGINT'] as const)
  process.on(signal, () => {
    if (draining) return;
    draining = true;
    void runtime
      .drain()
      .then(() => {
        process.exitCode = 0;
      })
      .catch(() => {
        process.exitCode = 1;
        console.error('COLLABORATION_DRAIN_UNCONFIRMED');
      });
  });
