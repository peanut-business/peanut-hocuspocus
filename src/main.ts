import { CollaborationRuntime } from './runtime.js';
import { PhpClient } from './php-client.js';
import { readApiRoutes, required, validateExtraCa } from './configuration.js';
// The service key authenticates backend traffic; it is never a user token.
const secret = required('SERVICE_SECRET');
if (Buffer.byteLength(secret) < 32 || Buffer.byteLength(secret) > 4096 || /[\r\n\0]/.test(secret))
  throw new Error('SERVICE_SECRET_INVALID');
if (process.env.DB_HOST)
  throw new Error('SERVICE_CONFIGURATION_INVALID');
validateExtraCa();
// Compose supplies the listener; deployments terminate WSS at their TLS ingress.
const runtime = new CollaborationRuntime(
  new PhpClient({
    baseUrl: required('BASE_URL'),
    routes: readApiRoutes(),
    instanceId: required('INSTANCE_ID'),
    serviceInstanceId: required('SERVICE_INSTANCE_ID'),
    keyId: required('KEY_ID'),
    secret,
    timeoutMs: Number(process.env.REQUEST_TIMEOUT_MS ?? '20000'),
  }),
  process.env.LISTEN_ADDRESS ?? '0.0.0.0',
  Number(process.env.LISTEN_PORT ?? process.env.PORT ?? '80')
);
await runtime.server.listen();
runtime.startMaintenance();
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
