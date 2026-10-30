import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { lstat, readdir, readFile, mkdir, mkdtemp, writeFile, rename, rm, realpath } from 'node:fs/promises';
import { dirname, isAbsolute, join, resolve } from 'node:path';

// Consume an already built clean checkout; never compile, install, publish or start services.
const root = resolve(import.meta.dirname, '..');
const sha = value => createHash('sha256').update(value).digest('hex');
async function absent(path) {
  try { await lstat(path); } catch (error) { if (error.code === 'ENOENT') return; throw error; }
  throw new Error('RELEASE_OUTPUT_ALREADY_EXISTS');
}
async function ordinary(path) {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 || await realpath(path) !== path)
    throw new Error('RELEASE_INPUT_NOT_ORDINARY');
  return readFile(path);
}
async function distFiles(directory, prefix = 'dist') {
  const stat = await lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || await realpath(directory) !== directory)
    throw new Error('RELEASE_DIST_UNSAFE');
  const paths = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.isDirectory()) paths.push(...await distFiles(join(directory, entry.name), prefix + '/' + entry.name));
    else if (entry.isFile() && entry.name.endsWith('.js')) paths.push(prefix + '/' + entry.name);
    else throw new Error('RELEASE_DIST_MUST_CONTAIN_JAVASCRIPT_ONLY');
  }
  return paths;
}

let temporary;
try {
  if (process.argv.length !== 3 || !isAbsolute(process.argv[2]))
    throw new Error('USAGE: npm run package -- /absolute/new-release-directory');
  const output = resolve(process.argv[2]);
  const parent = dirname(output);
  if (await realpath(parent) !== parent) throw new Error('RELEASE_OUTPUT_PARENT_UNSAFE');
  for (const path of [output, output + '.tar.gz', output + '.tar.gz.sha256']) await absent(path);
  const git = (...args) => execFileSync('git', args, { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  if (git('status', '--porcelain', '--untracked-files=no')) throw new Error('RELEASE_REQUIRES_CLEAN_SOURCE');
  const source = git('rev-parse', 'HEAD');
  const pkg = JSON.parse(await ordinary(join(root, 'package.json')));
  const compiled = await distFiles(join(root, 'dist'));
  for (const name of ['main', 'runtime', 'php-client', 'configuration', 'contracts', 'digests', 'projector', 'business-control']) {
    if (!compiled.includes(`dist/src/${name}.js`)) throw new Error('RELEASE_BUILD_REQUIRED');
  }
  temporary = await mkdtemp(join(parent, '.hocus-release-'));
  const content = join(temporary, 'content');
  await mkdir(content);
  const paths = [...compiled, 'config/api.json', 'compose.yaml', '.env.example',
    'docker/ca.example.yaml', 'docker/network.example.yaml', 'pnpm-lock.yaml', 'README.md', 'LICENSE'];
  for (const relative of paths) {
    const bytes = await ordinary(join(root, relative));
    if (relative.startsWith('dist/') && /sourceMappingURL\s*=/.test(bytes.toString('utf8')))
      throw new Error('RELEASE_SOURCE_MAP_FORBIDDEN');
    await mkdir(dirname(join(content, relative)), { recursive: true });
    await writeFile(join(content, relative), bytes, { mode: 0o644, flag: 'wx' });
  }
  // Keep declarations matching the lock; --prod excludes devDependencies on the target.
  pkg.scripts = { start: pkg.scripts.start };
  await writeFile(join(content, 'package.json'), JSON.stringify(pkg, null, 2) + '\n', { flag: 'wx' });
  const files = [];
  for (const relative of [...paths, 'package.json'].sort()) {
    const bytes = await readFile(join(content, relative));
    files.push({ path: relative, bytes: bytes.length, sha256: sha(bytes) });
  }
  await writeFile(join(content, 'manifest.json'), JSON.stringify({
    protocol: 'peanut.hocuspocus.release.v1', version: pkg.version, source,
    node: pkg.engines.node, package_manager: pkg.packageManager, source_included: false,
    dependencies: 'prepare production dependencies from lock on deployment target',
    build_provenance: 'not_attested_by_packager', files,
  }, null, 2) + '\n', { flag: 'wx' });
  const archive = join(temporary, 'release.tar.gz');
  execFileSync('tar', ['-czf', archive, '-C', content, '.'], { stdio: ['ignore', 'pipe', 'pipe'] });
  const archiveSha = sha(await readFile(archive));
  await writeFile(join(temporary, 'release.sha256'), archiveSha + '\n', { flag: 'wx' });
  for (const path of [output, output + '.tar.gz', output + '.tar.gz.sha256']) await absent(path);
  await rename(content, output);
  await rename(archive, output + '.tar.gz');
  await rename(join(temporary, 'release.sha256'), output + '.tar.gz.sha256');
  console.log(JSON.stringify({ status: 'packaged', directory: output, archive: output + '.tar.gz', sha256: archiveSha, source }));
} catch (error) {
  console.error(error instanceof Error && /^[A-Z_][A-Z_ :/a-z.-]*$/.test(error.message) ? error.message : 'RELEASE_FAILED');
  process.exitCode = 1;
} finally {
  if (temporary) await rm(temporary, { recursive: true });
}
