#!/usr/bin/env python3
"""Own only this fixed service's Docker start, observation and confirmed stop."""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'node:22.23.2-bookworm-slim@sha256:48e4b67d85f87bd551df43704e24d252f56cc5f8e9718841aace50f19948f0f9'


def repository_path(value):
    if not value or Path(value).is_absolute() or '..' in Path(value).parts:
        raise ValueError('SERVICE_REPOSITORY_PATH_REQUIRED')
    path = ROOT / value
    if path.resolve() != path:
        raise ValueError('SERVICE_REPOSITORY_PATH_REQUIRED')
    return path


def environment(path):
    if not path.is_absolute() or path.resolve() != path or not path.is_file() or path.is_symlink():
        raise ValueError('SERVICE_CONFIGURATION_REFERENCE_INVALID')
    stat = path.stat()
    if stat.st_nlink != 1 or stat.st_mode & 0o777 != 0o600:
        raise ValueError('SERVICE_CONFIGURATION_PERMISSIONS_INVALID')
    values = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith('#'):
            continue
        key, separator, value = line.partition('=')
        if not separator or key in values or not re.fullmatch(r'[A-Z][A-Z0-9_]*', key):
            raise ValueError('SERVICE_CONFIGURATION_INVALID')
        values[key] = value
    return values


def decode(value):
    raw = base64.b64decode(value, validate=True)
    if len(raw) > 16384:
        raise ValueError('SERVICE_OWNER_INPUT_INVALID')
    return json.loads(raw)


def execute(args):
    environment_path = repository_path(str(args.environment))
    values = environment(environment_path)
    expected = {'COLLABORATION_PROGRAM_ROOT': str(ROOT),
                'COLLABORATION_SECRET_DIRECTORY': str(ROOT / 'private/collaboration/service'),
                'COLLABORATION_RUNTIME_ENV_FILE': str(ROOT / 'private/runtime.env')}
    if any(str(repository_path(values.get(key))) != value for key, value in expected.items()) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,62}', values.get('COMPOSE_PROJECT_NAME', '')):
        raise ValueError('SERVICE_OWNER_BINDING_INVALID')
    if not Path(expected['COLLABORATION_SECRET_DIRECTORY']).is_dir():
        raise ValueError('SERVICE_OWNER_BINDING_INVALID')
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}', values.get('COLLABORATION_NETWORK', '')):
        raise ValueError('SERVICE_INTERFACE_NETWORK_INVALID')
    runtime = environment(ROOT / 'private/runtime.env')
    if any(key in runtime for key in ('DB_HOST', 'COLLABORATION_SECRET', 'COLLABORATION_TICKET_SECRET')):
        raise ValueError('SERVICE_RUNTIME_SECRET_REFERENCE_REQUIRED')
    if not (ROOT / 'dist/src/main.js').is_file() or not (ROOT / 'node_modules/@hocuspocus/server').is_dir():
        raise ValueError('SERVICE_PROGRAM_NOT_PREPARED')
    docker = shutil.which('docker')
    if docker is None:
        raise ValueError('SERVICE_DOCKER_UNAVAILABLE')
    # Compose resolves relative inputs from docker/, not from the repository.
    # Normalize only the validated inputs for this child; never rewrite config.
    compose_environment = os.environ.copy()
    compose_environment.update(expected)
    for key in ('COMPOSE_PROJECT_NAME', 'COLLABORATION_NETWORK'):
        compose_environment[key] = values[key]
    compose = [docker, 'compose', '--env-file', str(environment_path), '-f', str(ROOT / 'docker/compose.yaml')]

    def run(command, timeout=40):
        result = subprocess.run(command, cwd=ROOT / 'docker', env=compose_environment, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError('SERVICE_NATIVE_EFFECT_UNCONFIRMED')
        return result.stdout.decode()

    def inspect():
        ids = run(compose + ['ps', '-a', '-q', 'collaboration']).splitlines()
        if len(ids) != 1:
            raise RuntimeError('SERVICE_CONTAINER_IDENTITY_UNKNOWN')
        detail = json.loads(run([docker, 'inspect', '--format', '{{json .}}', ids[0]]))
        state = detail['State']
        labels = detail['Config']['Labels']
        discovered = run([docker, 'ps', '-a', '--no-trunc', '--filter', 'label=com.docker.compose.project=' + values['COMPOSE_PROJECT_NAME'], '--filter', 'label=com.docker.compose.service=collaboration', '--format', '{{.ID}}']).splitlines()
        image = run([docker, 'image', 'inspect', '--format', '{{.Id}}', IMAGE]).strip()
        mounts = [mount for mount in detail['Mounts'] if mount.get('Type') == 'bind']
        required = [(str(ROOT), '/srv/peanut-hocuspocus'), (expected['COLLABORATION_SECRET_DIRECTORY'], '/run/peanut-collaboration')]
        if discovered != ids or detail['Id'] != ids[0] or detail['Image'] != image or len(mounts) != 2 or any(not any(m['Source'] == source and m['Destination'] == destination and m['RW'] is False for m in mounts) for source, destination in required):
            raise RuntimeError('SERVICE_CONTAINER_BINDING_UNCONFIRMED')
        if labels.get('com.docker.compose.project') != values['COMPOSE_PROJECT_NAME'] or labels.get('com.docker.compose.service') != 'collaboration' or labels.get('com.docker.compose.project.working_dir') != str(ROOT / 'docker') or state.get('OOMKilled') is not False or state.get('Paused') is not False or state.get('Restarting') is not False or type(state.get('Running')) is not bool:
            raise RuntimeError('SERVICE_CONTAINER_STATE_UNCONFIRMED')
        if state['Status'] != ('running' if state['Running'] else 'exited') or not state['Running'] and state.get('ExitCode') != 0:
            raise RuntimeError('SERVICE_CONTAINER_STATE_UNCONFIRMED')
        return {'id': detail['Id'], 'image_id': image, 'compose_project': values['COMPOSE_PROJECT_NAME'],
                'compose_service': 'collaboration', 'compose_working_dir': str(ROOT / 'docker'),
                'service_mount': {'source': required[1][0], 'destination': required[1][1], 'read_only': True},
                'program_mount': {'source': str(ROOT), 'destination': '/srv/peanut-hocuspocus', 'read_only': True},
                'state': state['Status'], 'running': state['Running']}

    run(compose + ['config', '--quiet'])
    if args.action == 'start':
        run(compose + ['up', '-d', '--wait', '--no-build', '--no-deps', 'collaboration'], 120)
        return inspect()
    before = inspect()
    if args.action == 'status':
        return before
    receipt = decode(args.durable_receipt or '')
    original = decode(args.original_container or '')
    identity = {key: value for key, value in before.items() if key not in ('state', 'running')}
    if original != identity or set(receipt) != {'protocol', 'drain_id', 'plan_sha256', 'targets_sha256', 'durably_drained', 'room_count'} or receipt.get('protocol') != 2 or receipt.get('durably_drained') is not True or type(receipt.get('room_count')) is not int or receipt['room_count'] < 0:
        raise ValueError('SERVICE_ORIGINAL_DURABLE_DRAIN_REQUIRED')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', receipt.get('drain_id', '')) or any(not re.fullmatch(r'[a-f0-9]{64}', receipt.get(key, '')) for key in ('plan_sha256', 'targets_sha256')):
        raise ValueError('SERVICE_ORIGINAL_DURABLE_DRAIN_REQUIRED')
    run(compose + ['stop', '--timeout', '-1', 'collaboration'])
    after = inspect()
    if after['running'] or {key: value for key, value in after.items() if key not in ('state', 'running')} != original:
        raise RuntimeError('SERVICE_ORIGINAL_STOP_UNCONFIRMED')
    return after


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'status', 'stop'))
    parser.add_argument('--environment', type=Path, required=True, help='0600 configuration file relative to this repository root, independent of cwd')
    parser.add_argument('--durable-receipt')
    parser.add_argument('--original-container')
    try:
        result = execute(parser.parse_args())
        print(json.dumps(result, sort_keys=True))
        return 0
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, subprocess.SubprocessError):
        print(json.dumps({'state': 'unknown', 'error_code': 'SERVICE_OWNER_UNCONFIRMED'}))
        return 4


if __name__ == '__main__':
    sys.exit(main())
