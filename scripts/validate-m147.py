"""Foreground, resumable M147 library validation; authoritative results in JSON.

Run: python -u scripts/validate-m147.py
Each ordinary libtest case runs once in its own process, serially. Output is
inherited by the terminal (not redirected). A completed case is saved only after
exit zero AND a successful, non-ignored test inventory match. Interrupted cases
are rerun. This is per-case coverage, not one uninterrupted full-suite run.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / 'docs/performance-m147.json'


def save(data):
    encoded = json.dumps(data, ensure_ascii=False, indent=2) + '\n'
    with LEDGER.open('w', encoding='utf-8') as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    assert json.loads(LEDGER.read_text(encoding='utf-8')) == data


def main():
    os.chdir(ROOT)
    env = dict(os.environ)
    scratch = Path(env.get('M147_SCRATCH_DIR', ROOT / 'target/test-data')).resolve()
    assert scratch.is_dir()
    env.update(TEMP=str(scratch), TMP=str(scratch))
    # Cargo resolves the executable; no guessed hashed test filename.
    built = subprocess.run(['cargo', 'test', '--release', '--lib', '--no-run',
                            '--message-format=json'], capture_output=True,
                           text=True, encoding='utf-8', check=True, env=env)
    executables = [x['executable'] for line in built.stdout.splitlines()
                   if (x := json.loads(line)).get('reason') == 'compiler-artifact'
                   and x.get('profile', {}).get('test') and x.get('executable')]
    assert len(executables) == 1, executables
    exe = Path(executables[0])
    digest = hashlib.sha256(exe.read_bytes()).hexdigest()

    def inventory(ignored=False):
        command = [str(exe), '--list', '--format=terse', '--color=never']
        if ignored:
            command.append('--ignored')
        output = subprocess.check_output(command, text=True, encoding='utf-8', env=env)
        return [line[:-6] for line in output.splitlines() if line.endswith(': test')]

    all_tests = inventory()
    ignored = set(inventory(True))
    active = [name for name in all_tests if name not in ignored]
    assert len(all_tests) == len(set(all_tests)) and ignored <= set(all_tests)
    ledger = json.loads(LEDGER.read_text(encoding='utf-8'))
    suite = ledger.get('release_suite_ssd', {})
    result = suite.get('result', {})
    if (suite.get('executable_sha256') == digest
            and result.get('exit_code') == 0
            and result.get('failed') == 0
            and result.get('passed') == len(active)
            and result.get('ignored') == len(ignored)
            and result.get('inventory') == len(all_tests)):
        print(f'M147_SUITE_ALREADY_PASSED {len(active)} passed; '
              f'{len(ignored)} ignored; single-process evidence; sha256={digest}', flush=True)
        return
    run = ledger.get('resumable_release_suite')
    if run is None or run['executable_sha256'] != digest:
        if run is not None:
            ledger.setdefault('superseded_resumable_suites', []).append(run)
        run = dict(executable_sha256=digest, inventory=all_tests,
                   ignored=sorted(ignored), cases={}, status='running',
                   method='serial exact cases in separate processes, stdout inherited')
        ledger['resumable_release_suite'] = run
    assert run['inventory'] == all_tests and run['ignored'] == sorted(ignored)
    run['status'] = 'running'
    save(ledger)
    print(f'M147_SUITE inventory={len(all_tests)} active={len(active)} '
          f'ignored={len(ignored)} completed={len(run["cases"])} sha256={digest}', flush=True)
    for name in active:
        if run['cases'].get(name, {}).get('exit_code') == 0:
            continue
        run['current'] = name
        save(ledger)
        print(f'M147_CASE_BEGIN {name}', flush=True)
        start = time.monotonic()
        result = subprocess.run([str(exe), name, '--exact', '--test-threads=1',
                                 '--color=never'], env=env)
        run['cases'][name] = dict(exit_code=result.returncode,
                                 seconds=time.monotonic() - start)
        if result.returncode:
            run['status'] = 'failed'
            save(ledger)
            raise SystemExit(result.returncode)
        save(ledger)
        print(f'M147_CASE_PASS completed={len(run["cases"])}/{len(active)}', flush=True)
    assert set(run['cases']) == set(active)
    assert all(x['exit_code'] == 0 for x in run['cases'].values())
    run['status'] = 'passed'
    run.pop('current', None)
    save(ledger)
    print(f'M147_SUITE_PASS {len(active)} passed; {len(ignored)} ignored; '
          f'{len(all_tests)} inventory; separate-process coverage', flush=True)


if __name__ == '__main__':
    main()
