"""Rebuild M147 SSD measurements from unfiltered native test receipts."""
import json
import os
from pathlib import Path
import re
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / 'docs/performance-m147.json'
PHASES = ['insert', 'checkpoint', 'first_reopen', 'random_read', 'scan_page',
          'search_first', 'update', 'health', 'vacuum', 'flush', 'bulk', 'bulk_reopen']
WORK = ['input_files', 'input_bytes', 'trivial_moves', 'moved_bytes',
        'output_files', 'output_bytes', 'owned_input_rows']
PATTERN = re.compile(
    r'LSM_ALLOC_PHASE rows=(\d+) round=(\d+) reference=(true|false) phase=(\w+) '
    r'seconds=([\d.]+) allocations=(\d+) prepare_ns=(\d+) write_ns=(\d+) '
    r'commit_ns=(\d+) stats=Stats \{ ([^}]+) \}')


def measure(run, digest):
    rows = run['rows']
    chunks = run['chunks']
    raw = ''.join(part['output'] for part in chunks)
    assert run['exit_code'] == 0
    assert f'executable_sha256="{digest}"' in raw
    assert f'LSM_ALLOC_SCALE_PASS rows={rows}\n' in raw
    assert 'test result: ok. 1 passed; 0 failed;' in raw
    receipts = {}
    for part in chunks:
        receipts.setdefault(part['chunk_id'], []).append(part)
    for parts in receipts.values():
        assert [p['part'] for p in parts] == list(range(parts[0]['parts']))
    matches = PATTERN.findall(raw)
    assert len(matches) == 48, (rows, len(matches))
    rounds = [[] for _ in range(4)]
    for n, arm, ref, phase, sec, alloc, prepare, write, commit, stats in matches:
        arm = int(arm)
        assert int(n) == rows and (ref == 'true') == (arm in [0, 3])
        rounds[arm].append(dict(phase=phase, seconds=float(sec), allocations=int(alloc),
                               prepare_ns=int(prepare), write_ns=int(write), commit_ns=int(commit),
                               stats={k: int(v) for k, v in re.findall(r'(\w+): (\d+)', stats)}))
    for arm, phases in enumerate(rounds):
        assert [p['phase'] for p in phases] == PHASES
        ref = str(arm in [0, 3]).lower()
        marker = f'LSM_ALLOC_VERIFIED rows={rows} round={arm} reference={ref} unique_keys={rows + rows // 10}'
        assert raw.count(marker + '\n') == 1
    for new in [1, 2]:
        for old in [0, 3]:
            assert rounds[new][0]['allocations'] + 3 * rows <= rounds[old][0]['allocations']
            for phase in [3, 8]:
                assert rounds[new][phase]['allocations'] < rounds[old][phase]['allocations']
            for new_phase, old_phase in zip(rounds[new], rounds[old]):
                assert [new_phase['stats'][k] for k in WORK] == [old_phase['stats'][k] for k in WORK]
    summary = {}
    for phase in PHASES + ['measured_total']:
        if phase == 'measured_total':
            values = [sum(p['seconds'] for p in arm) for arm in rounds]
            allocations = [sum(p['allocations'] for p in arm) for arm in rounds]
        else:
            idx = PHASES.index(phase)
            values = [arm[idx]['seconds'] for arm in rounds]
            allocations = [arm[idx]['allocations'] for arm in rounds]
        old, new = mean([values[0], values[3]]), mean(values[1:3])
        entry = dict(seconds_abba=values, old_seconds=old, new_seconds=new,
                     delta_percent=(new / old - 1) * 100,
                     allocations_abba=allocations)
        if phase in ['insert', 'update']:
            idx = PHASES.index(phase)
            entry['substages_ns_abba'] = [{k: arm[idx][k] for k in ['prepare_ns', 'write_ns', 'commit_ns']}
                                        for arm in rounds]
        summary[phase] = entry
    return dict(rounds=rounds, summary=summary, verified_arms=4, phase_records=48,
                compaction_work_equal_at_every_phase=True,
                measured_total_scope='Sum of 12 timed phases; excludes final value verification and teardown')


def main():
    data = json.loads(LEDGER.read_text(encoding='utf-8'))
    bench = data['ssd_benchmark']
    for run in bench['runs']:
        if run.get('exit_code') is None:
            print(f'M147_BENCH_PENDING rows={run["rows"]}')
            continue
        run['measurement'] = measure(run, bench['executable_sha256'])
        run['status'] = 'passed'
        for phase in ['insert', 'random_read', 'scan_page', 'search_first', 'update', 'vacuum', 'measured_total']:
            v = run['measurement']['summary'][phase]
            print(f'M147_COMPARISON rows={run["rows"]} phase={phase} '
                  f'old={v["old_seconds"]:.6f}s new={v["new_seconds"]:.6f}s delta={v["delta_percent"]:+.2f}%')
    complete = {r['rows'] for r in bench['runs'] if r.get('status') == 'passed'}
    bench['status'] = 'passed' if complete == set(bench['rows']) else 'running'
    with LEDGER.open('w', encoding='utf-8') as stream:
        stream.write(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    assert json.loads(LEDGER.read_text(encoding='utf-8')) == data
    print(f'M147_BENCH_COVERAGE {len(complete)}/{len(bench["rows"])} scales')


if __name__ == '__main__':
    main()
