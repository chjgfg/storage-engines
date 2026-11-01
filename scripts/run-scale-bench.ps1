# Windows PowerShell 5.1+。只运行一个库内测试进程，逐档串行；不会操作真实数据库。
[CmdletBinding()]
param(
    [string]$Rows = '600000,1000000,5000000,10000000,20000000',
    [ValidateRange(1, 100)][int]$Repeat = 1,
    [string]$OutputRoot = '',
    [string]$ScratchRoot = '',
    [switch]$AllowNetwork,
    [switch]$SelfTest,
    # 强杀/断电后，显式传上次结果目录，只清其 ownership marker 匹配的 scratch。
    [string]$CleanRun = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$Utf8 = New-Object Text.UTF8Encoding($false)

function Save-Json($Path, $Value) {
    [IO.File]::WriteAllText($Path, (ConvertTo-Json -InputObject $Value -Depth 12), $script:Utf8)
}

function Source-Manifest {
    $files = @((Get-Item (Join-Path $script:Repo 'Cargo.toml')))
    # Cargo.lock 可由首次构建生成，单独在构建后记录；不把正常生成锁文件误报成源码改动。
    $files += Get-ChildItem -LiteralPath (Join-Path $script:Repo 'src') -Recurse -File -Filter '*.rs'
    $rows = @($files | Sort-Object FullName | ForEach-Object {
        [ordered]@{
            path = $_.FullName.Substring($script:Repo.Length + 1).Replace('\', '/')
            sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    })
    $text = ($rows | ForEach-Object { $_.path + ' ' + $_.sha256 }) -join "`n"
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $hash = [BitConverter]::ToString($sha.ComputeHash($script:Utf8.GetBytes($text))).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
    return @{ hash = $hash; files = $rows }
}

function Remove-OwnedScratch($Path, $Token) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    $dir = Get-Item -LiteralPath $Path -Force
    if (-not $dir.PSIsContainer -or ($dir.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw "拒绝清理非普通目录: $Path"
    }
    $marker = Join-Path $Path '.scale-owner'
    if (-not (Test-Path -LiteralPath $marker) -or [IO.File]::ReadAllText($marker).Trim() -ne $Token) {
        throw "ownership marker 不符，未删除: $Path"
    }
    # 引擎不创建链接。遇到链接不递归删除，避免误碰目录外的文件。
    $links = @(Get-ChildItem -LiteralPath $Path -Recurse -Force | Where-Object {
        $_.Attributes -band [IO.FileAttributes]::ReparsePoint
    })
    if ($links.Count -gt 0) { throw "scratch 含链接，保留现场: $Path" }
    Remove-Item -LiteralPath $Path -Recurse -Force -Confirm:$false
    if (Test-Path -LiteralPath $Path) { throw "scratch 清理未完成: $Path" }
}

# 不用 2>&1 包装 native stderr（PS5 会变 ErrorRecord），两条流逐行排空，日志不会整份留在 RAM。
function Invoke-LoggedProcess {
    param([string]$Exe, [string]$Arguments, [string]$Log, [hashtable]$Environment = @{},
          [scriptblock]$OnLine = $null, [hashtable]$State = @{})
    $info = New-Object Diagnostics.ProcessStartInfo
    $info.FileName = $Exe
    $info.Arguments = $Arguments
    $info.WorkingDirectory = $script:Repo
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    $info.StandardOutputEncoding = $script:Utf8
    $info.StandardErrorEncoding = $script:Utf8
    foreach ($key in $Environment.Keys) {
        if ($null -eq $Environment[$key]) { $info.EnvironmentVariables.Remove($key) }
        else { $info.EnvironmentVariables[$key] = [string]$Environment[$key] }
    }
    $process = New-Object Diagnostics.Process
    $process.StartInfo = $info
    $writer = New-Object IO.StreamWriter($Log, $false, $script:Utf8)
    $started = $false
    try {
        if (-not $process.Start()) { throw "无法启动 $Exe" }
        $started = $true
        $tasks = @($process.StandardOutput.ReadLineAsync(), $process.StandardError.ReadLineAsync())
        $streams = @($process.StandardOutput, $process.StandardError)
        while ($null -ne $tasks[0] -or $null -ne $tasks[1]) {
            $pending = @($tasks | Where-Object { $null -ne $_ })
            [void][Threading.Tasks.Task]::WaitAny([Threading.Tasks.Task[]]$pending, 250)
            for ($i = 0; $i -lt 2; $i++) {
                if ($null -eq $tasks[$i] -or -not $tasks[$i].IsCompleted) { continue }
                $line = $tasks[$i].GetAwaiter().GetResult()
                if ($null -eq $line) { $tasks[$i] = $null; continue }
                $writer.WriteLine($line)
                $writer.Flush()
                if ($null -ne $OnLine) { & $OnLine $line $State | Out-Null }
                # cargo 的 JSON 仅写日志；编译诊断的 rendered 文本在下一分支显示。
                if ($line.StartsWith('{"reason":')) {
                    $json = $line | ConvertFrom-Json
                    if ($json.reason -eq 'compiler-message' -and $null -ne $json.message.rendered) {
                        [Console]::Write($json.message.rendered)
                    }
                } else { [Console]::WriteLine($line) }
                $tasks[$i] = $streams[$i].ReadLineAsync()
            }
        }
        $process.WaitForExit()
        return $process.ExitCode
    } finally {
        if ($started -and -not $process.HasExited) {
            try { $process.Kill() }
            catch { if (-not $process.HasExited) { throw } }
            $process.WaitForExit()
        }
        $writer.Dispose()
        $process.Dispose()
    }
}

# 一个仓库一次只允许一个台架脚本（不同 OutputRoot 也不能绕过这把锁）。
$targetDir = Join-Path $Repo 'target'
[void][IO.Directory]::CreateDirectory($targetDir)
$gate = [IO.File]::Open((Join-Path $targetDir 'scale-bench.lock'), [IO.FileMode]::OpenOrCreate,
    [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
$runDir = $null
$summary = New-Object 'Collections.Generic.List[object]'
$failed = $false
try {
    if ($CleanRun) {
        $manifestPath = Join-Path ([IO.Path]::GetFullPath($CleanRun)) 'run.json'
        $manifest = [IO.File]::ReadAllText($manifestPath) | ConvertFrom-Json
        if ($manifest.tool -ne 'storage-engines-scale-v1' -or $manifest.repo -ne $Repo) {
            throw '不是本仓库的台架清理清单'
        }
        foreach ($row in @($manifest.cases)) { Remove-OwnedScratch $row.scratch $row.token }
        [Console]::WriteLine('本次清单的 scratch 已清理；日志未删除。')
        return
    }

    if ($SelfTest) {
        $token = [guid]::NewGuid().ToString('N')
        $path = Join-Path $Repo ('data/scale-runner-selftest-' + $token)
        [void][IO.Directory]::CreateDirectory($path)
        [IO.File]::WriteAllText((Join-Path $path '.scale-owner'), $token, $Utf8)
        try {
            $denied = $false
            try { Remove-OwnedScratch $path 'wrong-token' } catch { $denied = $true }
            if (-not $denied -or -not (Test-Path -LiteralPath $path)) { throw 'ownership guard 自检失败' }
            $state = @{ lines = (New-Object 'Collections.Generic.List[string]') }
            $exit = Invoke-LoggedProcess -Exe (Join-Path $PSHOME 'powershell.exe') `
                -Arguments '-NoProfile -NonInteractive -Command "[Console]::Out.WriteLine(''probe-out''); [Console]::Error.WriteLine(''probe-err''); exit 7"' `
                -Log (Join-Path $path 'probe.log') -State $state -OnLine { param($Line, $State) $State.lines.Add($Line) }
            if ($exit -ne 7 -or -not $state.lines.Contains('probe-out') -or -not $state.lines.Contains('probe-err')) {
                throw '子进程退出码/两条日志流自检失败'
            }
        } finally { Remove-OwnedScratch $path $token }
        if (Test-Path -LiteralPath $path) { throw '失败子进程后的 cleanup 自检失败' }
        [Console]::WriteLine('SELFTEST PASS: exit=7 captured, stdout+stderr captured, ownership checked, scratch removed.')
        return
    }

    $sizes = @($Rows.Split(',') | ForEach-Object {
        $n = 0L
        if (-not [long]::TryParse($_.Trim(), [ref]$n) -or $n -le 0 -or $n -gt 2000000000) {
            throw "非法行数: $_"
        }
        $n
    })
    if (-not $OutputRoot) { $OutputRoot = Join-Path $Repo 'data/bench-results' }
    if (-not $ScratchRoot) { $ScratchRoot = Join-Path $Repo 'data/bench-scratch' }
    $OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
    $ScratchRoot = [IO.Path]::GetFullPath($ScratchRoot)
    [void][IO.Directory]::CreateDirectory($OutputRoot)
    [void][IO.Directory]::CreateDirectory($ScratchRoot)
    $id = (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N')
    $runDir = Join-Path $OutputRoot $id
    [void][IO.Directory]::CreateDirectory($runDir)
    $source = Source-Manifest
    Save-Json (Join-Path $runDir 'source.json') $source
    $cases = New-Object 'Collections.Generic.List[object]'
    foreach ($size in $sizes) {
        for ($pass = 1; $pass -le $Repeat; $pass++) {
            $token = [guid]::NewGuid().ToString('N')
            $cases.Add([ordered]@{ rows = $size; pass = $pass; token = $token
                scratch = (Join-Path $ScratchRoot ('scale-' + $token)) })
        }
    }
    $os = Get-CimInstance Win32_OperatingSystem
    $cpu = @(Get-CimInstance Win32_Processor | Select-Object Name,NumberOfCores,NumberOfLogicalProcessors)
    $commit = (& git -C $Repo rev-parse HEAD) -join ''
    $gitStatus = @(& git -C $Repo status --short)
    $manifest = [ordered]@{
        tool = 'storage-engines-scale-v1'; repo = $Repo; started_utc = [DateTime]::UtcNow.ToString('o')
        head = $commit; git_status = $gitStatus; source_sha256 = $source.hash
        rustc = ((& rustc --version) -join ''); powershell = $PSVersionTable.PSVersion.ToString()
        cpu = $cpu; memory_total_bytes = ([long]$os.TotalVisibleMemorySize * 1024)
        memory_free_bytes = ([long]$os.FreePhysicalMemory * 1024)
        scratch_root = $ScratchRoot; output_root = $OutputRoot; cases = @($cases.ToArray())
        memory_metric = 'Rust allocator live/peak delta, decimal MB; NOT process RSS'
        public_contract = 'same API/parameters/transaction+TTL semantics/disk format; verify internal changes'
        benchmark_policy = 'TTL read/write measured; then auxiliary keys persisted outside timing; independent migration manifest'
    }
    Save-Json (Join-Path $runDir 'run.json') $manifest
    [Console]::WriteLine("结果目录: $runDir")
    [Console]::WriteLine("规模: $($sizes -join ', ')；重复: $Repeat；逐档串行，失败即停。")

    $build = @{ executable = $null }
    $cargo = (Get-Command cargo -ErrorAction Stop).Source
    $buildArgs = 'test --release --lib --no-run --message-format=json-render-diagnostics'
    if (-not $AllowNetwork) { $buildArgs += ' --offline' }
    $buildExit = Invoke-LoggedProcess -Exe $cargo -Arguments $buildArgs -Log (Join-Path $runDir 'build.log') -State $build -OnLine {
        param($Line, $State)
        if (-not $Line.StartsWith('{')) { return }
        $item = $Line | ConvertFrom-Json
        if ($item.reason -eq 'compiler-artifact' -and $item.profile.test -and
            @($item.target.kind) -contains 'lib' -and $null -ne $item.executable) {
            $State.executable = [string]$item.executable
        }
    }
    if ($buildExit -ne 0 -or -not $build.executable) { throw '构建未完成，见 build.log；缺依赖时可加 -AllowNetwork。' }
    if ((Source-Manifest).hash -ne $source.hash) { throw '构建期间源码发生变化，拒绝混用版本；请重新运行。' }
    $manifest['executable_sha256'] = (Get-FileHash -LiteralPath $build.executable -Algorithm SHA256).Hash.ToLowerInvariant()
    $manifest['cargo_lock_sha256'] = (Get-FileHash -LiteralPath (Join-Path $Repo 'Cargo.lock') -Algorithm SHA256).Hash.ToLowerInvariant()
    Save-Json (Join-Path $runDir 'run.json') $manifest

    foreach ($case in $cases) {
        if ((Source-Manifest).hash -ne $source.hash) { throw '各档之间源码发生变化，停止，避免混合版本结果。' }
        if ((Get-FileHash -LiteralPath $build.executable -Algorithm SHA256).Hash.ToLowerInvariant() -ne $manifest.executable_sha256) {
            throw '测试二进制在档位之间被替换，拒绝混用构建。'
        }
        $name = 'rows_' + $case.rows + '_pass_' + $case.pass
        if (Test-Path -LiteralPath $case.scratch) { throw "scratch 路径已存在，拒绝复用: $($case.scratch)" }
        [void][IO.Directory]::CreateDirectory($case.scratch)
        [IO.File]::WriteAllText((Join-Path $case.scratch '.scale-owner'), $case.token, $Utf8)
        $state = @{ metrics = (New-Object 'Collections.Generic.List[object]'); engines = @{}; done = $false; rows = $case.rows; pass = $case.pass }
        $record = [ordered]@{ rows = $case.rows; pass = $case.pass; status = 'RUNNING'; exit_code = $null
            elapsed_seconds = $null; metrics = 0; cleanup = 'PENDING'; error = $null; log = ($name + '.log') }
        $summary.Add($record)
        Save-Json (Join-Path $runDir 'summary.json') @($summary.ToArray())
        $clock = [Diagnostics.Stopwatch]::StartNew()
        try {
            $childEnv = @{ SCALE_ROWS = $case.rows; SCALE_SCRATCH_DIR = $case.scratch
                TMP = $case.scratch; TEMP = $case.scratch; RUST_BACKTRACE = '1'
                VACUUM_STAGE_SPLIT = $null; OPEN_STAGE_SPLIT = $null }
            $exit = Invoke-LoggedProcess -Exe $build.executable -Arguments '--exact engine::tests::scale_baseline_all_engines --ignored --nocapture --test-threads=1' `
                -Log (Join-Path $runDir $record.log) -Environment $childEnv -State $state -OnLine {
                    param($Line, $State)
                    if ($Line -match '^SCALE_ENGINE_DONE rows=(\d+) engine=(\w+)$' -and [long]$Matches[1] -eq $State.rows) {
                        $State.engines[$Matches[2]] = $true
                    }
                    if ($Line -eq "SCALE_DONE rows=$($State.rows) engines=4") { $State.done = $true }
                    if ($Line -match '^(bitcask|bplus_tree|lsm_tree|memory)\s+(.+?)\s+(-?\d+(?:\.\d+)?)\s+(\S+)\s+常驻\s+(-?\d+(?:\.\d+)?) MB\s+峰值 \+\s*(\d+(?:\.\d+)?) MB\s*(.*)$') {
                        $State.metrics.Add([pscustomobject][ordered]@{ rows = $State.rows; pass = $State.pass
                            engine = $Matches[1]; operation = $Matches[2].Trim()
                            operation_key = [regex]::Replace($Matches[2].Trim(), '\d+', '#')
                            time_per_unit = $Matches[3]; unit = $Matches[4]; heap_live_delta_mb = $Matches[5]
                            heap_peak_delta_mb = $Matches[6]; extra = $Matches[7] })
                    }
                }
            $record.exit_code = $exit
            $record.metrics = $state.metrics.Count
            @($state.metrics.ToArray()) | Export-Csv -LiteralPath (Join-Path $runDir ($name + '.csv')) -NoTypeInformation -Encoding UTF8
            $expected = @('bitcask','bplus_tree','lsm_tree','memory')
            if ($exit -ne 0 -or -not $state.done -or $state.metrics.Count -ne 122 -or
                @($expected | Where-Object { -not $state.engines.ContainsKey($_) }).Count -ne 0) {
                throw "验收失败：exit=$exit，完成标记=$($state.done)，性能格=$($state.metrics.Count)/122"
            }
            $record.status = 'PASS'
        } catch {
            $record.status = 'FAIL'
            $record.error = $_.Exception.Message
            throw
        } finally {
            $clock.Stop()
            $record.elapsed_seconds = [Math]::Round($clock.Elapsed.TotalSeconds, 3)
            try {
                Remove-OwnedScratch $case.scratch $case.token
                $record.cleanup = 'OK'
            } catch {
                $record.cleanup = 'FAILED'
                $record.status = 'FAIL'
                $record.error = $_.Exception.Message
                throw
            } finally { Save-Json (Join-Path $runDir 'summary.json') @($summary.ToArray()) }
        }
    }
} catch {
    $failed = $true
    [Console]::Error.WriteLine($_.Exception.Message)
    if ($null -ne $runDir) { [IO.File]::WriteAllText((Join-Path $runDir 'failure.txt'), $_.ToString(), $Utf8) }
} finally { $gate.Dispose() }

if ($failed) { exit 1 }
[Console]::WriteLine("全部计划档位通过且 scratch 已清理。请返回 $runDir 下的 summary.json、CSV 和异常时的完整日志。")
