//! Scale-harness diagnostics, outside timed regions. Never enabled in the library.
use std::{assert, eprintln, format, io, panic, process::Command};

fn unix_ms() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_millis()
}

/// One OS lock for this checkout, independent of the scratch directory and PID.
/// Keep the file in place: unlinking a held lock would permit a second lock inode.
pub(crate) struct RunGuard { _file: std::fs::File, name: String, started: std::time::Instant }
impl RunGuard {
    pub(crate) fn acquire(name: &str) -> Self {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/scale-bench.lock");
        let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(&path).expect("open scale benchmark lock");
        let wait = std::time::Instant::now();
        eprintln!("SCALE_LOCK_WAIT name={name} pid={} unix_ms={} path={}", std::process::id(), unix_ms(), path.display());
        fs4::FileExt::lock_exclusive(&file).expect("lock scale benchmark");
        eprintln!("SCALE_LOCK_ACQUIRED name={name} pid={} unix_ms={} wait_ms={:.3}",
            std::process::id(), unix_ms(), wait.elapsed().as_secs_f64() * 1000.0);
        Self { _file: file, name: name.into(), started: std::time::Instant::now() }
    }
}
impl Drop for RunGuard {
    fn drop(&mut self) {
        eprintln!("SCALE_LOCK_RELEASE name={} pid={} unix_ms={} active_ms={:.3}",
            self.name, std::process::id(), unix_ms(), self.started.elapsed().as_secs_f64() * 1000.0);
        // Closing the file releases the OS lock, including on unwind/process exit.
    }
}

#[derive(Debug)]
pub(crate) enum ProcessMemory {
    Available { rss_bytes: u64, private_bytes: u64, page_faults: u64 },
    Unavailable(String),
}

#[cfg(windows)]
fn read_memory() -> io::Result<ProcessMemory> {
    // Win32 PROCESS_MEMORY_COUNTERS_EX, including its SIZE_T fields.
    #[repr(C)]
    struct Counters {
        cb: u32, faults: u32, peak_rss: usize, rss: usize,
        peak_paged: usize, paged: usize, peak_nonpaged: usize, nonpaged: usize,
        pagefile: usize, peak_pagefile: usize, private: usize,
    }
    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(process: *mut std::ffi::c_void, counters: *mut Counters, cb: u32) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" { fn GetCurrentProcess() -> *mut std::ffi::c_void; }
    // SAFETY: all-zero integer fields are valid; cb is initialized to the ABI size.
    let mut counters: Counters = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of::<Counters>() as u32;
    // SAFETY: valid current-process pseudo handle, writable structure and exact size.
    if unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters.cb) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ProcessMemory::Available { rss_bytes: counters.rss as u64,
        private_bytes: counters.private as u64, page_faults: counters.faults as u64 })
}

#[cfg(not(windows))]
fn read_memory() -> io::Result<ProcessMemory> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let kb = |name: &str| -> io::Result<u64> {
        status.lines().find_map(|line| line.strip_prefix(name))
            .and_then(|v| v.split_whitespace().next()).and_then(|v| v.parse::<u64>().ok())
            .map(|v| v * 1024).ok_or_else(|| io::Error::other("missing process memory field"))
    };
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let tail = stat.rsplit_once(')').ok_or_else(|| io::Error::other("invalid process stat"))?.1;
    let fields: Vec<_> = tail.split_whitespace().collect();
    let faults = |i| -> io::Result<u64> { fields.get(i).and_then(|v: &&str| v.parse().ok())
        .ok_or_else(|| io::Error::other("missing page fault count")) };
    Ok(ProcessMemory::Available { rss_bytes: kb("VmRSS:")?,
        private_bytes: kb("RssAnon:")?, page_faults: faults(7)? + faults(9)? })
}

pub(crate) fn memory(engine: &str, phase: &str) {
    let heap = crate::mem_probe::live();
    match read_memory().unwrap_or_else(|e| ProcessMemory::Unavailable(e.to_string())) {
        ProcessMemory::Available { rss_bytes, private_bytes, page_faults } =>
            eprintln!("SCALE_MEMORY engine={engine} phase={phase} heap_bytes={heap} rss_bytes={rss_bytes} private_bytes={private_bytes} page_faults={page_faults}"),
        ProcessMemory::Unavailable(error) =>
            eprintln!("SCALE_MEMORY engine={engine} phase={phase} heap_bytes={heap} process_metrics_unavailable={error:?}"),
    }
}

#[cfg(windows)]
fn cpu_ns() -> io::Result<u128> {
    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct FileTime { low: u32, high: u32 }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut std::ffi::c_void;
        fn GetProcessTimes(process: *mut std::ffi::c_void, creation: *mut FileTime,
            exit: *mut FileTime, kernel: *mut FileTime, user: *mut FileTime) -> i32;
    }
    let mut times = [FileTime::default(); 4];
    // SAFETY: current-process handle and four distinct initialized FILETIME buffers.
    if unsafe { GetProcessTimes(GetCurrentProcess(), &mut times[0], &mut times[1],
        &mut times[2], &mut times[3]) } == 0 { return Err(io::Error::last_os_error()); }
    let ticks = |t: FileTime| ((t.high as u64) << 32) | t.low as u64;
    Ok((ticks(times[2]) as u128 + ticks(times[3]) as u128) * 100)
}

#[cfg(not(windows))]
fn cpu_ns() -> io::Result<u128> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "process CPU timer unavailable on this target"))
}

/// Process CPU includes all threads; allocations and wall time are inclusive.
/// Fault deltas alone do not distinguish disk paging from other page faults.
pub(crate) struct Phase {
    wall: std::time::Instant,
    cpu: io::Result<u128>,
    allocations: u64,
    process: io::Result<ProcessMemory>,
}
impl Phase {
    pub(crate) fn begin(engine: &str, phase: &str) -> Self {
        eprintln!("SCALE_PHASE_BEGIN engine={engine} phase={phase} pid={} unix_ms={}", std::process::id(), unix_ms());
        Self::start()
    }
    pub(crate) fn start() -> Self {
        let process = read_memory();
        let cpu = cpu_ns();
        Self { wall: std::time::Instant::now(), cpu, process, allocations: crate::mem_probe::allocations() }
    }
    pub(crate) fn finish(self, engine: &str, phase: &str) {
        let wall_ms = self.wall.elapsed().as_secs_f64() * 1e3;
        let cpu_ms = self.cpu.and_then(|start| cpu_ns().map(|end| end.saturating_sub(start) as f64 / 1e6));
        let faults = self.process.and_then(|before| read_memory().and_then(|after| match (before, after) {
            (ProcessMemory::Available { page_faults: a, .. }, ProcessMemory::Available { page_faults: b, .. }) => Ok(b.saturating_sub(a)),
            _ => Err(io::Error::other("process page fault counters unavailable")),
        }));
        eprintln!("SCALE_PHASE engine={engine} phase={phase} wall_ms={wall_ms:.3} cpu_ms={cpu_ms:?} allocations={} page_faults_delta={faults:?} unix_ms={}",
            crate::mem_probe::allocations().saturating_sub(self.allocations), unix_ms());
    }
}

#[test]
#[ignore = "subprocess helper for scale lock test"]
fn scale_lock_child() {
    let path = std::env::var_os("SCALE_LOCK_TEST_PATH").expect("parent supplies lock path");
    let file = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let locked = fs4::FileExt::try_lock_exclusive(&file);
    if std::env::var("SCALE_LOCK_TEST_HELD").unwrap() == "yes" {
        let error = locked.expect_err("another process owns the benchmark lock");
        #[cfg(windows)]
        assert!(error.raw_os_error() == Some(33), "expected ERROR_LOCK_VIOLATION: {error}");
        #[cfg(not(windows))]
        assert!(error.kind() == io::ErrorKind::WouldBlock, "unexpected lock error: {error}");
    } else { locked.expect("closing the owner must release the lock"); }
}

#[test]
fn scale_lock_excludes_other_process_and_releases_on_close() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/scale-bench.lock");
    let owner = RunGuard::acquire("cross_process_guard");
    let child = |held| {
        let out = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "scale_probe::scale_lock_child", "--ignored", "--nocapture"])
            .env("SCALE_LOCK_TEST_PATH", &path).env("SCALE_LOCK_TEST_HELD", held).output().unwrap();
        assert!(out.status.success(), "child failed: {} {}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    };
    child("yes");
    drop(owner);
    child("no");
}

fn output(command: &mut Command) -> io::Result<String> {
    #[cfg(windows)] {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    let out = command.output()?;
    if !out.status.success() { return Err(io::Error::other(String::from_utf8_lossy(&out.stderr).into_owned())); }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn executable_sha256() -> io::Result<String> {
        let exe = std::env::current_exe()?;
        #[cfg(windows)]
        let hash = output(Command::new("powershell.exe").args(["-NoProfile", "-NonInteractive", "-Command",
            "$h=[Security.Cryptography.SHA256]::Create(); $f=[IO.File]::OpenRead($env:SCALE_PROBE_EXE); try { [BitConverter]::ToString($h.ComputeHash($f)).Replace('-', '') } finally { $f.Dispose(); $h.Dispose() }"])
            .env("SCALE_PROBE_EXE", &exe))?;
        #[cfg(not(windows))]
        let hash = output(Command::new("sha256sum").arg(&exe))?.split_whitespace().next()
            .ok_or_else(|| io::Error::other("missing SHA256"))?.to_owned();
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(io::Error::other("invalid executable SHA256"));
        }
        Ok(hash.to_ascii_lowercase())
}

pub(crate) fn fingerprint() {
    let sha = executable_sha256().unwrap_or_else(|e| format!("unavailable:{e}"));
    // This is the checkout at execution time, NOT a claimed compile-time Git ID.
    let git = output(Command::new("git").args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))).unwrap_or_else(|e| format!("unavailable:{e}"));
    let dirty = output(Command::new("git").args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))).map(|s| !s.is_empty());
    eprintln!("SCALE_BUILD harness=2 executable_sha256={sha:?} runtime_git={git:?} tracked_dirty={dirty:?}");
}

#[test]
fn scale_probe_reports_process_memory_and_executable() {
    let memory = read_memory().expect("process memory metrics");
    match &memory {
        ProcessMemory::Available { rss_bytes, private_bytes, .. } => {
            assert!(*rss_bytes > 0);
            assert!(*private_bytes > 0);
        }
        ProcessMemory::Unavailable(error) => panic!("{error}"),
    }
    let sha = executable_sha256().expect("executable SHA256");
    assert!(sha.len() == 64 && sha.bytes().all(|b| b.is_ascii_hexdigit()));
    eprintln!("SCALE_PROBE_VERIFIED sha256={sha} memory={memory:?}");
}
