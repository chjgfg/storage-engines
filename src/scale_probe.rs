//! Scale-harness diagnostics, outside timed regions. Never enabled in the library.
use std::{assert, eprintln, format, io, panic, process::Command};

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
