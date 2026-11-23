//! 本文件负责后台压实生命周期。

use super::*;

impl LSMTree {
    /// 打开列族目录 `dir/cf/<name>/`（独立 MANIFEST/SST/mem.wal）
    pub fn open_column_family(&self, name: &str, opts: LSMTreeOptions) -> std::io::Result<LSMTree> {
        let cf_dir = self.dir.join("cf").join(name);
        Self::open_with_config(cf_dir, opts)
    }

    pub(super) fn notify_bg(&self) {
        if let Some(bg) = &self.bg {
            let (lock, cv) = &*bg.notify;
            if let Ok(mut g) = lock.lock() {
                *g = true;
                cv.notify_one();
            }
        }
    }

    pub(super) fn start_bg_compact(&mut self) {
        if self.bg.is_some() {
            return;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let notify = Arc::new((Mutex::new(false), Condvar::new()));
        self.bg = Some(BgCompactCtrl {
            stop,
            notify,
            handle: None,
        });
    }

    /// 启用后台 compact：需 `Arc<Mutex<LSMTree>>`，自动起线程
    pub fn spawn_embedded_compactor(tree: Arc<Mutex<LSMTree>>) -> JoinHandle<()> {
        {
            let mut t = tree.lock().unwrap();
            t.opts.enable_bg_compact = true;
            t.start_bg_compact();
        }
        let stop = tree.lock().unwrap().bg.as_ref().unwrap().stop.clone();
        let notify = tree.lock().unwrap().bg.as_ref().unwrap().notify.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let (lock, cv) = &*notify;
                let guard = lock.lock().unwrap();
                let (mut guard, _) = cv.wait_timeout(guard, Duration::from_millis(100)).unwrap();
                *guard = false;
                drop(guard);
                if let Ok(mut t) = tree.lock() {
                    let _ = t.compact_once();
                }
            }
            if let Ok(mut t) = tree.lock() {
                let _ = t.compact_once();
            }
        })
    }
}

/// 后台 compact 循环（教学用）：持 `Arc<Mutex<LSMTree>>`，定期 `compact_once`。
///
/// 用法：
/// ```ignore
/// let tree = Arc::new(Mutex::new(LSMTree::open_with_config(dir, opts)?));
/// let stop = Arc::new(AtomicBool::new(false));
/// let h = spawn_bg_compactor(tree.clone(), stop.clone());
/// // ... 写入 ...
/// stop.store(true, Ordering::SeqCst);
/// h.join().ok();
/// ```
pub fn spawn_bg_compactor(
    tree: Arc<Mutex<LSMTree>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(50));
            if let Ok(mut t) = tree.lock() {
                let _ = t.compact_once();
            }
        }
        // 退出前再压一轮
        if let Ok(mut t) = tree.lock() {
            let _ = t.compact_once();
        }
    })
}
