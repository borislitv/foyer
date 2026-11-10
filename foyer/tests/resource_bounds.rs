//! Opt-in block-engine resource limits through public cache APIs.
#![forbid(unsafe_code)]
use foyer::{
    BlockEngineConfig, Compression, DefaultHasher, DeviceBuilder, FileDeviceBuilder, HybridCache, HybridCacheBuilder,
    RecoverMode,
};
use foyer::{Code, SubmissionLimits};
use foyer_common::properties::Properties;
use foyer_storage::test_utils::Switch;
use std::{
    fs::OpenOptions,
    hash::{BuildHasher, Hasher},
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::Path,
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Waker},
    time::Duration,
};

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn guarded_insert<V: foyer::StorageValue>(cache: &HybridCache<u64, V>, key: u64, value: V) -> foyer::Result<()> {
    // A separate source cache has no HybridCache eviction listener invoking
    // legacy void enqueue behind the fallible adapter's back.
    let source = foyer::CacheBuilder::new(1).build::<foyer::HybridCacheProperties>();
    let entry = source.insert_with_properties(key, value, foyer::HybridCacheProperties::default().with_phantom(true));
    cache.storage().try_enqueue(entry.piece(), true)
}

#[test]
fn completed_unrelated_payload_survives_graceful_and_abrupt_child_exit() {
    for mode in ["graceful", "abrupt"] {
        let dir = tempfile::tempdir().unwrap();
        let output = std::process::Command::new("timeout")
            .args(["--kill-after=2s", "15s"])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", "interruption_child", "--nocapture"])
            .env("FOYER_GUARD_CHILD_DIR", dir.path())
            .env("FOYER_GUARD_CHILD_EXIT", mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (cache, _) = bounded::<Vec<u8>>(dir.path(), 128 * 1024, 8, Switch::default()).await;
                assert_eq!(
                    cache.storage().load(&7).await.unwrap().kv(),
                    Some((7, b"completed unrelated".to_vec())),
                    "{mode}: completed inventory must not be reset"
                );
                assert!(
                    cache.storage().load(&1).await.unwrap().kv().is_none(),
                    "{mode}: completed delete must not resurrect"
                );
                assert_eq!(
                    cache.storage().load(&8).await.unwrap().kv(),
                    if mode == "graceful" {
                        Some((8, b"pending optional".to_vec()))
                    } else {
                        None
                    },
                    "{mode}: only actually completed work may be published"
                );
                cache.close().await.unwrap();
            });
    }
}

#[tokio::test]
async fn interruption_child() {
    let Some(path) = std::env::var_os("FOYER_GUARD_CHILD_DIR") else {
        return;
    };
    let switch = Switch::default();
    let (cache, _) = bounded::<Vec<u8>>(Path::new(&path), 128 * 1024, 8, switch.clone()).await;
    guarded_insert(&cache, 1, b"completed then deleted".to_vec()).unwrap();
    cache.storage().wait_guarded().await.unwrap();
    guarded_insert(&cache, 7, b"completed unrelated".to_vec()).unwrap();
    cache.storage().wait_guarded().await.unwrap();
    cache.storage().try_delete(&1).unwrap();
    cache.storage().wait_guarded().await.unwrap();
    switch.on();
    guarded_insert(&cache, 8, b"pending optional".to_vec()).unwrap();
    for _ in 0..6 {
        cache.storage().try_delete(&2).unwrap();
    }
    assert_eq!(
        cache.storage().try_delete(&7).unwrap_err().kind(),
        foyer::ErrorKind::NoSpace
    );
    assert!(cache.storage().load(&8).await.unwrap().kv().is_none());
    if std::env::var("FOYER_GUARD_CHILD_EXIT").unwrap() == "abrupt" {
        std::process::exit(0);
    }
    switch.off();
    cache.close().await.unwrap();
}

#[test]
fn impossible_submission_capacity_is_rejected_without_allocating_it() {
    let result = SubmissionLimits::new(nz(usize::MAX), nz(1));
    assert!(matches!(result,Err(error) if error.kind()==foyer::ErrorKind::OutOfRange));
}

#[tokio::test]
async fn guarded_legacy_void_mutations_fail_explicitly_before_changing_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let (cache, limits) = bounded::<Vec<u8>>(dir.path(), 128 * 1024, 8, Switch::default()).await;
    guarded_insert(&cache, 7, b"unchanged".to_vec()).unwrap();
    cache.storage().wait_guarded().await.unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cache.storage().delete(&7))).is_err());
    let source = foyer::CacheBuilder::new(1).build::<foyer::HybridCacheProperties>();
    let entry = source.insert(7, b"unsupported overwrite".to_vec());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cache
            .storage()
            .enqueue(entry.piece(), true)))
        .is_err()
    );
    assert_eq!(
        cache.storage().load(&7).await.unwrap().kv(),
        Some((7, b"unchanged".to_vec()))
    );
    assert_eq!((limits.items(), limits.bytes()), (0, 0));
    assert!(!limits.is_poisoned());
    cache.close().await.unwrap();
}

#[tokio::test]
async fn unsafe_recovery_returns_error_without_writing_cache_media() {
    let dir = tempfile::tempdir().unwrap();
    let (cache, _) = bounded::<Vec<u8>>(dir.path(), 128 * 1024, 8, Switch::default()).await;
    guarded_insert(&cache, 7, b"completed protected payload".to_vec()).unwrap();
    cache.storage().wait_guarded().await.unwrap();
    cache.close().await.unwrap();
    drop(cache);
    // The existing tombstone API rejects a nonzero legacy slot zero as unsafe
    // rollover history. Use that documented error boundary, not a new format.
    let path = dir.path().join("bounded");
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.write_all(b"unsafe!!").unwrap();
    drop(file);
    let before = std::fs::read(&path).unwrap();
    for mode in [RecoverMode::Strict, RecoverMode::Quiet, RecoverMode::None] {
        let config = BlockEngineConfig::new(
            FileDeviceBuilder::new(&path)
                .with_capacity(2 * 1024 * 1024)
                .build()
                .unwrap(),
        )
        .with_block_size(64 * 1024)
        .with_buffer_pool_size(64 * 1024)
        .with_index_capacity(nz(128))
        .with_tombstone_log(true)
        .with_read_limits(nz(16 * 1024), Compression::None)
        .with_submission_limits(SubmissionLimits::new(nz(128 * 1024), nz(8)).unwrap());
        let result = HybridCacheBuilder::<u64, Vec<u8>>::new()
            .memory(1)
            .storage()
            .with_recover_mode(mode)
            .with_engine_config(config)
            .build()
            .await;
        let error = result.unwrap_err();
        assert_eq!(
            error.kind(),
            if mode == RecoverMode::Strict {
                foyer::ErrorKind::Recover
            } else {
                foyer::ErrorKind::Config
            }
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "{mode:?}: rejected inventory must not erase or rewrite media"
        );
    }
}

async fn bounded<V: foyer::StorageValue>(
    path: &Path,
    bytes: usize,
    items: usize,
    switch: Switch,
) -> (HybridCache<u64, V>, SubmissionLimits) {
    bounded_io(path, bytes, items, switch, None).await
}

async fn bounded_io<V: foyer::StorageValue>(
    path: &Path,
    bytes: usize,
    items: usize,
    switch: Switch,
    io: Option<Box<dyn foyer::IoEngineConfig>>,
) -> (HybridCache<u64, V>, SubmissionLimits) {
    let device = FileDeviceBuilder::new(path.join("bounded"))
        .with_capacity(2 * 1024 * 1024)
        .build()
        .unwrap();
    let limits = SubmissionLimits::new(nz(bytes), nz(items)).unwrap();
    let config = BlockEngineConfig::new(device)
        .with_block_size(64 * 1024)
        .with_buffer_pool_size(64 * 1024)
        .with_index_capacity(nz(128))
        .with_tombstone_log(true)
        .with_read_limits(nz(16 * 1024), Compression::None)
        .with_submission_limits(limits.clone())
        .with_flush_switch(switch);
    let builder = HybridCacheBuilder::new()
        .with_flush_on_close(false)
        .memory(1)
        .storage()
        .with_recover_mode(RecoverMode::Strict)
        .with_engine_config(config);
    let builder = if let Some(io) = io {
        builder.with_io_engine_config(io)
    } else {
        builder
    };
    let cache = builder.build().await.unwrap();
    (cache, limits)
}

#[tokio::test]
async fn rejected_required_invalidation_preserves_mapping_and_does_not_poison() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        let (cache, limits) = bounded::<Vec<u8>>(dir.path(), 256 * 1024, 4, switch.clone()).await;
        guarded_insert(&cache, 1, b"old bytes".to_vec()).unwrap();
        cache.storage().wait_guarded().await.unwrap();
        switch.on();
        // Exhaust the item budget with unrelated work. A rejected mutation has
        // not been accepted: it must leave the old mapping and session usable.
        for _ in 0..4 {
            cache.storage().try_delete(&2).unwrap();
        }
        assert_eq!(limits.items(), 4);
        assert_eq!(limits.bytes(), 4 * SubmissionLimits::METADATA_BYTES);
        assert!(!limits.is_poisoned());
        assert_eq!(
            cache.storage().try_delete(&1).unwrap_err().kind(),
            foyer::ErrorKind::NoSpace
        );
        assert!(!limits.is_poisoned());
        assert_eq!(limits.items(), 4);
        for key in 2..100 {
            assert_eq!(
                cache.storage().try_delete(&key).unwrap_err().kind(),
                foyer::ErrorKind::NoSpace
            );
        }
        assert_eq!(limits.items(), 4, "rejected work must not grow the queue");
        assert_eq!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"old bytes".to_vec()))
        );
        assert!(cache.storage().may_contains(&1));
        switch.off();
        cache.storage().wait_guarded().await.unwrap();
        assert_eq!((limits.items(), limits.bytes()), (0, 0));
        cache.close().await.unwrap();
        drop(cache);
        let (cache, limits) = bounded::<Vec<u8>>(dir.path(), 256 * 1024, 4, Switch::default()).await;
        assert_eq!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"old bytes".to_vec()))
        );
        cache.storage().try_delete(&1).unwrap();
        cache.storage().wait_guarded().await.unwrap();
        assert!(cache.storage().load(&1).await.unwrap().kv().is_none());
        guarded_insert(&cache, 1, b"new generation".to_vec()).unwrap();
        cache.storage().wait_guarded().await.unwrap();
        assert_eq!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"new generation".to_vec()))
        );
        assert_eq!(limits.items(), 0);
        cache.close().await.unwrap();
        drop(cache);
        let (cache, _) = bounded::<Vec<u8>>(dir.path(), 256 * 1024, 4, Switch::default()).await;
        assert_eq!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"new generation".to_vec())),
            "ordered recovery preserves valid inventory"
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn byte_overflow_rejects_replacement_without_accepting_any_mutation() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        let (cache, limits) = bounded::<Vec<u8>>(dir.path(), 17 * 1024 + 512, 100, switch.clone()).await;
        guarded_insert(&cache, 1, b"old bytes".to_vec()).unwrap();
        cache.storage().wait_guarded().await.unwrap();
        switch.on();
        // Plenty of items remain, but another payload cannot fit. The temporary
        // invalidation reservation must roll back without accepting an overwrite.
        guarded_insert(&cache, 2, vec![2]).unwrap();
        assert_eq!(
            guarded_insert(&cache, 1, vec![3]).unwrap_err().kind(),
            foyer::ErrorKind::NoSpace
        );
        assert!(limits.bytes() <= 17 * 1024 + 512);
        assert!(limits.items() < 100);
        assert_eq!(limits.items(), 2);
        assert_eq!(limits.bytes(), 16 * 1024 + 2 * SubmissionLimits::METADATA_BYTES);
        assert_eq!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"old bytes".to_vec()))
        );
        cache.storage().try_delete(&2).unwrap();
        assert_eq!(
            cache.storage().try_delete(&1).unwrap_err().kind(),
            foyer::ErrorKind::NoSpace
        );
        assert!(!limits.is_poisoned());
        switch.off();
        cache.storage().wait_guarded().await.unwrap();
        cache.close().await.unwrap();
        drop(cache);
        let (cache, _) = bounded::<Vec<u8>>(dir.path(), 17 * 1024 + 512, 100, Switch::default()).await;
        assert_eq!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"old bytes".to_vec()))
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn completed_delete_survives_reopen_while_unrelated_completed_payload_is_retained() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        let (cache, limits) = bounded::<Vec<u8>>(dir.path(), 17 * 1024 + 512, 100, switch.clone()).await;
        guarded_insert(&cache, 1, b"old bytes".to_vec()).unwrap();
        cache.storage().wait_guarded().await.unwrap();
        switch.on();
        guarded_insert(&cache, 2, vec![2]).unwrap();
        cache.storage().try_delete(&1).unwrap();
        assert!(!limits.is_poisoned());
        assert_eq!(limits.bytes(), 17 * 1024 + 512);
        assert!(cache.storage().load(&1).await.unwrap().kv().is_none());
        switch.off();
        cache.storage().wait().await;
        assert_eq!(limits.bytes(), 0);
        assert!(cache.storage().load(&1).await.unwrap().kv().is_none());
        assert_eq!(cache.storage().load(&2).await.unwrap().kv(), Some((2, vec![2])));
        cache.close().await.unwrap();
        drop(cache);
        let (cache, _) = bounded::<Vec<u8>>(dir.path(), 17 * 1024 + 512, 100, Switch::default()).await;
        assert!(
            cache.storage().load(&1).await.unwrap().kv().is_none(),
            "clean reopen must preserve the funded invalidation"
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn delete_of_pending_piece_never_returns_it_and_unrelated_reads_progress() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        let (cache, limits) = bounded::<Vec<u8>>(dir.path(), 128 * 1024, 8, switch.clone()).await;
        guarded_insert(&cache, 7, b"independent readable bytes".to_vec()).unwrap();
        cache.storage().wait().await;
        switch.on();
        guarded_insert(&cache, 1, b"pending obsolete bytes".to_vec()).unwrap();
        cache.storage().try_delete(&1).unwrap();
        assert!(!limits.is_poisoned());
        assert!(
            cache.storage().load(&1).await.unwrap().kv().is_none(),
            "pending sources must not bypass invalidation"
        );
        assert_eq!(
            cache.storage().load(&7).await.unwrap().kv(),
            Some((7, b"independent readable bytes".to_vec()))
        );
        for _ in 0..128 {
            let mut wait = Box::pin(cache.storage().wait());
            assert!(wait.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            drop(wait);
        }
        assert_eq!(
            limits.items(),
            3,
            "cancelled control waits must not consume/return payload credits"
        );
        switch.off();
        cache.storage().wait().await;
        assert_eq!(limits.items(), 0);
        assert!(cache.storage().load(&1).await.unwrap().kv().is_none());
        guarded_insert(&cache, 8, b"progress after drain".to_vec()).unwrap();
        cache.storage().wait().await;
        assert_eq!(
            cache.storage().load(&8).await.unwrap().kv(),
            Some((8, b"progress after drain".to_vec()))
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[derive(Debug)]
struct ReadGate {
    armed: std::sync::atomic::AtomicBool,
    reads: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    fail_write: std::sync::atomic::AtomicBool,
    failed: tokio::sync::Notify,
    hold_log: std::sync::atomic::AtomicBool,
    log_entered: tokio::sync::Notify,
    log_release: tokio::sync::Semaphore,
}
impl ReadGate {
    fn new() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            reads: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            fail_write: std::sync::atomic::AtomicBool::new(false),
            failed: tokio::sync::Notify::new(),
            hold_log: std::sync::atomic::AtomicBool::new(false),
            log_entered: tokio::sync::Notify::new(),
            log_release: tokio::sync::Semaphore::new(0),
        }
    }
}
#[derive(Debug)]
struct GateConfig(Arc<ReadGate>);
#[derive(Debug)]
struct GateIo {
    inner: Arc<dyn foyer::IoEngine>,
    gate: Arc<ReadGate>,
}
impl foyer::IoEngineConfig for GateConfig {
    fn build(
        self: Box<Self>,
        ctx: foyer::IoEngineBuildContext,
    ) -> futures_util::future::BoxFuture<'static, foyer::Result<Arc<dyn foyer::IoEngine>>> {
        Box::pin(async move {
            let inner = Box::new(foyer::PsyncIoEngineConfig::new()).build(ctx).await?;
            Ok(Arc::new(GateIo { inner, gate: self.0 }) as Arc<dyn foyer::IoEngine>)
        })
    }
}
impl foyer::IoEngine for GateIo {
    fn read(
        &self,
        buf: foyer_storage::IoReadBuffer,
        partition: &dyn foyer_storage::Partition,
        offset: u64,
    ) -> foyer::IoHandle {
        let held = buf.len() == 20 * 1024 && self.gate.armed.load(Ordering::SeqCst);
        let io = self.inner.read(buf, partition, offset);
        let gate = self.gate.clone();
        foyer::IoHandle::from(Box::pin(async move {
            if held {
                gate.reads.fetch_add(1, Ordering::SeqCst);
                gate.entered.notify_one();
                gate.release.acquire().await.unwrap().forget();
            }
            io.await
        }) as futures_util::future::BoxFuture<'static, _>)
    }
    fn write(
        &self,
        buf: foyer_storage::IoWriteBuffer,
        partition: &dyn foyer_storage::Partition,
        offset: u64,
    ) -> foyer::IoHandle {
        let fail = partition.id() != 0 && self.gate.fail_write.swap(false, Ordering::SeqCst);
        let hold = partition.id() == 0 && self.gate.hold_log.load(Ordering::SeqCst);
        let io = self.inner.write(buf, partition, offset);
        let gate = self.gate.clone();
        foyer::IoHandle::from(Box::pin(async move {
            if hold {
                gate.log_entered.notify_one();
                gate.log_release.acquire().await.unwrap().forget();
            }
            let (buf, res) = io.await;
            if fail {
                gate.failed.notify_one();
                (
                    buf,
                    Err(foyer::Error::io_error(std::io::Error::other(
                        "injected completed-write failure",
                    ))),
                )
            } else {
                (buf, res)
            }
        }) as futures_util::future::BoxFuture<'static, _>)
    }
}
#[derive(Debug)]
struct KeepHash(u64);
impl foyer::StorageFilterCondition for KeepHash {
    fn filter(&self, _: &Arc<foyer::Statistics>, hash: u64, _: usize) -> foyer::StorageFilterResult {
        if hash == self.0 {
            foyer::StorageFilterResult::Admit
        } else {
            foyer::StorageFilterResult::Reject
        }
    }
}

#[tokio::test]
async fn failed_write_keeps_credits_until_sibling_log_io_drains_and_reports_failed_completion() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let gate = Arc::new(ReadGate::new());
        let (cache, limits) = bounded_io::<Vec<u8>>(
            dir.path(),
            128 * 1024,
            8,
            Switch::default(),
            Some(Box::new(GateConfig(gate.clone()))),
        )
        .await;
        guarded_insert(&cache, 1, b"old bytes".to_vec()).unwrap();
        cache.storage().wait().await;
        guarded_insert(&cache, 7, b"unrelated completed".to_vec()).unwrap();
        cache.storage().wait_guarded().await.unwrap();
        gate.fail_write.store(true, Ordering::SeqCst);
        gate.hold_log.store(true, Ordering::SeqCst);
        guarded_insert(&cache, 1, b"replacement".to_vec()).unwrap();
        gate.failed.notified().await;
        gate.log_entered.notified().await;
        assert_eq!(
            (limits.items(), limits.bytes()),
            (2, 16 * 1024 + 2 * SubmissionLimits::METADATA_BYTES),
            "failed payload must not return credits while sibling log buffer is active"
        );
        assert!(cache.storage().load(&1).await.unwrap().kv().is_none());
        gate.hold_log.store(false, Ordering::SeqCst);
        gate.log_release.add_permits(1);
        assert_eq!(
            cache.storage().wait_guarded().await.unwrap_err().kind(),
            foyer::ErrorKind::Io
        );
        assert!(limits.is_poisoned());
        assert_eq!((limits.items(), limits.bytes()), (0, 0));
        assert!(cache.close().await.is_err());
        drop(cache);
        let (cache, _) = bounded::<Vec<u8>>(dir.path(), 128 * 1024, 8, Switch::default()).await;
        // The wrapper reports a write error AFTER the real write completed.
        // No success was returned to the caller; recovery may find the new
        // bytes or a miss, but never resurrect the invalidated old value.
        assert_ne!(
            cache.storage().load(&1).await.unwrap().kv(),
            Some((1, b"old bytes".to_vec()))
        );
        assert_eq!(
            cache.storage().load(&7).await.unwrap().kv(),
            Some((7, b"unrelated completed".to_vec()))
        );
        guarded_insert(&cache, 2, b"post-failure progress".to_vec()).unwrap();
        cache.storage().wait().await;
        assert_eq!(
            cache.storage().load(&2).await.unwrap().kv(),
            Some((2, b"post-failure progress".to_vec()))
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn raw_hot_reinsertion_remains_funded_after_wait_cancellation_and_makes_progress() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let dir = tempfile::tempdir().unwrap();
        let gate = Arc::new(ReadGate::new());
        let device = FileDeviceBuilder::new(dir.path().join("cache"))
            .with_capacity(2 * 1024 * 1024)
            .build()
            .unwrap();
        let limits = SubmissionLimits::new(nz(256 * 1024), nz(8)).unwrap();
        let config = BlockEngineConfig::new(device)
            .with_block_size(64 * 1024)
            .with_buffer_pool_size(64 * 1024)
            .with_index_capacity(nz(128))
            .with_tombstone_log(true)
            .with_read_limits(nz(64 * 1024), Compression::None)
            .with_submission_limits(limits.clone())
            .with_eviction_pickers(vec![Box::<foyer::FifoPicker>::default()])
            .with_reinsertion_filter(
                foyer::StorageFilter::new().with_condition(KeepHash(DefaultHasher::default().hash_one(0_u64))),
            );
        let cache: HybridCache<u64, Vec<u8>> = HybridCacheBuilder::new()
            .with_flush_on_close(false)
            .memory(1)
            .storage()
            .with_io_engine_config(Box::new(GateConfig(gate.clone())) as Box<dyn foyer::IoEngineConfig>)
            .with_recover_mode(RecoverMode::Strict)
            .with_engine_config(config)
            .build()
            .await
            .unwrap();
        guarded_insert(&cache, 0, vec![0x71; 16 * 1024]).unwrap();
        cache.storage().wait().await;
        gate.armed.store(true, Ordering::SeqCst);
        let worker_cache = cache.clone();
        let observed = limits.clone();
        let worker = tokio::spawn(async move {
            for key in 1..256_u64 {
                worker_cache.storage().touch(&0);
                guarded_insert(&worker_cache, key, vec![key as u8; 32 * 1024]).unwrap();
                worker_cache.storage().wait().await;
                assert!(observed.bytes() <= 256 * 1024 && observed.items() <= 8);
            }
        });
        gate.entered.notified().await;
        assert_eq!(
            gate.reads.load(Ordering::SeqCst),
            1,
            "real raw hot read reached the completion gate"
        );
        assert!(limits.bytes() >= 20 * 1024 + SubmissionLimits::METADATA_BYTES && limits.items() >= 1);
        // Normal pending payloads reserve 60-KiB workspaces (region minus blob
        // index); the one raw hot
        // backing is independently 20 KiB, and must be counted in addition.
        assert_eq!(
            (limits.bytes() - limits.items() * SubmissionLimits::METADATA_BYTES) % (60 * 1024),
            20 * 1024
        );
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        assert!(
            limits.bytes() >= 20 * 1024 + SubmissionLimits::METADATA_BYTES,
            "cancelling the wait cannot return live raw-buffer credits"
        );
        assert!(limits.bytes() <= 256 * 1024 && limits.items() <= 8);
        assert!(!limits.is_poisoned());
        gate.armed.store(false, Ordering::SeqCst);
        gate.release.add_permits(1);
        cache.storage().wait().await;
        assert_eq!((limits.items(), limits.bytes()), (0, 0));
        assert_eq!(
            cache.storage().load(&0).await.unwrap().kv(),
            Some((0, vec![0x71; 16 * 1024])),
            "hot relocation must actually finish"
        );
        guarded_insert(&cache, 999, b"forward progress".to_vec()).unwrap();
        cache.storage().wait().await;
        assert_eq!(
            cache.storage().load(&999).await.unwrap().kv(),
            Some((999, b"forward progress".to_vec()))
        );
        cache.close().await.unwrap();
        assert_eq!(limits.items(), 0);
    })
    .await
    .unwrap();
}

struct Held {
    data: Arc<Vec<u8>>,
    encoded: Arc<tokio::sync::Notify>,
}

struct PanickingCodec;
impl Code for PanickingCodec {
    fn encode(&self, _: &mut impl Write) -> foyer::Result<()> {
        panic!("injected external codec failure");
    }
    fn decode(_: &mut impl Read) -> foyer::Result<Self> {
        Err(foyer::Error::new(foyer::ErrorKind::Unsupported, "test-only codec"))
    }
    fn estimated_size(&self) -> usize {
        1
    }
}

#[tokio::test]
async fn stopped_worker_cannot_report_successful_guarded_completion() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let (cache, limits) = bounded::<PanickingCodec>(dir.path(), 128 * 1024, 8, Switch::default()).await;
        guarded_insert(&cache, 1, PanickingCodec).unwrap();
        assert!(cache.storage().wait_guarded().await.is_err());
        assert!(limits.is_poisoned());
        assert!(cache.close().await.is_err());
    })
    .await
    .unwrap();
}
impl Code for Held {
    fn encode(&self, w: &mut impl Write) -> foyer::Result<()> {
        w.write_all(&self.data).map_err(foyer::Error::io_error)?;
        self.encoded.notify_one();
        Ok(())
    }
    fn decode(r: &mut impl Read) -> foyer::Result<Self> {
        let mut data = vec![];
        r.take(16 * 1024)
            .read_to_end(&mut data)
            .map_err(foyer::Error::io_error)?;
        Ok(Self {
            data: Arc::new(data),
            encoded: Arc::new(tokio::sync::Notify::new()),
        })
    }
    fn estimated_size(&self) -> usize {
        1
    } // Deliberately inaccurate: guarded charges must not trust this.
}

#[tokio::test]
async fn cancelled_wait_and_close_do_not_release_serialized_source_credits() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        switch.on();
        let (cache, limits) = bounded::<Held>(dir.path(), 64 * 1024, 4, switch.clone()).await;
        let source = Arc::new(vec![0x33; 8192]);
        let weak = Arc::downgrade(&source);
        let encoded = Arc::new(tokio::sync::Notify::new());
        guarded_insert(
            &cache,
            1,
            Held {
                data: source.clone(),
                encoded: encoded.clone(),
            },
        )
        .unwrap();
        drop(source);
        encoded.notified().await;
        let held = (limits.items(), limits.bytes());
        assert_eq!(held, (2, 16 * 1024 + 2 * SubmissionLimits::METADATA_BYTES));
        let mut wait = Box::pin(cache.storage().wait());
        assert!(wait.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        drop(wait);
        let mut close = Box::pin(cache.close());
        assert!(
            close
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(close);
        assert_eq!((limits.items(), limits.bytes()), held);
        assert!(weak.upgrade().is_some());
        switch.off();
        cache.close().await.unwrap();
        assert_eq!((limits.items(), limits.bytes()), (0, 0));
        assert!(weak.upgrade().is_none());
    })
    .await
    .unwrap();
}

const PAGE: usize = 4096;
async fn open(path: &Path, compression: Compression, guarded: bool) -> HybridCache<u64, Vec<u8>> {
    let config = BlockEngineConfig::new(
        FileDeviceBuilder::new(path.join("cache"))
            .with_capacity(1024 * 1024)
            .build()
            .unwrap(),
    )
    .with_block_size(64 * 1024)
    .with_buffer_pool_size(64 * 1024)
    .with_recover_concurrency(1);
    let config = if guarded {
        config.with_read_limits(NonZeroUsize::new(PAGE).unwrap(), Compression::None)
    } else {
        config
    };
    HybridCacheBuilder::new()
        .with_flush_on_close(false)
        .memory(1)
        .storage()
        .with_compression(compression)
        .with_recover_mode(RecoverMode::Strict)
        .with_engine_config(config)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn forged_record_length_is_rejected_before_payload_io() {
    let dir = tempfile::tempdir().unwrap();
    let cache = open(dir.path(), Compression::None, false).await;
    drop(
        cache
            .writer(7)
            .storage()
            .insert(b"independent finite record".to_vec())
            .unwrap(),
    );
    cache.storage().wait().await;
    cache.close().await.unwrap();
    drop(cache);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path().join("cache"))
        .unwrap();
    let mut index = vec![0; PAGE];
    file.read_exact(&mut index).unwrap();
    index[48..52].copy_from_slice(&(8192_u32).to_be_bytes());
    let mut h = DefaultHasher::default().build_hasher();
    h.write(&index[8..]);
    index[..8].copy_from_slice(&(h.finish() ^ u64::from_be_bytes(*b"FOYERBL2")).to_be_bytes());
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&index).unwrap();
    drop(file);
    let cache = open(dir.path(), Compression::None, true).await;
    let before = cache.storage().statistics().disk_read_bytes();
    assert!(
        cache.storage().load(&7).await.unwrap().kv().is_none(),
        "oversized recovered record must be a miss"
    );
    assert_eq!(
        cache.storage().statistics().disk_read_bytes(),
        before,
        "cap must reject before record read/allocation"
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn none_only_reader_rejects_compressed_header_before_decode() {
    for forbidden in [Compression::Zstd, Compression::Lz4] {
        let dir = tempfile::tempdir().unwrap();
        let cache = open(dir.path(), Compression::None, false).await;
        drop(cache.writer(9).storage().insert(vec![0x51; 1024]).unwrap());
        cache.storage().wait().await;
        cache.close().await.unwrap();
        drop(cache);
        // Header compression is outside the payload checksum. Change only the
        // selector: a rejected header must not attempt to decode these finite bytes.
        let mut file = OpenOptions::new().write(true).open(dir.path().join("cache")).unwrap();
        file.seek(SeekFrom::Start((PAGE + 35) as u64)).unwrap();
        file.write_all(&[forbidden.to_u8()]).unwrap();
        drop(file);
        let cache = open(dir.path(), Compression::None, true).await;
        assert!(
            cache.storage().load(&9).await.unwrap().kv().is_none(),
            "None-only adapter must not instantiate a Zstd decoder"
        );
        cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn exact_serialized_cap_is_readable_but_one_byte_over_is_not_read() {
    // 36-byte Foyer header, u64 key and Vec's usize-length prefix (8 bytes here).
    let exact_payload = PAGE - 36 - 8 - std::mem::size_of::<usize>();
    for payload_len in [exact_payload, exact_payload + 1] {
        let dir = tempfile::tempdir().unwrap();
        let expected = vec![0x37; payload_len];
        let cache = open(dir.path(), Compression::None, false).await;
        drop(cache.writer(1).storage().insert(expected.clone()).unwrap());
        cache.storage().wait().await;
        cache.close().await.unwrap();
        drop(cache);
        let cache = open(dir.path(), Compression::None, true).await;
        let before = cache.storage().statistics().disk_read_bytes();
        let got = cache.storage().load(&1).await.unwrap().kv();
        if payload_len == exact_payload {
            assert_eq!(got, Some((1, expected)));
        } else {
            assert!(got.is_none());
            assert_eq!(cache.storage().statistics().disk_read_bytes(), before);
        }
        cache.close().await.unwrap();
    }
}
