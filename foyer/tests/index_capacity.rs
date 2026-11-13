//! Public-API metadata-pressure and recovery contracts on disposable files.
#![forbid(unsafe_code)]
use foyer::{BlockEngineConfig, DeviceBuilder, FileDeviceBuilder, HybridCache, HybridCacheBuilder, RecoverMode};
use foyer_storage::test_utils::Switch;
use std::{num::NonZeroUsize, path::Path, time::Duration};

const KIB: usize = 1024;

#[tokio::test]
async fn continuously_read_old_record_survives_cold_metadata_pressure_then_ages_out() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let (cache, usage) = open(directory.path(), 8).await;
        drop(cache.writer(0).storage().insert(b"old hot record".to_vec()).unwrap());
        cache.storage().wait().await;
        for key in 1..128_u64 {
            assert_eq!(
                cache.storage().load(&0).await.unwrap().kv(),
                Some((0, b"old hot record".to_vec())),
                "hot key lost before cold insertion {key}"
            );
            drop(cache.writer(key).storage().insert(key.to_le_bytes().to_vec()).unwrap());
            cache.storage().wait().await;
            assert!(usage() <= 8);
            assert_eq!(
                cache.storage().load(&key).await.unwrap().kv(),
                Some((key, key.to_le_bytes().to_vec()))
            );
        }
        for key in 128..256_u64 {
            drop(cache.writer(key).storage().insert(key.to_le_bytes().to_vec()).unwrap());
            cache.storage().wait().await;
            assert!(usage() <= 8);
        }
        assert!(
            cache.storage().load(&0).await.unwrap().kv().is_none(),
            "formerly hot key must not remain pinned after access stops"
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_l1_touch_retains_hot_disk_record_without_io_or_creation() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let (cache, usage) = open(directory.path(), 8).await;
        for key in 0..8_u64 {
            drop(cache.writer(key).storage().insert(key.to_le_bytes().to_vec()).unwrap());
            cache.storage().wait().await;
        }
        for key in 8..128_u64 {
            let stats = cache.storage().statistics();
            let before = (stats.disk_read_bytes(), stats.disk_write_bytes(), usage());
            assert!(cache.storage().touch(&0));
            assert!(!cache.storage().touch(&9999));
            assert_eq!(
                (stats.disk_read_bytes(), stats.disk_write_bytes(), usage()),
                before,
                "touch must be memory-only and non-creating"
            );
            drop(cache.writer(key).storage().insert(key.to_le_bytes().to_vec()).unwrap());
            cache.storage().wait().await;
            assert!(cache.storage().may_contains(&0));
            assert!(usage() <= 8);
        }
        assert_eq!(
            cache.storage().load(&0).await.unwrap().kv(),
            Some((0, 0_u64.to_le_bytes().to_vec()))
        );
        for key in 128..256_u64 {
            drop(cache.writer(key).storage().insert(key.to_le_bytes().to_vec()).unwrap());
            cache.storage().wait().await;
        }
        assert!(
            !cache.storage().touch(&0),
            "a stopped access stream must not pin the old record"
        );
        assert!(!cache.storage().may_contains(&9999));
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn probes_admission_and_recovery_do_not_grant_reference_bits() {
    let directory = tempfile::tempdir().unwrap();
    let (cache, _) = open(directory.path(), 8).await;
    for key in 0..8_u64 {
        drop(cache.writer(key).storage().insert(vec![key as u8]).unwrap());
        cache.storage().wait().await;
    }
    cache.close().await.unwrap();
    drop(cache);
    let (cache, usage) = open(directory.path(), 8).await;
    assert!(cache.storage().may_contains(&0));
    for key in 8..32_u64 {
        // These index probes must not turn an otherwise cold record hot.
        cache.storage().may_contains(&0);
        drop(cache.writer(key).storage().insert(vec![key as u8]).unwrap());
        cache.storage().wait().await;
        assert!(usage() <= 8);
    }
    assert!(cache.storage().load(&0).await.unwrap().kv().is_none());
    cache.close().await.unwrap();
}

#[tokio::test]
async fn all_hot_sweep_is_bounded_and_can_skip_optional_insertion() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let (cache, usage) = open(directory.path(), 64).await;
        for key in 0..64_u64 {
            drop(cache.writer(key).storage().insert(vec![key as u8]).unwrap());
            cache.storage().wait().await;
        }
        for key in 0..64_u64 {
            assert_eq!(
                cache.storage().load(&key).await.unwrap().kv(),
                Some((key, vec![key as u8]))
            );
        }
        drop(cache.writer(1000).storage().insert(b"optional".to_vec()).unwrap());
        cache.storage().wait().await;
        assert_eq!(usage(), 64);
        assert!(
            cache.storage().load(&1000).await.unwrap().kv().is_none(),
            "one 64-slot sweep must stop rather than loop over all-hot slots"
        );
        for key in 0..64_u64 {
            assert!(cache.storage().may_contains(&key));
        }
        // No more accesses: cleared bits permit later admissions and retirement.
        for key in 1001..1100_u64 {
            drop(cache.writer(key).storage().insert(vec![1]).unwrap());
            cache.storage().wait().await;
            assert!(usage() <= 64);
        }
        assert!(!cache.storage().may_contains(&0));
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[derive(Debug)]
struct KeepHash(u64);
impl foyer::StorageFilterCondition for KeepHash {
    fn filter(&self, _: &std::sync::Arc<foyer::Statistics>, hash: u64, _: usize) -> foyer::StorageFilterResult {
        if hash == self.0 {
            foyer::StorageFilterResult::Admit
        } else {
            foyer::StorageFilterResult::Reject
        }
    }
}

#[tokio::test]
async fn hot_current_relocation_survives_a_floor_raised_by_other_keys() {
    use std::hash::BuildHasher;
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let config = BlockEngineConfig::new(
            FileDeviceBuilder::new(directory.path().join("cache"))
                .with_capacity(16 * 1024 * KIB)
                .build()
                .unwrap(),
        )
        .with_block_size(256 * KIB)
        .with_buffer_pool_size(64 * KIB)
        .with_index_capacity(NonZeroUsize::new(8).unwrap())
        .with_tombstone_log(true)
        // Force the oldest physical region to recycle; invalid-ratio selection
        // could otherwise indefinitely prefer fully cold regions around it.
        .with_eviction_pickers(vec![Box::<foyer::FifoPicker>::default()])
        .with_recovery_scratch_limit(NonZeroUsize::new(64 * KIB).unwrap())
        .with_reinsertion_filter(
            foyer::StorageFilter::new().with_condition(KeepHash(foyer::DefaultHasher::default().hash_one(0_u64))),
        );
        let usage = config.index_usage();
        let cache = HybridCacheBuilder::new()
            .memory(1)
            .storage()
            .with_recover_mode(RecoverMode::Strict)
            .with_engine_config(config)
            .build()
            .await
            .unwrap();
        drop(
            cache
                .writer(0_u64)
                .storage()
                .insert(b"hot relocated bytes".to_vec())
                .unwrap(),
        );
        cache.storage().wait().await;
        for key in 1..768_u64 {
            assert_eq!(
                cache.storage().load(&0_u64).await.unwrap().kv(),
                Some((0_u64, b"hot relocated bytes".to_vec())),
                "current hot version lost at physical pressure {key}"
            );
            drop(cache.writer(key).storage().insert(vec![key as u8; 32 * KIB]).unwrap());
            cache.storage().wait().await;
            assert!(usage.entries() <= 8);
        }
        assert!(
            cache.storage().statistics().disk_write_bytes() > 16 * 1024 * KIB,
            "fixture must actually recycle regions"
        );
        assert_eq!(
            cache.storage().load(&0_u64).await.unwrap().kv(),
            Some((0_u64, b"hot relocated bytes".to_vec()))
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn streaming_tombstones_fit_small_scratch_and_preserve_next_sequence() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let (cache, usage) = open_with(directory.path(), 8, 64 * KIB, 1024 * KIB, Switch::default())
            .await
            .unwrap();
        drop(cache.writer(0).storage().insert(b"old deleted bytes".to_vec()).unwrap());
        cache.storage().wait().await;
        for _ in 0..40 {
            for _ in 0..16 {
                cache.storage().delete(&0);
            }
            assert_eq!(usage(), 1, "overlapping tombstones share one pending slot");
            cache.storage().wait().await;
            assert!(usage() <= 8);
            assert!(
                usage() <= 1,
                "completed tombstones may retire their slot, never grow an index history"
            );
        }
        cache.close().await.unwrap();
        drop(cache);
        let (cache, usage) = open_with(directory.path(), 8, 64 * KIB, 1024 * KIB, Switch::default())
            .await
            .unwrap();
        assert!(usage() <= 8);
        assert!(cache.storage().load(&0).await.unwrap().kv().is_none());
        drop(
            cache
                .writer(0)
                .storage()
                .insert(b"new post-reopen bytes".to_vec())
                .unwrap(),
        );
        cache.storage().wait().await;
        assert_eq!(
            cache.storage().load(&0).await.unwrap().kv(),
            Some((0, b"new post-reopen bytes".to_vec())),
            "next sequence must exceed streamed tombstones, not just recovered payloads"
        );
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn streaming_mutable_mode_rejects_unordered_multi_flusher_log_appends() {
    let directory = tempfile::tempdir().unwrap();
    let config = BlockEngineConfig::new(
        FileDeviceBuilder::new(directory.path().join("cache"))
            .with_capacity(16 * 1024 * KIB)
            .build()
            .unwrap(),
    )
    .with_index_capacity(NonZeroUsize::new(8).unwrap())
    .with_tombstone_log(true)
    .with_recovery_scratch_limit(NonZeroUsize::new(64 * KIB).unwrap())
    .with_flushers(2);
    let result: foyer::Result<HybridCache<u64, Vec<u8>>> = HybridCacheBuilder::new()
        .memory(1)
        .storage()
        .with_engine_config(config)
        .build()
        .await;
    assert!(matches!(result,Err(error) if error.to_string().contains("exactly one flusher")));
}
async fn open(path: &Path, limit: usize) -> (HybridCache<u64, Vec<u8>>, impl Fn() -> usize) {
    open_with(path, limit, 1024 * KIB, 1024 * KIB, Switch::default())
        .await
        .unwrap()
}

async fn open_with(
    path: &Path,
    limit: usize,
    scratch: usize,
    queue: usize,
    switch: Switch,
) -> foyer::Result<(HybridCache<u64, Vec<u8>>, impl Fn() -> usize)> {
    let config = BlockEngineConfig::new(
        FileDeviceBuilder::new(path.join("cache"))
            .with_capacity(16 * 1024 * KIB)
            .build()
            .unwrap(),
    )
    .with_block_size(256 * KIB)
    .with_buffer_pool_size(64 * KIB)
    .with_indexer_shards(4)
    .with_index_capacity(NonZeroUsize::new(limit).unwrap())
    .with_recovery_scratch_limit(NonZeroUsize::new(scratch).unwrap())
    .with_tombstone_log(true)
    .with_submit_queue_size_threshold(queue)
    .with_flush_switch(switch);
    let usage = config.index_usage();
    let cache = HybridCacheBuilder::new()
        .memory(1)
        .storage()
        .with_recover_mode(RecoverMode::Strict)
        .with_engine_config(config)
        .build()
        .await?;
    Ok((cache, move || usage.entries()))
}

#[tokio::test]
async fn sustained_insertion_keeps_a_bounded_changing_inventory() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let directory = tempfile::tempdir().unwrap();
        let (cache, usage) = open(directory.path(), 8).await;
        for key in 0..4096_u64 {
            drop(
                cache
                    .writer(key)
                    .storage()
                    .insert(vec![(key % 251) as u8; 4096])
                    .unwrap(),
            );
            cache.storage().wait().await;
            let count = usage();
            assert!(count <= 8, "retained {count} entries with limit 8 at {key}");
            assert_eq!(
                cache.storage().load(&key).await.unwrap().kv(),
                Some((key, vec![(key % 251) as u8; 4096]))
            );
        }
        assert!(usage() > 0, "bounded-index usage must be observable");
        assert!(!cache.storage().may_contains(&0));
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shrinking_reopen_keeps_newest_versions_and_persistent_deletions() {
    let directory = tempfile::tempdir().unwrap();
    let (cache, usage) = open(directory.path(), 32).await;
    for key in 0..24 {
        drop(cache.writer(key).storage().insert(vec![key as u8]).unwrap());
        cache.storage().wait().await;
    }
    for version in 0..10 {
        drop(cache.writer(7).storage().insert(vec![100 + version]).unwrap());
        cache.storage().wait().await;
    }
    assert_eq!(cache.storage().load(&7).await.unwrap().kv(), Some((7, vec![109])));
    cache.storage().delete(&6);
    cache.storage().wait().await;
    assert!(usage() <= 32);
    cache.close().await.unwrap();
    drop(cache);
    let (cache, usage) = open(directory.path(), 4).await;
    assert!(usage() <= 4);
    assert_eq!(cache.storage().load(&7).await.unwrap().kv(), Some((7, vec![109])));
    assert_eq!(cache.storage().load(&23).await.unwrap().kv(), Some((23, vec![23])));
    assert!(cache.storage().load(&6).await.unwrap().kv().is_none());
    cache.close().await.unwrap();
}

#[tokio::test]
async fn overlapping_delete_and_delayed_old_completions_do_not_resurrect() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        let (cache, usage) = open_with(directory.path(), 8, 1024 * KIB, 1024 * KIB, switch.clone())
            .await
            .unwrap();
        switch.on();
        drop(cache.writer(1).storage().insert(b"old".to_vec()).unwrap());
        drop(cache.writer(1).storage().insert(b"new".to_vec()).unwrap());
        cache.storage().delete(&1);
        for key in 2..14 {
            drop(cache.writer(key).storage().insert(vec![key as u8]).unwrap());
            assert!(usage() <= 8);
        }
        switch.off();
        cache.storage().wait().await;
        assert!(cache.storage().load(&1).await.unwrap().kv().is_none());
        assert_eq!(cache.storage().load(&13).await.unwrap().kv(), Some((13, vec![13])));
        cache.close().await.unwrap();
        drop(cache);
        let (cache, usage) = open(directory.path(), 8).await;
        assert!(usage() <= 8);
        assert!(cache.storage().load(&1).await.unwrap().kv().is_none());
        assert_eq!(cache.storage().load(&13).await.unwrap().kv(), Some((13, vec![13])));
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejected_overwrite_persists_invalidation_instead_of_serving_old_bytes() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        let (cache, usage) = open_with(directory.path(), 8, 1024 * KIB, 1, switch.clone())
            .await
            .unwrap();
        drop(
            cache
                .writer(42)
                .storage()
                .insert(b"old completed bytes".to_vec())
                .unwrap(),
        );
        cache.storage().wait().await;
        switch.on();
        drop(cache.writer(99).storage().insert(vec![99; 4096]).unwrap());
        drop(
            cache
                .writer(42)
                .storage()
                .insert(b"new rejected bytes".to_vec())
                .unwrap(),
        );
        assert!(usage() <= 8);
        switch.off();
        cache.storage().wait().await;
        assert!(cache.storage().load(&42).await.unwrap().kv().is_none());
        cache.close().await.unwrap();
        drop(cache);
        let (cache, _) = open(directory.path(), 8).await;
        assert!(cache.storage().load(&42).await.unwrap().kv().is_none());
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn insufficient_recovery_scratch_fails_without_modifying_completed_storage() {
    let directory = tempfile::tempdir().unwrap();
    let (cache, _) = open(directory.path(), 8).await;
    drop(cache.writer(4).storage().insert(b"retained".to_vec()).unwrap());
    cache.storage().wait().await;
    cache.close().await.unwrap();
    drop(cache);
    let before = std::fs::read(directory.path().join("cache")).unwrap();
    let result = open_with(directory.path(), 4, 1, 1024 * KIB, Switch::default()).await;
    assert!(matches!(result,Err(error) if error.to_string().contains("scratch budget")));
    assert_eq!(std::fs::read(directory.path().join("cache")).unwrap(), before);
    let (cache, usage) = open(directory.path(), 4).await;
    assert!(usage() <= 4);
    assert_eq!(
        cache.storage().load(&4).await.unwrap().kv(),
        Some((4, b"retained".to_vec()))
    );
    cache.close().await.unwrap();
}

#[tokio::test]
async fn serialized_buffer_rejection_also_preserves_overwrite_invalidation() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let switch = Switch::default();
        let (cache, usage) = open_with(directory.path(), 32, 1024 * KIB, 1024 * KIB, switch.clone())
            .await
            .unwrap();
        drop(
            cache
                .writer(42)
                .storage()
                .insert(b"old completed bytes".to_vec())
                .unwrap(),
        );
        cache.storage().wait().await;
        switch.on();
        // The channel byte estimate fits, but 20 page-aligned records cannot fit
        // the 64-KiB serialized buffer while its flusher is deliberately held.
        for key in 100..120 {
            drop(cache.writer(key).storage().insert(vec![key as u8]).unwrap());
        }
        drop(
            cache
                .writer(42)
                .storage()
                .insert(b"new dropped bytes".to_vec())
                .unwrap(),
        );
        switch.off();
        cache.storage().wait().await;
        assert!(usage() <= 32);
        assert!(cache.storage().load(&42).await.unwrap().kv().is_none());
        cache.close().await.unwrap();
        drop(cache);
        let (cache, _) = open(directory.path(), 32).await;
        assert!(cache.storage().load(&42).await.unwrap().kv().is_none());
        cache.close().await.unwrap();
    })
    .await
    .unwrap();
}

#[derive(Default)]
struct CollisionHasher;
impl std::hash::Hasher for CollisionHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}

#[tokio::test]
async fn bounded_index_preserves_full_key_collision_rejection_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    for reopen in 0..2 {
        let config = BlockEngineConfig::new(
            FileDeviceBuilder::new(directory.path().join("cache"))
                .with_capacity(16 * 1024 * KIB)
                .build()
                .unwrap(),
        )
        .with_block_size(256 * KIB)
        .with_buffer_pool_size(64 * KIB)
        .with_index_capacity(NonZeroUsize::new(4).unwrap())
        .with_recovery_scratch_limit(NonZeroUsize::new(1024 * KIB).unwrap())
        .with_tombstone_log(true);
        let usage = config.index_usage();
        let cache = HybridCacheBuilder::new()
            .memory(1)
            .with_hash_builder(std::hash::BuildHasherDefault::<CollisionHasher>::default())
            .storage()
            .with_recover_mode(RecoverMode::Strict)
            .with_engine_config(config)
            .build()
            .await
            .unwrap();
        if reopen == 0 {
            drop(cache.writer(1_u64).storage().insert(b"first key".to_vec()).unwrap());
            cache.storage().wait().await;
            drop(cache.writer(2_u64).storage().insert(b"second key".to_vec()).unwrap());
            cache.storage().wait().await;
        }
        assert_eq!(usage.entries(), 1);
        assert!(cache.storage().load(&1_u64).await.unwrap().kv().is_none());
        assert_eq!(
            cache.storage().load(&2_u64).await.unwrap().kv(),
            Some((2_u64, b"second key".to_vec()))
        );
        cache.close().await.unwrap();
    }
}
