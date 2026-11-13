// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Completed invalidations must outlive bounded circular-log history.
#![forbid(unsafe_code)]

use foyer::{BlockEngineConfig, DeviceBuilder, FileDeviceBuilder, HybridCache, HybridCacheBuilder, RecoverMode};
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::Path,
    time::Duration,
};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Default,
    Streaming,
    Bounded,
}
const MODES: [Mode; 3] = [Mode::Default, Mode::Streaming, Mode::Bounded];

async fn open(path: &Path, mode: Mode, capacity: usize) -> foyer::Result<HybridCache<u64, Vec<u8>>> {
    let mut config = BlockEngineConfig::new(
        FileDeviceBuilder::new(path.join("cache"))
            .with_capacity(16 * 1024 * 1024)
            .build()
            .unwrap(),
    )
    .with_block_size(256 * 1024)
    .with_buffer_pool_size(64 * 1024)
    .with_tombstone_log(true);
    if matches!(mode, Mode::Default) {
        // Check the unbounded default recovery path, including multiple flushers.
        config = config.with_flushers(2);
    } else {
        config = config.with_recovery_scratch_limit(NonZeroUsize::new(64 * 1024).unwrap());
    }
    if matches!(mode, Mode::Bounded) {
        config = config.with_index_capacity(NonZeroUsize::new(capacity).unwrap());
    }
    HybridCacheBuilder::new()
        .memory(1)
        .storage()
        .with_recover_mode(RecoverMode::Strict)
        .with_engine_config(config)
        .build()
        .await
}

async fn put(cache: &HybridCache<u64, Vec<u8>>, key: u64, bytes: &[u8]) {
    drop(cache.writer(key).storage().insert(bytes.to_vec()).unwrap());
    cache.storage().wait().await;
}

async fn wrap(cache: &HybridCache<u64, Vec<u8>>) {
    // 16 MiB funds 4096 tombstone slots. Do not write payload regions while
    // retiring history: old deleted payloads remain physically readable.
    for _ in 0..32 {
        for _ in 0..128 {
            cache.storage().delete(&9000);
        }
        cache.storage().wait().await;
    }
}

#[tokio::test]
async fn unwrapped_history_preserves_unchanged_older_payloads() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for mode in MODES {
            let directory = tempfile::tempdir().unwrap();
            let cache = open(directory.path(), mode, 8).await.unwrap();
            put(&cache, 1, b"unchanged old bytes").await;
            put(&cache, 2, b"deleted bytes").await;
            put(&cache, 3, b"old overwritten bytes").await;
            cache.storage().delete(&2);
            put(&cache, 3, b"new replacement").await;
            // In bounded mode the five prior invalidations plus these 4090
            // records exactly fill slots 1..4095, without overflowing history.
            for _ in 0..4090 {
                cache.storage().delete(&9000);
            }
            cache.close().await.unwrap();
            drop(cache);
            let cache = open(directory.path(), mode, 8).await.unwrap();
            assert_eq!(
                cache.storage().load(&1).await.unwrap().kv(),
                Some((1, b"unchanged old bytes".to_vec())),
                "{mode:?}"
            );
            assert!(cache.storage().load(&2).await.unwrap().kv().is_none());
            assert_eq!(
                cache.storage().load(&3).await.unwrap().kv(),
                Some((3, b"new replacement".to_vec()))
            );
            cache.close().await.unwrap();
        }
    })
    .await
    .expect("unwrapped recovery deadline");
}

#[tokio::test]
async fn completed_deletes_and_overwrites_do_not_resurrect_across_wrap_cycles() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for mode in MODES {
            let directory = tempfile::tempdir().unwrap();
            let cache = open(directory.path(), mode, 8).await.unwrap();
            put(&cache, 1, b"deleted old bytes").await;
            put(&cache, 3, b"overwritten old bytes").await;
            cache.storage().delete(&1);
            put(&cache, 3, b"replacement bytes").await;
            wrap(&cache).await;
            cache.close().await.unwrap();
            drop(cache);
            for cycle in 0..3 {
                let cache = open(directory.path(), mode, 8).await.unwrap();
                assert!(
                    cache.storage().load(&1).await.unwrap().kv().is_none(),
                    "{mode:?} cycle {cycle}: deleted payload resurrected"
                );
                let replacement = cache.storage().load(&3).await.unwrap().kv();
                assert!(replacement.is_none() || replacement == Some((3, b"replacement bytes".to_vec())));
                put(&cache, 10 + cycle, b"completed after checkpoint").await;
                // The kept payload is older than the latest retained deletion,
                // but newer than history actually overwritten. A max-record
                // cutoff would incorrectly lose it.
                cache.storage().delete(&9001);
                cache.storage().wait().await;
                cache.close().await.unwrap();
                drop(cache);
                let cache = open(directory.path(), mode, 8).await.unwrap();
                assert_eq!(
                    cache.storage().load(&(10 + cycle)).await.unwrap().kv(),
                    Some((10 + cycle, b"completed after checkpoint".to_vec())),
                    "new completed payload must survive reopen without another history loss"
                );
                cache.storage().delete(&(10 + cycle));
                wrap(&cache).await;
                cache.close().await.unwrap();
                drop(cache);
                let cache = open(directory.path(), mode, 8).await.unwrap();
                assert!(cache.storage().load(&(10 + cycle)).await.unwrap().kv().is_none());
                cache.close().await.unwrap();
                drop(cache);
            }
        }
    })
    .await
    .expect("wrapped recovery deadline");
}

#[tokio::test]
async fn metadata_rejected_overwrite_stays_invalid_after_history_wrap() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let directory = tempfile::tempdir().unwrap();
        let cache = open(directory.path(), Mode::Bounded, 64).await.unwrap();
        for key in 0..63 {
            put(&cache, key, &[key as u8]).await;
        }
        // Give the stale payload a newer sequence than most cold records so
        // newest-sequence recovery alone cannot accidentally hide the bug.
        put(&cache, 999, b"old evicted payload").await;
        for key in 0..63 {
            assert!(cache.storage().load(&key).await.unwrap().kv().is_some());
        }
        put(&cache, 63, &[63]).await;
        assert!(!cache.storage().may_contains(&999));
        for key in 0..64 {
            assert!(cache.storage().load(&key).await.unwrap().kv().is_some());
        }
        put(&cache, 999, b"optional rejected overwrite").await;
        assert!(
            cache.storage().load(&999).await.unwrap().kv().is_none(),
            "all-hot admission must really reject this overwrite"
        );
        wrap(&cache).await;
        cache.close().await.unwrap();
        drop(cache);
        let cache = open(directory.path(), Mode::Bounded, 64).await.unwrap();
        assert!(cache.storage().load(&999).await.unwrap().kv().is_none());
        cache.close().await.unwrap();
    })
    .await
    .expect("rejected overwrite recovery deadline");
}

#[tokio::test]
async fn damaged_or_incomplete_rollover_fails_closed() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for mode in MODES {
            for damage in ["checksum", "preparing", "legacy-wrapped", "legacy-hole"] {
                let directory = tempfile::tempdir().unwrap();
                let cache = open(directory.path(), mode, 8).await.unwrap();
                put(&cache, 1, b"deleted payload").await;
                cache.storage().delete(&1);
                cache.storage().delete(&9001);
                cache.storage().wait().await;
                if damage == "checksum" {
                    wrap(&cache).await;
                }
                cache.close().await.unwrap();
                drop(cache);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(directory.path().join("cache"))
                    .unwrap();
                match damage {
                    "checksum" => {
                        file.seek(SeekFrom::Start(24)).unwrap();
                        let mut byte = [0];
                        file.read_exact(&mut byte).unwrap();
                        byte[0] ^= 1;
                        file.seek(SeekFrom::Start(24)).unwrap();
                        file.write_all(&byte).unwrap();
                    }
                    "preparing" => {
                        file.write_all(b"FOYERTP2").unwrap();
                    }
                    "legacy-wrapped" => {
                        file.write_all(&[1; 16]).unwrap();
                    }
                    "legacy-hole" => {
                        file.seek(SeekFrom::Start(16)).unwrap();
                        file.write_all(&[0; 16]).unwrap();
                    }
                    _ => unreachable!(),
                }
                file.sync_all().unwrap();
                drop(file);
                assert!(
                    open(directory.path(), mode, 8).await.is_err(),
                    "{mode:?}: {damage} must not become a partial recovered cache"
                );
            }
        }
    })
    .await
    .expect("damaged checkpoint deadline");
}
