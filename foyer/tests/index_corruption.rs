// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Damaged index geometry through small disposable files and public cache APIs.
#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]

use std::{
    fs::OpenOptions,
    hash::{BuildHasher, Hasher},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    process::Command,
    time::Duration,
};

use foyer::{
    BlockEngineConfig, DefaultHasher, DeviceBuilder, FileDeviceBuilder, HybridCache, HybridCacheBuilder, RecoverMode,
};

const PAGE: usize = 4096;
const REGION: usize = 256 * 1024;
const INDEX_HEADER: usize = 28;
const INDEX_ENTRY: usize = 24;
const CASE_ENV: &str = "FOYER_STRUCTURAL_INDEX_CASE";
const DIRECTORY_ENV: &str = "FOYER_STRUCTURAL_INDEX_DIRECTORY";
const CASES: &[&str] = &[
    "valid-partial",
    "sequence-regression",
    "misaligned",
    "before-index",
    "overlap",
    "gap",
    "backward",
    "short-header",
    "zero-length",
    "region-length",
    "aligned-region-end",
    "later-relative-end",
    "count-overflow",
    "empty-count",
    "zero-step",
];

async fn open(directory: &Path) -> HybridCache<u64, Vec<u8>> {
    HybridCacheBuilder::new()
        .with_flush_on_close(false)
        .memory(1)
        .storage()
        .with_recover_mode(RecoverMode::Strict)
        .with_engine_config(
            BlockEngineConfig::new(
                FileDeviceBuilder::new(directory.join("cache"))
                    .with_capacity(16 * 1024 * 1024)
                    .build()
                    .unwrap(),
            )
            .with_block_size(REGION)
            .with_buffer_pool_size(512 * 1024)
            .with_submit_queue_size_threshold(1024 * 1024)
            .with_indexer_shards(4)
            .with_recover_concurrency(1)
            .with_flushers(1)
            .with_reclaimers(1),
        )
        .build()
        .await
        .unwrap()
}

async fn put(cache: &HybridCache<u64, Vec<u8>>, key: u64, value: &[u8]) {
    drop(cache.writer(key).storage().insert(value.to_vec()).unwrap());
    cache.storage().wait().await;
    assert!(cache.memory().get(&key).is_none());
    assert_eq!(
        cache.storage().load(&key).await.unwrap().kv(),
        Some((key, value.to_vec()))
    );
}

fn write_u32(page: &mut [u8], offset: usize, value: u32) {
    page[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn read_u32(page: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(page[offset..offset + 4].try_into().unwrap())
}

// DefaultHasher wraps the same maintained XxHash64(seed=0) implementation as
// Checksummer::checksum64. Feed raw bytes, not Hash's length-prefixed slice.
fn seal(page: &mut [u8], legacy: bool) {
    let mut hasher = DefaultHasher::default().build_hasher();
    hasher.write(&page[8..]);
    let checksum = hasher.finish() ^ if legacy { 0 } else { u64::from_be_bytes(*b"FOYERBL2") };
    page[..8].copy_from_slice(&checksum.to_be_bytes());
}

async fn malformed_index(directory: &Path, case: &str, legacy: bool) {
    let first = b"known first record bytes".repeat(8000);
    let survivor = b"independent completed region bytes".repeat(6000);
    let cache = open(directory).await;
    put(&cache, 1, &first).await;
    put(&cache, 2, b"known same-region second record").await;
    put(&cache, 3, &survivor).await;
    cache.close().await.unwrap();
    drop(cache);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("cache"))
        .unwrap();
    let mut page = vec![0; PAGE];
    file.read_exact(&mut page).unwrap();
    assert_eq!(read_u32(&page, 8), 2, "two legitimate partially updated blob entries");
    assert_eq!(
        &page[INDEX_HEADER..INDEX_HEADER + 8],
        &DefaultHasher::default().hash_one(1_u64).to_be_bytes()
    );
    // Prove our checksum API and seed match the real file before forging input.
    let checksum = page[..8].to_vec();
    seal(&mut page, false);
    assert_eq!(&page[..8], checksum);
    let entry = if legacy {
        page.copy_within(INDEX_HEADER..INDEX_HEADER + 2 * INDEX_ENTRY, 12);
        page[12 + 2 * INDEX_ENTRY..].fill(0);
        12
    } else {
        INDEX_HEADER
    };
    let first_offset = read_u32(&page, entry + 16);
    assert_eq!(first_offset, PAGE as u32);
    let second = entry + INDEX_ENTRY;
    match case {
        "valid-partial" => {}
        "sequence-regression" => {
            // New incarnations permit reinsertion's older logical sequences;
            // legacy blobs must retain their conservative stale-tail cutoff.
            let sequence = u64::from_be_bytes(page[entry + 8..entry + 16].try_into().unwrap());
            assert!(sequence > 0);
            page[second + 8..second + 16].copy_from_slice(&(sequence - 1).to_be_bytes());
        }
        "misaligned" => write_u32(&mut page, entry + 16, first_offset + 1),
        "before-index" => write_u32(&mut page, entry + 16, 0),
        "overlap" => write_u32(&mut page, second + 16, first_offset + PAGE as u32),
        "gap" => {
            let offset = read_u32(&page, second + 16);
            write_u32(&mut page, second + 16, offset + PAGE as u32);
        }
        "backward" => write_u32(&mut page, second + 16, 0),
        "short-header" => write_u32(&mut page, entry + 20, 35),
        "zero-length" => write_u32(&mut page, entry + 20, 0),
        "region-length" => write_u32(&mut page, entry + 20, REGION as u32 + 1),
        "aligned-region-end" => write_u32(&mut page, entry + 20, (REGION - PAGE + 1) as u32),
        "later-relative-end" => {
            // Keep a valid first blob and place a forged second blob exactly
            // where scanning its first entry would naturally advance. The
            // address is relative to this later blob, not the region start.
            let next_blob = read_u32(&page, second + 16);
            let mut later = vec![0; PAGE];
            later[..entry].copy_from_slice(&page[..entry]);
            write_u32(&mut later, 8, 1);
            later[entry..entry + INDEX_ENTRY].copy_from_slice(&page[second..second + INDEX_ENTRY]);
            write_u32(&mut later, entry + 16, PAGE as u32);
            write_u32(&mut later, entry + 20, (REGION - PAGE) as u32);
            seal(&mut later, legacy);
            file.seek(SeekFrom::Start(u64::from(next_blob))).unwrap();
            file.write_all(&later).unwrap();
            write_u32(&mut page, 8, 1);
        }
        "count-overflow" => write_u32(&mut page, 8, ((PAGE - entry) / INDEX_ENTRY + 1) as u32),
        "empty-count" => write_u32(&mut page, 8, 0),
        "zero-step" => {
            write_u32(&mut page, 8, 1);
            write_u32(&mut page, entry + 16, 0);
            write_u32(&mut page, entry + 20, 0);
        }
        _ => panic!("unknown fixture {case}"),
    }
    seal(&mut page, legacy);
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&page).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let cache = open(directory).await;
    // Check the public recovered index before attempting a payload read.
    // All damaged lengths are bounded by this small fixture's region size.
    if matches!(case, "valid-partial" | "sequence-regression") || case.starts_with("later-") {
        assert_eq!(cache.storage().load(&1).await.unwrap().kv(), Some((1, first)));
    } else {
        assert!(
            !cache.storage().may_contains(&1),
            "{case}: malformed blob published first address"
        );
        assert!(cache.storage().load(&1).await.unwrap().is_miss());
    }
    if case == "valid-partial" || (case == "sequence-regression" && !legacy) {
        assert_eq!(
            cache.storage().load(&2).await.unwrap().kv(),
            Some((2, b"known same-region second record".to_vec()))
        );
    } else {
        assert!(
            !cache.storage().may_contains(&2),
            "{case}: malformed blob published second address"
        );
        assert!(cache.storage().load(&2).await.unwrap().is_miss());
    }
    assert_eq!(cache.storage().load(&3).await.unwrap().kv(), Some((3, survivor)));
    cache.close().await.unwrap();
}

#[tokio::test]
async fn recovered_index_addresses_are_structurally_bounded() {
    if let Ok(case) = std::env::var(CASE_ENV) {
        let (case, format) = case.split_once(':').unwrap();
        let directory = std::env::var_os(DIRECTORY_ENV).expect("parent-owned fixture directory");
        tokio::time::timeout(
            Duration::from_secs(8),
            malformed_index(Path::new(&directory), case, format == "legacy"),
        )
        .await
        .expect("bounded structural recovery deadline");
        return;
    }
    // A missing progress guard can loop during recovery. Every fixture runs in
    // an owned Linux child with address-space, CPU and wall-clock limits.
    // These standard util-linux/coreutils tools are explicit test prerequisites.
    for case in CASES {
        for format in ["legacy", "incarnation"] {
            let fixture = format!("{case}:{format}");
            // The parent cleans files even if a regression aborts/kills a child.
            let directory = tempfile::tempdir().unwrap();
            let output = Command::new("timeout")
                .args(["--kill-after=2s", "15s", "prlimit", "--as=1073741824", "--cpu=10", "--"])
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "recovered_index_addresses_are_structurally_bounded",
                    "--nocapture",
                ])
                .env(CASE_ENV, &fixture)
                .env(DIRECTORY_ENV, directory.path())
                .output()
                .expect("index corruption tests require timeout and prlimit");
            assert!(
                output.status.success(),
                "{fixture}: child failed ({:?})\n{}\n{}",
                output.status.code(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
