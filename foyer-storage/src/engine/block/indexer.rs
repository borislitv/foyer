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

use std::{
    collections::{BTreeSet, HashMap, hash_map::Entry},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use foyer_common::{error::Result, hasher::ModHasher, spawn::Spawner};
use futures_util::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};

use crate::engine::block::{manager::BlockId, serde::Sequence};

#[derive(Debug, Clone)]
pub enum Index {
    Address(EntryAddress),
    Tombstone(Sequence),
    Pending(Sequence),
}

impl Index {
    fn sequence(&self) -> Sequence {
        match self {
            Index::Address(addr) => addr.sequence,
            Index::Tombstone(seq) | Index::Pending(seq) => *seq,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashedEntryAddress {
    pub hash: u64,
    pub address: EntryAddress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryAddress {
    pub block: BlockId,
    pub offset: u32,
    pub len: u32,

    pub sequence: Sequence,
}

type IndexerShard = HashMap<u64, Index, ModHasher>;

/// Read-only count of occupied bounded-index slots, including pending updates and tombstones.
#[derive(Debug, Clone, Default)]
pub struct IndexUsage(Arc<AtomicUsize>);

impl IndexUsage {
    /// Occupied logical slots. Each slot also owns one bounded CLOCK-order node.
    pub fn entries(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct BoundedEntry {
    index: Index,
    stamp: u128,
    referenced: bool,
}

#[derive(Debug)]
struct BoundedIndex {
    entries: HashMap<u64, BoundedEntry, ModHasher>,
    // This is part of the bounded index: one order node per occupied slot, never a history queue.
    order: BTreeSet<(u128, u64)>,
    floor: Option<(Sequence, u64)>,
    limit: usize,
    usage: IndexUsage,
    recovering: bool,
    clock: u128,
}

impl BoundedIndex {
    fn insert(&mut self, hash: u64, index: Index) -> Option<HashedEntryAddress> {
        let order = (index.sequence(), hash);
        let current = self.entries.get(&hash);
        if current.is_some_and(|old| matches!(old.index, Index::Tombstone(sequence) if sequence >= index.sequence()))
            && matches!(index, Index::Address(_))
        {
            return Indexer::address(index).map(|address| HashedEntryAddress { hash, address });
        }
        // A current-version relocation is authorized by the live slot, even if
        // eviction of other keys raised the unknown-completion sequence floor.
        let current_version = current.is_some_and(|old| old.index.sequence() == index.sequence());
        if (!current_version && self.floor.is_some_and(|floor| order <= floor))
            || self
                .entries
                .get(&hash)
                .is_some_and(|old| old.index.sequence() > index.sequence())
        {
            return Indexer::address(index).map(|address| HashedEntryAddress { hash, address });
        }
        if current_version {
            let old = self.entries.get_mut(&hash).unwrap();
            let previous = std::mem::replace(&mut old.index, index);
            return Indexer::address(previous).map(|address| HashedEntryAddress { hash, address });
        }
        let old = if let Some(old) = self.entries.remove(&hash) {
            self.order.remove(&(old.stamp, hash));
            Some((hash, old.index))
        } else if self.entries.len() == self.limit {
            let victim = if self.recovering {
                let first = *self.order.first().unwrap();
                if (u128::from(index.sequence()), hash) <= first {
                    return Indexer::address(index).map(|address| HashedEntryAddress { hash, address });
                }
                self.order.pop_first();
                Some(first.1)
            } else {
                self.clock_victim()
            };
            let Some(victim) = victim else {
                // All inspected slots got a second chance. Skip optional work,
                // but retain a bounded barrier against its delayed completions.
                self.floor = self.floor.max(Some(order));
                return Indexer::address(index).map(|address| HashedEntryAddress { hash, address });
            };
            let old = self.entries.remove(&victim).unwrap();
            self.floor = self.floor.max(Some((old.index.sequence(), victim)));
            Some((victim, old.index))
        } else {
            None
        };
        let stamp = if self.recovering {
            u128::from(index.sequence())
        } else {
            self.next_stamp()
        };
        self.order.insert((stamp, hash));
        self.entries.insert(
            hash,
            BoundedEntry {
                index,
                stamp,
                referenced: false,
            },
        );
        self.usage.0.store(self.entries.len(), Ordering::Relaxed);
        old.and_then(|(hash, old)| Indexer::address(old).map(|address| HashedEntryAddress { hash, address }))
    }

    fn next_stamp(&mut self) -> u128 {
        self.clock += 1;
        self.clock
    }

    fn clock_victim(&mut self) -> Option<u64> {
        // Fixed work per admission, even for an all-hot very large index. A
        // subsequent admission continues at the rotated hand; no entry is pinned.
        let probes = if self.limit == 1 {
            1
        } else {
            self.limit.saturating_mul(2).min(64)
        };
        for _ in 0..probes {
            let (_, hash) = self.order.pop_first().unwrap();
            let stamp = self.next_stamp();
            let entry = self.entries.get_mut(&hash).unwrap();
            if !entry.referenced {
                return Some(hash);
            }
            entry.referenced = false;
            entry.stamp = stamp;
            self.order.insert((stamp, hash));
        }
        None
    }

    fn remove(&mut self, hash: u64, sequence: Sequence, tombstones: bool) -> Option<EntryAddress> {
        let entry = self.entries.get(&hash)?;
        let index = &entry.index;
        if index.sequence() > sequence || (!tombstones && matches!(index, Index::Tombstone(_) | Index::Pending(_))) {
            return None;
        }
        // A delayed write/reinsertion may still carry an older sequence even
        // after physical reclamation removed the newest address. Preserve a
        // bounded global barrier rather than forgetting that newer version.
        // Conservatively losing other older cache copies is preferable to stale data.
        self.floor = self.floor.max(Some((index.sequence(), hash)));
        self.order.remove(&(entry.stamp, hash));
        let removed = self.entries.remove(&hash).unwrap();
        self.usage.0.store(self.entries.len(), Ordering::Relaxed);
        Indexer::address(removed.index)
    }
}

/// [`Indexer`] records key hash to entry address on fs.
#[derive(Debug, Clone)]
pub struct Indexer {
    shards: Arc<Vec<RwLock<IndexerShard>>>,
    shard_mask: usize,
    bounded: Option<Arc<Mutex<BoundedIndex>>>,
    recovery_floor: Arc<AtomicU64>,
}

impl Indexer {
    pub fn new(shards: usize) -> Self {
        let shard_mask = if shards.is_power_of_two() {
            shards - 1
        } else {
            usize::MAX
        };
        let shards = (0..shards)
            .map(|_| RwLock::new(HashMap::with_hasher(ModHasher::default())))
            .collect_vec();
        Self {
            shards: Arc::new(shards),
            shard_mask,
            bounded: None,
            recovery_floor: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_capacity(mut self, capacity: Option<NonZeroUsize>, usage: IndexUsage) -> Self {
        if let Some(capacity) = capacity {
            self.bounded = Some(Arc::new(Mutex::new(BoundedIndex {
                // Fund the bounded hash table at construction, not a whole-table
                // growth/rehash while holding a request-path admission lock.
                entries: HashMap::with_capacity_and_hasher(capacity.get(), ModHasher::default()),
                order: BTreeSet::new(),
                floor: None,
                limit: capacity.get(),
                usage,
                recovering: true,
                clock: 0,
            })));
        }
        self
    }

    pub fn is_bounded(&self) -> bool {
        self.bounded.is_some()
    }

    /// Install the log's persistent history-loss boundary before scanning data.
    /// Tombstones may already have been visited, so remove covered slots too.
    pub fn set_recovery_floor(&self, floor: u64) {
        if floor == 0 {
            return;
        }
        self.recovery_floor.store(floor, Ordering::Relaxed);
        if let Some(bounded) = &self.bounded {
            let mut bounded = bounded.lock();
            bounded.entries.retain(|_, entry| entry.index.sequence() > floor);
            let BoundedIndex { entries, order, .. } = &mut *bounded;
            order.retain(|(_, hash)| entries.contains_key(hash));
            bounded.floor = bounded.floor.max(Some((floor, u64::MAX)));
            bounded.usage.0.store(bounded.entries.len(), Ordering::Relaxed);
        }
        for shard in self.shards.iter() {
            shard.write().retain(|_, index| index.sequence() > floor);
        }
    }

    pub fn recovery_floor(&self) -> u64 {
        self.recovery_floor.load(Ordering::Relaxed)
    }

    pub fn finish_recovery(&self) {
        if let Some(bounded) = &self.bounded {
            let mut bounded = bounded.lock();
            bounded.clock = bounded.order.last().map_or(0, |(stamp, _)| *stamp);
            bounded.recovering = false;
        }
    }

    pub fn touch(&self, hash: u64) -> bool {
        if let Some(bounded) = &self.bounded {
            let mut bounded = bounded.lock();
            if bounded
                .entries
                .get(&hash)
                .is_some_and(|entry| matches!(entry.index, Index::Address(_)))
            {
                let stamp = bounded.next_stamp();
                let entry = bounded.entries.get_mut(&hash).unwrap();
                let old_stamp = entry.stamp;
                entry.referenced = true;
                entry.stamp = stamp;
                bounded.order.remove(&(old_stamp, hash));
                bounded.order.insert((stamp, hash));
                return true;
            }
        }
        false
    }

    pub fn has_sequence(&self, hash: u64, sequence: Sequence) -> bool {
        self.bounded.as_ref().is_some_and(|bounded| {
            bounded
                .lock()
                .entries
                .get(&hash)
                .is_some_and(|entry| entry.index.sequence() == sequence)
        })
    }

    pub fn reserve(&self, hash: u64, sequence: Sequence) -> Option<EntryAddress> {
        self.bounded
            .as_ref()
            .and_then(|bounded| bounded.lock().insert(hash, Index::Pending(sequence)))
            .map(|old| old.address)
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::insert_tombstone")
    )]
    pub fn insert_tombstone(&self, hash: u64, sequence: Sequence) -> Option<EntryAddress> {
        if let Some(bounded) = &self.bounded {
            return bounded
                .lock()
                .insert(hash, Index::Tombstone(sequence))
                .map(|old| old.address);
        }
        let shard = self.shard(hash);
        let mut shard = self.shards[shard].write();
        self.insert_inner(&mut shard, hash, Index::Tombstone(sequence))
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::insert_batch")
    )]
    pub fn insert_batch(&self, mut batch: Vec<HashedEntryAddress>) -> Vec<HashedEntryAddress> {
        if let Some(bounded) = &self.bounded {
            let mut bounded = bounded.lock();
            return batch
                .into_iter()
                .filter_map(|entry| bounded.insert(entry.hash, Index::Address(entry.address)))
                .collect();
        }
        let shards = self.partition_batch(&mut batch);
        drop(batch);
        shards
            .into_iter()
            .enumerate()
            .filter(|(_, batch)| !batch.is_empty())
            .flat_map(|(shard, batch)| self.insert_shard_batch(shard, batch))
            .collect()
    }

    pub(super) async fn insert_recovery_batch(
        &self,
        batch: &mut Vec<HashedEntryAddress>,
        concurrency: usize,
        spawner: Spawner,
    ) -> Result<()> {
        let this = self.clone();
        stream::iter(
            self.partition_batch(batch)
                .into_iter()
                .enumerate()
                .filter(|(_, batch)| !batch.is_empty())
                .map(move |(shard, batch)| {
                    let this = this.clone();
                    spawner.spawn(async move { this.insert_shard_batch_discard(shard, batch) })
                }),
        )
        .buffer_unordered(concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await
        .map(drop)
    }

    fn partition_batch(&self, batch: &mut Vec<HashedEntryAddress>) -> Vec<Vec<HashedEntryAddress>> {
        let mut counts = vec![0; self.shards.len()];
        for haddr in batch.iter() {
            counts[self.shard(haddr.hash)] += 1;
        }

        let mut shards = counts.iter().map(|count| Vec::with_capacity(*count)).collect_vec();
        for haddr in batch.drain(..) {
            let shard = self.shard(haddr.hash);
            shards[shard].push(haddr);
        }
        shards
    }

    fn insert_shard_batch(&self, shard: usize, batch: Vec<HashedEntryAddress>) -> Vec<HashedEntryAddress> {
        let mut olds = vec![];
        self.insert_shard_batch_with(shard, batch, |hash, address| {
            olds.push(HashedEntryAddress { hash, address });
        });
        olds
    }

    fn insert_shard_batch_discard(&self, shard: usize, batch: Vec<HashedEntryAddress>) {
        self.insert_shard_batch_with(shard, batch, |_, _| {});
    }

    fn insert_shard_batch_with(
        &self,
        shard: usize,
        batch: Vec<HashedEntryAddress>,
        mut on_old: impl FnMut(u64, EntryAddress),
    ) {
        let mut shard = self.shards[shard].write();
        let available = shard.capacity().saturating_sub(shard.len());
        if available < batch.len() {
            shard.reserve(batch.len());
        }
        for haddr in batch {
            if let Some(old) = self.insert_inner(&mut shard, haddr.hash, Index::Address(haddr.address)) {
                on_old(haddr.hash, old);
            }
        }
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::block::indexer::get"))]
    pub fn get(&self, hash: u64) -> Option<EntryAddress> {
        if let Some(bounded) = &self.bounded {
            return bounded
                .lock()
                .entries
                .get(&hash)
                .and_then(|entry| Self::address(entry.index.clone()));
        }
        let shard = self.shard(hash);
        match self.shards[shard].read().get(&hash) {
            Some(index) => match index {
                Index::Address(addr) => Some(addr.clone()),
                Index::Tombstone(_) | Index::Pending(_) => None,
            },
            None => None,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::remove")
    )]
    pub fn remove(&self, hash: u64) -> Option<EntryAddress> {
        if let Some(bounded) = &self.bounded {
            return bounded.lock().remove(hash, Sequence::MAX, false);
        }
        let shard = self.shard(hash);
        match self.shards[shard].write().entry(hash) {
            Entry::Occupied(o) => match o.get() {
                Index::Address(_) => self.extract_address(o.remove()),
                Index::Tombstone(_) | Index::Pending(_) => None,
            },
            Entry::Vacant(_) => None,
        }
    }

    #[cfg_attr(
        feature = "tracing",
        fastrace::trace(name = "foyer::storage::block::indexer::remove_batch")
    )]
    pub fn remove_batch<I>(&self, batch: I) -> Vec<EntryAddress>
    where
        I: IntoIterator<Item = (u64, Sequence)>,
    {
        if let Some(bounded) = &self.bounded {
            let mut bounded = bounded.lock();
            return batch
                .into_iter()
                .filter_map(|(hash, sequence)| bounded.remove(hash, sequence, true))
                .collect();
        }
        let shards = batch.into_iter().into_group_map_by(|(hash, _)| self.shard(*hash));

        let mut olds = vec![];
        for (s, hashes) in shards {
            let mut shard = self.shards[s].write();
            for (hash, sequence) in hashes {
                match shard.entry(hash) {
                    Entry::Occupied(o) => {
                        if sequence >= o.get().sequence()
                            && let Some(addr) = self.extract_address(o.remove())
                        {
                            olds.push(addr);
                        }
                    }
                    Entry::Vacant(_) => {}
                }
            }
        }
        olds
    }

    #[cfg_attr(feature = "tracing", fastrace::trace(name = "foyer::storage::block::indexer::clear"))]
    pub fn clear(&self) {
        if let Some(bounded) = &self.bounded {
            let mut bounded = bounded.lock();
            bounded.entries.clear();
            bounded.order.clear();
            bounded.usage.0.store(0, Ordering::Relaxed);
        }
        self.shards.iter().for_each(|shard| shard.write().clear());
    }

    pub(super) fn compact_and_count(&self) -> usize {
        if let Some(bounded) = &self.bounded {
            return bounded.lock().entries.len();
        }
        self.shards
            .iter()
            .map(|shard| {
                let mut shard = shard.write();
                if shard.capacity() > shard.len().saturating_mul(2) {
                    shard.shrink_to_fit();
                }
                shard.len()
            })
            .sum()
    }

    #[inline(always)]
    fn shard(&self, hash: u64) -> usize {
        // Select the shard from mixed high bits. `IndexerShard` uses the original hash with `ModHasher`, so reusing
        // its low bits here would make every key in a shard start in the same group of hash table buckets.
        const GOLDEN_RATIO: u64 = 0x9e37_79b9_7f4a_7c15;
        let mixed = (hash.wrapping_mul(GOLDEN_RATIO) >> 32) as usize;
        if self.shard_mask != usize::MAX {
            mixed & self.shard_mask
        } else {
            mixed % self.shards.len()
        }
    }

    fn insert_inner(&self, shard: &mut IndexerShard, hash: u64, index: Index) -> Option<EntryAddress> {
        let floor = self.recovery_floor();
        if floor != 0 && index.sequence() <= floor {
            return self.extract_address(index);
        }
        match shard.entry(hash) {
            Entry::Occupied(mut o) => {
                // `>` for updates.
                // '=' for reinsertions.
                let deleted_at_same_sequence = index.sequence() == o.get().sequence()
                    && matches!(o.get(), Index::Tombstone(_))
                    && matches!(index, Index::Address(_));
                if index.sequence() >= o.get().sequence() && !deleted_at_same_sequence {
                    self.extract_address(o.insert(index))
                } else {
                    self.extract_address(index)
                }
            }
            Entry::Vacant(v) => {
                v.insert(index);
                None
            }
        }
    }

    fn extract_address(&self, index: Index) -> Option<EntryAddress> {
        Self::address(index)
    }

    fn address(index: Index) -> Option<EntryAddress> {
        match index {
            Index::Address(addr) => Some(addr),
            Index::Tombstone(_) | Index::Pending(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn haddr(hash: u64, sequence: Sequence) -> HashedEntryAddress {
        HashedEntryAddress {
            hash,
            address: EntryAddress {
                block: 0,
                offset: 0,
                len: 1,
                sequence,
            },
        }
    }

    #[test]
    fn recovery_history_floor_rejects_old_states_in_both_index_modes() {
        for capacity in [None, NonZeroUsize::new(4)] {
            let usage = IndexUsage::default();
            let indexer = Indexer::new(4).with_capacity(capacity, usage.clone());
            indexer.insert_batch(vec![haddr(1, 10), haddr(2, 21)]);
            indexer.insert_tombstone(3, 15);
            indexer.set_recovery_floor(20);
            indexer.finish_recovery();
            assert!(indexer.get(1).is_none());
            assert_eq!(indexer.get(2).unwrap().sequence, 21);
            indexer.insert_batch(vec![haddr(1, 19), haddr(3, 15)]);
            assert!(
                indexer.get(1).is_none(),
                "a forgotten deletion must not resurrect an old address"
            );
            assert!(
                indexer.get(3).is_none(),
                "cleared tombstone slots must not authorize covered relocations"
            );
            indexer.insert_batch(vec![haddr(1, 22)]);
            assert_eq!(indexer.get(1).unwrap().sequence, 22);
            assert_eq!(indexer.recovery_floor(), 20);
            if capacity.is_some() {
                assert_eq!(usage.entries(), 2);
            }
        }
    }

    #[test]
    fn test_shard_selection_does_not_reuse_hash_low_bits() {
        let indexer = Indexer::new(64);
        let mut low_bits = [0u64; 64];
        for hash in 0..4096 {
            low_bits[indexer.shard(hash)] |= 1 << (hash & 63);
        }
        assert!(low_bits.iter().all(|bits| bits.count_ones() > 16));
    }

    #[test]
    fn test_compact_duplicate_heavy_batch() {
        let indexer = Indexer::new(4);
        indexer.insert_batch((0..10_000).map(|sequence| haddr(sequence % 10, sequence)).collect());

        let capacity_before = indexer
            .shards
            .iter()
            .map(|shard| shard.read().capacity())
            .sum::<usize>();
        assert_eq!(indexer.compact_and_count(), 10);
        let capacity_after = indexer
            .shards
            .iter()
            .map(|shard| shard.read().capacity())
            .sum::<usize>();

        assert!(capacity_after < capacity_before);
        for hash in 0..10 {
            assert_eq!(indexer.get(hash).unwrap().sequence, 9_990 + hash);
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_recovery_batches_match_insert_batch() {
        let expected = Indexer::new(7);
        let actual = Indexer::new(7);
        let mut batch = (0..10_000)
            .map(|i| {
                let mut entry = haddr((i * 31 % 257) as u64, (i * 17 % 101) as Sequence);
                entry.address.block = i as BlockId;
                entry
            })
            .collect_vec();
        batch[126] = haddr(u64::MAX, 42);
        batch[126].address.block = 1;
        batch[127] = haddr(u64::MAX, 42);
        batch[127].address.block = 2;

        expected.insert_batch(batch.clone());
        let mut entries = batch.into_iter();
        let mut chunk = Vec::with_capacity(127);
        loop {
            chunk.extend(entries.by_ref().take(127));
            if chunk.is_empty() {
                break;
            }
            actual
                .insert_recovery_batch(&mut chunk, 8, Spawner::current())
                .await
                .unwrap();
        }

        for hash in 0..257 {
            assert_eq!(actual.get(hash), expected.get(hash));
        }
        assert_eq!(actual.get(u64::MAX), expected.get(u64::MAX));
    }
}
