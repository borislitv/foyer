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

use std::sync::Arc;

use asyncband::mutex::Mutex;
use bytes::{Buf, BufMut};
use foyer_common::error::{Error, ErrorKind, Result};

use crate::{
    IoEngine,
    io::{PAGE, bytes::IoSliceMut, device::Partition},
    serde::Checksummer,
};

#[derive(Debug, Clone)]
pub struct Tombstone {
    pub hash: u64,
    pub sequence: u64,
}

impl Tombstone {
    const SERIALIZED_LEN: usize = size_of::<u64>() + size_of::<u64>();

    fn write(&self, mut buf: impl BufMut) {
        buf.put_u64(self.hash);
        buf.put_u64(self.sequence);
    }

    fn read(mut buf: impl Buf) -> Self {
        let hash = buf.get_u64();
        let sequence = buf.get_u64();
        Self { hash, sequence }
    }
}

#[derive(Debug, Clone)]
pub struct TombstoneLog {
    inner: Arc<Mutex<TombstoneLogInner>>,
    pages: usize,
    recovery_floor: u64,
}

#[derive(Debug)]
struct TombstoneLogInner {
    buffer: PageBuffer,
    slot: usize,
    checkpoint: Option<PageBuffer>,
    floor: u64,
}

impl TombstoneLog {
    pub const SLOTS_PER_PAGE: usize = PAGE / Tombstone::SERIALIZED_LEN;
    // Unwrapped logs keep the legacy layout. First overflow upgrades slot zero
    // and slot one to a checkpoint; older readers must not open upgraded logs.
    // Its whole-page checksum fails closed on recognizable torn checkpoints.
    // This is ordered I/O completion, not an fsync/stable-media guarantee; an
    // erased entire legacy log is indistinguishable from a new empty device.
    const PREPARING: &'static [u8; 8] = b"FOYERTP2";
    const CHECKPOINT: &'static [u8; 8] = b"FOYERTL2";
    const HEADER_SLOTS: usize = 2;

    /// All payload sequences at or below this durable history-loss boundary
    /// must be excluded during recovery, including hashes absent from the log.
    pub fn recovery_floor(&self) -> u64 {
        self.recovery_floor
    }

    fn seal_checkpoint(page: &mut [u8], floor: u64, slot: usize) {
        page[..8].copy_from_slice(Self::CHECKPOINT);
        page[8..16].copy_from_slice(&floor.to_be_bytes());
        page[16..24].copy_from_slice(&(slot as u64).to_be_bytes());
        page[24..32].fill(0);
        let checksum = Checksummer::checksum64(page);
        page[24..32].copy_from_slice(&checksum.to_be_bytes());
    }

    /// Open the tombstone log with given a dedicated device.
    ///
    /// The tombstone log will
    pub async fn open(
        partitions: Vec<Arc<dyn Partition>>,
        io_engine: Arc<dyn IoEngine>,
        tombstones: &mut Vec<Tombstone>,
    ) -> Result<Self> {
        Self::open_with_visitor(partitions, io_engine, |tombstone| tombstones.push(tombstone)).await
    }

    /// Restore the append cursor and visit records one page at a time. The caller
    /// owns any retained metadata; this reader does not collect a log inventory.
    pub async fn open_with_visitor(
        partitions: Vec<Arc<dyn Partition>>,
        io_engine: Arc<dyn IoEngine>,
        mut visitor: impl FnMut(Tombstone),
    ) -> Result<Self> {
        let pages = partitions.iter().map(|p| p.size()).sum::<usize>() / PAGE;
        let slots = pages * Self::SLOTS_PER_PAGE;
        let mut checkpoint = None;
        let mut legacy_next = 1;
        let mut legacy_gap = false;
        let mut partition_slot = 0;
        for partition in partitions.iter() {
            for offset in (0..partition.size()).step_by(PAGE) {
                tracing::trace!(offset, "[tombstone log]: recover at");
                let buf = IoSliceMut::new(PAGE);
                let (buffer, res) = io_engine.read(Box::new(buf), partition.as_ref(), offset as u64).await;
                res?;
                let mut buffer = *buffer.try_into_io_slice_mut().unwrap();
                if partition_slot == 0 && offset == 0 {
                    if &buffer[..8] == Self::CHECKPOINT {
                        let checksum = u64::from_be_bytes(buffer[24..32].try_into().unwrap());
                        buffer[24..32].fill(0);
                        let valid = Checksummer::checksum64(&buffer) == checksum;
                        buffer[24..32].copy_from_slice(&checksum.to_be_bytes());
                        let floor = u64::from_be_bytes(buffer[8..16].try_into().unwrap());
                        let cursor = u64::from_be_bytes(buffer[16..24].try_into().unwrap());
                        if !valid || floor == u64::MAX || cursor < Self::HEADER_SLOTS as u64 || cursor >= slots as u64 {
                            return Err(Error::new(ErrorKind::Recover, "invalid tombstone history checkpoint"));
                        }
                        checkpoint = Some((floor, cursor as usize));
                    } else if buffer[..16].iter().any(|byte| *byte != 0) {
                        // Old logs start at slot one. An occupied slot zero
                        // proves rollover, but cannot prove which later-sequence
                        // deletions unordered flushers already overwrote. A
                        // partial checkpoint upgrade is equally unsafe to read.
                        return Err(Error::new(
                            ErrorKind::Recover,
                            "unrecoverable legacy or incomplete tombstone rollover",
                        ));
                    }
                }

                for (slot, buf) in buffer.chunks_exact(Tombstone::SERIALIZED_LEN).enumerate() {
                    let physical_slot = partition_slot + offset / PAGE * Self::SLOTS_PER_PAGE + slot;
                    if physical_slot < checkpoint.map_or(1, |_| Self::HEADER_SLOTS) {
                        continue;
                    }
                    let tombstone = Tombstone::read(buf);
                    if tombstone.sequence == u64::MAX {
                        return Err(Error::new(ErrorKind::Recover, "exhausted tombstone sequence"));
                    }
                    if tombstone.sequence == 0 {
                        if buf.iter().any(|byte| *byte != 0) {
                            return Err(Error::new(ErrorKind::Recover, "incomplete tombstone record"));
                        }
                        legacy_gap = true;
                        continue;
                    }
                    if checkpoint.is_none() {
                        if legacy_gap {
                            return Err(Error::new(ErrorKind::Recover, "noncontiguous legacy tombstone history"));
                        }
                        legacy_next = physical_slot + 1;
                    }
                    visitor(tombstone);
                }
            }
            partition_slot += partition.size() / PAGE * Self::SLOTS_PER_PAGE;
        }
        // Physical occupancy, not maximum logical sequence, restores an
        // unwrapped legacy cursor even when multiple flushers append unordered.
        let (floor, slot) = checkpoint.unwrap_or((0, legacy_next));
        let (page, _) = Self::calculate_slot_addr(pages, slot);
        let checkpoint = if checkpoint.is_some() {
            Some(PageBuffer::open(io_engine.clone(), partitions.clone(), 0).await?)
        } else {
            None
        };
        let buffer = PageBuffer::open(io_engine, partitions, page as _).await?;

        Ok(Self {
            inner: Arc::new(Mutex::new(TombstoneLogInner {
                buffer,
                slot,
                checkpoint,
                floor,
            })),
            pages,
            recovery_floor: floor,
        })
    }

    fn calculate_slot_addr(pages: usize, slot: usize) -> (u32, usize) {
        let page = slot / Self::SLOTS_PER_PAGE;
        let page = page % pages;
        let offset = (slot % Self::SLOTS_PER_PAGE) * Tombstone::SERIALIZED_LEN;
        (page as u32, offset)
    }

    fn slot_addr(&self, slot: usize) -> (u32, usize) {
        Self::calculate_slot_addr(self.pages, slot)
    }

    pub async fn append(&self, tombstones: impl Iterator<Item = &Tombstone>) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let mut tombstones = tombstones;
        let mut next = tombstones.next();
        let slots = self.pages * Self::SLOTS_PER_PAGE;
        while next.is_some() {
            if inner.checkpoint.is_none() && inner.slot == slots {
                // First overflow is the only format transition. Until this
                // point not a single legacy record or unchanged payload loses
                // history. Prepare touches ONLY previously unused slot zero;
                // a torn prepare cannot erase an old deletion invisibly.
                inner.buffer.load(0).await?;
                let floor = Tombstone::read(&inner.buffer.as_ref()[16..32]).sequence;
                inner.buffer.as_mut()[..8].copy_from_slice(Self::PREPARING);
                inner.buffer.as_mut()[8..16].copy_from_slice(&floor.to_be_bytes());
                inner.buffer.flush().await?;
                Self::seal_checkpoint(inner.buffer.as_mut(), floor, Self::HEADER_SLOTS);
                inner.buffer.flush().await?;
                inner.checkpoint =
                    Some(PageBuffer::open(inner.buffer.io_engine.clone(), inner.buffer.partitions.clone(), 0).await?);
                inner.floor = floor;
                inner.slot = Self::HEADER_SLOTS;
            }
            let page = self.slot_addr(inner.slot).0;
            let checkpointed = inner.checkpoint.is_some();
            if (page != 0 || !checkpointed) && inner.buffer.page != page {
                inner.buffer.load(page).await?;
            }
            let TombstoneLogInner {
                buffer,
                slot,
                checkpoint,
                floor,
            } = &mut *inner;
            let target = if page == 0 && checkpointed {
                checkpoint.as_mut().unwrap()
            } else {
                &mut *buffer
            };
            while self.slot_addr(*slot).0 == page {
                let Some(tombstone) = next.take() else {
                    break;
                };
                let start = self.slot_addr(*slot).1;
                let end = start + Tombstone::SERIALIZED_LEN;
                *floor = (*floor).max(Tombstone::read(&target.as_ref()[start..end]).sequence);
                tombstone.write(&mut target.as_mut()[start..end]);
                next = tombstones.next();
                *slot += 1;
                if *slot == slots {
                    if !checkpointed {
                        break;
                    }
                    *slot = Self::HEADER_SLOTS;
                }
            }
            if checkpointed {
                if page == 0 {
                    Self::seal_checkpoint(target.as_mut(), *floor, *slot);
                    target.flush().await?;
                } else {
                    let checkpoint = checkpoint.as_mut().unwrap();
                    Self::seal_checkpoint(checkpoint.as_mut(), *floor, *slot);
                    // Publish the history-loss floor and planned physical cursor
                    // BEFORE overwriting another page. No monotonic append-order
                    // assumption is needed, including across reopen cycles.
                    checkpoint.flush().await?;
                    buffer.flush().await?;
                }
            } else {
                target.flush().await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PageBuffer {
    // NOTE: This is always `Some(..)`.
    buffer: Option<IoSliceMut>,

    io_engine: Arc<dyn IoEngine>,
    partitions: Vec<Arc<dyn Partition>>,
    page: u32,
}

impl AsRef<[u8]> for PageBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref().unwrap()
    }
}

impl AsMut<[u8]> for PageBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut().unwrap()
    }
}

impl PageBuffer {
    pub async fn open(io_engine: Arc<dyn IoEngine>, partitions: Vec<Arc<dyn Partition>>, page: u32) -> Result<Self> {
        let mut this = Self {
            buffer: Some(IoSliceMut::new(PAGE)),
            io_engine,
            partitions,
            page,
        };

        this.update().await?;

        Ok(this)
    }

    pub async fn update(&mut self) -> Result<()> {
        let buf = self.buffer.take().unwrap();
        let (partition, offset) = self.locate(self.page);
        let (buf, res) = self
            .io_engine
            .read(Box::new(buf), self.partitions[partition].as_ref(), offset)
            .await;
        let buf = *buf.try_into_io_slice_mut().unwrap();
        self.buffer = Some(buf);
        res?;
        Ok(())
    }

    pub async fn load(&mut self, page: u32) -> Result<()> {
        tracing::trace!(page, "[tombstone log page buffer]: load page");
        self.page = page;
        self.update().await
    }

    pub async fn flush(&mut self) -> Result<()> {
        tracing::trace!(page = self.page, "[tombstone log page buffer]: flush page");
        let buf = self.buffer.take().unwrap();
        let (partition, offset) = self.locate(self.page);
        let (buf, res) = self
            .io_engine
            .write(Box::new(buf), self.partitions[partition].as_ref(), offset)
            .await;
        let buf = *buf.try_into_io_slice_mut().unwrap();
        self.buffer = Some(buf);
        res?;
        Ok(())
    }

    fn locate(&self, mut page: u32) -> (usize, u64) {
        let mut partition = 0;

        loop {
            let partition_pages = (self.partitions[partition].size() / PAGE) as u32;
            if page < partition_pages {
                break (partition, PAGE as u64 * page as u64);
            }
            page -= partition_pages;
            partition += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use foyer_common::spawn::Spawner;
    use itertools::Itertools;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        IoEngineConfig, PsyncIoEngineConfig,
        io::{
            device::{DeviceBuilder, fs::FsDeviceBuilder},
            engine::IoEngineBuildContext,
        },
    };

    #[test_log::test(tokio::test)]
    async fn test_tombstone_log() {
        let dir = tempdir().unwrap();

        // 4 MB cache device => 16 KB tombstone log => 1K tombstones => 16K partition => 4 pages
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(4 * 1024 * 1024 + 16 * 1024)
            .build()
            .unwrap();
        let p0 = device.create_partition(8 * 1024).unwrap();
        let p1 = device.create_partition(8 * 1024).unwrap();
        let io_engine = PsyncIoEngineConfig::new()
            .boxed()
            .build(IoEngineBuildContext {
                spawner: Spawner::current(),
            })
            .await
            .unwrap();

        let log = TombstoneLog::open(vec![p0.clone(), p1.clone()], io_engine.clone(), &mut vec![])
            .await
            .unwrap();

        log.append(
            (0..3 * 1024 + 42)
                .map(|i| Tombstone {
                    hash: i + 1,
                    sequence: i + 1,
                })
                .collect_vec()
                .iter(),
        )
        .await
        .unwrap();

        {
            let inner = log.inner.lock().await;
            assert_eq!(
                inner.slot, 49,
                "two checkpoint slots are reserved only after first rollover"
            );
            let (page, _) = log.slot_addr(inner.slot);
            let buffer = if page == 0 {
                inner.checkpoint.as_ref().unwrap()
            } else {
                &inner.buffer
            };
            assert_eq!(buffer.page, page);
        }

        drop(log);

        let log = TombstoneLog::open(vec![p0.clone(), p1.clone()], io_engine.clone(), &mut vec![])
            .await
            .unwrap();

        {
            let inner = log.inner.lock().await;
            assert_eq!(inner.slot, 49);
            assert_eq!(log.recovery_floor(), 2092);
            let (page, _) = log.slot_addr(inner.slot);
            assert_eq!(inner.buffer.page, page);
        }
    }

    #[tokio::test]
    async fn checkpoint_preserves_unordered_history_loss_across_multiple_wraps() {
        let dir = tempdir().unwrap();
        let device = FsDeviceBuilder::new(dir.path()).with_capacity(PAGE).build().unwrap();
        let partitions = vec![device.create_partition(PAGE).unwrap()];
        let io = PsyncIoEngineConfig::new()
            .boxed()
            .build(IoEngineBuildContext {
                spawner: Spawner::current(),
            })
            .await
            .unwrap();
        let log = TombstoneLog::open(partitions.clone(), io.clone(), &mut vec![])
            .await
            .unwrap();
        let mut entries = vec![
            Tombstone { hash: 77, sequence: 1 },
            Tombstone {
                hash: 42,
                sequence: 9000,
            },
        ];
        entries.extend((2..255).map(|sequence| Tombstone { hash: 77, sequence }));
        log.append(entries.iter()).await.unwrap();
        drop(log);
        let log = TombstoneLog::open(partitions.clone(), io.clone(), &mut vec![])
            .await
            .unwrap();
        assert_eq!(
            log.inner.lock().await.slot,
            256,
            "restore physical cursor, not max-sequence location"
        );
        assert_eq!(log.recovery_floor(), 0, "no history was lost before actual overflow");
        for cycle in 0..3 {
            let start = 255 + cycle * 300;
            let low: Vec<_> = (start..start + 300)
                .map(|sequence| Tombstone { hash: 77, sequence })
                .collect();
            log.append(low.iter()).await.unwrap();
        }
        drop(log);
        let mut retained = Vec::new();
        let log = TombstoneLog::open(partitions.clone(), io.clone(), &mut retained)
            .await
            .unwrap();
        assert_eq!(retained.len(), 254);
        assert!(retained.iter().all(|entry| entry.sequence <= 1154));
        assert_eq!(
            log.recovery_floor(),
            9000,
            "overwritten high sequence must survive lower unordered appends"
        );
        let post_reopen = [Tombstone {
            hash: 88,
            sequence: 9001,
        }];
        log.append(post_reopen.iter()).await.unwrap();
        drop(log);
        let log = TombstoneLog::open(partitions, io, &mut vec![]).await.unwrap();
        assert_eq!(log.recovery_floor(), 9000);
    }

    #[tokio::test]
    async fn visitor_reopen_preserves_cursor_across_pages_and_partitions() {
        let dir = tempdir().unwrap();
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(4 * PAGE)
            .build()
            .unwrap();
        let partitions = vec![
            device.create_partition(2 * PAGE).unwrap(),
            device.create_partition(PAGE).unwrap(),
        ];
        let io = PsyncIoEngineConfig::new()
            .boxed()
            .build(IoEngineBuildContext {
                spawner: Spawner::current(),
            })
            .await
            .unwrap();
        let log = TombstoneLog::open_with_visitor(partitions.clone(), io.clone(), |_| {})
            .await
            .unwrap();
        let first: Vec<_> = (1..=600)
            .map(|sequence| Tombstone {
                hash: sequence + 1000,
                sequence,
            })
            .collect();
        log.append(first.iter()).await.unwrap();
        drop(log);
        let mut visited = Vec::new();
        let log = TombstoneLog::open_with_visitor(partitions.clone(), io.clone(), |entry| visited.push(entry))
            .await
            .unwrap();
        assert_eq!(visited.len(), 600);
        let next: Vec<_> = (601..=610)
            .map(|sequence| Tombstone {
                hash: sequence + 1000,
                sequence,
            })
            .collect();
        log.append(next.iter()).await.unwrap();
        drop(log);
        let mut compatible = Vec::new();
        let _log = TombstoneLog::open(partitions, io, &mut compatible).await.unwrap();
        compatible.sort_unstable_by_key(|entry| entry.sequence);
        assert_eq!(
            compatible.len(),
            610,
            "reopen must not overwrite an earlier page's still-live tombstones before wrap"
        );
        for (index, entry) in compatible.into_iter().enumerate() {
            assert_eq!(
                (entry.hash, entry.sequence),
                ((index + 1) as u64 + 1000, (index + 1) as u64)
            );
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test_log::test(tokio::test)]
    async fn test_page_buffer_locate_large_partitions() {
        use crate::io::{device::noop::NoopDeviceBuilder, engine::noop::NoopIoEngine};

        let gib = 1024usize * 1024 * 1024;
        // Mock devices exercise large partition sizes without allocating their backing storage.
        for size in [4 * gib - PAGE, 4 * gib, 4 * gib + PAGE, 6 * gib, 8 * gib] {
            let device = NoopDeviceBuilder::new(size + 2 * PAGE).build().unwrap();
            let partitions = vec![
                device.create_partition(size).unwrap(),
                device.create_partition(2 * PAGE).unwrap(),
            ];
            let buffer = PageBuffer::open(Arc::new(NoopIoEngine), partitions, 0).await.unwrap();
            let pages = (size / PAGE) as u32;

            assert_eq!(buffer.locate(0), (0, 0), "partition size: {size}");
            assert_eq!(
                buffer.locate(pages - 1),
                (0, (size - PAGE) as u64),
                "partition size: {size}"
            );
            assert_eq!(buffer.locate(pages), (1, 0), "partition size: {size}");
            assert_eq!(buffer.locate(pages + 1), (1, PAGE as u64), "partition size: {size}");
        }
    }
}
