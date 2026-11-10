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
    collections::VecDeque,
    fmt::Debug,
    future::{Future, poll_fn},
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Poll, ready},
    time::Instant,
};

use asyncband::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use foyer_common::{
    bits,
    code::{StorageKey, StorageValue},
    error::{Error, ErrorKind, Result},
    metrics::Metrics,
    properties::Properties,
    spawn::{SpawnHandle, Spawner},
};
use futures_core::future::BoxFuture;
use futures_util::{
    FutureExt,
    future::{Shared, join, join_all},
};
use itertools::Itertools;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[cfg(any(test, feature = "test_utils"))]
use crate::test_utils::*;
use crate::{
    Compression,
    engine::block::{
        buffer::{Batch, BlobPart, Block, Buffer, SplitCtx, Splitter},
        indexer::{EntryAddress, HashedEntryAddress, Indexer},
        manager::{BlockId, BlockManager, GetCleanBlockHandle},
        reclaimer::Reinsertion,
        serde::Sequence,
        tombstone::{Tombstone, TombstoneLog},
    },
    io::{
        PAGE,
        bytes::{IoSlice, IoSliceMut},
    },
    keeper::PieceRef,
};

/// Opt-in aggregate queued AND active submission bounds, shared by all flushers
/// and reclaimers. Bytes cover page-rounded enforced record maxima/raw buffers plus
/// submission metadata, NOT complete `Code` source backings (externally charged).
/// The fixed two flusher buffers and index must also be funded separately.
///
/// Guarded mutations require the fallible Store APIs. No persistence format or
/// recovery reset is introduced. Each controller belongs to one engine session.
#[derive(Debug, Clone)]
pub struct SubmissionLimits(Arc<SubmissionLimitInner>);

#[derive(Debug)]
struct SubmissionLimitInner {
    bytes: Arc<Semaphore>,
    items: Arc<Semaphore>,
    max_bytes: usize,
    max_items: usize,
    poisoned: AtomicBool,
    started: AtomicBool,
}

impl SubmissionLimits {
    /// Conservative per-item queue/credit/batch metadata allowance. Source
    /// backing allocations and fixed engine buffers are outside this counter.
    pub const METADATA_BYTES: usize = 512;
    /// Create nonblocking limits. Rejection does not accept or mutate a key.
    pub fn new(bytes: NonZeroUsize, items: NonZeroUsize) -> Result<Self> {
        if bytes.get() > u32::MAX as usize
            || bytes.get() > Semaphore::MAX_PERMITS
            || items.get() > Semaphore::MAX_PERMITS
        {
            return Err(Error::new(
                ErrorKind::OutOfRange,
                "submission limits exceed semaphore capacity",
            ));
        }
        Ok(Self(Arc::new(SubmissionLimitInner {
            bytes: Arc::new(Semaphore::new(bytes.get())),
            items: Arc::new(Semaphore::new(items.get())),
            max_bytes: bytes.get(),
            max_items: items.get(),
            poisoned: AtomicBool::new(false),
            started: AtomicBool::new(false),
        })))
    }

    /// Reserved serialized/raw bytes plus per-submission metadata (not source backings).
    pub fn bytes(&self) -> usize {
        self.0.max_bytes - self.0.bytes.available_permits()
    }
    /// Queued and active payload/invalidation/reinsertion submissions.
    pub fn items(&self) -> usize {
        self.0.max_items - self.0.items.available_permits()
    }
    /// An I/O/channel failure disables this session. Admission pressure alone
    /// never poisons it. No data is erased automatically, including on reopen.
    pub fn is_poisoned(&self) -> bool {
        self.0.poisoned.load(Ordering::Acquire)
    }
    pub(crate) fn poison(&self) {
        self.0.poisoned.store(true, Ordering::Release);
    }

    pub(crate) fn start(&self) -> Result<()> {
        if self.0.started.swap(true, Ordering::AcqRel) {
            return Err(Error::new(
                ErrorKind::OutOfRange,
                "submission limits instance already used",
            ));
        }
        Ok(())
    }

    pub(crate) fn finish(&self) -> Result<()> {
        if self.is_poisoned() {
            return Err(Error::new(
                ErrorKind::Io,
                "cache submissions failed; completion is not established",
            ));
        }
        Ok(())
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Option<SubmissionCredit> {
        if self.is_poisoned() {
            return None;
        }
        let bytes = u32::try_from(bytes).ok()?;
        let item = self.0.items.clone().try_acquire_owned().ok()?;
        let bytes = self.0.bytes.clone().try_acquire_many_owned(bytes).ok()?;
        Some(SubmissionCredit {
            _bytes: Some(bytes),
            _item: Some(item),
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct SubmissionCredit {
    _bytes: Option<OwnedSemaphorePermit>,
    _item: Option<OwnedSemaphorePermit>,
}

// Keep channel/control ownership explicit without exposing queue internals.
#[derive(Debug)]
pub(crate) struct FundedSubmission<K: StorageKey, V: StorageValue, P: Properties> {
    submission: Submission<K, V, P>,
    credit: SubmissionCredit,
}

#[derive(Debug, Default)]
struct FenceState {
    generation: u128,
    pending: Option<(u128, Shared<BoxFuture<'static, ()>>)>,
}

pub enum Submission<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    CacheEntry {
        piece: PieceRef<K, V, P>,
        estimated_size: usize,
        sequence: Sequence,
    },
    Tombstone {
        tombstone: Tombstone,
        stats: Option<InvalidStats>,
    },
    Reinsertion {
        reinsertion: Reinsertion,
    },
    Wait {
        tx: oneshot::Sender<()>,
    },
    Stop,
}

impl<K, V, P> Debug for Submission<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CacheEntry {
                piece,
                estimated_size,
                sequence,
            } => f
                .debug_struct("CacheEntry")
                .field("piece", piece)
                .field("estimated_size", estimated_size)
                .field("sequence", sequence)
                .finish(),
            Self::Tombstone { tombstone, stats } => f
                .debug_struct("Tombstone")
                .field("tombstone", tombstone)
                .field("stats", stats)
                .finish(),
            Self::Reinsertion { reinsertion } => {
                f.debug_struct("Reinsertion").field("reinsertion", reinsertion).finish()
            }
            Self::Wait { .. } => f.debug_struct("Wait").finish(),
            Self::Stop => f.debug_struct("Stop").finish(),
        }
    }
}

#[derive(Debug)]
pub struct Flusher<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    id: usize,
    tx: UnboundedSender<FundedSubmission<K, V, P>>,
    submit_queue_size: Arc<AtomicUsize>,
    limits: Option<SubmissionLimits>,
    record_limit: usize,
    fence: Arc<Mutex<FenceState>>,

    metrics: Arc<Metrics>,
}

impl<K, V, P> Clone for Flusher<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            tx: self.tx.clone(),
            submit_queue_size: self.submit_queue_size.clone(),
            limits: self.limits.clone(),
            record_limit: self.record_limit,
            fence: self.fence.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V, P> Flusher<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    pub fn new(
        id: usize,
        submit_queue_size: Arc<AtomicUsize>,
        metrics: Arc<Metrics>,
        limits: Option<SubmissionLimits>,
        record_limit: usize,
    ) -> (Self, UnboundedReceiver<FundedSubmission<K, V, P>>) {
        let (tx, rx) = asyncband::mpsc::unbounded();
        let this = Self {
            id,
            tx,
            submit_queue_size,
            limits,
            record_limit,
            fence: Arc::new(Mutex::new(FenceState::default())),
            metrics,
        };
        (this, rx)
    }

    #[expect(clippy::too_many_arguments)]
    pub fn run(
        &self,
        rx: UnboundedReceiver<FundedSubmission<K, V, P>>,
        block_size: usize,
        io_buffer_size: usize,
        blob_index_size: usize,
        compression: Compression,
        indexer: Indexer,
        block_manager: BlockManager,
        tombstone_log: Option<TombstoneLog>,
        metrics: Arc<Metrics>,
        spawner: &Spawner,
        #[cfg(any(test, feature = "test_utils"))] flush_switch: Switch,
    ) -> Result<SpawnHandle<Result<()>>> {
        let id = self.id;
        let io_buffer_size = bits::align_down(PAGE, io_buffer_size);
        assert!(io_buffer_size > 0);

        bits::assert_aligned(PAGE, io_buffer_size);
        bits::assert_aligned(PAGE, blob_index_size);

        let max_entry_size = (block_size - blob_index_size).min(self.record_limit);

        let bytes = IoSliceMut::new(io_buffer_size);
        let rotate_buffer = Some(IoSliceMut::new(io_buffer_size));

        let buffer = Buffer::new(bytes, max_entry_size, metrics.clone());
        let buffer = Some(buffer);

        let current_block_handle = block_manager.get_clean_block();

        let ctx = SplitCtx::new(block_size, blob_index_size);

        let runner = Runner {
            id,
            rx: Some(rx),
            buffer,
            ctx,
            tombstone_infos: vec![],
            waiters: vec![],
            piece_refs: vec![],
            rotate_buffer,
            queue_init: None,
            submit_queue_size: self.submit_queue_size.clone(),
            block_manager,
            indexer,
            tombstone_log,
            compression,
            spawner: spawner.clone(),
            metrics: metrics.clone(),
            io_tasks: VecDeque::with_capacity(1),
            current_block_handle,
            max_entry_size,
            stopping: false,
            credits: vec![],
            limits: self.limits.clone(),
            #[cfg(any(test, feature = "test_utils"))]
            flush_switch,
        };

        Ok(spawner.spawn(runner.run()))
    }

    /// Reserve raw reinsertion storage BEFORE its I/O allocation. The caller
    /// must retain this credit across read completion and transfer it to submit.
    pub(crate) fn reserve_reinsertion(&self, bytes: usize) -> Option<SubmissionCredit> {
        if bytes > self.record_limit {
            return None;
        }
        self.reserve(bits::align_up(PAGE, bytes))
    }

    pub(crate) fn has_limits(&self) -> bool {
        self.limits.is_some()
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Option<SubmissionCredit> {
        match &self.limits {
            Some(limits) => limits.reserve(bytes.checked_add(SubmissionLimits::METADATA_BYTES)?),
            None => Some(SubmissionCredit::default()),
        }
    }

    pub(crate) fn reserve_payload(&self) -> Option<SubmissionCredit> {
        self.reserve(bits::align_up(PAGE, self.record_limit))
    }

    pub fn submit(&self, submission: Submission<K, V, P>) -> bool {
        let bytes = match &submission {
            // Charge the enforced record maximum rather than trusting a user's
            // Code::estimated_size. Both serialized buffers are separately fixed.
            Submission::CacheEntry { .. } => Some(bits::align_up(PAGE, self.record_limit)),
            Submission::Tombstone { .. } => Some(0),
            Submission::Reinsertion { reinsertion } => Some(reinsertion.slice.len()),
            // Fences carry no payload. Their callers own their wait futures;
            // reclaimers coalesce them rather than enqueue one per region.
            Submission::Wait { .. } | Submission::Stop => {
                return self.submit_funded(submission, SubmissionCredit::default());
            }
        };
        let Some(credit) = bytes.and_then(|bytes| self.reserve(bytes)) else {
            return false;
        };
        self.submit_funded(submission, credit)
    }

    pub(crate) fn submit_funded(&self, submission: Submission<K, V, P>, credit: SubmissionCredit) -> bool {
        let mut fence = self.fence.lock().unwrap();
        tracing::trace!(id = self.id, "[block engine flusher]: submit task: {submission:?}");
        if let Submission::CacheEntry { estimated_size, .. } = &submission {
            self.submit_queue_size.fetch_add(*estimated_size, Ordering::Relaxed);
        }
        if let Err(e) = self.tx.send(FundedSubmission { submission, credit }) {
            if let Some(limits) = &self.limits {
                limits.poison();
            }
            tracing::error!(
                id = self.id,
                "[block engine flusher]: error raised when submitting task, error: {e}"
            );
            return false;
        }
        fence.generation += 1;
        true
    }

    pub fn wait(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut fence = self.fence.lock().unwrap();
        if let Some((generation, pending)) = &fence.pending
            && *generation == fence.generation
        {
            return pending.clone();
        }
        let (tx, rx) = oneshot::channel();
        // One fence per accepted-work generation; cancelled/repeated waiters
        // share it instead of accumulating an uncharged control-message queue.
        if self
            .tx
            .send(FundedSubmission {
                submission: Submission::Wait { tx },
                credit: SubmissionCredit::default(),
            })
            .is_err()
            && let Some(limits) = &self.limits
        {
            limits.poison();
        }
        let limits = self.limits.clone();
        let pending = async move {
            if rx.await.is_err()
                && let Some(limits) = limits
            {
                limits.poison();
            }
        }
        .boxed()
        .shared();
        fence.pending = Some((fence.generation, pending.clone()));
        pending
    }
}

#[derive(Debug, Clone)]
pub struct InvalidStats {
    pub block: BlockId,
    pub size: usize,
}

#[derive(Debug, Clone)]
pub struct TombstoneInfo {
    pub tombstone: Tombstone,
    pub stats: Option<InvalidStats>,
}

struct IoTaskCtx<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    handle: Option<GetCleanBlockHandle>,
    waiters: Vec<oneshot::Sender<()>>,
    piece_refs: Vec<PieceRef<K, V, P>>,
    init: Instant,
    io_slice: IoSlice,
    tombstone_infos: Vec<TombstoneInfo>,
    credits: Vec<SubmissionCredit>,
}

struct Runner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    id: usize,

    rx: Option<UnboundedReceiver<FundedSubmission<K, V, P>>>,

    // NOTE: writer is always `Some(..)`.
    buffer: Option<Buffer>,
    ctx: SplitCtx,
    tombstone_infos: Vec<TombstoneInfo>,
    piece_refs: Vec<PieceRef<K, V, P>>,
    waiters: Vec<oneshot::Sender<()>>,
    queue_init: Option<Instant>,

    /// IoBuffer rotates between writer and inflight io task.
    ///
    /// Use this field to avoid allocation.
    rotate_buffer: Option<IoSliceMut>,

    submit_queue_size: Arc<AtomicUsize>,

    current_block_handle: GetCleanBlockHandle,

    block_manager: BlockManager,
    indexer: Indexer,
    tombstone_log: Option<TombstoneLog>,

    compression: Compression,

    spawner: Spawner,

    metrics: Arc<Metrics>,

    io_tasks: VecDeque<BoxFuture<'static, IoTaskCtx<K, V, P>>>,

    max_entry_size: usize,
    stopping: bool,
    credits: Vec<SubmissionCredit>,
    limits: Option<SubmissionLimits>,

    #[cfg(any(test, feature = "test_utils"))]
    flush_switch: Switch,
}

impl<K, V, P> Runner<K, V, P>
where
    K: StorageKey,
    V: StorageValue,
    P: Properties,
{
    fn next_io_task_finish(&mut self) -> impl Future<Output = IoTaskCtx<K, V, P>> + '_ {
        poll_fn(|cx| {
            if let Some(io_task) = self.io_tasks.front_mut() {
                let res = ready!(io_task.poll_unpin(cx));
                drop(self.io_tasks.pop_front().unwrap());
                return Poll::Ready(res);
            }
            Poll::Pending
        })
    }

    pub async fn run(mut self) -> Result<()> {
        let mut rx = self.rx.take().unwrap();

        loop {
            while let Ok(submission) = rx.try_recv() {
                self.recv(submission);
            }

            #[cfg(not(any(test, feature = "test_utils")))]
            let can_flush = true;
            #[cfg(any(test, feature = "test_utils"))]
            let can_flush = !self.flush_switch.is_on();

            let need_flush = !self.buffer.as_ref().unwrap().is_empty()
                || !self.waiters.is_empty()
                || !self.tombstone_infos.is_empty();
            let no_io_task = self.io_tasks.is_empty();

            // A stop is sent only after reclamation and its reinsertions are
            // quiescent. Finish all accepted I/O before releasing the runner.
            if self.stopping && !need_flush && no_io_task {
                break;
            }

            if can_flush && need_flush && no_io_task {
                let (io_buffer, infos) = self.buffer.take().unwrap().finish();

                let efficiency =
                    infos.last().map(|info| info.offset + info.len).unwrap_or_default() as f64 / io_buffer.len() as f64;
                self.metrics.storage_block_engine_buffer_efficiency.record(efficiency);

                let shared_io_slice = io_buffer.into_io_slice();
                let batch = Splitter::split(&mut self.ctx, shared_io_slice, infos);

                let tombstone_infos = std::mem::take(&mut self.tombstone_infos);
                let waiters = std::mem::take(&mut self.waiters);
                let piece_refs = std::mem::take(&mut self.piece_refs);
                let credits = std::mem::take(&mut self.credits);

                let init = self.queue_init.take().unwrap();

                let io_task = self.submit_io_task(batch, piece_refs, tombstone_infos, waiters, init, credits);
                self.io_tasks.push_back(io_task);

                let io_buffer = self.rotate_buffer.take().unwrap();
                let buffer = Buffer::new(io_buffer, self.max_entry_size, self.metrics.clone());
                self.buffer = Some(buffer);
            }

            tokio::select! {
                biased;
                IoTaskCtx { handle, waiters, init, io_slice, tombstone_infos, piece_refs, credits } = self.next_io_task_finish() => {
                    if let Some(handle) = handle {
                        self.current_block_handle = handle;
                    }
                    self.handle_io_complete(piece_refs, waiters, tombstone_infos, init, credits);
                    // `try_into_io_buffer` must return `Some(..)` here.
                    self.rotate_buffer = io_slice.try_into_io_slice_mut();
                }
                Ok(submission) = rx.recv(), if !self.stopping => {
                    self.recv(submission);
                }
                // Graceful shutdown.
                else => break,
            }
        }
        Ok(())
    }

    fn recv(&mut self, funded: FundedSubmission<K, V, P>) {
        let FundedSubmission { submission, credit } = funded;
        tracing::trace!(
            id = self.id,
            ?submission,
            "[block engine flush runner]: recv submission"
        );

        if self.queue_init.is_none() {
            self.queue_init = Some(Instant::now());
        }

        let report = |written: bool| {
            if !written {
                self.metrics.storage_queue_buffer_overflow.increase(1);
            }
        };

        match submission {
            Submission::CacheEntry {
                piece,
                estimated_size,
                sequence,
            } => {
                let enqueued = self.buffer.as_mut().unwrap().push(
                    piece.key(),
                    piece.value(),
                    piece.hash(),
                    self.compression,
                    sequence,
                );
                if enqueued {
                    self.piece_refs.push(piece);
                    self.credits.push(credit);
                }
                report(enqueued);
                self.submit_queue_size.fetch_sub(estimated_size, Ordering::Relaxed);
            }

            Submission::Tombstone { tombstone, stats } => {
                self.tombstone_infos.push(TombstoneInfo { tombstone, stats });
                self.credits.push(credit);
            }
            Submission::Reinsertion { reinsertion } => {
                // Skip reinsertion if the entry is not in the indexer.
                if self.indexer.get(reinsertion.hash).is_some() {
                    let written = self.buffer.as_mut().unwrap().push_slice(
                        &reinsertion.slice[..reinsertion.len],
                        reinsertion.hash,
                        reinsertion.sequence,
                    );
                    report(written);
                    if written {
                        self.credits.push(credit);
                    }
                }
            }
            Submission::Wait { tx } => self.waiters.push(tx),
            Submission::Stop => self.stopping = true,
        }
    }

    fn submit_io_task(
        &self,
        batch: Batch,
        piece_refs: Vec<PieceRef<K, V, P>>,
        tombstone_infos: Vec<TombstoneInfo>,
        waiters: Vec<oneshot::Sender<()>>,
        init: Instant,
        credits: Vec<SubmissionCredit>,
    ) -> BoxFuture<'static, IoTaskCtx<K, V, P>> {
        let id = self.id;
        let limits = self.limits.clone();

        tracing::trace!(
            id,
            ?batch,
            ?tombstone_infos,
            waiters = waiters.len(),
            "[flusher] commit batch"
        );

        let block_handle_iter = if batch.blocks.is_empty() {
            vec![]
        } else {
            std::iter::once(self.current_block_handle.clone())
                .chain((0..batch.blocks.len() - 1).map(|_| self.block_manager.get_clean_block()))
                .collect_vec()
        };

        let bytes = batch.bytes;
        let blocks = batch.blocks.len();
        // Write blocks concurrently.
        let futures = batch
            .blocks
            .into_iter()
            .zip_eq(block_handle_iter)
            .enumerate()
            .map(|(i, (Block { blob_parts }, block_handle))| {
                let indexer = self.indexer.clone();
                let block_manager = self.block_manager.clone();
                let metrics = self.metrics.clone();
                let limits = self.limits.clone();

                async move {
                    // Wait for block is clean.
                    let block = block_handle.clone().await;

                    let tasks = blob_parts.into_iter().map(
                        |BlobPart {
                             blob_block_offset,
                             index,
                             part_blob_offset,
                             data,
                             indices,
                         }| {
                            let offset = blob_block_offset + part_blob_offset;
                            let len = data.len();

                            bits::assert_aligned(PAGE, offset);
                            bits::assert_aligned(PAGE, len);

                            let block = block.clone();
                            let limits = limits.clone();
                            async move {
                                if len > 0 {
                                    tracing::trace!(id, block = block.id(), offset, len, "[flusher]: write blob data");

                                    let (_, res) = block.write(Box::new(data), offset as _).await;
                                    if let Err(e) = res.as_ref() {
                                        if let Some(limits) = &limits {
                                            limits.poison();
                                        }
                                        tracing::error!(
                                            id,
                                            blob_block_offset,
                                            part_blob_offset,
                                            ?indices,
                                            ?res,
                                            ?e,
                                            "[flusher]: flush data error"
                                        );
                                    }
                                    res?;

                                    tracing::trace!(id, offset = blob_block_offset, "[flusher]: write blob index");

                                    let (_, res) = block.write(Box::new(index), blob_block_offset as _).await;
                                    if let Err(e) = res.as_ref() {
                                        if let Some(limits) = &limits {
                                            limits.poison();
                                        }
                                        tracing::error!(
                                            id,
                                            blob_block_offset,
                                            part_blob_offset,
                                            ?indices,
                                            ?res,
                                            ?e,
                                            "[flusher]: flush data error"
                                        );
                                    }
                                    res?;
                                } else {
                                    tracing::trace!(
                                        id,
                                        block = block.id(),
                                        "[flusher]: skip write block, because the window is empty"
                                    );
                                }

                                Ok::<_, Error>((block.id(), blob_block_offset, indices))
                            }
                        },
                    );
                    // Do not return early and release credits while a sibling
                    // detached I/O operation still owns its buffer.
                    let infos = join_all(tasks).await.into_iter().collect::<Result<Vec<_>>>()?;

                    let mut addrs = Vec::with_capacity(infos.iter().map(|(_, _, indices)| indices.len()).sum());
                    for (block, blob_offset, indices) in infos {
                        for index in indices {
                            let addr = HashedEntryAddress {
                                hash: index.hash,
                                address: EntryAddress {
                                    block,
                                    offset: blob_offset as u32 + index.offset,
                                    len: index.len,
                                    sequence: index.sequence,
                                },
                            };
                            tracing::trace!(id, ?addr, "[flusher]: append address");
                            addrs.push(addr);
                        }
                    }

                    let olds = indexer.insert_batch(addrs);
                    metrics.storage_block_engine_indexer_conflict.increase(olds.len() as _);

                    // Window expect window is full, make it evictable.
                    let id = block.id();
                    if i != blocks - 1 {
                        block_manager.on_writing_finish(block);
                    }
                    tracing::trace!(id, "[flusher]: write block finish.");

                    Ok::<_, Error>(block_handle)
                }
            })
            .collect_vec();

        let future = {
            let block_manager = self.block_manager.clone();
            let tombstone_log = self.tombstone_log.clone();
            let tombstone_infos = tombstone_infos.clone();
            let limits = self.limits.clone();
            async move {
                if let Some(log) = tombstone_log {
                    let result = log
                        .append(tombstone_infos.iter().map(|info: &TombstoneInfo| &info.tombstone))
                        .await;
                    if result.is_err()
                        && let Some(limits) = &limits
                    {
                        limits.poison();
                    }
                    result?;
                }
                for TombstoneInfo { tombstone: _, stats } in tombstone_infos {
                    if let Some(stats) = stats {
                        block_manager
                            .block(stats.block)
                            .statistics()
                            .invalid
                            .fetch_add(stats.size, Ordering::Relaxed);
                    }
                }
                Ok::<_, Error>(())
            }
        };

        let handle = self
            .spawner
            .spawn(async move {
                let (writes, log) = join(join_all(futures), future).await;
                let result = writes
                    .into_iter()
                    .collect::<Result<Vec<_>>>()
                    .and_then(|states| log.map(|()| states));
                // Ownership belongs to actual work, not the join waiter: even
                // dropping the runner's join future cannot free live credits.
                (result, credits, piece_refs)
            })
            .map(move |jres| match jres {
                Ok((Ok(mut states), credits, piece_refs)) => IoTaskCtx {
                    handle: states.pop(),
                    piece_refs,
                    waiters,
                    init,
                    io_slice: bytes,
                    tombstone_infos,
                    credits,
                },
                Ok((Err(e), credits, piece_refs)) => {
                    if let Some(limits) = &limits {
                        limits.poison();
                    }
                    tracing::error!(id, ?e, "[block engine flusher]: io task error");
                    IoTaskCtx {
                        handle: None,
                        piece_refs,
                        waiters,
                        init,
                        io_slice: bytes,
                        tombstone_infos,
                        credits,
                    }
                }
                Err(e) => {
                    if let Some(limits) = &limits {
                        limits.poison();
                    }
                    tracing::error!(id, ?e, "[block engine flusher]: join io task error");
                    IoTaskCtx {
                        handle: None,
                        piece_refs: vec![],
                        waiters,
                        init,
                        io_slice: bytes,
                        tombstone_infos,
                        credits: vec![],
                    }
                }
            });

        Box::pin(handle)
    }

    fn handle_io_complete(
        &self,
        piece_refs: Vec<PieceRef<K, V, P>>,
        waiters: Vec<oneshot::Sender<()>>,
        tombstone_infos: Vec<TombstoneInfo>,
        init: Instant,
        credits: Vec<SubmissionCredit>,
    ) {
        drop(piece_refs);

        self.indexer.remove_batch(
            tombstone_infos
                .iter()
                .map(|info| (info.tombstone.hash, info.tombstone.sequence)),
        );

        // Actual I/O and source/keeper ownership have ended before fence observers
        // see completion; cancelling a wait/close never releases these credits.
        drop(credits);

        for waiter in waiters {
            let _ = waiter.send(());
        }

        self.metrics.storage_queue_rotate.increase(1);
        self.metrics
            .storage_queue_rotate_duration
            .record(init.elapsed().as_secs_f64());
    }
}
