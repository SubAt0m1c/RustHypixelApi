use std::{array::from_fn, cell::{Cell, OnceCell}, cmp::{self, min}, fs, i32, io::{self, IoSlice}, mem::{MaybeUninit, transmute}, num::NonZero, pin::Pin, result, sync::{Arc, LazyLock, MutexGuard, OnceLock, atomic::{AtomicI32, AtomicUsize, Ordering}}, task::{Context, Poll}};

use bytes::{Buf, Bytes, BytesMut, buf::Chain};
use futures_util::{stream::Next, task::AtomicWaker};
use ::io_uring::{IoUring, opcode, squeue::Entry, types};
use parking_lot::Mutex;
use polling::{Event, Events, Poller};
use portable_atomic::AtomicU8;
use sharded_slab::{Clear, Pool, Slab};
use crate::io_uring;

static AVAILABLE_PARALLELISM: LazyLock<usize> = LazyLock::new(|| {
    std::thread::available_parallelism().map_or(2, |i| i.get() * 2).next_power_of_two()
});

pub struct SubmissionOp<MUT: IoBufMut, CONST: IntoFixedIoVec> {
    file: RawFd,
    typ: SubmissionType<MUT, CONST>,
}

impl<MUT: IoBufMut, CONST: IntoFixedIoVec> SubmissionOp<MUT, CONST> {
    fn entry(&self, user_data: u64) -> Entry {
        match &self.typ {
            SubmissionType::Read { ref mut buffer } => opcode::Read::new(self.file, buffer.stable_mut_ptr(), buffer.bytes_len()).build().user_data(user_data),
            SubmissionType::Writev { buffer } => opcode::Writev::new(self.file, buffer.slices.as_ptr(), N as u32).build().user_data(user_data),
        }
    }
}

pub enum SubmissionType<MUT: IoBufMut, CONST: IntoFixedIoVec> {
    Read {
        buffer: MUT,
    },
    Writev {
        buffer: CONST,
    }
}

pub struct ShardedUring {
    shards: Arc<[Shard]>,
}

impl ShardedUring {
    pub fn new() -> Self {
        let shards = (0..*AVAILABLE_PARALLELISM).map(|i| Shard::new()).collect::<Arc<[Shard]>>();
        Self { shards }
    }

    pub fn submit(&self, operation: ) {
        let shard = self.assign_shard();
        let locked = shard.inner.uring.lock();
        let entry = opcode::Writev::new(fd, iovec, len)
        locked.submission().push(entry)
    }
    
    fn assign_shard(&self) -> &Shard {
        let a = self.get_shard(fastrand::usize(..*AVAILABLE_PARALLELISM));
        let b = self.get_shard(fastrand::usize(..*AVAILABLE_PARALLELISM));
        min(a, b)
    }
}

#[derive(PartialEq, Eq, PartialOrd)]
pub struct Shard {
    pub inner: Arc<ShardedInner>,
}

impl Shard {
    fn new() -> Self {
        Self {
            inner: Arc::new(ShardedInner { uring: Mutex::new(IoUring::new(256).unwrap()), operations: Slab::new(), contention: AtomicUsize::new(0) })
        }
    }
}

impl Ord for Shard {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

#[derive(PartialEq, Eq, PartialOrd)]
pub struct ShardedInner {
    uring: Mutex<IoUring>,
    operations: Slab<Operation>,
    contention: AtomicUsize
}

impl Ord for ShardedInner {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.contention.load(Ordering::Relaxed).cmp(&other.contention.load(Ordering::Relaxed))
    }
}

pub struct Operation {
    waker: AtomicWaker,
    result: AtomicI32, // sentinal I32::MAX for unset. shouldnt be an os code i hope.
    state: AtomicU8,
}

pub const UNSET_RESULT: i32 = i32::MAX;

impl Operation {
    fn result(&self) -> Result<usize, io::Error> {
        let res = self.result.load(Ordering::Relaxed);
        if res >= 0 {
            Ok(res as usize)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        }
    }
}

const STATE_PENDING: u8 = 0;
const STATE_COMPLETED: u8 = 1;
const STATE_CANCELLED: u8 = 2;

impl ShardedInner {
    fn new() -> Self {
        ShardedInner {
            uring: Mutex::new(IoUring::new(256).unwrap()),
            operations: Slab::new(),
            contention: AtomicUsize::new(0),
        }
    }

    fn drive_completions(&self) -> Result<bool, io::Error> {
        let uring = match self.uring.try_lock() {
            Some(guard) => guard,
            None => return Ok(false),
        };

        uring.submit()?;

        let mut cq = uring.completion();
        let processed_any = !cq.is_empty();

        while let Some(cqe) = cq.next() {
            let key = cqe.user_data() as usize; 

            if let Some(op) = self.operations.get(key) {
                let _ = op.result.store(cqe.result(), Ordering::Relaxed);
                if op.state.compare_exchange(
                    STATE_PENDING,
                    STATE_COMPLETED,
                    Ordering::Release,
                    Ordering::Relaxed
                ).is_ok() {
                    op.waker.wake();
                } else {
                    drop(op);
                    self.operations.clear(key)
                }
            }

            self.contention.fetch_sub(1, Ordering::Relaxed);
        }

        Ok(processed_any)
    }
}

pub struct Completion {
    shard: Shard,
    key: usize,
}

impl Future for Completion {
    type Output = Result<usize, io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let op = self.shard.inner.operations.get(self.key).expect("what.");
        
        op.waker.register(cx.waker());
        if op.state.load(Ordering::Acquire) == STATE_COMPLETED {
            let result = op.result();
            drop(op);
            self.shard.inner.operations.remove(self.key);
            return Poll::Ready(result)
        }
        Poll::Pending
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        if let Some(op) = self.shard.inner.operations.get(self.key) {
            if op.state.compare_exchange(
                STATE_PENDING,
                STATE_CANCELLED,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_err() {
                drop(op);
                self.shard.operations.clear(self.key);
            }
        }
    }
}

fn drive_reactor_thread(shards: Arc<[Shard]>) {
    let poller = Poller::new().unwrap();
    for (i, shard) in shards.iter().enumerate() {
        let fd = shard.inner.uring.lock().unwrap().as_raw_fd();
        unsafe { poller.add(fd, Event::readable(i))?; }
    }

    let mut events = Events::new();
    loop {
        events.clear();
        poller.wait(&mut events, None)?;
        for event in events.iter() {
            let shard: &Shard = &shards[event.key];
            let _ = shard.inner.drive_completions();

            let fd = shard.inner.uring.lock().unwrap().as_raw_fd();
            unsafe { poller.modify(fd, Event::readable(event.key))?; }
        }
    }
}

#[repr(C)]
pub struct IoVec { pub base: *mut u8, pub len: usize } // compatible with libc::iovec probably

pub struct FixedIoVec<B: IoBuf, const N: usize> {
    buffers: [B; N],
    slices: [IoVec; N],
}

impl<B: IoBuf, const N: usize> FixedIoVec<B, N> {
    pub fn new(bufs: [B; N]) -> Self {
        let mut this = Self {
            buffers: bufs,
            slices: from_fn(|_| IoVec {
                base: std::ptr::null_mut(),
                len: 0,
            }),
        };
    
        this.slices = from_fn(|i| IoVec {
            base: this.buffers[i].stable_ptr() as *mut u8,
            len: this.buffers[i].bytes_len(),
        });
    
        this
    }
}

pub trait IntoFixedIoVec<B: IoBuf, const N: usize> {
    fn into_fixed(self) -> FixedIoVec<B, N>;
    fn from_fixed(fixed: FixedIoVec<B, N>) -> Self;
}

impl<B: IoBuf, const N: usize> IntoFixedIoVec<B, N> for [B; N] {
    fn into_fixed(self) -> FixedIoVec<B, N> {
        FixedIoVec::new(self)
    }

    fn from_fixed(fixed: FixedIoVec<B, N>) -> Self {
        fixed.buffers
    }
}

/// SAFETY: The caller must ensure that the buffer does not move or be dropped before the operation completes.
pub unsafe trait IoBuf: Unpin + Send + 'static {
    fn stable_ptr(&self) -> *const u8;
    fn bytes_len(&self) -> u32;
}

/// SAFETY: The caller must ensure that the buffer does not move or be dropped before the operation completes.
pub unsafe trait IoBufMut: IoBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8;
    unsafe fn set_init(&mut self, len: usize);
}

unsafe impl IoBuf for Bytes {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_len(&self) -> u32 {
        self.len() as u32
    }
}

unsafe impl IoBufMut for BytesMut {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, len: usize) {
        unsafe { self.set_len(len); }
    }
}
