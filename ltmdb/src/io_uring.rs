use std::{array::from_fn, cell::{Cell, OnceCell}, cmp::{self, min}, ffi::c_void, fs::{self, File}, i32, io::{self, IoSlice}, marker::PhantomData, mem::{MaybeUninit, transmute}, num::NonZero, os::fd::{AsRawFd, RawFd}, pin::Pin, result, sync::{Arc, LazyLock, MutexGuard, OnceLock, atomic::{AtomicI32, AtomicUsize, Ordering}}, task::{Context, Poll}};

use bytes::{Buf, Bytes, BytesMut, buf::Chain};
use futures_util::{stream::Next, task::AtomicWaker};
use ::io_uring::{IoUring, opcode, squeue::Entry, types};
use libc::iovec;
use parking_lot::Mutex;
use polling::{Event, Events, Poller};
use portable_atomic::AtomicU8;
use sharded_slab::{Clear, Pool, Slab};
use crate::io_uring;

static AVAILABLE_PARALLELISM: LazyLock<usize> = LazyLock::new(|| {
    std::thread::available_parallelism().map_or(2, |i| i.get() * 2).next_power_of_two()
});

pub struct SubmissionOp<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize>{
    file: RawFd,
    typ: SubmissionType<MUT, CONST, B, N>,
}

impl<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize> SubmissionOp<MUT, CONST> {
    fn entry(&self, user_data: u64) -> Entry {
        match &self.typ {
            SubmissionType::Read { ref mut buffer } => opcode::Read::new(self.file, buffer.stable_mut_ptr(), buffer.bytes_len() as u32).build().user_data(user_data),
            SubmissionType::Writev { buffer, _type: _ } => opcode::Writev::new(self.file, unsafe { buffer.slices().as_ptr() }, N as u32).build().user_data(user_data),
        }
    }
}

pub enum SubmissionType<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize>{
    Read {
        buffer: MUT,
    },
    Writev {
        buffer: FixedIoVec<B, N>,
        _type: PhantomData<CONST>
    }
}

impl<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize> SubmissionType<MUT, CONST, B, N> {
    fn read(buf: MUT) -> Self {
        Self::Read { buffer: buf }
    }

    fn write(buf: impl IntoFixedIoVec) -> Self {
        Self::Writev { buffer: buf.into_fixed(), _type: PhantomData }
    }

    fn buffer_read(self) -> Option<MUT> {
        match self {
            Self::Read { buffer } => Some(buffer),
            _ => None
        }
    }

    fn buffer_write(self) -> Option<CONST> {
        match self {
            Self::Writev { buffer, _type: _} => Some(CONST::from_fixed(buffer)),
            _ => None
        }
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

    pub fn submit<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize>(&self, file: &File, typ: SubmissionType<MUT, CONST, B, N>) -> Completion {
        let op = SubmissionOp {
            file: file.as_raw_fd(),
            typ
        };
        let shard = self.assign_shard().clone();
        let mut locked = shard.inner.uring.lock();
        let user_data = shard.inner.operations.insert(Operation::new(operation)).expect("todo");
        let entry = shard.inner.operations.get(user_data).expect("what").op().entry(u64::from(user_data));
        unsafe { locked.submission().push(&entry); };

        Completion { shard: shard, key: user_data, _data: PhantomData }
    }
    
    fn assign_shard(&self) -> &Shard {
        let a = &self.shards[fastrand::usize(..*AVAILABLE_PARALLELISM)];
        let b = &self.shards[fastrand::usize(..*AVAILABLE_PARALLELISM)];
        min(a, b)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
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
pub struct ShardedInner {
    uring: Mutex<IoUring>,
    operations: Slab<Operation>,
    contention: AtomicUsize
}

impl PartialEq for ShardedInner {
    fn eq(&self, other: &Self) -> bool {
        self.contention.load(Ordering::Relaxed) == other.contention.load(Ordering::Relaxed)
    }
}

impl Eq for ShardedInner {}

impl PartialOrd for ShardedInner {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ShardedInner {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.contention.load(Ordering::Relaxed).cmp(&other.contention.load(Ordering::Relaxed))
    }
}

pub struct Operation<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize> {
    waker: AtomicWaker,
    result: AtomicI32, // sentinal I32::MAX for unset. Not an os code.
    state: AtomicU8,
    op: SubmissionOp<MUT, CONST, B, N>
}

impl<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize> Operation<MUT, CONST, B, N> {
    fn new(op: SubmissionOp<MUT, CONST, B, N>) -> Self {
        Self {
            waker: AtomicWaker::new(),
            result: AtomicI32::new(i32::MAX),
            state: AtomicU8::new(STATE_UNSET),
            op
        }
    } 

    fn op(&self) -> &SubmissionOp<MUT, CONST> {
        &self.op
    }
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
const STATE_UNSET: u8 = 3;

impl ShardedInner {
    fn new() -> Self {
        ShardedInner {
            uring: Mutex::new(IoUring::new(256).unwrap()),
            operations: Slab::new(),
            contention: AtomicUsize::new(0),
        }
    }

    fn drive_completions(&self) -> Result<bool, io::Error> {
        let mut uring = match self.uring.try_lock() {
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
                    self.operations.remove(key);
                    self.contention.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }

        Ok(processed_any)
    }
}

pub struct Completion<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize> {
    shard: Shard,
    key: usize,
    _data: PhantomData<(MUT, CONST)>
}

impl<MUT: IoBufMut, CONST: IntoFixedIoVec<B, N>, B: IoBuf, const N: usize> Future for Completion<MUT, CONST, B, N> {
    type Output = Result<(SubmissionType<MUT, CONST, B, N>, usize), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let op = self.shard.inner.operations.get(self.key).expect("what.");
        
        op.waker.register(cx.waker());
        if op.state.load(Ordering::Acquire) == STATE_COMPLETED {
            let result = match op.result() {
                Ok(res) => res,
                Err(err) => return Poll::Ready(Err(err))
            };
            drop(op);
            let op: Operation<MUT, CONST, B, N> = self.shard.inner.operations.take(self.key).expect("herm");
            self.contention.fetch_sub(1, Ordering::Relaxed);
            return Poll::Ready(Ok((op.op(), result)))
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

impl IoVec {
    fn libc(&self) -> iovec {
        iovec {
            iov_base: self.base as *mut c_void,
            iov_len: self.len
        }
    }
}

pub struct FixedIoVec<B: IoBuf, const N: usize> {
    buffers: [B; N],
    slices: [iovec; N],
}

impl<B: IoBuf, const N: usize> FixedIoVec<B, N> {
    pub fn new(bufs: [B; N]) -> Self {
        let mut this = Self {
            buffers: bufs,
            slices: from_fn(|_| iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            }),
        };
    
        this.slices = from_fn(|i| iovec {
            iov_base: this.buffers[i].stable_ptr() as *mut c_void,
            iov_len: this.buffers[i].bytes_len(),
        });
    
        this
    }

    /// SAFETY: Caller must ensure FixedIoVec does not move while this reference is alive.
    pub unsafe fn slices(&mut self) -> &[iovec] {
        self.slices = from_fn(|i| iovec {
            iov_base: this.buffers[i].stable_ptr() as *mut c_void,
            iov_len: this.buffers[i].bytes_len(),
        });
        &self.slices
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
    fn bytes_len(&self) -> usize;
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

    fn bytes_len(&self) -> usize {
        self.len()
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
