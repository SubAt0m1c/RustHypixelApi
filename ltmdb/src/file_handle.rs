use std::{fs::{self, File, OpenOptions}, io::{self, ErrorKind, IoSlice}, path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use bytes::{Buf, BytesMut};
use crate::{Result, ResultExt, error::Error, runtime::SendRuntime};

pub(crate) struct FileHandle {
    inner: Arc<Inner>
}

struct Inner {
    file: File,
    offset: AtomicU64,
    path: PathBuf
}

impl FileHandle {
    pub async fn new<RT: SendRuntime>(path: PathBuf) -> Result<Self> {
        let inner_path = path.clone();
        let (file, offset) = RT::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(inner_path)?;
            let end = file.metadata()?.len();
            Ok::<_, Error>((file, end))
        }).await??;

        Ok(Self {
            inner: Arc::new(Inner { file, offset: AtomicU64::new(offset), path })
        })
    }

    /// This function can be used in pathways that are already being run on a sync worker thread.
    pub fn new_sync(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        let offset = AtomicU64::new(file.metadata()?.len());
        
        Ok(Self {
            inner: Arc::new(Inner { file, offset, path })
        })
    }

    // buf + trait spam so chains dont need to be really concrete.
    pub fn append_from<RT: SendRuntime, B: Buf + Send + Sync + 'static>(&self, buf: B) ->  impl Future<Output = Result<u64>> + use<RT, B> {
        let inner = self.inner.clone();
        let len = buf.remaining() as u64;
        RT::spawn_blocking(move || {
            let start = inner.offset.fetch_add(len, Ordering::Relaxed);
            // SAFETY: We have ensured enough unique access room with offset reserving. 
            unsafe { write_all_buf_at(&inner.file, start, buf)? }
            Ok::<_, Error>(start)
        }).flatten()
    }

    pub fn read_to<RT: SendRuntime>(&self, offset: u64, mut buf: BytesMut) -> impl Future<Output = Result<BytesMut>> + use<RT> {
        let inner = self.inner.clone();
        RT::spawn_blocking(move || {
            read_exact(&inner.file, offset, &mut buf)?;
            Ok::<_, Error>(buf)
        }).flatten()
    }

    pub fn delete<RT: SendRuntime>(&self) -> impl Future<Output = Result<()>> + use<RT> {
        let inner = self.inner.clone();
        RT::spawn_blocking(move || fs::remove_file(&inner.path).map_err(Into::into)).flatten()
    }   
}

/// SAFETY: Caller must ensure the offset does not align with another concurrent write.
unsafe fn write_all_buf_at<B: Buf>(
    file: &File,
    mut offset: u64,
    mut buf: B,
) -> io::Result<()> {
    while buf.has_remaining() {
        let mut iovecs = [IoSlice::new(&[]); 64];
        let n = buf.chunks_vectored(&mut iovecs);
        let written = unsafe { writev_at(file, offset, &iovecs[..n])? };
        offset += written as u64;
        buf.advance(written);
    }

    Ok(())
}

fn read_exact(file: &File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match read_at(file, offset, buf) {
            Ok(0) => return Err(io::Error::new(ErrorKind::UnexpectedEof, "failed to fill whole buffer")),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// SAFETY: Caller must ensure the offset does not align with another concurrent write.
unsafe fn writev_at(file: &File, mut offset: u64, iovecs: &[IoSlice]) -> io::Result<usize> {
    let mut total = 0;

    for io_slice in iovecs {
        let mut slice = io_slice.as_ref();

        while !slice.is_empty() {
            #[cfg(windows)]
            let n = std::os::windows::fs::FileExt::seek_write(file, io_slice, offset)?;
            #[cfg(unix)]
            let n = std::os::unix::fs::FileExt::write_at(file, io_slice, offset)?;
            
            offset += n as u64;
            total += n;
            slice = &slice[n..];
        }
    }

    Ok(total)
}

fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    #[cfg(unix)]
    let read = std::os::unix::fs::FileExt::read_at(file, buf, offset);
    #[cfg(windows)]
    let read = std::os::windows::fs::FileExt::seek_read(file, buf, offset);
    read
}