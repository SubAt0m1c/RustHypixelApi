use std::{fs::{self, File, OpenOptions}, io::{self, ErrorKind, IoSlice, Write}, path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}};

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

    pub fn append_from<RT: SendRuntime, B: Buf + Send + Sync + 'static>(&self, buf: B) ->  impl Future<Output = Result<u64>> + use<RT, B> {
        let inner = self.inner.clone();
        let len = buf.remaining() as u64;
        RT::spawn_blocking(move || {
            let start = inner.offset.fetch_add(len, Ordering::Relaxed); // reserve unique space to prevent concurrent writes to the same location.
            write_all_buf_at(&inner.file, start, buf)?;
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

/// Writes an entire buffer to a file at a specific offset.
/// 
/// The caller should ensure it has unique access to the offset and length it will write to. 
/// If this is not enforced, the os will handle the concurrent writes, which may lead to data 
/// loss.
fn write_all_buf_at<B: Buf>(
    file: &File,
    mut offset: u64,
    mut buf: B,
) -> io::Result<()> {
    while buf.has_remaining() {
        let mut iovecs = [IoSlice::new(&[]); 64];
        let n = buf.chunks_vectored(&mut iovecs);
        let written = writev_at(file, offset, &iovecs[..n])?;
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

/// Attempts to write to a file from a specific offset. Returns the number of bytes written.
/// 
/// The caller should ensure it has unique access to the offset and length it will write to. 
/// If this is not enforced, the os will handle the concurrent writes, which may lead to data 
/// loss.
fn writev_at<B: Buf>(file: &File, offset: u64, buffer: &mut B) -> io::Result<usize> {
    let written: usize;

    #[cfg(unix)]
    {
        use nix::sys::uio::pwritev;
        
        let mut iovecs = [IoSlice::new(&[]); 64];
        let n = buffer.chunks_vectored(&mut iovecs);

        written = pwritev(file, &iovecs[..n], offset as i64)?;
        buffer.advance(written);
    }

    #[cfg(windows)]
    {
        for io_slice in iovecs {
            let mut slice = io_slice.as_ref();

            while !slice.is_empty() {
                let n = std::os::windows::fs::FileExt::seek_write(file, io_slice, offset)?;
                
                offset += n as u64;
                total += n;
                slice = &slice[n..];
            }
        }
    }

    Ok(written)
}

fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    #[cfg(unix)]
    let read = std::os::unix::fs::FileExt::read_at(file, buf, offset);
    #[cfg(windows)]
    let read = std::os::windows::fs::FileExt::seek_read(file, buf, offset);
    read
}