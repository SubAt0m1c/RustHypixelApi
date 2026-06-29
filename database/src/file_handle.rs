use std::{fs::{self, File, OpenOptions}, io::{self, ErrorKind, IoSlice}, path::PathBuf, sync::{atomic::{AtomicU64, Ordering}, Arc}};

use bytes::{Buf, BufMut};
use parking_lot::RwLock;

use crate::{error::Error, runtime::SendRuntime, Result};

pub struct FileHandle {
    inner: Arc<Inner>
}

struct Inner {
    // windows may have an issue with deleting a file with an open reference, so we take the file out with a lock.
    // because its a RwLock, concurrent readers (and writers to the file itself) will not lock, only locking on file deletion.
    file: RwLock<Option<File>>, 
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
            Ok::<(File, u64), Error>((file, end))
        }).await??;

        Ok(Self {
            inner: Arc::new(Inner { file: RwLock::new(Some(file)), offset: AtomicU64::new(offset), path })
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
            inner: Arc::new(Inner { file: RwLock::new(Some(file)), offset, path })
        })
    }

    
    pub async fn append_from<RT: SendRuntime, B: Buf + Send + Sync + 'static>(&self, buf: B) -> Result<u64> {
        let inner = self.inner.clone();
        let len = buf.remaining() as u64;
        RT::spawn_blocking(move || {
            let lock = inner.file.read();
            let file = lock.as_ref().ok_or(Error::filenotfound("File removed already!"))?;
            
            let start = inner.offset.fetch_add(len, Ordering::Relaxed);
            // SAFETY: We have ensured enough unique access room with self.reserve. 
            unsafe { Self::write_all_buf_at(&file, start, buf)? }
            Ok::<u64, Error>(start)
        }).await.flatten()
    }
    
    pub async fn read_to<RT: SendRuntime, B: BufMut + Send + Sync + 'static + AsMut<[u8]>>(&self, offset: u64, mut buf: B) -> Result<Option<B>> {
        let inner = self.inner.clone();
        RT::spawn_blocking(move || {
            let lock = inner.file.read();
            let Some(file) = lock.as_ref() else { return Ok(None)};

            Self::read_exact(file, offset, buf.as_mut())?;
            Ok::<Option<B>, Error>(Some(buf))
        }).await.map_err(Into::into).flatten()
    }

    pub fn delete<RT: SendRuntime>(&self) -> impl Future<Output = Result<()>> + use<RT> {
        let inner = self.inner.clone();
        async move {
            drop(inner.file.write().take().ok_or(Error::filenotfound("File removed already!"))?);
            RT::spawn_blocking(move || fs::remove_file(&inner.path).map_err(Error::IoError)).await.flatten()
        }
    }   
    
    /// SAFETY: Caller must ensure the offset does not align with another concurrent write.
    /// 
    /// idrk if this matters tbh. 
    unsafe fn write_all_buf_at<B: Buf>(
        file: &File,
        mut offset: u64,
        mut buf: B,
    ) -> io::Result<()> {
        while buf.has_remaining() {
            let mut iovecs = [IoSlice::new(&[]); 64];
            let n = buf.chunks_vectored(&mut iovecs);
            let written = unsafe { Self::writev_at(file, offset, &iovecs[..n])? };
            offset += written as u64;
            buf.advance(written);
        }
    
        Ok(())
    }

    fn read_exact(file: &File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match read_at(file, offset, buf) {
                Ok(0) => break,
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() { Err(io::Error::new(ErrorKind::UnexpectedEof, "failed to fill whole buffer")) } else { Ok(()) }
    }
    
    /// SAFETY: Caller must ensure the offset does not align with another concurrent write.
    unsafe fn writev_at(file: &File, offset: u64, iovecs: &[IoSlice]) -> io::Result<usize> {
        let mut off = offset;
        let mut total = 0;
    
        for io_slice in iovecs {
            #[cfg(windows)]
            std::os::windows::fs::FileExt::seek_write(file, &mut buf, offset)?;
            #[cfg(unix)]
            let n = std::os::unix::fs::FileExt::write_at(file, io_slice, off)?;
            
            off += n as u64;
            total += n;
    
            if n == 0 {
                break;
            }
        }
    
        Ok(total)
    }
}

fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    #[cfg(unix)]
    let read = std::os::unix::fs::FileExt::read_at(file, buf.as_mut(), offset);
    #[cfg(windows)]
    let read = std::os::windows::fs::FileExt::seek_read(file, buf.as_mut(), position);
    read
}