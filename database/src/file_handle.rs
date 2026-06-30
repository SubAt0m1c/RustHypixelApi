use std::{fs::{self, File, OpenOptions}, io::{self, ErrorKind, IoSlice, Write}, path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;

use crate::{error::Error, runtime::SendRuntime, Result};

pub(crate) struct FileHandle {
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
            Ok::<_, Error>((file, end))
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

    // buf + trait spam so chains dont need to be really concrete.
    pub async fn append_from<RT: SendRuntime, B: Buf + Send + Sync + 'static>(&self, buf: B) -> Result<u64> {
        let inner = self.inner.clone();
        let len = buf.remaining() as u64;
        RT::spawn_blocking(move || {
            let lock = inner.file.read();
            let file = lock.as_ref().ok_or(Error::filenotfound("File removed already!"))?;
            
            let start = inner.offset.fetch_add(len, Ordering::Relaxed);
            // SAFETY: We have ensured enough unique access room with offset reserving. 
            unsafe { Self::write_all_buf_at(&file, start, buf)? }
            Ok::<_, Error>(start)
        }).await.flatten()
    }

    pub async fn read_to<RT: SendRuntime>(&self, offset: u64, mut buf: BytesMut) -> Result<Option<BytesMut>> {
        let inner = self.inner.clone();
        RT::spawn_blocking(move || {
            let lock = inner.file.read();
            let Some(file) = lock.as_ref() else { return Ok(None)};

            Self::read_exact(file, offset, &mut buf)?;
            Ok::<_, Error>(Some(buf))
        }).await.flatten()
    }

    pub fn delete<RT: SendRuntime>(&self) -> impl Future<Output = Result<()>> + use<RT> {
        let inner = self.inner.clone();
        async move {
            drop(inner.file.write().take().ok_or(Error::filenotfound("File removed already!"))?);
            RT::spawn_blocking(move || fs::remove_file(&inner.path).map_err(Error::IoError)).await.flatten()
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
            let written = unsafe { Self::writev_at(file, offset, &iovecs[..n])? };
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
                let n = std::os::unix::fs::FileExt::write_at(file, io_slice, off)?;
                
                offset += n as u64;
                total += n;
                slice = &slice[n..];
            }
        }
    
        Ok(total)
    }
}

fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    #[cfg(unix)]
    let read = std::os::unix::fs::FileExt::read_at(file, buf, offset);
    #[cfg(windows)]
    let read = std::os::windows::fs::FileExt::seek_read(file, buf, offset);
    read
}