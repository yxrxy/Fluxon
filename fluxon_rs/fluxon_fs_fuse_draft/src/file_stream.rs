use std::cmp::min;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const REMOTE_CHUNK_BYTES: usize = fluxon_fs_core::s3_gateway::FS_S3_OBJECT_PIECE_BYTES;

#[cfg(feature = "fsagent_backend")]
use fluxon_fs::agent::{FluxonFsFileSessionHandle, FluxonFsFileSessionOpenOptions};

use crate::backend::{FuseBackendError, FuseBackendStat, FuseExportBackend};
use crate::error::FuseAdapterError;
use crate::open_action::{OpenAction, OpenFlagsView, classify_open_action};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseStreamStat {
    pub exists: bool,
    pub is_file: bool,
    pub is_dir: bool,
    pub size: u64,
    pub ctime_ns: i64,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u64,
    pub ino: u64,
    pub rdev: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseFileStatus {
    file_length: u64,
}

impl FuseFileStatus {
    pub fn new(file_length: u64) -> Self {
        Self { file_length }
    }

    pub fn file_length(&self) -> u64 {
        self.file_length
    }

    pub fn set_file_length(&mut self, file_length: u64) {
        self.file_length = file_length;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseCreateFileStatus {
    file_status: FuseFileStatus,
    mode: u32,
}

impl FuseCreateFileStatus {
    pub fn new(file_length: u64, mode: u32) -> Self {
        Self {
            file_status: FuseFileStatus::new(file_length),
            mode,
        }
    }

    pub fn file_length(&self) -> u64 {
        self.file_status.file_length()
    }

    pub fn set_file_length(&mut self, file_length: u64) {
        self.file_status.set_file_length(file_length);
    }

    pub fn mode(&self) -> u32 {
        self.mode
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseDetachedFileState {
    pub size: u64,
    pub ctime_ns: i64,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u64,
    pub ino: u64,
    pub rdev: u32,
}

pub trait FuseFileStream: Send {
    fn read(&mut self, size: u32, offset: i64) -> Result<Vec<u8>, FuseAdapterError>;
    fn write(&mut self, data: &[u8], offset: i64) -> Result<usize, FuseAdapterError>;
    fn flush(&mut self) -> Result<(), FuseAdapterError>;
    fn truncate(&mut self, size: u64) -> Result<(), FuseAdapterError>;
    fn fallocate(&mut self, mode: i32, offset: u64, length: u64) -> Result<(), FuseAdapterError>;
    fn close(&mut self) -> Result<(), FuseAdapterError>;
    fn stat(&self) -> FuseStreamStat;
    fn snapshot_detached(&mut self) -> Result<(), FuseAdapterError> {
        Ok(())
    }
    fn detached_state(&self) -> Option<FuseDetachedFileState> {
        None
    }
    fn is_writable(&self) -> bool;
    fn is_dirty(&self) -> bool;
}

enum FuseStreamFactoryInner {
    Backend(Arc<dyn FuseExportBackend>),
    #[cfg(feature = "fsagent_backend")]
    FsAgent(Arc<fluxon_fs::agent::FluxonFsAgent>),
}

pub struct FuseFileStreamFactory {
    inner: FuseStreamFactoryInner,
}

impl FuseFileStreamFactory {
    pub fn new(backend: Arc<dyn FuseExportBackend>) -> Self {
        Self {
            inner: FuseStreamFactoryInner::Backend(backend),
        }
    }

    #[cfg(feature = "fsagent_backend")]
    pub fn new_fsagent(agent: Arc<fluxon_fs::agent::FluxonFsAgent>) -> Self {
        Self {
            inner: FuseStreamFactoryInner::FsAgent(agent),
        }
    }

    pub fn create(
        &self,
        relpath: String,
        file_abs: String,
        flags: i32,
        mode: Option<u32>,
        committed: FuseBackendStat,
        create_owner: Option<(u32, u32)>,
    ) -> Result<Box<dyn FuseFileStream>, FuseAdapterError> {
        #[cfg(not(feature = "fsagent_backend"))]
        let _ = (&file_abs, &create_owner);
        let open_action = classify_open_action(flags).ok_or_else(|| FuseAdapterError::InvalidArgument {
            detail: format!("invalid open access mode flags=0x{:x}", flags),
        })?;
        match &self.inner {
            FuseStreamFactoryInner::Backend(backend) => match open_action {
                OpenAction::ReadOnly => Ok(Box::new(FuseFileInStream::new(
                    backend.clone(),
                    relpath,
                    committed,
                ))),
                OpenAction::WriteOnly => Ok(Box::new(FuseFileOutStream::new(
                    backend.clone(),
                    relpath,
                    committed,
                    OpenFlagsView::new(flags),
                    mode,
                    create_owner,
                )?)),
                OpenAction::ReadWrite => Ok(Box::new(FuseFileInOrOutStream::new(
                    backend.clone(),
                    relpath,
                    committed,
                    OpenFlagsView::new(flags),
                    mode,
                    create_owner,
                )?)),
            },
            #[cfg(feature = "fsagent_backend")]
            FuseStreamFactoryInner::FsAgent(agent) => Ok(Box::new(FuseFsAgentSessionStream::new(
                agent.clone(),
                file_abs,
                flags,
                mode,
                create_owner,
            )?)),
        }
    }
}

#[cfg(feature = "fsagent_backend")]
struct FuseFsAgentSessionStream {
    session: FluxonFsFileSessionHandle,
    open_action: OpenAction,
    append_mode: bool,
    dirty: bool,
    cached_stat: FuseStreamStat,
}

#[cfg(feature = "fsagent_backend")]
impl FuseFsAgentSessionStream {
    fn new(
        agent: Arc<fluxon_fs::agent::FluxonFsAgent>,
        file_abs: String,
        flags: i32,
        mode: Option<u32>,
        create_owner: Option<(u32, u32)>,
    ) -> Result<Self, FuseAdapterError> {
        let open_action = classify_open_action(flags).ok_or_else(|| FuseAdapterError::InvalidArgument {
            detail: format!("invalid open access mode flags=0x{:x}", flags),
        })?;
        let open_flags = OpenFlagsView::new(flags);
        let options = FluxonFsFileSessionOpenOptions {
            readable: open_action.is_readable(),
            writable: open_action.is_writable(),
            create: open_flags.contains_create(),
            create_new: open_flags.contains_create() && open_flags.contains_exclusive(),
            truncate: open_flags.contains_truncate(),
            append: open_flags.contains_append(),
            create_mode: mode.map(i64::from),
            create_uid: create_owner.map(|value| i64::from(value.0)),
            create_gid: create_owner.map(|value| i64::from(value.1)),
        };
        let session = agent
            .open_file_session_with_options(file_abs.as_str(), options)
            .map_err(fsagent_err_to_adapter)?;
        let cached_stat = session_remote_stat_to_stream(session.stat().map_err(fsagent_err_to_adapter)?);
        Ok(Self {
            session,
            open_action,
            append_mode: open_flags.contains_append(),
            dirty: false,
            cached_stat,
        })
    }

    fn refresh_cached_stat(&mut self) -> Result<(), FuseAdapterError> {
        self.cached_stat =
            session_remote_stat_to_stream(self.session.stat().map_err(fsagent_err_to_adapter)?);
        Ok(())
    }
}

#[cfg(feature = "fsagent_backend")]
impl FuseFileStream for FuseFsAgentSessionStream {
    fn read(&mut self, size: u32, offset: i64) -> Result<Vec<u8>, FuseAdapterError> {
        if !self.open_action.is_readable() {
            return Err(FuseAdapterError::BadFileDescriptor { fh: 0 });
        }
        self.session
            .read(offset, i64::from(size))
            .map_err(fsagent_err_to_adapter)
    }

    fn write(&mut self, data: &[u8], offset: i64) -> Result<usize, FuseAdapterError> {
        if !self.open_action.is_writable() {
            return Err(FuseAdapterError::BadFileDescriptor { fh: 0 });
        }
        let write_offset = if self.append_mode {
            let stat = self.session.stat().map_err(fsagent_err_to_adapter)?;
            stat.size
        } else {
            offset
        };
        self.session
            .write(write_offset, data)
            .map_err(fsagent_err_to_adapter)?;
        self.dirty = true;
        self.refresh_cached_stat()?;
        Ok(data.len())
    }

    fn flush(&mut self) -> Result<(), FuseAdapterError> {
        self.session.flush().map_err(fsagent_err_to_adapter)?;
        self.dirty = false;
        self.refresh_cached_stat()
    }

    fn truncate(&mut self, size: u64) -> Result<(), FuseAdapterError> {
        if !self.open_action.is_writable() {
            return Err(FuseAdapterError::BadFileDescriptor { fh: 0 });
        }
        self.session
            .truncate(i64::try_from(size).map_err(|_| FuseAdapterError::InvalidArgument {
                detail: format!("truncate size out of range: {}", size),
            })?)
            .map_err(fsagent_err_to_adapter)?;
        self.dirty = true;
        self.refresh_cached_stat()
    }

    fn fallocate(&mut self, mode: i32, offset: u64, length: u64) -> Result<(), FuseAdapterError> {
        if !self.open_action.is_writable() {
            return Err(FuseAdapterError::BadFileDescriptor { fh: 0 });
        }
        self.session
            .fallocate(
                i64::from(mode),
                i64::try_from(offset).map_err(|_| FuseAdapterError::InvalidArgument {
                    detail: format!("fallocate offset out of range: {}", offset),
                })?,
                i64::try_from(length).map_err(|_| FuseAdapterError::InvalidArgument {
                    detail: format!("fallocate length out of range: {}", length),
                })?,
            )
            .map_err(fsagent_err_to_adapter)?;
        self.dirty = false;
        self.refresh_cached_stat()
    }

    fn close(&mut self) -> Result<(), FuseAdapterError> {
        if self.dirty {
            self.flush()?;
        }
        self.session.close().map_err(fsagent_err_to_adapter)?;
        self.dirty = false;
        Ok(())
    }

    fn stat(&self) -> FuseStreamStat {
        self.cached_stat.clone()
    }

    fn is_writable(&self) -> bool {
        self.open_action.is_writable()
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(feature = "fsagent_backend")]
fn session_remote_stat_to_stream(stat: fluxon_fs::agent::RemoteStat) -> FuseStreamStat {
    FuseStreamStat {
        exists: stat.exists,
        is_file: stat.is_file,
        is_dir: stat.is_dir,
        size: stat.size.max(0) as u64,
        ctime_ns: stat.ctime_ns,
        atime_ns: stat.atime_ns,
        mtime_ns: stat.mtime_ns,
        mode: stat.mode.max(0) as u32,
        uid: stat.uid.max(0) as u32,
        gid: stat.gid.max(0) as u32,
        nlink: stat.nlink.max(0) as u64,
        ino: stat.ino.max(0) as u64,
        rdev: stat.rdev.max(0) as u32,
    }
}

#[derive(Debug, Clone)]
enum DirtyOp {
    Truncate { size: u64 },
    Write { offset: u64, data: Vec<u8> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteOpenState {
    Active,
    RequireTruncateZero,
}

struct BufferedWriteStreamState {
    backend: Arc<dyn FuseExportBackend>,
    relpath: String,
    committed: FuseBackendStat,
    create_status: FuseCreateFileStatus,
    create_owner: Option<(u32, u32)>,
    dirty_ops: Vec<DirtyOp>,
    projected_size: u64,
    write_frontier: u64,
    append_mode: bool,
    write_open_state: WriteOpenState,
    allow_sparse_writes: bool,
    projected_uid: u32,
    projected_gid: u32,
    projected_ctime_ns: i64,
    projected_nlink: u64,
    detached_bytes: Option<Vec<u8>>,
}

impl BufferedWriteStreamState {
    fn new(
        backend: Arc<dyn FuseExportBackend>,
        relpath: String,
        committed: FuseBackendStat,
        open_flags: OpenFlagsView,
        create_mode: Option<u32>,
        create_owner: Option<(u32, u32)>,
    ) -> Result<Self, FuseAdapterError> {
        let mode = resolve_create_mode(relpath.as_str(), &committed, open_flags, create_mode)?;
        let init_truncate_zero =
            open_flags.contains_truncate() || (!committed.exists && open_flags.contains_create());
        let mut dirty_ops = Vec::new();
        let projected_size = if init_truncate_zero {
            dirty_ops.push(DirtyOp::Truncate { size: 0 });
            0
        } else {
            committed.size
        };
        let write_open_state = if committed.exists && committed.size > 0 && !init_truncate_zero {
            WriteOpenState::RequireTruncateZero
        } else {
            WriteOpenState::Active
        };
        let projected_uid = if committed.exists {
            committed.uid
        } else if let Some((uid, _gid)) = create_owner {
            uid
        } else {
            current_euid()
        };
        let projected_gid = if committed.exists {
            committed.gid
        } else if let Some((_uid, gid)) = create_owner {
            gid
        } else {
            current_egid()
        };
        let projected_ctime_ns = if committed.exists {
            committed.ctime_ns
        } else {
            current_time_ns()
        };
        let projected_nlink = if committed.exists {
            committed.nlink
        } else {
            1
        };
        Ok(Self {
            backend,
            relpath,
            committed,
            create_status: FuseCreateFileStatus::new(projected_size, mode),
            create_owner,
            dirty_ops,
            projected_size,
            write_frontier: 0,
            append_mode: open_flags.contains_append(),
            write_open_state,
            allow_sparse_writes: false,
            projected_uid,
            projected_gid,
            projected_ctime_ns,
            projected_nlink,
            detached_bytes: None,
        })
    }

    fn write(&mut self, data: &[u8], offset: i64) -> Result<usize, FuseAdapterError> {
        if offset < 0 {
            return Err(FuseAdapterError::InvalidArgument {
                detail: format!("offset must be >= 0 (got {})", offset),
            });
        }
        if data.is_empty() {
            return Ok(0);
        }
        self.ensure_write_active()?;
        let logical_offset = if self.append_mode {
            self.write_frontier
        } else {
            offset as u64
        };
        let bytes_written = self.write_frontier;
        let write_size = u64::try_from(data.len()).unwrap();
        if !self.allow_sparse_writes
            && logical_offset < bytes_written
            && logical_offset + write_size > bytes_written
        {
            return Err(FuseAdapterError::Os {
                errno: libc::EOPNOTSUPP,
                path: self.relpath.clone(),
                detail: format!(
                    "only sequential write is supported for {}: offset={} size={} bytes_written={}",
                    self.relpath,
                    logical_offset,
                    data.len(),
                    bytes_written
                ),
            });
        }
        if logical_offset > bytes_written {
            self.allow_sparse_writes = true;
        }
        if logical_offset + write_size <= bytes_written {
            return Ok(data.len());
        }
        let chunk_bytes = REMOTE_CHUNK_BYTES as usize;
        let mut cursor = 0usize;
        while cursor < data.len() {
            let end = min(data.len(), cursor + chunk_bytes);
            let part = data[cursor..end].to_vec();
            self.dirty_ops.push(DirtyOp::Write {
                offset: logical_offset + cursor as u64,
                data: part,
            });
            cursor = end;
        }
        self.write_frontier = logical_offset + write_size;
        self.projected_size = self.projected_size.max(self.write_frontier);
        self.create_status
            .set_file_length(self.create_status.file_length().max(self.write_frontier));
        Ok(data.len())
    }

    fn truncate(&mut self, size: u64) -> Result<(), FuseAdapterError> {
        let current_size = self.create_status.file_length();
        if size == current_size {
            return Ok(());
        }
        if size == 0 {
            self.write_open_state = WriteOpenState::Active;
            self.dirty_ops.clear();
            self.dirty_ops.push(DirtyOp::Truncate { size: 0 });
            self.projected_size = 0;
            self.create_status.set_file_length(0);
            self.write_frontier = 0;
            self.allow_sparse_writes = false;
            return Ok(());
        }
        if self.write_open_state == WriteOpenState::RequireTruncateZero {
            self.write_open_state = WriteOpenState::Active;
        }
        self.dirty_ops.push(DirtyOp::Truncate { size });
        self.projected_size = size;
        self.create_status.set_file_length(size);
        self.write_frontier = self.write_frontier.min(size);
        if size < self.write_frontier {
            self.allow_sparse_writes = false;
        }
        Ok(())
    }

    fn fallocate(&mut self, mode: i32, offset: u64, length: u64) -> Result<(), FuseAdapterError> {
        if self.write_open_state == WriteOpenState::RequireTruncateZero {
            self.write_open_state = WriteOpenState::Active;
        }
        self.flush()?;
        self.backend
            .fallocate(self.relpath.as_str(), mode, offset, length)
            .map_err(FuseBackendError::into_adapter)?;
        let committed = self
            .backend
            .stat(self.relpath.as_str())
            .map_err(FuseBackendError::into_adapter)?;
        self.projected_size = committed.size;
        self.write_frontier = committed.size;
        self.committed = committed;
        self.create_status.set_file_length(self.projected_size);
        self.allow_sparse_writes = false;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), FuseAdapterError> {
        if self.dirty_ops.is_empty() {
            return Ok(());
        }
        for op in self.dirty_ops.iter().cloned() {
            match op {
                DirtyOp::Truncate { size } => {
                    self.backend
                        .truncate(self.relpath.as_str(), size)
                        .map_err(FuseBackendError::into_adapter)?;
                }
                DirtyOp::Write { offset, data } => {
                    self.backend
                        .write_chunk(self.relpath.as_str(), offset, data)
                        .map_err(FuseBackendError::into_adapter)?;
                }
            }
        }
        if !self.committed.exists {
            if let Some((uid, gid)) = self.create_owner {
                self.backend
                    .chown(self.relpath.as_str(), uid, gid)
                    .map_err(FuseBackendError::into_adapter)?;
            }
            self.backend
                .chmod(self.relpath.as_str(), self.create_status.mode())
                .map_err(FuseBackendError::into_adapter)?;
        }
        let committed = self
            .backend
            .stat(self.relpath.as_str())
            .map_err(FuseBackendError::into_adapter)?;
        self.projected_size = committed.size;
        self.committed = committed;
        self.create_status.set_file_length(self.projected_size);
        self.allow_sparse_writes = false;
        self.dirty_ops.clear();
        Ok(())
    }

    fn stat(&self) -> FuseStreamStat {
        let projected_time_ns = if self.committed.exists {
            self.committed.mtime_ns
        } else {
            self.projected_ctime_ns
        };
        FuseStreamStat {
            exists: true,
            is_file: true,
            is_dir: false,
            size: self.create_status.file_length(),
            ctime_ns: if self.committed.exists {
                self.committed.ctime_ns
            } else {
                self.projected_ctime_ns
            },
            atime_ns: if self.committed.exists {
                self.committed.atime_ns
            } else {
                projected_time_ns
            },
            mtime_ns: projected_time_ns,
            mode: self.create_status.mode(),
            uid: self.projected_uid,
            gid: self.projected_gid,
            nlink: self.projected_nlink,
            ino: if self.committed.exists { self.committed.ino } else { 0 },
            rdev: if self.committed.exists { self.committed.rdev } else { 0 },
        }
    }

    fn is_dirty(&self) -> bool {
        !self.dirty_ops.is_empty()
    }

    fn read(&mut self, size: u32, offset: i64) -> Result<Vec<u8>, FuseAdapterError> {
        if offset < 0 {
            return Err(FuseAdapterError::InvalidArgument {
                detail: format!("offset must be >= 0 (got {})", offset),
            });
        }
        if let Some(bytes) = self.detached_bytes.as_ref() {
            let start = usize::try_from(offset as u64).unwrap_or(usize::MAX);
            if start >= bytes.len() {
                return Ok(Vec::new());
            }
            let end = min(bytes.len(), start.saturating_add(size as usize));
            return Ok(bytes[start..end].to_vec());
        }
        self.flush()?;
        if !self.committed.exists {
            return Ok(Vec::new());
        }
        read_committed_window(
            self.backend.as_ref(),
            self.relpath.as_str(),
            offset as u64,
            size as usize,
            self.create_status.file_length(),
            self.committed.mtime_ns,
        )
    }

    fn detached_state(&self) -> Option<FuseDetachedFileState> {
        Some(FuseDetachedFileState {
            size: self.create_status.file_length(),
            ctime_ns: if self.committed.exists {
                self.committed.ctime_ns
            } else {
                self.projected_ctime_ns
            },
            atime_ns: if self.committed.exists {
                self.committed.atime_ns
            } else {
                self.projected_ctime_ns
            },
            mtime_ns: if self.committed.exists {
                self.committed.mtime_ns
            } else {
                self.projected_ctime_ns
            },
            mode: self.create_status.mode(),
            uid: self.projected_uid,
            gid: self.projected_gid,
            nlink: self.projected_nlink.saturating_sub(1),
            ino: if self.committed.exists { self.committed.ino } else { 0 },
            rdev: if self.committed.exists { self.committed.rdev } else { 0 },
        })
    }

    fn snapshot_detached(&mut self) -> Result<(), FuseAdapterError> {
        if self.detached_bytes.is_some() {
            return Ok(());
        }
        self.flush()?;
        let bytes = read_committed_window(
            self.backend.as_ref(),
            self.relpath.as_str(),
            0,
            usize::try_from(self.create_status.file_length()).unwrap_or(usize::MAX),
            self.create_status.file_length(),
            self.committed.mtime_ns,
        )?;
        self.detached_bytes = Some(bytes);
        Ok(())
    }

    fn ensure_write_active(&self) -> Result<(), FuseAdapterError> {
        if self.write_open_state == WriteOpenState::Active {
            return Ok(());
        }
        Err(FuseAdapterError::AlreadyExists {
            path: self.relpath.clone(),
        })
    }
}

pub struct FuseFileInStream {
    backend: Arc<dyn FuseExportBackend>,
    relpath: String,
    committed: FuseBackendStat,
    file_status: FuseFileStatus,
    detached_bytes: Option<Vec<u8>>,
}

impl FuseFileInStream {
    pub fn new(
        backend: Arc<dyn FuseExportBackend>,
        relpath: String,
        committed: FuseBackendStat,
    ) -> Self {
        Self {
            backend,
            relpath,
            file_status: FuseFileStatus::new(committed.size),
            committed,
            detached_bytes: None,
        }
    }

    pub fn snapshot_detached_bytes(&mut self) -> Result<(), FuseAdapterError> {
        if self.detached_bytes.is_some() {
            return Ok(());
        }
        let bytes = read_committed_window(
            self.backend.as_ref(),
            self.relpath.as_str(),
            0,
            usize::try_from(self.file_status.file_length()).unwrap_or(usize::MAX),
            self.file_status.file_length(),
            self.committed.mtime_ns,
        )?;
        self.detached_bytes = Some(bytes);
        Ok(())
    }
}

impl FuseFileStream for FuseFileInStream {
    fn read(&mut self, size: u32, offset: i64) -> Result<Vec<u8>, FuseAdapterError> {
        if offset < 0 {
            return Err(FuseAdapterError::InvalidArgument {
                detail: format!("offset must be >= 0 (got {})", offset),
            });
        }
        if let Some(bytes) = self.detached_bytes.as_ref() {
            let start = usize::try_from(offset as u64).unwrap_or(usize::MAX);
            if start >= bytes.len() {
                return Ok(Vec::new());
            }
            let end = min(bytes.len(), start.saturating_add(size as usize));
            return Ok(bytes[start..end].to_vec());
        }
        if !self.committed.exists {
            return Ok(Vec::new());
        }
        read_committed_window(
            self.backend.as_ref(),
            self.relpath.as_str(),
            offset as u64,
            size as usize,
            self.file_status.file_length(),
            self.committed.mtime_ns,
        )
    }

    fn write(&mut self, _data: &[u8], _offset: i64) -> Result<usize, FuseAdapterError> {
        Err(FuseAdapterError::BadFileDescriptor { fh: 0 })
    }

    fn flush(&mut self) -> Result<(), FuseAdapterError> {
        Ok(())
    }

    fn truncate(&mut self, _size: u64) -> Result<(), FuseAdapterError> {
        Err(FuseAdapterError::BadFileDescriptor { fh: 0 })
    }

    fn fallocate(
        &mut self,
        _mode: i32,
        _offset: u64,
        _length: u64,
    ) -> Result<(), FuseAdapterError> {
        Err(FuseAdapterError::BadFileDescriptor { fh: 0 })
    }

    fn close(&mut self) -> Result<(), FuseAdapterError> {
        Ok(())
    }

    fn stat(&self) -> FuseStreamStat {
        FuseStreamStat {
            exists: self.committed.exists,
            is_file: self.committed.is_file,
            is_dir: self.committed.is_dir,
            size: self.file_status.file_length(),
            ctime_ns: self.committed.ctime_ns,
            atime_ns: self.committed.atime_ns,
            mtime_ns: self.committed.mtime_ns,
            mode: self.committed.mode,
            uid: self.committed.uid,
            gid: self.committed.gid,
            nlink: self.committed.nlink,
            ino: self.committed.ino,
            rdev: self.committed.rdev,
        }
    }

    fn is_writable(&self) -> bool {
        false
    }

    fn is_dirty(&self) -> bool {
        false
    }

    fn detached_state(&self) -> Option<FuseDetachedFileState> {
        Some(FuseDetachedFileState {
            size: self.file_status.file_length(),
            ctime_ns: self.committed.ctime_ns,
            atime_ns: self.committed.atime_ns,
            mtime_ns: self.committed.mtime_ns,
            mode: self.committed.mode,
            uid: self.committed.uid,
            gid: self.committed.gid,
            nlink: self.committed.nlink.saturating_sub(1),
            ino: self.committed.ino,
            rdev: self.committed.rdev,
        })
    }

    fn snapshot_detached(&mut self) -> Result<(), FuseAdapterError> {
        self.snapshot_detached_bytes()
    }
}

pub struct FuseFileOutStream {
    inner: BufferedWriteStreamState,
}

impl FuseFileOutStream {
    pub fn new(
        backend: Arc<dyn FuseExportBackend>,
        relpath: String,
        committed: FuseBackendStat,
        open_flags: OpenFlagsView,
        create_mode: Option<u32>,
        create_owner: Option<(u32, u32)>,
    ) -> Result<Self, FuseAdapterError> {
        Ok(Self {
            inner: BufferedWriteStreamState::new(
                backend,
                relpath,
                committed,
                open_flags,
                create_mode,
                create_owner,
            )?,
        })
    }
}

impl FuseFileStream for FuseFileOutStream {
    fn read(&mut self, _size: u32, _offset: i64) -> Result<Vec<u8>, FuseAdapterError> {
        self.inner.read(_size, _offset)
    }

    fn write(&mut self, data: &[u8], offset: i64) -> Result<usize, FuseAdapterError> {
        self.inner.write(data, offset)
    }

    fn flush(&mut self) -> Result<(), FuseAdapterError> {
        self.inner.flush()
    }

    fn truncate(&mut self, size: u64) -> Result<(), FuseAdapterError> {
        self.inner.truncate(size)
    }

    fn fallocate(&mut self, mode: i32, offset: u64, length: u64) -> Result<(), FuseAdapterError> {
        self.inner.fallocate(mode, offset, length)
    }

    fn close(&mut self) -> Result<(), FuseAdapterError> {
        self.inner.flush()
    }

    fn stat(&self) -> FuseStreamStat {
        self.inner.stat()
    }

    fn is_writable(&self) -> bool {
        true
    }

    fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    fn detached_state(&self) -> Option<FuseDetachedFileState> {
        self.inner.detached_state()
    }

    fn snapshot_detached(&mut self) -> Result<(), FuseAdapterError> {
        self.inner.snapshot_detached()
    }
}

pub struct FuseFileInOrOutStream {
    backend: Arc<dyn FuseExportBackend>,
    relpath: String,
    committed: FuseBackendStat,
    open_flags: OpenFlagsView,
    create_mode: Option<u32>,
    create_owner: Option<(u32, u32)>,
    in_stream: Option<FuseFileInStream>,
    out_stream: Option<FuseFileOutStream>,
}

impl FuseFileInOrOutStream {
    pub fn new(
        backend: Arc<dyn FuseExportBackend>,
        relpath: String,
        committed: FuseBackendStat,
        open_flags: OpenFlagsView,
        create_mode: Option<u32>,
        create_owner: Option<(u32, u32)>,
    ) -> Result<Self, FuseAdapterError> {
        let mut out_stream = None;
        if open_flags.contains_truncate() || open_flags.contains_create() {
            out_stream = Some(FuseFileOutStream::new(
                backend.clone(),
                relpath.clone(),
                committed.clone(),
                open_flags,
                create_mode,
                create_owner,
            )?);
        }
        Ok(Self {
            backend,
            relpath,
            committed,
            open_flags,
            create_mode,
            create_owner,
            in_stream: None,
            out_stream,
        })
    }

    fn ensure_out_stream(&mut self) -> Result<&mut FuseFileOutStream, FuseAdapterError> {
        if self.out_stream.is_none() {
            self.out_stream = Some(FuseFileOutStream::new(
                self.backend.clone(),
                self.relpath.clone(),
                self.committed.clone(),
                self.open_flags,
                self.create_mode,
                self.create_owner,
            )?);
        }
        Ok(self.out_stream.as_mut().unwrap())
    }
}

impl FuseFileStream for FuseFileInOrOutStream {
    fn read(&mut self, size: u32, offset: i64) -> Result<Vec<u8>, FuseAdapterError> {
        if let Some(stream) = self.out_stream.as_mut() {
            return stream.read(size, offset);
        }
        if self.in_stream.is_none() {
            self.in_stream = Some(FuseFileInStream::new(
                self.backend.clone(),
                self.relpath.clone(),
                self.committed.clone(),
            ));
        }
        self.in_stream.as_mut().unwrap().read(size, offset)
    }

    fn write(&mut self, data: &[u8], offset: i64) -> Result<usize, FuseAdapterError> {
        if self.in_stream.is_some() {
            return Err(mode_mix_error(
                self.relpath.as_str(),
                "write after read is not supported",
            ));
        }
        self.ensure_out_stream()?.write(data, offset)
    }

    fn flush(&mut self) -> Result<(), FuseAdapterError> {
        if let Some(stream) = self.in_stream.as_mut() {
            return stream.flush();
        }
        if let Some(stream) = self.out_stream.as_mut() {
            return stream.flush();
        }
        Ok(())
    }

    fn truncate(&mut self, size: u64) -> Result<(), FuseAdapterError> {
        if self.in_stream.is_some() {
            return Err(mode_mix_error(
                self.relpath.as_str(),
                "truncate after read is not supported",
            ));
        }
        self.ensure_out_stream()?.truncate(size)
    }

    fn fallocate(&mut self, mode: i32, offset: u64, length: u64) -> Result<(), FuseAdapterError> {
        if self.in_stream.is_some() {
            return Err(mode_mix_error(
                self.relpath.as_str(),
                "fallocate after read is not supported",
            ));
        }
        self.ensure_out_stream()?.fallocate(mode, offset, length)
    }

    fn close(&mut self) -> Result<(), FuseAdapterError> {
        if let Some(stream) = self.in_stream.as_mut() {
            return stream.close();
        }
        if let Some(stream) = self.out_stream.as_mut() {
            return stream.close();
        }
        Ok(())
    }

    fn stat(&self) -> FuseStreamStat {
        if let Some(stream) = self.out_stream.as_ref() {
            return stream.stat();
        }
        if let Some(stream) = self.in_stream.as_ref() {
            return stream.stat();
        }
        FuseStreamStat {
            exists: self.committed.exists,
            is_file: self.committed.is_file,
            is_dir: self.committed.is_dir,
            size: self.committed.size,
            ctime_ns: self.committed.ctime_ns,
            atime_ns: self.committed.atime_ns,
            mtime_ns: self.committed.mtime_ns,
            mode: self.committed.mode,
            uid: self.committed.uid,
            gid: self.committed.gid,
            nlink: self.committed.nlink,
            ino: self.committed.ino,
            rdev: self.committed.rdev,
        }
    }

    fn is_writable(&self) -> bool {
        true
    }

    fn is_dirty(&self) -> bool {
        self.out_stream.as_ref().is_some_and(FuseFileOutStream::is_dirty)
    }

    fn detached_state(&self) -> Option<FuseDetachedFileState> {
        if let Some(stream) = self.out_stream.as_ref() {
            return stream.detached_state();
        }
        if let Some(stream) = self.in_stream.as_ref() {
            return stream.detached_state();
        }
        Some(FuseDetachedFileState {
            size: self.committed.size,
            ctime_ns: self.committed.ctime_ns,
            atime_ns: self.committed.atime_ns,
            mtime_ns: self.committed.mtime_ns,
            mode: self.committed.mode,
            uid: self.committed.uid,
            gid: self.committed.gid,
            nlink: self.committed.nlink.saturating_sub(1),
            ino: self.committed.ino,
            rdev: self.committed.rdev,
        })
    }

    fn snapshot_detached(&mut self) -> Result<(), FuseAdapterError> {
        if let Some(stream) = self.out_stream.as_mut() {
            return stream.snapshot_detached();
        }
        if let Some(stream) = self.in_stream.as_mut() {
            return stream.snapshot_detached();
        }
        Ok(())
    }
}

impl FuseFileOutStream {
    fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }
}

fn resolve_create_mode(
    relpath: &str,
    committed: &FuseBackendStat,
    open_flags: OpenFlagsView,
    create_mode: Option<u32>,
) -> Result<u32, FuseAdapterError> {
    match create_mode {
        Some(mode) => Ok(mode),
        None if committed.exists => Ok(committed.mode),
        None if open_flags.contains_create() => Err(FuseAdapterError::InvalidArgument {
            detail: format!(
                "missing explicit create mode for new writable path relpath={}",
                relpath
            ),
        }),
        None => Ok(committed.mode),
    }
}

fn mode_mix_error(relpath: &str, detail: &str) -> FuseAdapterError {
    FuseAdapterError::Os {
        errno: libc::EOPNOTSUPP,
        path: relpath.to_string(),
        detail: detail.to_string(),
    }
}

fn read_committed_window(
    backend: &dyn FuseExportBackend,
    relpath: &str,
    offset: u64,
    want: usize,
    committed_size: u64,
    committed_mtime_ns: i64,
) -> Result<Vec<u8>, FuseAdapterError> {
    if want == 0 {
        return Ok(Vec::new());
    }
    let bytes = backend
        .read_committed(
            relpath,
            offset,
            u32::try_from(want).unwrap(),
            committed_size,
            committed_mtime_ns,
        )
        .map_err(FuseBackendError::into_adapter)?;
    Ok(bytes)
}

fn current_time_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos().min(i64::MAX as u128) as i64,
        Err(_) => 0,
    }
}

fn current_euid() -> u32 {
    unsafe { libc::geteuid() }
}

fn current_egid() -> u32 {
    unsafe { libc::getegid() }
}

#[cfg(feature = "fsagent_backend")]
fn fsagent_err_to_adapter(err: fluxon_fs::agent::FsAgentError) -> FuseAdapterError {
    match err {
        fluxon_fs::agent::FsAgentError::InvalidArgument { detail } => {
            FuseAdapterError::InvalidArgument { detail }
        }
        fluxon_fs::agent::FsAgentError::AccessDenied { path, detail } => {
            FuseAdapterError::AccessDenied { path, detail }
        }
        fluxon_fs::agent::FsAgentError::Os {
            errno,
            path,
            detail,
        } => FuseAdapterError::Os {
            errno,
            path,
            detail,
        },
        fluxon_fs::agent::FsAgentError::Io { path, detail } => FuseAdapterError::Os {
            errno: libc::EIO,
            path,
            detail,
        },
        fluxon_fs::agent::FsAgentError::Shutdown { detail } => FuseAdapterError::Os {
            errno: libc::EIO,
            path: ".".to_string(),
            detail,
        },
        fluxon_fs::agent::FsAgentError::Kv(err) => FuseAdapterError::Os {
            errno: libc::EIO,
            path: ".".to_string(),
            detail: format!("kv error: {}", err),
        },
    }
}
