use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::backend::{
    FuseBackendError, FuseBackendStat, FuseBackendStatFs, FuseExportBackend,
};
#[cfg(feature = "fsagent_backend")]
use crate::backend::FluxonFsAgentBackend;
use crate::error::FuseAdapterError;
use crate::file_entry::FuseFileEntry;
use crate::file_stream::{FuseDetachedFileState, FuseFileStreamFactory, FuseStreamStat};
use crate::open_action::{OpenAction, OpenFlagsView, classify_open_action};
use crate::path_lock::{PathLockGuard, PathLockManager, PathLockMode};
use crate::path_projection::FuseMountPathProjection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseStat {
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
pub struct FuseStatFs {
    pub block_size: u64,
    pub fragment_size: u64,
    pub blocks: u64,
    pub blocks_free: u64,
    pub blocks_available: u64,
    pub files: u64,
    pub files_free: u64,
    pub files_available: u64,
    pub name_max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseDirEntry {
    pub name: String,
    pub is_file: bool,
    pub is_dir: bool,
    pub mode: u32,
    pub ino: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuseOpenHandle {
    pub fh: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FluxonFuseAtimePolicy {
    NoAtime,
    RelAtime,
    StrictAtime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FluxonFuseSemantics {
    pub read_only: bool,
    pub suid_enabled: bool,
    pub dev_enabled: bool,
    pub exec_enabled: bool,
    pub atime_policy: FluxonFuseAtimePolicy,
    pub dir_atime_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FluxonFuseMountConfig {
    pub mountpoint_dir_abs: String,
    pub export_name: String,
    pub semantics: FluxonFuseSemantics,
}

#[derive(Default)]
struct CallbackOwnerState {
    next_fh: u64,
    by_fh: BTreeMap<u64, Arc<Mutex<FuseFileEntry>>>,
    by_path: BTreeMap<String, BTreeSet<u64>>,
}

#[derive(Default)]
struct MetadataOwnerState {
    atime_by_path: BTreeMap<String, i64>,
}

pub struct FluxonFuseFileSystem {
    mount: FluxonFuseMountConfig,
    backend: Arc<dyn FuseExportBackend>,
    stream_factory: FuseFileStreamFactory,
    path_projection: FuseMountPathProjection,
    backend_path_projection: FuseMountPathProjection,
    path_locks: PathLockManager,
    state: Mutex<CallbackOwnerState>,
    metadata_state: Mutex<MetadataOwnerState>,
    idle_cv: Condvar,
}

impl FluxonFuseFileSystem {
    pub fn new(
        mount: FluxonFuseMountConfig,
        backend: Arc<dyn FuseExportBackend>,
    ) -> Result<Self, FuseAdapterError> {
        if mount.export_name.trim().is_empty() {
            return Err(FuseAdapterError::InvalidArgument {
                detail: "export_name must be non-empty".to_string(),
            });
        }
        let path_projection = FuseMountPathProjection::new(mount.mountpoint_dir_abs.clone())?;
        let backend_path_projection = FuseMountPathProjection::new(mount.mountpoint_dir_abs.clone())?;
        Ok(Self {
            mount,
            stream_factory: FuseFileStreamFactory::new(backend.clone()),
            backend,
            path_projection,
            backend_path_projection,
            path_locks: PathLockManager::new(),
            state: Mutex::new(CallbackOwnerState::default()),
            metadata_state: Mutex::new(MetadataOwnerState::default()),
            idle_cv: Condvar::new(),
        })
    }

    #[cfg(feature = "fsagent_backend")]
    pub fn new_with_fsagent(
        mount: FluxonFuseMountConfig,
        fsagent_mount_dir_abs: String,
        agent: Arc<fluxon_fs::agent::FluxonFsAgent>,
    ) -> Result<Self, FuseAdapterError> {
        if mount.export_name.trim().is_empty() {
            return Err(FuseAdapterError::InvalidArgument {
                detail: "export_name must be non-empty".to_string(),
            });
        }
        let path_projection = FuseMountPathProjection::new(mount.mountpoint_dir_abs.clone())?;
        let backend_path_projection = FuseMountPathProjection::new(fsagent_mount_dir_abs.clone())?;
        let backend = Arc::new(FluxonFsAgentBackend::new(
            agent.clone(),
            fsagent_mount_dir_abs,
        )?);
        Ok(Self {
            mount,
            stream_factory: FuseFileStreamFactory::new_fsagent(agent),
            backend,
            path_projection,
            backend_path_projection,
            path_locks: PathLockManager::new(),
            state: Mutex::new(CallbackOwnerState::default()),
            metadata_state: Mutex::new(MetadataOwnerState::default()),
            idle_cv: Condvar::new(),
        })
    }

    pub fn mount_config(&self) -> &FluxonFuseMountConfig {
        &self.mount
    }

    fn read_access_lock_mode(&self, is_dir: bool) -> PathLockMode {
        if self.access_time_updates_enabled(is_dir) {
            return PathLockMode::Write;
        }
        PathLockMode::Read
    }

    fn access_time_updates_enabled(&self, is_dir: bool) -> bool {
        if self.mount.semantics.read_only {
            return false;
        }
        if self.mount.semantics.atime_policy == FluxonFuseAtimePolicy::NoAtime {
            return false;
        }
        if is_dir && !self.mount.semantics.dir_atime_enabled {
            return false;
        }
        true
    }

    fn effective_atime_ns(&self, relpath: &str, backend_atime_ns: i64) -> i64 {
        let metadata_state = self.metadata_state.lock();
        metadata_state
            .atime_by_path
            .get(relpath)
            .copied()
            .unwrap_or(backend_atime_ns)
    }

    fn apply_effective_atime(&self, relpath: &str, stat: &mut FuseStat) {
        stat.atime_ns = self.effective_atime_ns(relpath, stat.atime_ns);
    }

    fn maybe_update_access_time(
        &self,
        relpath: &str,
        is_dir: bool,
        ctime_ns: i64,
        mtime_ns: i64,
        backend_atime_ns: i64,
    ) {
        if !self.access_time_updates_enabled(is_dir) {
            return;
        }
        let effective_atime_ns = self.effective_atime_ns(relpath, backend_atime_ns);
        if !should_update_access_time(
            self.mount.semantics.atime_policy,
            ctime_ns,
            mtime_ns,
            effective_atime_ns,
        ) {
            return;
        }
        self.metadata_state
            .lock()
            .atime_by_path
            .insert(relpath.to_string(), current_time_ns());
    }

    fn freeze_access_time_overlay_if_disabled(
        &self,
        relpath: &str,
        is_dir: bool,
    ) -> Result<(), FuseAdapterError> {
        if self.access_time_updates_enabled(is_dir) {
            return Ok(());
        }
        if self
            .metadata_state
            .lock()
            .atime_by_path
            .contains_key(relpath)
        {
            return Ok(());
        }
        let stat = self
            .backend
            .lstat(relpath)
            .map_err(FuseBackendError::into_adapter)?;
        if stat.exists {
            self.metadata_state
                .lock()
                .atime_by_path
                .entry(relpath.to_string())
                .or_insert(stat.atime_ns);
        }
        Ok(())
    }

    fn remove_path_metadata(&self, relpath: &str) {
        self.metadata_state.lock().atime_by_path.remove(relpath);
    }

    fn remove_path_tree_metadata(&self, relpath: &str) {
        let prefix = format!("{}/", relpath.trim_end_matches('/'));
        let mut metadata_state = self.metadata_state.lock();
        metadata_state.atime_by_path.retain(|path, _| {
            path != relpath && !path.starts_with(prefix.as_str())
        });
    }

    fn rename_path_tree_metadata(&self, src_relpath: &str, dst_relpath: &str) {
        self.remove_path_tree_metadata(dst_relpath);
        let prefix = format!("{}/", src_relpath.trim_end_matches('/'));
        let mut metadata_state = self.metadata_state.lock();
        let mut updates: Vec<(String, i64)> = metadata_state
            .atime_by_path
            .iter()
            .filter(|(path, _)| path.as_str() == src_relpath || path.starts_with(prefix.as_str()))
            .map(|(path, atime_ns)| {
                let suffix = &path[src_relpath.len()..];
                (format!("{dst_relpath}{suffix}"), *atime_ns)
            })
            .collect();
        let removals: Vec<String> = metadata_state
            .atime_by_path
            .keys()
            .filter(|path| path.as_str() == src_relpath || path.starts_with(prefix.as_str()))
            .cloned()
            .collect();
        for path in removals {
            metadata_state.atime_by_path.remove(path.as_str());
        }
        for (path, atime_ns) in updates.drain(..) {
            metadata_state.atime_by_path.insert(path, atime_ns);
        }
    }

    pub fn access_time_metadata_snapshot(&self) -> BTreeMap<String, i64> {
        self.metadata_state.lock().atime_by_path.clone()
    }

    pub fn replace_access_time_metadata(&self, atime_by_path: BTreeMap<String, i64>) {
        self.metadata_state.lock().atime_by_path = atime_by_path;
    }

    pub fn getattr(&self, callback_path: &str) -> Result<FuseStat, FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Read);

        if let Some(entry) = self.single_writable_entry(projected.relpath()) {
            let entry = entry.lock();
            let stat = entry.stream().stat();
            let backend_stat = self
                .backend
                .lstat(projected.relpath())
                .map_err(FuseBackendError::into_adapter)?;
            let mut out = merge_stream_stat_with_backend(stat, backend_stat);
            self.apply_effective_atime(projected.relpath(), &mut out);
            return Ok(out);
        }

        let entries = self.entries_for_relpath(projected.relpath());
        for entry in entries {
            let entry = entry.lock();
            if !entry.is_detached() {
                continue;
            }
            if let Some(detached_state) = entry.detached_state() {
                let mut out = Self::detached_state_to_fuse(detached_state);
                self.apply_effective_atime(projected.relpath(), &mut out);
                return Ok(out);
            }
        }

        let stat = self
            .backend
            .lstat(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        let mut out = backend_stat_to_fuse(stat);
        self.apply_effective_atime(projected.relpath(), &mut out);
        Ok(out)
    }

    pub fn readdir(&self, callback_path: &str) -> Result<Vec<FuseDirEntry>, FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), self.read_access_lock_mode(true));
        self.freeze_access_time_overlay_if_disabled(projected.relpath(), true)?;
        let mut out = vec![
            FuseDirEntry {
                name: ".".to_string(),
                is_file: false,
                is_dir: true,
                mode: libc::S_IFDIR,
                ino: 0,
            },
            FuseDirEntry {
                name: "..".to_string(),
                is_file: false,
                is_dir: true,
                mode: libc::S_IFDIR,
                ino: 0,
            },
        ];
        let entries = self
            .backend
            .list_dir(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        out.extend(entries.into_iter().map(|entry| FuseDirEntry {
            name: entry.name,
            is_file: entry.is_file,
            is_dir: entry.is_dir,
            mode: entry.mode,
            ino: entry.ino,
        }));
        let stat = self
            .backend
            .lstat(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        if stat.exists {
            self.maybe_update_access_time(
                projected.relpath(),
                true,
                stat.ctime_ns,
                stat.mtime_ns,
                stat.atime_ns,
            );
        }
        Ok(out)
    }

    pub fn readlink(&self, callback_path: &str) -> Result<String, FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Read);
        self.freeze_access_time_overlay_if_disabled(projected.relpath(), false)?;
        self.backend
            .readlink(projected.relpath())
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn setxattr(
        &self,
        callback_path: &str,
        name: &str,
        value: &[u8],
        flags: i32,
    ) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .setxattr(projected.relpath(), name, value.to_vec(), flags)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn getxattr(&self, callback_path: &str, name: &str) -> Result<Vec<u8>, FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Read);
        self.backend
            .getxattr(projected.relpath(), name)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn listxattr(&self, callback_path: &str) -> Result<Vec<u8>, FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Read);
        self.backend
            .listxattr(projected.relpath())
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn removexattr(&self, callback_path: &str, name: &str) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .removexattr(projected.relpath(), name)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn open(&self, callback_path: &str, flags: i32) -> Result<FuseOpenHandle, FuseAdapterError> {
        self.open_internal(callback_path, flags, None, None)
    }

    pub fn create(
        &self,
        callback_path: &str,
        flags: i32,
        mode: u32,
    ) -> Result<FuseOpenHandle, FuseAdapterError> {
        self.create_with_owner(callback_path, flags, mode, None)
    }

    pub fn create_with_owner(
        &self,
        callback_path: &str,
        flags: i32,
        mode: u32,
        create_owner: Option<(u32, u32)>,
    ) -> Result<FuseOpenHandle, FuseAdapterError> {
        let requested_access_bits = flags & libc::O_ACCMODE;
        let access_bits = if requested_access_bits == libc::O_RDWR {
            libc::O_RDWR
        } else if requested_access_bits == libc::O_WRONLY {
            libc::O_WRONLY
        } else {
            libc::O_WRONLY
        };
        let normalized_flags = (flags & !libc::O_ACCMODE) | access_bits | libc::O_CREAT;
        self.open_internal(callback_path, normalized_flags, Some(mode), create_owner)
    }

    pub fn read(&self, fh: u64, size: u32, offset: i64) -> Result<Vec<u8>, FuseAdapterError> {
        let (entry, relpath) = self.entry_and_relpath(fh)?;
        let _lock = self
            .path_locks
            .lock(relpath.as_str(), self.read_access_lock_mode(false));
        self.freeze_access_time_overlay_if_disabled(relpath.as_str(), false)?;
        let mut entry = entry.lock();
        let is_detached = entry.is_detached();
        let bytes = entry.stream_mut().read(size, offset)?;
        if is_detached {
            return Ok(bytes);
        }
        let stat = self
            .backend
            .lstat(relpath.as_str())
            .map_err(FuseBackendError::into_adapter)?;
        if stat.exists {
            self.maybe_update_access_time(
                relpath.as_str(),
                false,
                stat.ctime_ns,
                stat.mtime_ns,
                stat.atime_ns,
            );
        }
        Ok(bytes)
    }

    pub fn write(&self, fh: u64, data: &[u8], offset: i64) -> Result<usize, FuseAdapterError> {
        let (entry, relpath) = self.entry_and_relpath(fh)?;
        let _lock = self.path_locks.lock(relpath.as_str(), PathLockMode::Read);
        let mut entry = entry.lock();
        if entry.is_detached() && !entry.stream().is_writable() {
            return Err(FuseAdapterError::BadFileDescriptor { fh });
        }
        entry.stream_mut().write(data, offset)
    }

    pub fn flush(&self, fh: u64) -> Result<(), FuseAdapterError> {
        let (entry, relpath) = self.entry_and_relpath(fh)?;
        let _lock = self.path_locks.lock(relpath.as_str(), PathLockMode::Write);
        let mut entry = entry.lock();
        if entry.is_detached() {
            return Ok(());
        }
        entry.stream_mut().flush()
    }

    pub fn release(&self, fh: u64) -> Result<(), FuseAdapterError> {
        let (entry, relpath) = self.entry_and_relpath(fh)?;
        let _lock = self.path_locks.lock(relpath.as_str(), PathLockMode::Write);
        {
            let mut entry_guard = entry.lock();
            entry_guard.stream_mut().close()?;
        }

        let mut state = self.state.lock();
        let projected_relpath = relpath.clone();
        state.by_fh.remove(&fh);
        if let Some(path_set) = state.by_path.get_mut(projected_relpath.as_str()) {
            path_set.remove(&fh);
            if path_set.is_empty() {
                state.by_path.remove(projected_relpath.as_str());
            }
        }
        if state.by_fh.is_empty() {
            self.idle_cv.notify_all();
        }
        Ok(())
    }

    pub fn truncate(&self, callback_path: &str, size: u64) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        if let Some(entry) = self.single_writable_entry(projected.relpath()) {
            let mut entry = entry.lock();
            return entry.stream_mut().truncate(size);
        }
        self.backend
            .truncate(projected.relpath(), size)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn ftruncate(&self, fh: u64, size: u64) -> Result<(), FuseAdapterError> {
        let (entry, relpath) = self.entry_and_relpath(fh)?;
        let _lock = self.path_locks.lock(relpath.as_str(), PathLockMode::Write);
        let mut entry = entry.lock();
        entry.stream_mut().truncate(size)
    }

    pub fn fallocate(
        &self,
        fh: u64,
        mode: i32,
        offset: u64,
        length: u64,
    ) -> Result<(), FuseAdapterError> {
        let (entry, relpath) = self.entry_and_relpath(fh)?;
        let _lock = self.path_locks.lock(relpath.as_str(), PathLockMode::Write);
        let mut entry = entry.lock();
        entry.stream_mut().fallocate(mode, offset, length)
    }

    pub fn fiemap(
        &self,
        fh: u64,
        request: &[u8],
        out_size: u32,
    ) -> Result<Vec<u8>, FuseAdapterError> {
        let (entry, relpath) = self.entry_and_relpath(fh)?;
        let _lock = self.path_locks.lock(relpath.as_str(), PathLockMode::Write);
        let mut entry = entry.lock();
        if entry.stream().is_dirty() {
            entry.stream_mut().flush()?;
        }
        self.backend
            .fiemap(relpath.as_str(), request, out_size)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn mkdir(&self, callback_path: &str, mode: u32) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .mkdir(projected.relpath(), mode)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn mkfifo(&self, callback_path: &str, mode: u32) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .mkfifo(projected.relpath(), mode)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn mknod(&self, callback_path: &str, mode: u32, rdev: u32) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .mknod(projected.relpath(), mode, rdev)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn rmdir(&self, callback_path: &str) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.refuse_path_tree_with_live_handles(projected.relpath())?;
        self.backend
            .rmdir(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        self.remove_path_tree_metadata(projected.relpath());
        Ok(())
    }

    pub fn unlink(&self, callback_path: &str) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        let stat = self
            .backend
            .lstat(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        if !stat.exists {
            return Err(FuseAdapterError::NotFound {
                path: projected.callback_path().to_string(),
            });
        }
        if stat.is_dir {
            return Err(FuseAdapterError::IsDirectory {
                path: projected.callback_path().to_string(),
            });
        }
        if stat.nlink > 1 {
            self.refuse_path_with_live_handles(projected.relpath())?;
        } else {
            self.prepare_regular_file_unlink(projected.relpath())?;
        }
        self.backend
            .unlink(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        self.remove_path_metadata(projected.relpath());
        Ok(())
    }

    pub fn link(&self, src_path: &str, dst_path: &str) -> Result<(), FuseAdapterError> {
        let src = self.path_projection.project(src_path)?;
        let dst = self.path_projection.project(dst_path)?;
        let _locks =
            self.lock_paths_ordered([src.lock_key(), dst.lock_key()], PathLockMode::Write);
        self.refuse_path_with_live_handles(dst.relpath())?;
        self.backend
            .link(src.relpath(), dst.relpath())
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn rename(&self, src_path: &str, dst_path: &str) -> Result<(), FuseAdapterError> {
        self.rename_with_flags(src_path, dst_path, 0)
    }

    pub fn rename_with_flags(
        &self,
        src_path: &str,
        dst_path: &str,
        flags: u32,
    ) -> Result<(), FuseAdapterError> {
        let src = self.path_projection.project(src_path)?;
        let dst = self.path_projection.project(dst_path)?;
        if src.relpath() == dst.relpath() {
            return Ok(());
        }
        let rename_policy = classify_rename_policy(flags)?;
        let _locks =
            self.lock_paths_ordered([src.lock_key(), dst.lock_key()], PathLockMode::Write);
        self.refuse_path_tree_with_live_handles(src.relpath())?;
        let src_stat = self
            .backend
            .lstat(src.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        if !src_stat.exists {
            return Err(FuseAdapterError::NotFound {
                path: src.callback_path().to_string(),
            });
        }
        if rename_policy == FuseRenamePolicy::Exchange {
            return Err(FuseAdapterError::Os {
                errno: libc::ENOTSUP,
                path: src.callback_path().to_string(),
                detail: "RENAME_EXCHANGE is not supported in the draft adapter".to_string(),
            });
        }
        let dst_stat = self
            .backend
            .lstat(dst.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        if dst_stat.exists {
            if dst_stat.is_dir {
                self.refuse_path_tree_with_live_handles(dst.relpath())?;
            }
            match rename_policy {
                FuseRenamePolicy::Replace => {
                    if dst_stat.is_dir {
                        self.backend
                            .rmdir(dst.relpath())
                            .map_err(FuseBackendError::into_adapter)?;
                        self.remove_path_tree_metadata(dst.relpath());
                    } else {
                        self.backend
                            .unlink(dst.relpath())
                            .map_err(FuseBackendError::into_adapter)?;
                        self.remove_path_metadata(dst.relpath());
                    }
                }
                FuseRenamePolicy::NoReplace => {
                    return Err(FuseAdapterError::AlreadyExists {
                        path: dst.callback_path().to_string(),
                    });
                }
                FuseRenamePolicy::Exchange => unreachable!(),
            }
        }
        self.backend
            .rename(src.relpath(), dst.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        self.rename_path_tree_metadata(src.relpath(), dst.relpath());
        Ok(())
    }

    pub fn chmod(&self, callback_path: &str, mode: u32) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .chmod(projected.relpath(), mode)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn chown(&self, callback_path: &str, uid: u32, gid: u32) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .chown(projected.relpath(), uid, gid)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn lchown(&self, callback_path: &str, uid: u32, gid: u32) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .lchown(projected.relpath(), uid, gid)
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn utimens(
        &self,
        callback_path: &str,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        let current_stat = self
            .backend
            .lstat(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        if current_stat.is_file || current_stat.is_dir {
            self.backend
                .utimens(projected.relpath(), atime_ns, mtime_ns)
                .map_err(FuseBackendError::into_adapter)?;
        } else {
            self.backend
                .lutimens(projected.relpath(), atime_ns, mtime_ns)
                .map_err(FuseBackendError::into_adapter)?;
        }
        if let Some(atime_ns) = atime_ns {
            self.metadata_state
                .lock()
                .atime_by_path
                .insert(projected.relpath().to_string(), atime_ns);
        }
        Ok(())
    }

    pub fn symlink(&self, linkname: &str, callback_path: &str) -> Result<(), FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);
        self.backend
            .symlink(linkname, projected.relpath())
            .map_err(FuseBackendError::into_adapter)
    }

    pub fn statfs(&self, callback_path: &str) -> Result<FuseStatFs, FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Read);
        let stat = self
            .backend
            .statfs(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        Ok(backend_statfs_to_fuse(stat))
    }

    pub fn umount(&self, force: bool, timeout: Duration) -> Result<(), FuseAdapterError> {
        let start = Instant::now();
        let mut state = self.state.lock();
        while !state.by_fh.is_empty() {
            if force {
                return Ok(());
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(FuseAdapterError::Busy {
                    path: self.mount.mountpoint_dir_abs.clone(),
                    detail: format!(
                        "timed out waiting for {} open handles to drain",
                        state.by_fh.len()
                    ),
                });
            }
            let left = timeout.saturating_sub(elapsed);
            self.idle_cv.wait_for(&mut state, left);
        }
        Ok(())
    }

    fn open_internal(
        &self,
        callback_path: &str,
        flags: i32,
        mode: Option<u32>,
        create_owner: Option<(u32, u32)>,
    ) -> Result<FuseOpenHandle, FuseAdapterError> {
        let projected = self.path_projection.project(callback_path)?;
        let open_action = classify_open_action(flags).ok_or_else(|| FuseAdapterError::InvalidArgument {
            detail: format!("invalid open access mode flags=0x{:x}", flags),
        })?;
        let _lock = self
            .path_locks
            .lock(projected.lock_key(), PathLockMode::Write);

        if open_action.is_writable() {
            self.refuse_second_writable_handle(projected.relpath())?;
        }

        let open_flags = OpenFlagsView::new(flags);
        let stat = self
            .backend
            .stat(projected.relpath())
            .map_err(FuseBackendError::into_adapter)?;
        if stat.is_dir {
            return Err(FuseAdapterError::IsDirectory {
                path: projected.callback_path().to_string(),
            });
        }
        if open_flags.contains_exclusive() && open_flags.contains_create() && stat.exists {
            return Err(FuseAdapterError::AlreadyExists {
                path: projected.callback_path().to_string(),
            });
        }
        if !stat.exists && !open_flags.contains_create() && open_action.is_readable() {
            return Err(FuseAdapterError::NotFound {
                path: projected.callback_path().to_string(),
            });
        }
        if !stat.exists && !open_flags.contains_create() && open_action == OpenAction::WriteOnly {
            return Err(FuseAdapterError::NotFound {
                path: projected.callback_path().to_string(),
            });
        }
        if stat.exists && open_action.is_readable() && !self.access_time_updates_enabled(false) {
            self.metadata_state
                .lock()
                .atime_by_path
                .entry(projected.relpath().to_string())
                .or_insert(stat.atime_ns);
        }
        let stream = self.stream_factory.create(
            projected.relpath().to_string(),
            self.backend_path_projection
                .abs_path_for_relpath(projected.relpath())?,
            flags,
            mode,
            stat,
            create_owner,
        )?;

        let mut state = self.state.lock();
        let fh = state.next_fh;
        state.next_fh = state.next_fh.saturating_add(1);
        let entry = Arc::new(Mutex::new(FuseFileEntry::new(
            fh,
            projected.relpath().to_string(),
            open_action,
            stream,
        )));
        state.by_fh.insert(fh, entry);
        state
            .by_path
            .entry(projected.relpath().to_string())
            .or_default()
            .insert(fh);
        Ok(FuseOpenHandle { fh })
    }

    fn entry_and_relpath(
        &self,
        fh: u64,
    ) -> Result<(Arc<Mutex<FuseFileEntry>>, String), FuseAdapterError> {
        let state = self.state.lock();
        let entry = state
            .by_fh
            .get(&fh)
            .cloned()
            .ok_or(FuseAdapterError::BadFileDescriptor { fh })?;
        let relpath = entry.lock().projected_relpath().to_string();
        Ok((entry, relpath))
    }

    fn single_writable_entry(&self, relpath: &str) -> Option<Arc<Mutex<FuseFileEntry>>> {
        let state = self.state.lock();
        let handles = state.by_path.get(relpath)?;
        for fh in handles {
            let entry = state.by_fh.get(fh)?.clone();
            let is_writable = {
                let entry_guard = entry.lock();
                !entry_guard.is_detached() && entry_guard.stream().is_writable()
            };
            if is_writable {
                return Some(entry);
            }
        }
        None
    }

    fn prepare_regular_file_unlink(&self, relpath: &str) -> Result<(), FuseAdapterError> {
        let entries = self.entries_for_relpath(relpath);
        for entry in &entries {
            let mut entry_guard = entry.lock();
            if entry_guard.is_detached() {
                continue;
            }
            if entry_guard.stream().is_writable() {
                entry_guard.stream_mut().flush()?;
            }
            entry_guard.stream_mut().snapshot_detached()?;
            entry_guard.mark_detached();
        }
        if entries.is_empty() {
            return Ok(());
        }
        let mut state = self.state.lock();
        state.by_path.remove(relpath);
        Ok(())
    }

    fn entries_for_relpath(&self, relpath: &str) -> Vec<Arc<Mutex<FuseFileEntry>>> {
        let state = self.state.lock();
        let Some(handles) = state.by_path.get(relpath) else {
            return Vec::new();
        };
        handles
            .iter()
            .filter_map(|fh| state.by_fh.get(fh).cloned())
            .collect()
    }

    fn refuse_second_writable_handle(&self, relpath: &str) -> Result<(), FuseAdapterError> {
        if self.single_writable_entry(relpath).is_some() {
            return Err(FuseAdapterError::Busy {
                path: relpath.to_string(),
                detail: "writable handle already exists for path".to_string(),
            });
        }
        Ok(())
    }

    fn refuse_path_with_live_handles(&self, relpath: &str) -> Result<(), FuseAdapterError> {
        let state = self.state.lock();
        if matches!(state.by_path.get(relpath), Some(handles) if !handles.is_empty()) {
            return Err(FuseAdapterError::Busy {
                path: relpath.to_string(),
                detail: "path has live handles".to_string(),
            });
        }
        Ok(())
    }

    fn refuse_path_tree_with_live_handles(&self, relpath: &str) -> Result<(), FuseAdapterError> {
        let state = self.state.lock();
        let prefix = format!("{}/", relpath.trim_end_matches('/'));
        if state.by_path.iter().any(|(path, handles)| {
            !handles.is_empty() && (path == relpath || path.starts_with(prefix.as_str()))
        }) {
            return Err(FuseAdapterError::Busy {
                path: relpath.to_string(),
                detail: "path subtree has live handles".to_string(),
            });
        }
        Ok(())
    }

    fn detached_state_to_fuse(state: &FuseDetachedFileState) -> FuseStat {
        FuseStat {
            exists: true,
            is_file: true,
            is_dir: false,
            size: state.size,
            ctime_ns: state.ctime_ns,
            atime_ns: state.atime_ns,
            mtime_ns: state.mtime_ns,
            mode: state.mode,
            uid: state.uid,
            gid: state.gid,
            nlink: state.nlink,
            ino: state.ino,
            rdev: state.rdev,
        }
    }

    fn lock_paths_ordered<const N: usize>(
        &self,
        keys: [&str; N],
        mode: PathLockMode,
    ) -> Vec<PathLockGuard> {
        let mut ordered_keys: Vec<&str> = keys.into_iter().collect();
        ordered_keys.sort_unstable();
        ordered_keys.dedup();
        ordered_keys
            .into_iter()
            .map(|key| self.path_locks.lock(key, mode))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuseRenamePolicy {
    Replace,
    NoReplace,
    Exchange,
}

fn classify_rename_policy(flags: u32) -> Result<FuseRenamePolicy, FuseAdapterError> {
    match flags {
        0 => Ok(FuseRenamePolicy::Replace),
        1 => Ok(FuseRenamePolicy::NoReplace),
        2 => Ok(FuseRenamePolicy::Exchange),
        _ => Err(FuseAdapterError::InvalidArgument {
            detail: format!("unsupported rename flags=0x{:x}", flags),
        }),
    }
}

fn backend_stat_to_fuse(stat: FuseBackendStat) -> FuseStat {
    FuseStat {
        exists: stat.exists,
        is_file: stat.is_file,
        is_dir: stat.is_dir,
        size: stat.size,
        ctime_ns: stat.ctime_ns,
        atime_ns: stat.atime_ns,
        mtime_ns: stat.mtime_ns,
        mode: stat.mode,
        uid: stat.uid,
        gid: stat.gid,
        nlink: stat.nlink,
        ino: stat.ino,
        rdev: stat.rdev,
    }
}

fn backend_statfs_to_fuse(stat: FuseBackendStatFs) -> FuseStatFs {
    FuseStatFs {
        block_size: stat.block_size,
        fragment_size: stat.fragment_size,
        blocks: stat.blocks,
        blocks_free: stat.blocks_free,
        blocks_available: stat.blocks_available,
        files: stat.files,
        files_free: stat.files_free,
        files_available: stat.files_available,
        name_max: stat.name_max,
    }
}

fn stream_stat_to_fuse(stat: FuseStreamStat) -> FuseStat {
    FuseStat {
        exists: stat.exists,
        is_file: stat.is_file,
        is_dir: stat.is_dir,
        size: stat.size,
        ctime_ns: stat.ctime_ns,
        atime_ns: stat.atime_ns,
        mtime_ns: stat.mtime_ns,
        mode: stat.mode,
        uid: stat.uid,
        gid: stat.gid,
        nlink: stat.nlink,
        ino: stat.ino,
        rdev: stat.rdev,
    }
}

fn merge_stream_stat_with_backend(stream: FuseStreamStat, backend: FuseBackendStat) -> FuseStat {
    if !backend.exists {
        return stream_stat_to_fuse(stream);
    }
    FuseStat {
        exists: stream.exists,
        is_file: stream.is_file,
        is_dir: stream.is_dir,
        size: stream.size,
        ctime_ns: backend.ctime_ns,
        atime_ns: backend.atime_ns,
        mtime_ns: backend.mtime_ns,
        mode: backend.mode,
        uid: backend.uid,
        gid: backend.gid,
        nlink: backend.nlink,
        ino: backend.ino,
        rdev: backend.rdev,
    }
}

fn should_update_access_time(
    policy: FluxonFuseAtimePolicy,
    ctime_ns: i64,
    mtime_ns: i64,
    atime_ns: i64,
) -> bool {
    match policy {
        FluxonFuseAtimePolicy::NoAtime => false,
        FluxonFuseAtimePolicy::StrictAtime => true,
        FluxonFuseAtimePolicy::RelAtime => {
            if atime_ns <= ctime_ns || atime_ns <= mtime_ns {
                return true;
            }
            current_time_ns().saturating_sub(atime_ns) >= Duration::from_secs(24 * 60 * 60).as_nanos() as i64
        }
    }
}

fn current_time_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    i64::try_from(nanos).unwrap()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::Mutex;

    use crate::backend::{
        FuseBackendDirEntry, FuseBackendError, FuseBackendStat, FuseBackendStatFs,
        FuseExportBackend,
    };

    use super::{FluxonFuseFileSystem, FluxonFuseMountConfig};

    #[derive(Debug, Clone)]
    struct MockMetadata {
        mode: u32,
        ctime_ns: i64,
        atime_ns: i64,
        mtime_ns: i64,
        uid: u32,
        gid: u32,
        nlink: u64,
    }

    #[derive(Debug, Clone)]
    struct MockFile {
        data: Vec<u8>,
        metadata: MockMetadata,
    }

    #[derive(Debug, Clone)]
    struct MockDir {
        metadata: MockMetadata,
    }

    #[derive(Debug, Clone)]
    enum MockNode {
        File(MockFile),
        Dir(MockDir),
    }

    #[derive(Default)]
    struct MockBackend {
        nodes: Mutex<BTreeMap<String, MockNode>>,
        fail_write_once_for: Mutex<BTreeSet<String>>,
    }

    impl MockBackend {
        fn new() -> Self {
            let out = Self::default();
            out.nodes.lock().insert(
                ".".to_string(),
                MockNode::Dir(MockDir {
                    metadata: dir_metadata(),
                }),
            );
            out
        }

        fn fail_next_write_for(&self, relpath: &str) {
            self.fail_write_once_for
                .lock()
                .insert(relpath.to_string());
        }

        fn metadata(&self, relpath: &str) -> Option<MockMetadata> {
            self.nodes.lock().get(relpath).map(MockNode::metadata).cloned()
        }
    }

    impl MockNode {
        fn metadata(&self) -> &MockMetadata {
            match self {
                Self::File(file) => &file.metadata,
                Self::Dir(dir) => &dir.metadata,
            }
        }

        fn metadata_mut(&mut self) -> &mut MockMetadata {
            match self {
                Self::File(file) => &mut file.metadata,
                Self::Dir(dir) => &mut dir.metadata,
            }
        }
    }

    impl FuseExportBackend for MockBackend {
        fn stat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError> {
            let nodes = self.nodes.lock();
            let Some(node) = nodes.get(relpath) else {
                return Ok(FuseBackendStat {
                    exists: false,
                    is_file: false,
                    is_dir: false,
                    size: 0,
                    ctime_ns: 0,
                    atime_ns: 0,
                    mtime_ns: 0,
                    mode: 0,
                    uid: 0,
                    gid: 0,
                    nlink: 0,
                    ino: 0,
                    rdev: 0,
                });
            };
            let metadata = node.metadata();
            match node {
                MockNode::File(file) => Ok(FuseBackendStat {
                    exists: true,
                    is_file: true,
                    is_dir: false,
                    size: file.data.len() as u64,
                    ctime_ns: metadata.ctime_ns,
                    atime_ns: metadata.atime_ns,
                    mtime_ns: metadata.mtime_ns,
                    mode: metadata.mode,
                    uid: metadata.uid,
                    gid: metadata.gid,
                    nlink: metadata.nlink,
                    ino: mock_ino_for_relpath(relpath),
                    rdev: 0,
                }),
                MockNode::Dir(_) => Ok(FuseBackendStat {
                    exists: true,
                    is_file: false,
                    is_dir: true,
                    size: 0,
                    ctime_ns: metadata.ctime_ns,
                    atime_ns: metadata.atime_ns,
                    mtime_ns: metadata.mtime_ns,
                    mode: metadata.mode,
                    uid: metadata.uid,
                    gid: metadata.gid,
                    nlink: metadata.nlink,
                    ino: mock_ino_for_relpath(relpath),
                    rdev: 0,
                }),
            }
        }

        fn lstat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError> {
            self.stat(relpath)
        }

        fn list_dir(&self, relpath: &str) -> Result<Vec<FuseBackendDirEntry>, FuseBackendError> {
            let nodes = self.nodes.lock();
            if !matches!(nodes.get(relpath), Some(MockNode::Dir(_))) {
                return Err(FuseBackendError::NotDirectory {
                    path: relpath.to_string(),
                });
            }
            let mut out: Vec<FuseBackendDirEntry> = Vec::new();
            let prefix = if relpath == "." {
                "".to_string()
            } else {
                format!("{}/", relpath)
            };
            for key in nodes.keys() {
                if key == relpath || !key.starts_with(prefix.as_str()) {
                    continue;
                }
                let tail = &key[prefix.len()..];
                if tail.is_empty() || tail.contains('/') {
                    continue;
                }
                match nodes.get(key).unwrap() {
                    MockNode::File(file) => out.push(FuseBackendDirEntry {
                        name: tail.to_string(),
                        is_file: true,
                        is_dir: false,
                        mode: file.metadata.mode,
                        ino: mock_ino_for_relpath(key.as_str()),
                    }),
                    MockNode::Dir(dir) => out.push(FuseBackendDirEntry {
                        name: tail.to_string(),
                        is_file: false,
                        is_dir: true,
                        mode: dir.metadata.mode,
                        ino: mock_ino_for_relpath(key.as_str()),
                    }),
                }
            }
            out.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(out)
        }

        fn readlink(&self, relpath: &str) -> Result<String, FuseBackendError> {
            Err(FuseBackendError::Os {
                errno: libc::ENOTSUP,
                path: relpath.to_string(),
                detail: "mock backend does not support readlink".to_string(),
            })
        }

        fn read_committed(
            &self,
            relpath: &str,
            offset: u64,
            size: u32,
            _committed_size: u64,
            _committed_mtime_ns: i64,
        ) -> Result<Vec<u8>, FuseBackendError> {
            let nodes = self.nodes.lock();
            match nodes.get(relpath) {
                Some(MockNode::File(file)) => {
                    let start = offset as usize;
                    if start >= file.data.len() {
                        return Ok(Vec::new());
                    }
                    let end = std::cmp::min(file.data.len(), start + size as usize);
                    Ok(file.data[start..end].to_vec())
                }
                Some(MockNode::Dir(_)) => Err(FuseBackendError::IsDirectory {
                    path: relpath.to_string(),
                }),
                None => Err(FuseBackendError::NotFound {
                    path: relpath.to_string(),
                }),
            }
        }

        fn write_chunk(&self, relpath: &str, offset: u64, data: Vec<u8>) -> Result<(), FuseBackendError> {
            if self.fail_write_once_for.lock().remove(relpath) {
                return Err(FuseBackendError::Os {
                    errno: libc::EIO,
                    path: relpath.to_string(),
                    detail: "injected write failure".to_string(),
                });
            }
            let mut nodes = self.nodes.lock();
            let entry = nodes.entry(relpath.to_string()).or_insert_with(|| {
                MockNode::File(MockFile {
                    data: Vec::new(),
                    metadata: file_metadata(0o644),
                })
            });
            let MockNode::File(file) = entry else {
                return Err(FuseBackendError::IsDirectory {
                    path: relpath.to_string(),
                });
            };
            let start = offset as usize;
            if file.data.len() < start {
                file.data.resize(start, 0);
            }
            if file.data.len() < start + data.len() {
                file.data.resize(start + data.len(), 0);
            }
            file.data[start..start + data.len()].copy_from_slice(data.as_slice());
            bump_ctime_and_mtime(&mut file.metadata);
            Ok(())
        }

        fn truncate(&self, relpath: &str, size: u64) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            let entry = nodes.entry(relpath.to_string()).or_insert_with(|| {
                MockNode::File(MockFile {
                    data: Vec::new(),
                    metadata: file_metadata(0o644),
                })
            });
            let MockNode::File(file) = entry else {
                return Err(FuseBackendError::IsDirectory {
                    path: relpath.to_string(),
                });
            };
            file.data.resize(size as usize, 0);
            bump_ctime_and_mtime(&mut file.metadata);
            Ok(())
        }

        fn mkdir(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            if nodes.contains_key(relpath) {
                return Err(FuseBackendError::AlreadyExists {
                    path: relpath.to_string(),
                });
            }
            nodes.insert(
                relpath.to_string(),
                MockNode::Dir(MockDir {
                    metadata: dir_metadata_with_mode(mode),
                }),
            );
            Ok(())
        }

        fn mkfifo(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            if nodes.contains_key(relpath) {
                return Err(FuseBackendError::AlreadyExists {
                    path: relpath.to_string(),
                });
            }
            nodes.insert(
                relpath.to_string(),
                MockNode::File(MockFile {
                    data: Vec::new(),
                    metadata: file_metadata(mode),
                }),
            );
            Ok(())
        }

        fn mknod(&self, relpath: &str, mode: u32, _rdev: u32) -> Result<(), FuseBackendError> {
            self.mkfifo(relpath, mode)
        }

        fn rmdir(&self, relpath: &str) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            let Some(MockNode::Dir(_)) = nodes.get(relpath) else {
                return Err(FuseBackendError::NotDirectory {
                    path: relpath.to_string(),
                });
            };
            let prefix = if relpath == "." {
                "".to_string()
            } else {
                format!("{}/", relpath)
            };
            if nodes
                .keys()
                .any(|key| key != relpath && key.starts_with(prefix.as_str()))
            {
                return Err(FuseBackendError::Os {
                    errno: libc::ENOTEMPTY,
                    path: relpath.to_string(),
                    detail: "directory not empty".to_string(),
                });
            }
            nodes.remove(relpath);
            Ok(())
        }

        fn unlink(&self, relpath: &str) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            match nodes.get(relpath) {
                Some(MockNode::File(_)) => {
                    nodes.remove(relpath);
                }
                Some(MockNode::Dir(_)) => {
                    return Err(FuseBackendError::IsDirectory {
                        path: relpath.to_string(),
                    });
                }
                None => {
                    return Err(FuseBackendError::NotFound {
                        path: relpath.to_string(),
                    });
                }
            }
            Ok(())
        }

        fn link(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            if nodes.contains_key(dst_relpath) {
                return Err(FuseBackendError::AlreadyExists {
                    path: dst_relpath.to_string(),
                });
            }
            let Some(node) = nodes.get(src_relpath).cloned() else {
                return Err(FuseBackendError::NotFound {
                    path: src_relpath.to_string(),
                });
            };
            if matches!(node, MockNode::Dir(_)) {
                return Err(FuseBackendError::Os {
                    errno: libc::EPERM,
                    path: src_relpath.to_string(),
                    detail: "mock backend does not support directory hard links".to_string(),
                });
            }
            nodes.insert(dst_relpath.to_string(), node);
            Ok(())
        }

        fn rename(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            if !nodes.contains_key(src_relpath) {
                return Err(FuseBackendError::NotFound {
                    path: src_relpath.to_string(),
                });
            }
            let src_prefix = format!("{}/", src_relpath.trim_end_matches('/'));
            let dst_prefix = format!("{}/", dst_relpath.trim_end_matches('/'));
            let mut remap: Vec<(String, MockNode)> = Vec::new();
            let keys: Vec<String> = nodes.keys().cloned().collect();
            for key in keys {
                if key == src_relpath || key.starts_with(src_prefix.as_str()) {
                    let node = nodes.remove(key.as_str()).unwrap();
                    let new_key = if key == src_relpath {
                        dst_relpath.to_string()
                    } else {
                        format!("{}{}", dst_prefix, &key[src_prefix.len()..])
                    };
                    remap.push((new_key, node));
                }
            }
            for (key, node) in remap {
                nodes.insert(key, node);
            }
            Ok(())
        }

        fn chmod(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            let Some(node) = nodes.get_mut(relpath) else {
                return Err(FuseBackendError::NotFound {
                    path: relpath.to_string(),
                });
            };
            let metadata = node.metadata_mut();
            metadata.mode = mode;
            metadata.ctime_ns += 1;
            Ok(())
        }

        fn chown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            let Some(node) = nodes.get_mut(relpath) else {
                return Err(FuseBackendError::NotFound {
                    path: relpath.to_string(),
                });
            };
            let metadata = node.metadata_mut();
            metadata.uid = uid;
            metadata.gid = gid;
            metadata.ctime_ns += 1;
            Ok(())
        }

        fn lchown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError> {
            self.chown(relpath, uid, gid)
        }

        fn utimens(
            &self,
            relpath: &str,
            atime_ns: Option<i64>,
            mtime_ns: Option<i64>,
        ) -> Result<(), FuseBackendError> {
            let mut nodes = self.nodes.lock();
            let Some(node) = nodes.get_mut(relpath) else {
                return Err(FuseBackendError::NotFound {
                    path: relpath.to_string(),
                });
            };
            let metadata = node.metadata_mut();
            metadata.atime_ns = atime_ns.unwrap_or(metadata.atime_ns);
            metadata.mtime_ns = mtime_ns.unwrap_or(metadata.mtime_ns);
            metadata.ctime_ns += 1;
            Ok(())
        }

        fn symlink(&self, _linkname: &str, relpath: &str) -> Result<(), FuseBackendError> {
            Err(FuseBackendError::Os {
                errno: libc::ENOTSUP,
                path: relpath.to_string(),
                detail: format!("mock backend does not support symlink for {}", relpath),
            })
        }

        fn statfs(&self, _relpath: &str) -> Result<FuseBackendStatFs, FuseBackendError> {
            Ok(FuseBackendStatFs {
                block_size: 4096,
                fragment_size: 4096,
                blocks: 1024,
                blocks_free: 768,
                blocks_available: 768,
                files: 2048,
                files_free: 2000,
                files_available: 2000,
                name_max: 255,
            })
        }
    }

    fn file_metadata(mode: u32) -> MockMetadata {
        MockMetadata {
            mode,
            ctime_ns: 1,
            atime_ns: 1,
            mtime_ns: 1,
            uid: 0,
            gid: 0,
            nlink: 1,
        }
    }

    fn dir_metadata() -> MockMetadata {
        dir_metadata_with_mode(0o755)
    }

    fn dir_metadata_with_mode(mode: u32) -> MockMetadata {
        MockMetadata {
            mode,
            ctime_ns: 1,
            atime_ns: 1,
            mtime_ns: 1,
            uid: 0,
            gid: 0,
            nlink: 2,
        }
    }

    fn bump_ctime_and_mtime(metadata: &mut MockMetadata) {
        metadata.ctime_ns += 1;
        metadata.mtime_ns += 1;
    }

    fn mock_ino_for_relpath(relpath: &str) -> u64 {
        if relpath == "." {
            return 1;
        }
        let mut out = 1469598103934665603u64;
        for byte in relpath.as_bytes() {
            out ^= u64::from(*byte);
            out = out.wrapping_mul(1099511628211);
        }
        out
    }

    fn visible_names(entries: &[crate::adapter::FuseDirEntry]) -> Vec<String> {
        entries
            .iter()
            .filter(|entry| entry.name != "." && entry.name != "..")
            .map(|entry| entry.name.clone())
            .collect()
    }

    fn write_file(fs: &FluxonFuseFileSystem, path: &str, bytes: &[u8]) {
        let handle = fs.create(path, libc::O_WRONLY, 0o644).unwrap();
        fs.write(handle.fh, bytes, 0).unwrap();
        fs.release(handle.fh).unwrap();
    }

    fn read_file(fs: &FluxonFuseFileSystem, path: &str) -> Vec<u8> {
        let handle = fs.open(path, libc::O_RDONLY).unwrap();
        let bytes = fs.read(handle.fh, 4096, 0).unwrap();
        fs.release(handle.fh).unwrap();
        bytes
    }

    fn rw_relatime_semantics() -> crate::adapter::FluxonFuseSemantics {
        crate::adapter::FluxonFuseSemantics {
            read_only: false,
            suid_enabled: true,
            dev_enabled: true,
            exec_enabled: true,
            atime_policy: crate::adapter::FluxonFuseAtimePolicy::RelAtime,
            dir_atime_enabled: true,
        }
    }

    fn new_fs() -> FluxonFuseFileSystem {
        FluxonFuseFileSystem::new(
            FluxonFuseMountConfig {
                mountpoint_dir_abs: "/tmp/mock_fuse".to_string(),
                export_name: "demo".to_string(),
                semantics: rw_relatime_semantics(),
            },
            Arc::new(MockBackend::new()),
        )
        .unwrap()
    }

    fn new_fs_with_backend() -> (FluxonFuseFileSystem, Arc<MockBackend>) {
        let backend = Arc::new(MockBackend::new());
        let fs = new_fs_with_semantics_and_backend(rw_relatime_semantics(), backend.clone());
        (fs, backend)
    }

    fn new_fs_with_semantics(
        semantics: crate::adapter::FluxonFuseSemantics,
    ) -> FluxonFuseFileSystem {
        let backend = Arc::new(MockBackend::new());
        new_fs_with_semantics_and_backend(semantics, backend)
    }

    fn new_fs_with_semantics_and_backend(
        semantics: crate::adapter::FluxonFuseSemantics,
        backend: Arc<MockBackend>,
    ) -> FluxonFuseFileSystem {
        let fs = FluxonFuseFileSystem::new(
            FluxonFuseMountConfig {
                mountpoint_dir_abs: "/tmp/mock_fuse".to_string(),
                export_name: "demo".to_string(),
                semantics,
            },
            backend,
        )
        .unwrap();
        fs
    }

    #[test]
    fn write_is_not_visible_until_flush() {
        let fs = new_fs();
        let handle = fs.create("/hello.txt", libc::O_WRONLY, 0o644).unwrap();
        fs.write(handle.fh, b"abc", 0).unwrap();

        let stat_before = fs.getattr("/hello.txt").unwrap();
        assert_eq!(stat_before.size, 3);

        let listing_before = fs.readdir("/").unwrap();
        assert!(visible_names(&listing_before).is_empty());

        fs.flush(handle.fh).unwrap();
        let listing_after = fs.readdir("/").unwrap();
        assert_eq!(visible_names(&listing_after), vec!["hello.txt".to_string()]);
    }

    #[test]
    fn release_drains_dirty_state() {
        let fs = new_fs();
        let handle = fs.create("/release.txt", libc::O_WRONLY, 0o644).unwrap();
        fs.write(handle.fh, b"xyz", 0).unwrap();
        fs.release(handle.fh).unwrap();

        let stat = fs.getattr("/release.txt").unwrap();
        assert_eq!(stat.size, 3);
    }

    #[test]
    fn path_truncate_uses_open_writer_authority() {
        let fs = new_fs();
        let handle = fs.create("/truncate.txt", libc::O_RDWR, 0o644).unwrap();
        fs.write(handle.fh, b"abcdef", 0).unwrap();
        fs.truncate("/truncate.txt", 2).unwrap();

        let stat_before_release = fs.getattr("/truncate.txt").unwrap();
        assert_eq!(stat_before_release.size, 2);

        fs.release(handle.fh).unwrap();

        let stat = fs.getattr("/truncate.txt").unwrap();
        assert_eq!(stat.size, 2);
        assert_eq!(read_file(&fs, "/truncate.txt"), b"ab".to_vec());
    }

    #[test]
    fn second_writable_handle_is_rejected() {
        let fs = new_fs();
        let _h1 = fs.create("/busy.txt", libc::O_WRONLY, 0o644).unwrap();
        let err = fs.open("/busy.txt", libc::O_WRONLY | libc::O_CREAT).unwrap_err();
        assert_eq!(err.errno(), libc::EBUSY);
    }

    #[test]
    fn flush_failure_keeps_dirty_state_for_retry() {
        let (fs, backend) = new_fs_with_backend();
        let handle = fs.create("/retry.txt", libc::O_WRONLY, 0o644).unwrap();
        fs.write(handle.fh, b"retry-data", 0).unwrap();

        backend.fail_next_write_for("retry.txt");
        let flush_err = fs.flush(handle.fh).unwrap_err();
        assert_eq!(flush_err.errno(), libc::EIO);

        let stat_before_retry = fs.getattr("/retry.txt").unwrap();
        assert_eq!(stat_before_retry.size, 10);

        fs.flush(handle.fh).unwrap();
        fs.release(handle.fh).unwrap();

        let stat_after_retry = fs.getattr("/retry.txt").unwrap();
        assert_eq!(stat_after_retry.size, 10);
        assert_eq!(read_file(&fs, "/retry.txt"), b"retry-data".to_vec());
    }

    #[test]
    fn unlink_allows_live_regular_file_handles_but_rename_and_rmdir_stay_busy() {
        let fs = new_fs();
        fs.mkdir("/dir", 0o755).unwrap();
        write_file(&fs, "/dir/file.txt", b"payload");
        let file_handle = fs.open("/dir/file.txt", libc::O_RDONLY).unwrap();

        fs.unlink("/dir/file.txt").unwrap();

        let missing = fs.getattr("/dir/file.txt").unwrap();
        assert!(!missing.exists);

        fs.release(file_handle.fh).unwrap();

        let recreate = fs.create("/dir/file.txt", libc::O_WRONLY, 0o644).unwrap();
        let rename_err = fs.rename("/dir/file.txt", "/dir/file2.txt").unwrap_err();
        assert_eq!(rename_err.errno(), libc::EBUSY);
        fs.release(recreate.fh).unwrap();

        let parent_busy = fs.create("/dir/parent_busy.txt", libc::O_WRONLY, 0o644).unwrap();
        let rename_parent_err = fs.rename("/dir", "/dir2").unwrap_err();
        assert_eq!(rename_parent_err.errno(), libc::EBUSY);
        fs.release(parent_busy.fh).unwrap();

        let dir_open = fs.open("/dir", libc::O_RDONLY).unwrap_err();
        assert_eq!(dir_open.errno(), libc::EISDIR);

        let subtree_busy = fs.create("/dir/subtree_busy.txt", libc::O_WRONLY, 0o644).unwrap();
        let subtree_rmdir_err = fs.rmdir("/dir").unwrap_err();
        assert_eq!(subtree_rmdir_err.errno(), libc::EBUSY);
        fs.release(subtree_busy.fh).unwrap();

        let dir_handle = fs.create("/dir/child.txt", libc::O_WRONLY, 0o644).unwrap();
        let rmdir_busy_err = fs.rmdir("/dir").unwrap_err();
        assert_eq!(rmdir_busy_err.errno(), libc::EBUSY);
        fs.release(dir_handle.fh).unwrap();

        let rmdir_non_empty_err = fs.rmdir("/dir").unwrap_err();
        assert_eq!(rmdir_non_empty_err.errno(), libc::ENOTEMPTY);
    }

    #[test]
    fn umount_waits_for_open_handles_unless_forced() {
        let fs = new_fs();
        let handle = fs.create("/busy.txt", libc::O_WRONLY, 0o644).unwrap();

        let err = fs.umount(false, Duration::from_millis(1)).unwrap_err();
        assert_eq!(err.errno(), libc::EBUSY);

        fs.umount(true, Duration::from_millis(1)).unwrap();
        fs.release(handle.fh).unwrap();
        fs.umount(false, Duration::from_millis(1)).unwrap();
    }

    #[test]
    fn readdir_includes_standard_dot_entries() {
        let fs = new_fs();
        write_file(&fs, "/child.txt", b"abc");

        let listing = fs.readdir("/").unwrap();
        let names: Vec<String> = listing.into_iter().map(|entry| entry.name).collect();
        assert_eq!(names, vec![".".to_string(), "..".to_string(), "child.txt".to_string()]);
    }

    #[test]
    fn read_write_stream_latches_read_mode() {
        let fs = new_fs();
        write_file(&fs, "/mode.txt", b"abcdef");

        let handle = fs.open("/mode.txt", libc::O_RDWR).unwrap();
        let bytes = fs.read(handle.fh, 3, 0).unwrap();
        assert_eq!(bytes, b"abc".to_vec());

        let err = fs.write(handle.fh, b"z", 0).unwrap_err();
        assert_eq!(err.errno(), libc::EOPNOTSUPP);
        fs.release(handle.fh).unwrap();
    }

    #[test]
    fn read_write_stream_requires_truncate_zero_before_overwrite() {
        let fs = new_fs();
        write_file(&fs, "/existing.txt", b"abcdef");

        let handle = fs.open("/existing.txt", libc::O_RDWR).unwrap();
        let write_err = fs.write(handle.fh, b"xy", 0).unwrap_err();
        assert_eq!(write_err.errno(), libc::EEXIST);

        fs.truncate("/existing.txt", 0).unwrap();
        fs.write(handle.fh, b"xy", 0).unwrap();
        let bytes = fs.read(handle.fh, 2, 0).unwrap();
        assert_eq!(bytes, b"xy".to_vec());

        fs.release(handle.fh).unwrap();
        assert_eq!(read_file(&fs, "/existing.txt"), b"xy".to_vec());
    }

    #[test]
    fn deleted_open_read_write_handle_can_still_read() {
        let fs = new_fs();
        let create_handle = fs.create("/deleted.txt", libc::O_WRONLY, 0o644).unwrap();
        fs.release(create_handle.fh).unwrap();
        let handle = fs.open("/deleted.txt", libc::O_RDWR).unwrap();

        fs.write(handle.fh, b"Hello,_World!", 0).unwrap();
        fs.unlink("/deleted.txt").unwrap();

        let bytes = fs.read(handle.fh, 13, 0).unwrap();
        assert_eq!(bytes, b"Hello,_World!".to_vec());

        fs.release(handle.fh).unwrap();
    }

    #[test]
    fn reopened_read_only_handle_reads_large_sparse_offset() {
        let fs = new_fs();
        let handle = fs.create("/large.txt", libc::O_WRONLY, 0o755).unwrap();

        fs.write(handle.fh, b"a", 2 * 1024 * 1024 * 1024 + 1).unwrap();
        fs.release(handle.fh).unwrap();

        let read_handle = fs.open("/large.txt", libc::O_RDONLY).unwrap();
        let bytes = fs.read(read_handle.fh, 1, 2 * 1024 * 1024 * 1024 + 1).unwrap();
        assert_eq!(bytes, b"a".to_vec());
        fs.release(read_handle.fh).unwrap();
    }

    #[test]
    fn sparse_extension_write_is_supported_and_duplicate_prefix_is_ignored() {
        let fs = new_fs();
        let handle = fs.create("/seq.txt", libc::O_WRONLY, 0o644).unwrap();

        fs.write(handle.fh, b"abc", 0).unwrap();
        let duplicate = fs.write(handle.fh, b"ab", 0).unwrap();
        assert_eq!(duplicate, 2);

        fs.write(handle.fh, b"z", 5).unwrap();

        fs.release(handle.fh).unwrap();
        let bytes = read_file(&fs, "/seq.txt");
        assert_eq!(bytes.len(), 6);
        assert_eq!(&bytes[..3], b"abc");
        assert_eq!(&bytes[3..5], &[0, 0]);
        assert_eq!(bytes[5], b'z');
    }

    #[test]
    fn truncate_grow_keeps_write_frontier_after_flush() {
        let fs = new_fs();
        let handle = fs.create("/grow.txt", libc::O_WRONLY, 0o644).unwrap();

        fs.write(handle.fh, b"abc", 0).unwrap();
        fs.truncate("/grow.txt", 5).unwrap();
        fs.flush(handle.fh).unwrap();

        let sparse_err = fs.write(handle.fh, b"x", 5).unwrap_err();
        assert_eq!(sparse_err.errno(), libc::EOPNOTSUPP);

        fs.write(handle.fh, b"de", 3).unwrap();
        fs.release(handle.fh).unwrap();
        assert_eq!(read_file(&fs, "/grow.txt"), b"abcde".to_vec());
    }

    #[test]
    fn handle_ftruncate_resizes_existing_writable_file() {
        let fs = new_fs();
        write_file(&fs, "/existing.txt", b"abcdef");

        let handle = fs.open("/existing.txt", libc::O_WRONLY).unwrap();
        fs.ftruncate(handle.fh, 3).unwrap();
        fs.release(handle.fh).unwrap();

        let stat = fs.getattr("/existing.txt").unwrap();
        assert_eq!(stat.size, 3);
        assert_eq!(read_file(&fs, "/existing.txt"), b"abc".to_vec());
    }

    #[test]
    fn read_only_handle_ftruncate_is_bad_fd() {
        let fs = new_fs();
        write_file(&fs, "/readonly.txt", b"abcdef");

        let handle = fs.open("/readonly.txt", libc::O_RDONLY).unwrap();
        let err = fs.ftruncate(handle.fh, 2).unwrap_err();
        assert_eq!(err.errno(), libc::EBADF);
        fs.release(handle.fh).unwrap();
    }

    #[test]
    fn rename_with_flags_respects_noreplace_and_replace() {
        let fs = new_fs();
        write_file(&fs, "/src.txt", b"src");
        write_file(&fs, "/dst.txt", b"dst");

        let noreplace_err = fs.rename_with_flags("/src.txt", "/dst.txt", 1).unwrap_err();
        assert_eq!(noreplace_err.errno(), libc::EEXIST);

        let exchange_err = fs.rename_with_flags("/src.txt", "/dst.txt", 2).unwrap_err();
        assert_eq!(exchange_err.errno(), libc::ENOTSUP);

        fs.rename_with_flags("/src.txt", "/dst.txt", 0).unwrap();
        assert_eq!(read_file(&fs, "/dst.txt"), b"src".to_vec());
    }

    #[test]
    fn rename_replace_allows_live_destination_file_handle() {
        let fs = new_fs();
        write_file(&fs, "/src.txt", b"src");
        write_file(&fs, "/dst.txt", b"dst");

        let handle = fs.open("/dst.txt", libc::O_RDONLY).unwrap();
        fs.rename("/src.txt", "/dst.txt").unwrap();

        fs.release(handle.fh).unwrap();
        assert_eq!(read_file(&fs, "/dst.txt"), b"src".to_vec());
    }

    #[test]
    fn access_time_semantics_follow_mount_policy_for_files() {
        let relatime_fs = new_fs();
        write_file(&relatime_fs, "/file.txt", b"abc");
        let before = relatime_fs.getattr("/file.txt").unwrap();
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(read_file(&relatime_fs, "/file.txt"), b"abc".to_vec());
        let after_first = relatime_fs.getattr("/file.txt").unwrap();
        assert!(after_first.atime_ns > before.atime_ns);
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(read_file(&relatime_fs, "/file.txt"), b"abc".to_vec());
        let after_second = relatime_fs.getattr("/file.txt").unwrap();
        assert_eq!(after_second.atime_ns, after_first.atime_ns);

        let noatime_fs = new_fs_with_semantics(crate::adapter::FluxonFuseSemantics {
            read_only: false,
            suid_enabled: true,
            dev_enabled: true,
            exec_enabled: true,
            atime_policy: crate::adapter::FluxonFuseAtimePolicy::NoAtime,
            dir_atime_enabled: true,
        });
        write_file(&noatime_fs, "/file.txt", b"abc");
        let noatime_before = noatime_fs.getattr("/file.txt").unwrap();
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(read_file(&noatime_fs, "/file.txt"), b"abc".to_vec());
        let noatime_after = noatime_fs.getattr("/file.txt").unwrap();
        assert_eq!(noatime_after.atime_ns, noatime_before.atime_ns);

        let strictatime_fs = new_fs_with_semantics(crate::adapter::FluxonFuseSemantics {
            read_only: false,
            suid_enabled: true,
            dev_enabled: true,
            exec_enabled: true,
            atime_policy: crate::adapter::FluxonFuseAtimePolicy::StrictAtime,
            dir_atime_enabled: true,
        });
        write_file(&strictatime_fs, "/file.txt", b"abc");
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(read_file(&strictatime_fs, "/file.txt"), b"abc".to_vec());
        let strict_after_first = strictatime_fs.getattr("/file.txt").unwrap();
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(read_file(&strictatime_fs, "/file.txt"), b"abc".to_vec());
        let strict_after_second = strictatime_fs.getattr("/file.txt").unwrap();
        assert!(strict_after_second.atime_ns > strict_after_first.atime_ns);
    }

    #[test]
    fn access_time_semantics_disable_directory_updates_when_requested() {
        let nodiratime_fs = new_fs_with_semantics(crate::adapter::FluxonFuseSemantics {
            read_only: false,
            suid_enabled: true,
            dev_enabled: true,
            exec_enabled: true,
            atime_policy: crate::adapter::FluxonFuseAtimePolicy::StrictAtime,
            dir_atime_enabled: false,
        });
        nodiratime_fs.mkdir("/dir", 0o755).unwrap();
        let nodiratime_before = nodiratime_fs.getattr("/dir").unwrap();
        std::thread::sleep(Duration::from_millis(1));
        let listing = nodiratime_fs.readdir("/dir").unwrap();
        assert_eq!(visible_names(&listing), Vec::<String>::new());
        let nodiratime_after = nodiratime_fs.getattr("/dir").unwrap();
        assert_eq!(nodiratime_after.atime_ns, nodiratime_before.atime_ns);

        let readonly_fs = new_fs_with_semantics(crate::adapter::FluxonFuseSemantics {
            read_only: true,
            suid_enabled: true,
            dev_enabled: true,
            exec_enabled: true,
            atime_policy: crate::adapter::FluxonFuseAtimePolicy::StrictAtime,
            dir_atime_enabled: true,
        });
        write_file(&readonly_fs, "/file.txt", b"abc");
        let readonly_before = readonly_fs.getattr("/file.txt").unwrap();
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(read_file(&readonly_fs, "/file.txt"), b"abc".to_vec());
        let readonly_after = readonly_fs.getattr("/file.txt").unwrap();
        assert_eq!(readonly_after.atime_ns, readonly_before.atime_ns);
    }

    #[test]
    fn access_time_metadata_snapshot_round_trips_across_instances() {
        let backend = Arc::new(MockBackend::new());
        let fs = new_fs_with_semantics_and_backend(rw_relatime_semantics(), backend.clone());
        write_file(&fs, "/file.txt", b"abc");

        let backend_before = backend.metadata("file.txt").unwrap().atime_ns;
        std::thread::sleep(Duration::from_millis(1));
        assert_eq!(read_file(&fs, "/file.txt"), b"abc".to_vec());

        let snapshot = fs.access_time_metadata_snapshot();
        let overlay_atime = snapshot.get("file.txt").copied().unwrap();
        assert!(overlay_atime > backend_before);
        assert_eq!(backend.metadata("file.txt").unwrap().atime_ns, backend_before);

        let fs_after_remount = new_fs_with_semantics_and_backend(rw_relatime_semantics(), backend);
        fs_after_remount.replace_access_time_metadata(snapshot);
        assert_eq!(fs_after_remount.getattr("/file.txt").unwrap().atime_ns, overlay_atime);
    }

    #[test]
    fn chown_statfs_and_symlink_surface_callbacks() {
        let (fs, backend) = new_fs_with_backend();
        write_file(&fs, "/meta.txt", b"m");

        fs.chown("/meta.txt", 11, 22).unwrap();
        let metadata = backend.metadata("meta.txt").unwrap();
        assert_eq!(metadata.uid, 11);
        assert_eq!(metadata.gid, 22);

        let statfs = fs.statfs("/").unwrap();
        assert_eq!(statfs.block_size, 4096);
        assert_eq!(statfs.name_max, 255);

        let symlink_err = fs.symlink("/target", "/link.txt").unwrap_err();
        assert_eq!(symlink_err.errno(), libc::ENOTSUP);
    }

    #[test]
    fn getattr_passthroughs_backend_metadata_for_live_writer() {
        let fs = new_fs();
        write_file(&fs, "/meta.txt", b"m");

        let handle = fs.open("/meta.txt", libc::O_WRONLY).unwrap();
        fs.chmod("/meta.txt", 0o2755).unwrap();
        fs.chown("/meta.txt", 11, 22).unwrap();

        let stat = fs.getattr("/meta.txt").unwrap();
        assert_eq!(stat.mode & 0o7777, 0o2755);
        assert_eq!(stat.uid, 11);
        assert_eq!(stat.gid, 22);

        fs.release(handle.fh).unwrap();
    }

    #[test]
    fn utimens_preserves_omitted_timestamp_field() {
        let fs = new_fs();
        write_file(&fs, "/time.txt", b"m");
        let stat_before = fs.getattr("/time.txt").unwrap();

        fs.utimens("/time.txt", Some(17), None).unwrap();
        let stat_after_atime = fs.getattr("/time.txt").unwrap();
        assert_eq!(stat_after_atime.atime_ns, 17);
        assert_eq!(stat_after_atime.mtime_ns, stat_before.mtime_ns);

        fs.utimens("/time.txt", None, Some(29)).unwrap();
        let stat_after_mtime = fs.getattr("/time.txt").unwrap();
        assert_eq!(stat_after_mtime.atime_ns, 17);
        assert_eq!(stat_after_mtime.mtime_ns, 29);
    }

    #[test]
    fn rename_locks_are_ordered_and_directory_subtree_moves() {
        let fs = new_fs();
        fs.mkdir("/srcdir", 0o755).unwrap();
        write_file(&fs, "/srcdir/file.txt", b"subtree");

        fs.rename("/srcdir", "/dstdir").unwrap();

        let stat = fs.getattr("/dstdir/file.txt").unwrap();
        assert_eq!(stat.size, 7);
        assert_eq!(read_file(&fs, "/dstdir/file.txt"), b"subtree".to_vec());
    }
}
