use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, OsStr};
use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fuser::{
    BackgroundSession, BsdFileFlags, Config, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, IoctlFlags, LockOwner, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyOpen,
    ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow,
};
use parking_lot::Mutex;

use crate::adapter::{FluxonFuseFileSystem, FuseDirEntry, FuseStat};
use crate::error::FuseAdapterError;

pub use fuser::Config as FuserConfig;
pub use fuser::MountOption as FuserMountOption;
pub use fuser::SessionACL as FuserSessionAcl;

const ROOT_INO: u64 = 1;
const ATTR_TTL: Duration = Duration::ZERO;
const DEFAULT_BLOCK_SIZE: u32 = 4096;

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct RuntimeFiemapHeader {
    fm_start: u64,
    fm_length: u64,
    fm_flags: u32,
    fm_mapped_extents: u32,
    fm_extent_count: u32,
    fm_reserved: u32,
}

#[cfg(target_os = "linux")]
const IOC_NRBITS: u32 = 8;
#[cfg(target_os = "linux")]
const IOC_TYPEBITS: u32 = 8;
#[cfg(target_os = "linux")]
const IOC_SIZEBITS: u32 = 14;
#[cfg(target_os = "linux")]
const IOC_NRSHIFT: u32 = 0;
#[cfg(target_os = "linux")]
const IOC_TYPESHIFT: u32 = IOC_NRSHIFT + IOC_NRBITS;
#[cfg(target_os = "linux")]
const IOC_SIZESHIFT: u32 = IOC_TYPESHIFT + IOC_TYPEBITS;
#[cfg(target_os = "linux")]
const IOC_DIRSHIFT: u32 = IOC_SIZESHIFT + IOC_SIZEBITS;
#[cfg(target_os = "linux")]
const IOC_WRITE: u32 = 1;
#[cfg(target_os = "linux")]
const IOC_READ: u32 = 2;

#[cfg(target_os = "linux")]
const fn ioc(dir: u32, type_: u32, nr: u32, size: u32) -> u32 {
    (dir << IOC_DIRSHIFT) | (type_ << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT) | (size << IOC_SIZESHIFT)
}

#[cfg(target_os = "linux")]
const FS_IOC_FIEMAP_CMD: u32 = ioc(
    IOC_READ | IOC_WRITE,
    b'f' as u32,
    11,
    std::mem::size_of::<RuntimeFiemapHeader>() as u32,
);

struct RuntimeBridgeState {
    next_ino: u64,
    primary_path_by_ino: BTreeMap<u64, String>,
    paths_by_ino: BTreeMap<u64, BTreeSet<String>>,
    ino_by_path: BTreeMap<String, u64>,
    detached_attr_by_ino: BTreeMap<u64, FuseStat>,
}

impl RuntimeBridgeState {
    fn new() -> Self {
        let mut primary_path_by_ino = BTreeMap::new();
        primary_path_by_ino.insert(ROOT_INO, "/".to_string());
        let mut paths_by_ino = BTreeMap::new();
        paths_by_ino.insert(ROOT_INO, BTreeSet::from(["/".to_string()]));
        let mut ino_by_path = BTreeMap::new();
        ino_by_path.insert("/".to_string(), ROOT_INO);
        Self {
            next_ino: ROOT_INO + 1,
            primary_path_by_ino,
            paths_by_ino,
            ino_by_path,
            detached_attr_by_ino: BTreeMap::new(),
        }
    }

    fn ensure_path(&mut self, callback_path: &str) -> INodeNo {
        if let Some(ino) = self.ino_by_path.get(callback_path).copied() {
            return INodeNo(ino);
        }
        let ino = self.next_ino;
        self.next_ino = self.next_ino.saturating_add(1);
        self.primary_path_by_ino
            .insert(ino, callback_path.to_string());
        self.paths_by_ino
            .insert(ino, BTreeSet::from([callback_path.to_string()]));
        self.ino_by_path.insert(callback_path.to_string(), ino);
        INodeNo(ino)
    }

    fn alias_ino(&mut self, ino: INodeNo, callback_path: &str) {
        if let Some(existing_ino) = self.ino_by_path.get(callback_path).copied() {
            if existing_ino == ino.0 {
                return;
            }
            self.remove_single_path(callback_path);
        }
        self.ino_by_path.insert(callback_path.to_string(), ino.0);
        self.paths_by_ino
            .entry(ino.0)
            .or_default()
            .insert(callback_path.to_string());
        self.primary_path_by_ino
            .entry(ino.0)
            .or_insert_with(|| callback_path.to_string());
    }

    fn path_for_ino(&self, ino: INodeNo) -> Option<&str> {
        self.primary_path_by_ino
            .get(&ino.0)
            .map(|value| value.as_str())
    }

    fn known_ino_for_path(&self, callback_path: &str) -> Option<INodeNo> {
        self.ino_by_path.get(callback_path).copied().map(INodeNo)
    }

    fn remove_single_path(&mut self, callback_path: &str) {
        let Some(ino) = self.ino_by_path.remove(callback_path) else {
            return;
        };
        let Some(paths) = self.paths_by_ino.get_mut(&ino) else {
            self.primary_path_by_ino.remove(&ino);
            return;
        };
        paths.remove(callback_path);
        if paths.is_empty() {
            self.paths_by_ino.remove(&ino);
            self.primary_path_by_ino.remove(&ino);
            return;
        }
        if matches!(
            self.primary_path_by_ino.get(&ino),
            Some(primary) if primary == callback_path
        ) {
            let next_primary = paths.iter().next().unwrap().clone();
            self.primary_path_by_ino.insert(ino, next_primary);
        }
    }

    fn remove_path_tree(&mut self, callback_path: &str) {
        let prefix = format!("{}/", callback_path.trim_end_matches('/'));
        let targets: Vec<String> = self
            .ino_by_path
            .keys()
            .filter(|path| path.as_str() == callback_path || path.starts_with(prefix.as_str()))
            .cloned()
            .collect();
        for path in targets {
            self.remove_single_path(path.as_str());
        }
    }

    fn rename_path_tree(&mut self, src_path: &str, dst_path: &str) {
        self.remove_path_tree(dst_path);
        let prefix = format!("{}/", src_path.trim_end_matches('/'));
        let mut updates: Vec<(String, String)> = self
            .ino_by_path
            .keys()
            .filter(|path| path.as_str() == src_path || path.starts_with(prefix.as_str()))
            .map(|path| {
                let suffix = &path[src_path.len()..];
                (path.clone(), format!("{dst_path}{suffix}"))
            })
            .collect();
        updates.sort_by(|a, b| a.0.len().cmp(&b.0.len()));
        for (old_path, new_path) in updates {
            let ino = self.ino_by_path.remove(old_path.as_str()).unwrap();
            self.ino_by_path.insert(new_path.clone(), ino);
            let paths = self.paths_by_ino.get_mut(&ino).unwrap();
            paths.remove(old_path.as_str());
            paths.insert(new_path.clone());
            if matches!(
                self.primary_path_by_ino.get(&ino),
                Some(primary) if primary == &old_path
            ) {
                self.primary_path_by_ino.insert(ino, new_path);
            }
        }
    }

    fn record_detached_attr(&mut self, ino: INodeNo, attr: FuseStat) {
        self.detached_attr_by_ino.insert(ino.0, attr);
    }

    fn detached_attr_for_ino(&self, ino: INodeNo) -> Option<&FuseStat> {
        if self
            .paths_by_ino
            .get(&ino.0)
            .is_some_and(|paths| !paths.is_empty())
        {
            return None;
        }
        self.detached_attr_by_ino.get(&ino.0)
    }
}

struct FluxonFuserBridge {
    inner: Arc<FluxonFuseFileSystem>,
    state: Mutex<RuntimeBridgeState>,
    host_statfs: HostStatFs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostStatFs {
    block_size: u32,
    fragment_size: u32,
    blocks: u64,
    blocks_free: u64,
    blocks_available: u64,
    files: u64,
    files_free: u64,
    name_max: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CallerIdentity {
    uid: u32,
    gid: u32,
    pid: u32,
}

impl FluxonFuserBridge {
    fn new(inner: Arc<FluxonFuseFileSystem>) -> io::Result<Self> {
        let host_statfs = read_host_statfs(inner.mount_config().mountpoint_dir_abs.as_str())?;
        Ok(Self {
            inner,
            state: Mutex::new(RuntimeBridgeState::new()),
            host_statfs,
        })
    }

    fn callback_path_for_ino(&self, ino: INodeNo) -> Result<String, fuser::Errno> {
        let state = self.state.lock();
        state
            .path_for_ino(ino)
            .map(|value| value.to_string())
            .ok_or(fuser::Errno::ENOENT)
    }

    fn known_ino_for_callback_path(&self, callback_path: &str) -> Option<INodeNo> {
        let state = self.state.lock();
        state.known_ino_for_path(callback_path)
    }

    fn callback_path_for_child(
        &self,
        parent: INodeNo,
        name: &OsStr,
    ) -> Result<String, fuser::Errno> {
        let name = name.to_str().ok_or(fuser::Errno::EINVAL)?;
        if name.contains('/') || name == "." || name == ".." {
            return Err(fuser::Errno::EINVAL);
        }
        let parent_path = self.callback_path_for_ino(parent)?;
        Ok(join_callback_path(parent_path.as_str(), name))
    }

    fn inode_for_callback_path(&self, callback_path: &str) -> INodeNo {
        let mut state = self.state.lock();
        state.ensure_path(callback_path)
    }

    fn alias_ino_for_callback_path(&self, ino: INodeNo, callback_path: &str) {
        let mut state = self.state.lock();
        state.alias_ino(ino, callback_path);
    }

    fn parent_ino_for_callback_path(&self, callback_path: &str) -> INodeNo {
        let parent_path = parent_callback_path(callback_path);
        self.inode_for_callback_path(parent_path.as_str())
    }

    fn current_attr_for_path(&self, callback_path: &str) -> Result<(INodeNo, FileAttr), fuser::Errno> {
        let stat = self
            .inner
            .getattr(callback_path)
            .map_err(errno_from_adapter_error)?;
        if !stat.exists {
            return Err(fuser::Errno::ENOENT);
        }
        let mut state = self.state.lock();
        let ino = state.ensure_path(callback_path);
        Ok((ino, file_attr_from_stat(ino, &stat)))
    }

    fn detached_attr_for_ino(&self, ino: INodeNo) -> Option<FileAttr> {
        let state = self.state.lock();
        let stat = state.detached_attr_for_ino(ino)?.clone();
        Some(file_attr_from_stat(ino, &stat))
    }

    fn record_detached_attr(&self, ino: INodeNo, stat: FuseStat) {
        let mut state = self.state.lock();
        state.record_detached_attr(ino, stat);
    }

    fn remove_path_tree(&self, callback_path: &str) {
        let mut state = self.state.lock();
        state.remove_path_tree(callback_path);
    }

    fn rename_path_tree(&self, src_path: &str, dst_path: &str) {
        let mut state = self.state.lock();
        state.rename_path_tree(src_path, dst_path);
    }

    fn open_flags_bits(flags: OpenFlags) -> i32 {
        flags.0
    }

    fn caller_identity(req: &Request) -> CallerIdentity {
        CallerIdentity {
            uid: req.uid(),
            gid: req.gid(),
            pid: req.pid(),
        }
    }

    fn current_stat_for_path(&self, callback_path: &str) -> Result<FuseStat, fuser::Errno> {
        let stat = self
            .inner
            .getattr(callback_path)
            .map_err(errno_from_adapter_error)?;
        if !stat.exists {
            return Err(fuser::Errno::ENOENT);
        }
        Ok(stat)
    }

    fn create_owner_for_parent(
        &self,
        parent: INodeNo,
        caller: CallerIdentity,
    ) -> Result<(u32, u32), fuser::Errno> {
        let parent_path = self.callback_path_for_ino(parent)?;
        let parent_stat = self.current_stat_for_path(parent_path.as_str())?;
        if !parent_stat.is_dir {
            return Err(fuser::Errno::ENOTDIR);
        }
        let gid = if parent_stat.mode & (libc::S_ISGID as u32) != 0 {
            parent_stat.gid
        } else {
            caller.gid
        };
        Ok((caller.uid, gid))
    }

    fn apply_created_owner(
        &self,
        callback_path: &str,
        owner: (u32, u32),
        nofollow: bool,
    ) -> Result<(), fuser::Errno> {
        let chown_result = if nofollow {
            self.inner.lchown(callback_path, owner.0, owner.1)
        } else {
            self.inner.chown(callback_path, owner.0, owner.1)
        };
        chown_result.map_err(errno_from_adapter_error)
    }

    fn authorize_chown(
        &self,
        caller: CallerIdentity,
        current_stat: &FuseStat,
        next_uid: u32,
        next_gid: u32,
    ) -> Result<(), fuser::Errno> {
        if caller.uid == 0 {
            return Ok(());
        }
        if current_stat.uid != caller.uid {
            return Err(fuser::Errno::EPERM);
        }
        if next_uid != current_stat.uid {
            return Err(fuser::Errno::EPERM);
        }
        if next_gid == current_stat.gid {
            return Ok(());
        }
        let groups = request_group_ids(caller)?;
        if groups.contains(&next_gid) {
            return Ok(());
        }
        Err(fuser::Errno::EPERM)
    }

    fn authorize_mode_change(
        &self,
        caller: CallerIdentity,
        current_stat: &FuseStat,
        next_mode: u32,
    ) -> Result<(), fuser::Errno> {
        if caller.uid == 0 || current_stat.uid == caller.uid {
            return Ok(());
        }
        if is_killpriv_only_mode_update(current_stat.mode, next_mode) {
            return Ok(());
        }
        Err(fuser::Errno::EPERM)
    }
}

impl Filesystem for FluxonFuserBridge {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let callback_path = match self.callback_path_for_child(parent, name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let (_ino, attr) = match self.current_attr_for_path(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.entry(&ATTR_TTL, &attr, Generation(0));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        if let Some(attr) = self.detached_attr_for_ino(ino) {
            reply.attr(&ATTR_TTL, &attr);
            return;
        }
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let (_ino, attr) = match self.current_attr_for_path(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.attr(&ATTR_TTL, &attr);
    }

    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
        ) {
        let caller = Self::caller_identity(req);
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let current_stat = if mode.is_some() || uid.is_some() || gid.is_some() {
            match self.current_stat_for_path(callback_path.as_str()) {
                Ok(value) => Some(value),
                Err(err) => {
                    reply.error(err);
                    return;
                }
            }
        } else {
            None
        };
        if let Some(mode) = mode {
            let current_stat = current_stat.as_ref().unwrap();
            let merged_mode = merge_setattr_mode(current_stat.mode, mode);
            if let Err(err) = self.authorize_mode_change(caller, current_stat, merged_mode) {
                reply.error(err);
                return;
            }
            if let Err(err) = self.inner.chmod(callback_path.as_str(), merged_mode) {
                reply.error(errno_from_adapter_error(err));
                return;
            }
        }
        if uid.is_some() || gid.is_some() {
            let current_stat = current_stat.as_ref().unwrap();
            let next_uid = uid.unwrap_or(current_stat.uid);
            let next_gid = gid.unwrap_or(current_stat.gid);
            if let Err(err) = self.authorize_chown(caller, current_stat, next_uid, next_gid) {
                reply.error(err);
                return;
            }
            let chown_result = if file_type_from_mode(current_stat.mode) == FileType::Symlink {
                self.inner.lchown(callback_path.as_str(), next_uid, next_gid)
            } else {
                self.inner.chown(callback_path.as_str(), next_uid, next_gid)
            };
            if let Err(err) = chown_result {
                reply.error(errno_from_adapter_error(err));
                return;
            }
        }
        if let Some(size) = size {
            if let Some(fh) = fh {
                match self.inner.ftruncate(fh.0, size) {
                    Ok(()) => {}
                    Err(FuseAdapterError::BadFileDescriptor { .. }) => {
                        reply.error(fuser::Errno::EINVAL);
                        return;
                    }
                    Err(err) => {
                        reply.error(errno_from_adapter_error(err));
                        return;
                    }
                }
            } else if let Err(err) = self.inner.truncate(callback_path.as_str(), size) {
                reply.error(errno_from_adapter_error(err));
                return;
            }
        }
        if atime.is_some() || mtime.is_some() {
            let atime_ns = atime.map(time_or_now_to_ns);
            let mtime_ns = mtime.map(time_or_now_to_ns);
            if let Err(err) = self
                .inner
                .utimens(callback_path.as_str(), atime_ns, mtime_ns)
            {
                reply.error(errno_from_adapter_error(err));
                return;
            }
        }
        let (_ino, attr) = match self.current_attr_for_path(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.attr(&ATTR_TTL, &attr);
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let caller = Self::caller_identity(req);
        let effective_mode = mode & !umask;
        let callback_path = match self.callback_path_for_child(parent, name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let create_owner = match self.create_owner_for_parent(parent, caller) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let create_result = match effective_mode & libc::S_IFMT {
            libc::S_IFIFO => self.inner.mkfifo(callback_path.as_str(), effective_mode),
            _ => self.inner.mknod(callback_path.as_str(), effective_mode, rdev),
        };
        if let Err(err) = create_result {
            reply.error(errno_from_adapter_error(err));
            return;
        }
        if let Err(err) = self.apply_created_owner(callback_path.as_str(), create_owner, false) {
            reply.error(err);
            return;
        }
        let (_ino, attr) = match self.current_attr_for_path(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.entry(&ATTR_TTL, &attr, Generation(0));
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let caller = Self::caller_identity(req);
        let effective_mode = mode & !umask;
        let callback_path = match self.callback_path_for_child(parent, name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let create_owner = match self.create_owner_for_parent(parent, caller) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if let Err(err) = self.inner.mkdir(callback_path.as_str(), effective_mode) {
            reply.error(errno_from_adapter_error(err));
            return;
        }
        if let Err(err) = self.apply_created_owner(callback_path.as_str(), create_owner, false) {
            reply.error(err);
            return;
        }
        let (_ino, attr) = match self.current_attr_for_path(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.entry(&ATTR_TTL, &attr, Generation(0));
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let callback_path = match self.callback_path_for_child(parent, name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let detached = self
            .known_ino_for_callback_path(callback_path.as_str())
            .map(|ino| {
                self.inner
                    .getattr(callback_path.as_str())
                    .map(|stat| (ino, stat))
                    .map_err(errno_from_adapter_error)
            })
            .transpose();
        let detached = match detached {
            Ok(value) => value.filter(|(_, stat)| stat.exists),
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.inner.unlink(callback_path.as_str()) {
            Ok(()) => {
                if let Some((ino, mut stat)) = detached {
                    stat.nlink = 0;
                    self.record_detached_attr(ino, stat);
                }
                self.remove_path_tree(callback_path.as_str());
                reply.ok();
            }
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let callback_path = match self.callback_path_for_child(parent, name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.inner.rmdir(callback_path.as_str()) {
            Ok(()) => {
                self.remove_path_tree(callback_path.as_str());
                reply.ok();
            }
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let caller = Self::caller_identity(req);
        let callback_path = match self.callback_path_for_child(parent, link_name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let create_owner = match self.create_owner_for_parent(parent, caller) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let target = match target.to_str() {
            Some(value) => value,
            None => {
                reply.error(fuser::Errno::EINVAL);
                return;
            }
        };
        if let Err(err) = self.inner.symlink(target, callback_path.as_str()) {
            reply.error(errno_from_adapter_error(err));
            return;
        }
        if let Err(err) = self.apply_created_owner(callback_path.as_str(), create_owner, true) {
            reply.error(err);
            return;
        }
        let (_ino, attr) = match self.current_attr_for_path(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.entry(&ATTR_TTL, &attr, Generation(0));
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.inner.readlink(callback_path.as_str()) {
            Ok(target) => reply.data(target.as_bytes()),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let src_path = match self.callback_path_for_child(parent, name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let dst_path = match self.callback_path_for_child(newparent, newname) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let overwritten_dst = self
            .known_ino_for_callback_path(dst_path.as_str())
            .map(|ino| {
                self.inner
                    .getattr(dst_path.as_str())
                    .map(|stat| (ino, stat))
                    .map_err(errno_from_adapter_error)
            })
            .transpose();
        let overwritten_dst = match overwritten_dst {
            Ok(value) => value.filter(|(_, stat)| stat.exists),
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self
            .inner
            .rename_with_flags(src_path.as_str(), dst_path.as_str(), flags.bits())
        {
            Ok(()) => {
                if let Some((ino, mut stat)) = overwritten_dst {
                    stat.nlink = 0;
                    self.record_detached_attr(ino, stat);
                }
                self.rename_path_tree(src_path.as_str(), dst_path.as_str());
                reply.ok();
            }
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn link(
        &self,
        _req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let src_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let dst_path = match self.callback_path_for_child(newparent, newname) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        if let Err(err) = self.inner.link(src_path.as_str(), dst_path.as_str()) {
            reply.error(errno_from_adapter_error(err));
            return;
        }
        self.alias_ino_for_callback_path(ino, dst_path.as_str());
        let (_ino, attr) = match self.current_attr_for_path(dst_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.entry(&ATTR_TTL, &attr, Generation(0));
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self
            .inner
            .open(callback_path.as_str(), Self::open_flags_bits(flags))
        {
            Ok(handle) => reply.opened(FileHandle(handle.fh), FopenFlags::empty()),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        if offset > i64::MAX as u64 {
            reply.error(fuser::Errno::EINVAL);
            return;
        }
        match self.inner.read(fh.0, size, offset as i64) {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        if offset > i64::MAX as u64 {
            reply.error(fuser::Errno::EINVAL);
            return;
        }
        match self.inner.write(fh.0, data, offset as i64) {
            Ok(written) => reply.written(written as u32),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        match self.inner.flush(fh.0) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.inner.release(fh.0) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.inner.flush(fh.0) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        match self.inner.fallocate(fh.0, mode, offset, length) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn ioctl(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: IoctlFlags,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: ReplyIoctl,
    ) {
        #[cfg(target_os = "linux")]
        {
            if cmd == FS_IOC_FIEMAP_CMD {
                match self.inner.fiemap(fh.0, in_data, out_size) {
                    Ok(data) => reply.ioctl(0, data.as_slice()),
                    Err(err) => reply.error(errno_from_adapter_error(err)),
                }
                return;
            }
        }
        reply.error(fuser::Errno::ENOSYS);
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let stat = match self.inner.getattr(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(errno_from_adapter_error(err));
                return;
            }
        };
        if !stat.exists {
            reply.error(fuser::Errno::ENOENT);
            return;
        }
        if !stat.is_dir {
            reply.error(fuser::Errno::ENOTDIR);
            return;
        }
        reply.opened(FileHandle(ino.0), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let entries = match self.inner.readdir(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(errno_from_adapter_error(err));
                return;
            }
        };
        let parent_ino = self.parent_ino_for_callback_path(callback_path.as_str());
        for (index, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            let (entry_ino, kind) = self.readdir_entry_identity(
                ino,
                parent_ino,
                callback_path.as_str(),
                &entry,
            );
            if reply.add(entry_ino, (index + 1) as u64, kind, entry.name) {
                break;
            }
        }
        reply.ok();
    }

    fn statfs(&self, _req: &Request, ino: INodeNo, reply: ReplyStatfs) {
        if self.callback_path_for_ino(ino).is_err() {
            reply.error(fuser::Errno::ENOENT);
            return;
        }
        reply.statfs(
            self.host_statfs.blocks,
            self.host_statfs.blocks_free,
            self.host_statfs.blocks_available,
            self.host_statfs.files,
            self.host_statfs.files_free,
            self.host_statfs.block_size,
            self.host_statfs.name_max,
            self.host_statfs.fragment_size,
        )
    }

    fn setxattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        if position != 0 {
            reply.error(fuser::Errno::EINVAL);
            return;
        }
        let name = match name.to_str() {
            Some(value) => value,
            None => {
                reply.error(fuser::Errno::EINVAL);
                return;
            }
        };
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self
            .inner
            .setxattr(callback_path.as_str(), name, value, flags)
        {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let name = match name.to_str() {
            Some(value) => value,
            None => {
                reply.error(fuser::Errno::EINVAL);
                return;
            }
        };
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.inner.getxattr(callback_path.as_str(), name) {
            Ok(data) => reply_xattr_data(reply, size, data.as_slice()),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.inner.listxattr(callback_path.as_str()) {
            Ok(data) => reply_xattr_data(reply, size, data.as_slice()),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name = match name.to_str() {
            Some(value) => value,
            None => {
                reply.error(fuser::Errno::EINVAL);
                return;
            }
        };
        let callback_path = match self.callback_path_for_ino(ino) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        match self.inner.removexattr(callback_path.as_str(), name) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(errno_from_adapter_error(err)),
        }
    }

    fn access(&self, _req: &Request, ino: INodeNo, _mask: fuser::AccessFlags, reply: ReplyEmpty) {
        match self.callback_path_for_ino(ino) {
            Ok(callback_path) => match self.inner.getattr(callback_path.as_str()) {
                Ok(stat) if stat.exists => reply.ok(),
                Ok(_) => reply.error(fuser::Errno::ENOENT),
                Err(err) => reply.error(errno_from_adapter_error(err)),
            },
            Err(err) => reply.error(err),
        }
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let caller = Self::caller_identity(req);
        let effective_mode = mode & !umask;
        let callback_path = match self.callback_path_for_child(parent, name) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let create_owner = match self.create_owner_for_parent(parent, caller) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        let handle = match self
            .inner
            .create_with_owner(callback_path.as_str(), flags, effective_mode, Some(create_owner))
        {
            Ok(value) => value,
            Err(err) => {
                reply.error(errno_from_adapter_error(err));
                return;
            }
        };
        let (_ino, attr) = match self.current_attr_for_path(callback_path.as_str()) {
            Ok(value) => value,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        reply.created(
            &ATTR_TTL,
            &attr,
            Generation(0),
            FileHandle(handle.fh),
            FopenFlags::empty(),
        );
    }
}

impl FluxonFuserBridge {
    fn readdir_entry_identity(
        &self,
        current_ino: INodeNo,
        parent_ino: INodeNo,
        parent_path: &str,
        entry: &FuseDirEntry,
    ) -> (INodeNo, FileType) {
        if entry.name == "." {
            return (current_ino, FileType::Directory);
        }
        if entry.name == ".." {
            return (parent_ino, FileType::Directory);
        }
        let child_path = join_callback_path(parent_path, entry.name.as_str());
        let child_ino = self.inode_for_callback_path(child_path.as_str());
        let kind = if entry.mode == 0 {
            if entry.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            }
        } else {
            file_type_from_mode(entry.mode)
        };
        (child_ino, kind)
    }
}

pub struct FluxonFuserMountHandle {
    filesystem: Arc<FluxonFuseFileSystem>,
    background_session: Option<BackgroundSession>,
}

impl FluxonFuserMountHandle {
    pub fn mountpoint_dir_abs(&self) -> &str {
        self.filesystem.mount_config().mountpoint_dir_abs.as_str()
    }

    pub fn wait_until_mounted(&self, timeout: Duration) -> io::Result<()> {
        wait_for_mount_state(self.mountpoint_dir_abs(), true, timeout)
    }

    pub fn wait_until_unmounted(&self, timeout: Duration) -> io::Result<()> {
        wait_for_mount_state(self.mountpoint_dir_abs(), false, timeout)
    }

    pub fn umount_and_join(&mut self, force: bool, timeout: Duration) -> io::Result<()> {
        self.filesystem
            .umount(force, timeout)
            .map_err(io_error_from_adapter_error)?;
        let session = self.background_session.take().unwrap();
        session.umount_and_join()?;
        self.wait_until_unmounted(timeout)
    }
}

pub fn spawn_fuser_mount(
    filesystem: Arc<FluxonFuseFileSystem>,
    config: Config,
) -> io::Result<FluxonFuserMountHandle> {
    let bridge = FluxonFuserBridge::new(filesystem.clone())?;
    let background_session =
        fuser::spawn_mount2(bridge, filesystem.mount_config().mountpoint_dir_abs.as_str(), &config)?;
    Ok(FluxonFuserMountHandle {
        filesystem,
        background_session: Some(background_session),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FluxonPjdfstestConfig {
    pub suite_root_dir_abs: String,
    pub test_targets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FluxonXfstestsConfig {
    pub suite_root_dir_abs: String,
    pub host_options_path_abs: String,
    pub extra_exclude_tests_path_abs: String,
    pub test_targets: Vec<String>,
}

pub fn run_pjdfstest(
    mountpoint_dir_abs: &str,
    config: &FluxonPjdfstestConfig,
) -> io::Result<()> {
    if !Path::new(mountpoint_dir_abs).is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("mountpoint_dir_abs must be absolute: {mountpoint_dir_abs}"),
        ));
    }
    if !Path::new(config.suite_root_dir_abs.as_str()).is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "suite_root_dir_abs must be absolute: {}",
                config.suite_root_dir_abs
            ),
        ));
    }
    if config.test_targets.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "test_targets must be non-empty",
        ));
    }
    let mut command = Command::new("prove");
    command.arg("-rv");
    for target in &config.test_targets {
        command.arg(format!(
            "{}/tests/{}",
            config.suite_root_dir_abs.trim_end_matches('/'),
            target
        ));
    }
    let status = command.current_dir(mountpoint_dir_abs).status()?;
    if status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "pjdfstest exited unsuccessfully: {status}"
    )))
}

pub fn run_xfstests(config: &FluxonXfstestsConfig) -> io::Result<()> {
    if !Path::new(config.suite_root_dir_abs.as_str()).is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "suite_root_dir_abs must be absolute: {}",
                config.suite_root_dir_abs
            ),
        ));
    }
    if !Path::new(config.host_options_path_abs.as_str()).is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "host_options_path_abs must be absolute: {}",
                config.host_options_path_abs
            ),
        ));
    }
    if !Path::new(config.extra_exclude_tests_path_abs.as_str()).is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "extra_exclude_tests_path_abs must be absolute: {}",
                config.extra_exclude_tests_path_abs
            ),
        ));
    }
    if config.test_targets.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "test_targets must be non-empty",
        ));
    }
    let mut command = Command::new("./check");
    command.arg("-fuse");
    command.arg("-x");
    command.arg("acl");
    command.arg("-E");
    command.arg(config.extra_exclude_tests_path_abs.as_str());
    for target in &config.test_targets {
        command.arg(target);
    }
    command.env("CHECK_OPTIONS", "");
    command.env("HOST_OPTIONS", config.host_options_path_abs.as_str());
    let status = command.current_dir(config.suite_root_dir_abs.as_str()).status()?;
    if status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "xfstests exited unsuccessfully: {status}"
    )))
}

fn errno_from_adapter_error(err: FuseAdapterError) -> fuser::Errno {
    fuser::Errno::from_i32(err.errno())
}

fn reply_xattr_data(reply: ReplyXattr, size: u32, data: &[u8]) {
    let data_len = match u32::try_from(data.len()) {
        Ok(value) => value,
        Err(_) => {
            reply.error(fuser::Errno::EOVERFLOW);
            return;
        }
    };
    if size == 0 {
        reply.size(data_len);
        return;
    }
    if size < data_len {
        reply.error(fuser::Errno::ERANGE);
        return;
    }
    reply.data(data);
}

fn read_host_statfs(path: &str) -> io::Result<HostStatFs> {
    let c_path = CString::new(path)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount path contains NUL"))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok(HostStatFs {
        block_size: stat.f_bsize as u32,
        fragment_size: stat.f_frsize as u32,
        blocks: stat.f_blocks,
        blocks_free: stat.f_bfree,
        blocks_available: stat.f_bavail,
        files: stat.f_files,
        files_free: stat.f_ffree,
        name_max: stat.f_namemax as u32,
    })
}

fn io_error_from_adapter_error(err: FuseAdapterError) -> io::Error {
    io::Error::from_raw_os_error(err.errno())
}

fn io_error_to_errno(err: io::Error) -> fuser::Errno {
    fuser::Errno::from_i32(err.raw_os_error().unwrap_or(libc::EIO))
}

fn request_group_ids(caller: CallerIdentity) -> Result<BTreeSet<u32>, fuser::Errno> {
    if caller.pid == 0 {
        return Ok(BTreeSet::from([caller.gid]));
    }
    let status = fs::read_to_string(format!("/proc/{}/status", caller.pid))
        .map_err(io_error_to_errno)?;
    let groups_line = status
        .lines()
        .find(|line| line.starts_with("Groups:"))
        .ok_or(fuser::Errno::EIO)?;
    let mut out = BTreeSet::from([caller.gid]);
    for token in groups_line["Groups:".len()..].split_whitespace() {
        let gid = token.parse::<u32>().map_err(|_| fuser::Errno::EIO)?;
        out.insert(gid);
    }
    Ok(out)
}

fn file_attr_from_stat(ino: INodeNo, stat: &FuseStat) -> FileAttr {
    let kind = file_type_from_mode(stat.mode);
    let ctime = system_time_from_ns(stat.ctime_ns);
    let atime = system_time_from_ns(stat.atime_ns);
    let mtime = system_time_from_ns(stat.mtime_ns);
    FileAttr {
        ino,
        size: stat.size,
        blocks: stat.size.div_ceil(512),
        atime,
        mtime,
        ctime,
        crtime: ctime,
        kind,
        perm: (stat.mode & 0o7777) as u16,
        nlink: stat.nlink.min(u32::MAX as u64) as u32,
        uid: stat.uid,
        gid: stat.gid,
        rdev: stat.rdev,
        blksize: DEFAULT_BLOCK_SIZE,
        flags: 0,
    }
}

fn merge_setattr_mode(current_mode: u32, requested_mode: u32) -> u32 {
    (current_mode & libc::S_IFMT) | (requested_mode & 0o7777)
}

fn is_killpriv_only_mode_update(current_mode: u32, next_mode: u32) -> bool {
    if current_mode & libc::S_IFMT != libc::S_IFREG {
        return false;
    }
    if current_mode & 0o6000 == 0 {
        return false;
    }
    next_mode == (current_mode & !0o6000)
}

fn file_type_from_mode(mode: u32) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn system_time_from_ns(value: i64) -> SystemTime {
    if value <= 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH
        .checked_add(Duration::from_nanos(value as u64))
        .unwrap_or(UNIX_EPOCH)
}

fn time_or_now_to_ns(value: TimeOrNow) -> i64 {
    match value {
        TimeOrNow::SpecificTime(value) => system_time_to_ns(value),
        TimeOrNow::Now => system_time_to_ns(SystemTime::now()),
    }
}

fn system_time_to_ns(value: SystemTime) -> i64 {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos().min(i64::MAX as u128) as i64,
        Err(_) => 0,
    }
}

fn join_callback_path(parent_path: &str, name: &str) -> String {
    if parent_path == "/" {
        return format!("/{name}");
    }
    format!("{parent_path}/{name}")
}

fn parent_callback_path(callback_path: &str) -> String {
    if callback_path == "/" {
        return "/".to_string();
    }
    match callback_path.rsplit_once('/') {
        Some(("", _)) | None => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
    }
}

fn wait_for_mount_state(
    mountpoint_dir_abs: &str,
    mounted: bool,
    timeout: Duration,
) -> io::Result<()> {
    let start = Instant::now();
    loop {
        if mountinfo_contains(mountpoint_dir_abs)? == mounted {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for mountpoint={} mounted={mounted}",
                    mountpoint_dir_abs
                ),
            ));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn mountinfo_contains(mountpoint_dir_abs: &str) -> io::Result<bool> {
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")?;
    Ok(mountinfo.lines().any(|line| {
        let mut parts = line.split(' ');
        let _mount_id = parts.next();
        let _parent_id = parts.next();
        let _major_minor = parts.next();
        let _root = parts.next();
        matches!(parts.next(), Some(value) if value == mountpoint_dir_abs)
    }))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::FileExt;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use fuser::{Config, MountOption};

    use crate::adapter::{FluxonFuseFileSystem, FluxonFuseMountConfig};
    use crate::backend::FluxonRpcKvExportBackend;
    #[cfg(feature = "fsagent_backend")]
    use crate::backend::FluxonFsAgentBackend;
    use crate::fluxon_rpc_kv::{FluxonInProcessFsExportMock, FluxonInProcessRpcKvApi};
    #[cfg(feature = "fsagent_backend")]
    use fluxon_fs::{
        agent::FluxonFsAgent, config::FS_EXPORT_DEFAULT_CACHE_MAX_BYTES_V1, new_fs_framework,
    };
    #[cfg(feature = "fsagent_backend")]
    use tokio::runtime::Runtime;

    use super::{
        FluxonPjdfstestConfig, FluxonXfstestsConfig, RuntimeBridgeState, file_attr_from_stat,
        join_callback_path, parent_callback_path, run_pjdfstest, run_xfstests,
        spawn_fuser_mount,
    };

    struct TestDir {
        path: String,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "fluxon_fs_fuser_runtime_{}_{}_{}",
                prefix,
                std::process::id(),
                nanos
            ));
            fs::create_dir_all(&path).unwrap();
            Self {
                path: path.to_string_lossy().to_string(),
            }
        }

        fn join(&self, child: &str) -> String {
            format!("{}/{}", self.path, child)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn new_test_filesystem(
        export_root_dir_abs: String,
        mountpoint_dir_abs: String,
    ) -> Arc<FluxonFuseFileSystem> {
        let api = FluxonInProcessRpcKvApi::new();
        let _mock =
            FluxonInProcessFsExportMock::new(api.clone(), "demo".to_string(), export_root_dir_abs)
                .unwrap();
        let backend = Arc::new(
            FluxonRpcKvExportBackend::new(
                api.rpc_client(),
                mountpoint_dir_abs.clone(),
                "demo".to_string(),
                None,
            )
            .unwrap(),
        );
        Arc::new(
            FluxonFuseFileSystem::new(
                FluxonFuseMountConfig {
                    mountpoint_dir_abs,
                    export_name: "demo".to_string(),
                    semantics: crate::adapter::FluxonFuseSemantics {
                        read_only: false,
                        suid_enabled: true,
                        dev_enabled: true,
                        exec_enabled: true,
                        atime_policy: crate::adapter::FluxonFuseAtimePolicy::RelAtime,
                        dir_atime_enabled: true,
                    },
                },
                backend,
            )
            .unwrap(),
        )
    }

    #[cfg(feature = "fsagent_backend")]
    struct FsAgentTestFixture {
        _export_mock: FluxonInProcessFsExportMock,
        _runtime: Runtime,
        agent: Arc<FluxonFsAgent>,
    }

    #[cfg(feature = "fsagent_backend")]
    fn build_test_fsagent_cache_yaml(export_name: &str, export_root_dir_abs: &str) -> String {
        let export_name_json = serde_json::to_string(export_name).unwrap();
        let export_root_dir_abs_json = serde_json::to_string(export_root_dir_abs).unwrap();
        let node_id_json = serde_json::to_string("mock-node").unwrap();
        format!(
            "stale_window_ms: 1000\nrules: []\nexports:\n  {export_name_json}:\n    remote_root_dir_abs: {export_root_dir_abs_json}\n    nodes:\n      - {node_id_json}\n    cache_max_bytes: {FS_EXPORT_DEFAULT_CACHE_MAX_BYTES_V1}\n"
        )
    }

    #[cfg(feature = "fsagent_backend")]
    fn new_test_fsagent_filesystem(
        export_root_dir_abs: String,
        mountpoint_dir_abs: String,
        fsagent_mount_dir_abs: String,
    ) -> (Arc<FluxonFuseFileSystem>, FsAgentTestFixture) {
        let api = FluxonInProcessRpcKvApi::new();
        let export_mock =
            FluxonInProcessFsExportMock::new(api.clone(), "demo".to_string(), export_root_dir_abs)
                .unwrap();
        let runtime = Runtime::new().unwrap();
        let agent = runtime.block_on(async {
            let lifecycle = new_fs_framework("runtime_fuser_fsagent_test".to_string());
            let agent = Arc::new(FluxonFsAgent::new_with_rpc_kv(
                lifecycle,
                Arc::new(api.clone()),
            ));
            agent
                .set_cache_config_yaml(
                    build_test_fsagent_cache_yaml("demo", export_mock.export_root_dir_abs()).as_str(),
                )
                .unwrap();
            agent.mount_remote_dir(fsagent_mount_dir_abs.as_str(), "demo").unwrap();
            agent
        });
        let filesystem = Arc::new(
            FluxonFuseFileSystem::new_with_fsagent(
                FluxonFuseMountConfig {
                    mountpoint_dir_abs,
                    export_name: "demo".to_string(),
                    semantics: crate::adapter::FluxonFuseSemantics {
                        read_only: false,
                        suid_enabled: true,
                        dev_enabled: true,
                        exec_enabled: true,
                        atime_policy: crate::adapter::FluxonFuseAtimePolicy::RelAtime,
                        dir_atime_enabled: true,
                    },
                },
                fsagent_mount_dir_abs,
                agent.clone(),
            )
            .unwrap(),
        );
        (
            filesystem,
            FsAgentTestFixture {
                _export_mock: export_mock,
                _runtime: runtime,
                agent,
            },
        )
    }

    #[test]
    fn runtime_state_moves_and_removes_subtrees() {
        let mut state = RuntimeBridgeState::new();
        state.ensure_path("/dir");
        state.ensure_path("/dir/file.txt");
        state.rename_path_tree("/dir", "/dst");
        assert!(state.ino_by_path.contains_key("/dst"));
        assert!(state.ino_by_path.contains_key("/dst/file.txt"));
        state.remove_path_tree("/dst");
        assert!(!state.ino_by_path.contains_key("/dst"));
        assert!(!state.ino_by_path.contains_key("/dst/file.txt"));
    }

    #[test]
    fn runtime_state_keeps_detached_attr_after_path_removal() {
        let mut state = RuntimeBridgeState::new();
        let ino = state.ensure_path("/dst.txt");
        state.record_detached_attr(
            ino,
            crate::adapter::FuseStat {
                exists: true,
                is_file: true,
                is_dir: false,
                size: 1,
                ctime_ns: 1,
                atime_ns: 1,
                mtime_ns: 1,
                mode: 0o644,
                uid: 0,
                gid: 0,
                nlink: 0,
                ino: ino.0,
                rdev: 0,
            },
        );
        state.remove_path_tree("/dst.txt");

        assert!(state.path_for_ino(ino).is_none());
        assert_eq!(state.detached_attr_for_ino(ino).unwrap().nlink, 0);
    }

    #[test]
    fn path_helpers_keep_root_stable() {
        assert_eq!(join_callback_path("/", "file.txt"), "/file.txt");
        assert_eq!(join_callback_path("/dir", "file.txt"), "/dir/file.txt");
        assert_eq!(parent_callback_path("/"), "/");
        assert_eq!(parent_callback_path("/child"), "/");
        assert_eq!(parent_callback_path("/dir/file.txt"), "/dir");
    }

    #[test]
    fn pjdfstest_runner_requires_non_empty_targets() {
        let err = run_pjdfstest(
            "/tmp",
            &FluxonPjdfstestConfig {
                suite_root_dir_abs: "/tmp/pjdfstest".to_string(),
                test_targets: Vec::new(),
            },
        )
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn xfstests_runner_requires_non_empty_targets() {
        let err = run_xfstests(&FluxonXfstestsConfig {
            suite_root_dir_abs: "/tmp/xfstests".to_string(),
            host_options_path_abs: "/tmp/xfstests.local.config".to_string(),
            extra_exclude_tests_path_abs: "/tmp/xfstests.exclude".to_string(),
            test_targets: Vec::new(),
        })
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn xfstests_runner_requires_absolute_host_options_path() {
        let err = run_xfstests(&FluxonXfstestsConfig {
            suite_root_dir_abs: "/tmp/xfstests".to_string(),
            host_options_path_abs: "xfstests.local.config".to_string(),
            extra_exclude_tests_path_abs: "/tmp/xfstests.exclude".to_string(),
            test_targets: vec!["generic/001".to_string()],
        })
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn file_attr_uses_distinct_atime_and_mtime() {
        let attr = file_attr_from_stat(
            super::INodeNo(super::ROOT_INO),
            &crate::adapter::FuseStat {
                exists: true,
                is_file: true,
                is_dir: false,
                size: 3,
                ctime_ns: 7,
                atime_ns: 11,
                mtime_ns: 29,
                mode: 0o2644,
                uid: 7,
                gid: 9,
                nlink: 3,
                ino: super::ROOT_INO,
                rdev: 0,
            },
        );

        assert_eq!(attr.ctime, UNIX_EPOCH + Duration::from_nanos(7));
        assert_eq!(attr.crtime, UNIX_EPOCH + Duration::from_nanos(7));
        assert_eq!(attr.atime, UNIX_EPOCH + Duration::from_nanos(11));
        assert_eq!(attr.mtime, UNIX_EPOCH + Duration::from_nanos(29));
        assert_eq!(attr.perm, 0o2644);
        assert_eq!(attr.nlink, 3);
        assert_eq!(attr.uid, 7);
        assert_eq!(attr.gid, 9);
    }

    #[test]
    fn root_chmod_round_trips_through_runtime_filesystem() {
        let temp = TestDir::new("root_chmod");
        let export_root_dir_abs = temp.join("export");
        let mountpoint_dir_abs = temp.join("mount");
        fs::create_dir_all(&export_root_dir_abs).unwrap();
        fs::create_dir_all(&mountpoint_dir_abs).unwrap();

        let filesystem = new_test_filesystem(export_root_dir_abs, mountpoint_dir_abs);
        filesystem.chmod("/", 0o777).unwrap();

        let stat = filesystem.getattr("/").unwrap();
        assert_eq!(stat.mode & 0o7777, 0o777);
    }

    #[cfg(feature = "fsagent_backend")]
    #[test]
    fn fsagent_root_chmod_preserves_directory_type_bits() {
        let temp = TestDir::new("fsagent_root_chmod");
        let export_root_dir_abs = temp.join("export");
        let mountpoint_dir_abs = temp.join("mount");
        let fsagent_mount_dir_abs = temp.join("fsagent_mount");
        fs::create_dir_all(&export_root_dir_abs).unwrap();
        fs::create_dir_all(&mountpoint_dir_abs).unwrap();
        fs::create_dir_all(&fsagent_mount_dir_abs).unwrap();

        let (filesystem, _fixture) = new_test_fsagent_filesystem(
            export_root_dir_abs.clone(),
            mountpoint_dir_abs,
            fsagent_mount_dir_abs,
        );
        let before = filesystem.getattr("/").unwrap();
        assert_ne!(before.mode & libc::S_IFMT, 0);

        filesystem.chmod("/", libc::S_IFDIR | 0o777).unwrap();

        let after = filesystem.getattr("/").unwrap();
        assert_eq!(after.mode & libc::S_IFMT, libc::S_IFDIR);
        assert_eq!(after.mode & 0o7777, 0o777);
    }

    #[test]
    fn setattr_mode_preserves_existing_file_type_bits() {
        assert_eq!(
            super::merge_setattr_mode(libc::S_IFDIR | 0o755, 0o2777),
            libc::S_IFDIR | 0o2777
        );
        assert_eq!(
            super::merge_setattr_mode(libc::S_IFREG | 0o644, 0o600),
            libc::S_IFREG | 0o600
        );
    }

    #[test]
    #[ignore]
    fn mount_round_trips_real_fuse_calls() {
        let temp = TestDir::new("mount");
        let export_root_dir_abs = temp.join("export");
        let mountpoint_dir_abs = temp.join("mount");
        fs::create_dir_all(&export_root_dir_abs).unwrap();
        fs::create_dir_all(&mountpoint_dir_abs).unwrap();

        let filesystem = new_test_filesystem(export_root_dir_abs.clone(), mountpoint_dir_abs.clone());
        let mut config = Config::default();
        config.mount_options = vec![
            MountOption::FSName("fluxon_fuse_draft".to_string()),
            MountOption::Subtype("fluxonfs".to_string()),
            MountOption::RW,
            MountOption::DefaultPermissions,
            MountOption::AutoUnmount,
        ];
        config.acl = super::FuserSessionAcl::All;
        config.n_threads = Some(1);
        config.clone_fd = false;
        let mut mount = spawn_fuser_mount(filesystem, config).unwrap();
        mount.wait_until_mounted(Duration::from_secs(3)).unwrap();

        fs::set_permissions(&mountpoint_dir_abs, fs::Permissions::from_mode(0o777)).unwrap();
        let root_mode = fs::metadata(&mountpoint_dir_abs).unwrap().permissions().mode();
        assert_eq!(root_mode & 0o7777, 0o777);

        fs::create_dir(format!("{}/dir", mountpoint_dir_abs)).unwrap();
        let file_path = format!("{}/dir/file.txt", mountpoint_dir_abs);
        let export_file_path = format!("{}/dir/file.txt", export_root_dir_abs);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path.as_str())
            .unwrap();
        file.write_all(b"hello").unwrap();
        assert!(!Path::new(export_file_path.as_str()).exists());
        drop(file);
        assert_eq!(fs::read(export_file_path.as_str()).unwrap(), b"hello");

        fs::rename(
            format!("{}/dir/file.txt", mountpoint_dir_abs),
            format!("{}/dir/renamed.txt", mountpoint_dir_abs),
        )
        .unwrap();
        assert_eq!(
            fs::read(format!("{}/dir/renamed.txt", export_root_dir_abs)).unwrap(),
            b"hello"
        );
        fs::remove_file(format!("{}/dir/renamed.txt", mountpoint_dir_abs)).unwrap();
        fs::remove_dir(format!("{}/dir", mountpoint_dir_abs)).unwrap();

        mount.umount_and_join(false, Duration::from_secs(3)).unwrap();
    }

    #[test]
    #[ignore]
    fn mount_supports_large_sparse_reopen_read_and_deleted_open_read() {
        let temp = TestDir::new("mount_sparse_deleted");
        let export_root_dir_abs = temp.join("export");
        let mountpoint_dir_abs = temp.join("mount");
        fs::create_dir_all(&export_root_dir_abs).unwrap();
        fs::create_dir_all(&mountpoint_dir_abs).unwrap();

        let filesystem = new_test_filesystem(export_root_dir_abs.clone(), mountpoint_dir_abs.clone());
        let mut config = Config::default();
        config.mount_options = vec![
            MountOption::FSName("fluxon_fuse_draft".to_string()),
            MountOption::Subtype("fluxonfs".to_string()),
            MountOption::RW,
            MountOption::DefaultPermissions,
            MountOption::AutoUnmount,
        ];
        config.acl = super::FuserSessionAcl::All;
        config.n_threads = Some(1);
        config.clone_fd = false;
        let mut mount = spawn_fuser_mount(filesystem, config).unwrap();
        mount.wait_until_mounted(Duration::from_secs(3)).unwrap();

        let sparse_path = format!("{}/large.txt", mountpoint_dir_abs);
        let sparse_writer = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(sparse_path.as_str())
            .unwrap();
        sparse_writer
            .write_at(b"a", 2 * 1024 * 1024 * 1024 + 1)
            .unwrap();
        drop(sparse_writer);
        let sparse_reader = fs::File::open(sparse_path.as_str()).unwrap();
        let mut sparse_buf = [0u8; 1];
        let read_count = sparse_reader
            .read_at(&mut sparse_buf, 2 * 1024 * 1024 * 1024 + 1)
            .unwrap();
        assert_eq!(read_count, 1);
        assert_eq!(sparse_buf, *b"a");
        fs::remove_file(sparse_path.as_str()).unwrap();

        let deleted_path = format!("{}/deleted.txt", mountpoint_dir_abs);
        let _created = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(deleted_path.as_str())
            .unwrap();
        drop(_created);
        let deleted_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(deleted_path.as_str())
            .unwrap();
        deleted_file.write_at(b"Hello,_World!", 0).unwrap();
        fs::remove_file(deleted_path.as_str()).unwrap();
        let mut deleted_buf = [0u8; 13];
        let deleted_read = deleted_file.read_at(&mut deleted_buf, 0).unwrap();
        assert_eq!(deleted_read, 13);
        assert_eq!(&deleted_buf, b"Hello,_World!");

        mount.umount_and_join(false, Duration::from_secs(3)).unwrap();
    }
}
