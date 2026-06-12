use std::borrow::Cow;
use std::sync::Arc;

use fluxon_fs_core::config::{
    FluxonFsExportRpcPaths, FluxonFsRequestIdentity, export_fallocate_rpc_path_for_export_name_v1,
    export_fiemap_rpc_path_for_export_name_v1, export_rpc_paths_for_export_name_v1,
};
use thiserror::Error;

use crate::error::FuseAdapterError;
use crate::fluxon_rpc_kv::{
    FLUXON_FS_RPC_ERR_KIND_KEY, FlatDict, FlatValue, FluxonFsRpcErrorKind, UserRpcClient,
};
use crate::path_projection::FuseMountPathProjection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseBackendStat {
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
pub struct FuseBackendDirEntry {
    pub name: String,
    pub is_file: bool,
    pub is_dir: bool,
    pub mode: u32,
    pub ino: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuseBackendStatFs {
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

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FuseBackendError {
    #[error("invalid argument: {detail}")]
    InvalidArgument { detail: String },

    #[error("not found: path={path}")]
    NotFound { path: String },

    #[error("already exists: path={path}")]
    AlreadyExists { path: String },

    #[error("is a directory: path={path}")]
    IsDirectory { path: String },

    #[error("not a directory: path={path}")]
    NotDirectory { path: String },

    #[error("access denied: path={path} detail={detail}")]
    AccessDenied { path: String, detail: String },

    #[error("not implemented: op={op} detail={detail}")]
    NotImplemented { op: &'static str, detail: String },

    #[error("os error: errno={errno} path={path} detail={detail}")]
    Os {
        errno: i32,
        path: String,
        detail: String,
    },
}

impl FuseBackendError {
    pub fn into_adapter(self) -> FuseAdapterError {
        match self {
            Self::InvalidArgument { detail } => FuseAdapterError::InvalidArgument { detail },
            Self::NotFound { path } => FuseAdapterError::NotFound { path },
            Self::AlreadyExists { path } => FuseAdapterError::AlreadyExists { path },
            Self::IsDirectory { path } => FuseAdapterError::IsDirectory { path },
            Self::NotDirectory { path } => FuseAdapterError::NotDirectory { path },
            Self::AccessDenied { path, detail } => FuseAdapterError::AccessDenied { path, detail },
            Self::NotImplemented { op, detail } => FuseAdapterError::NotImplemented { op, detail },
            Self::Os {
                errno,
                path,
                detail,
            } => FuseAdapterError::Os {
                errno,
                path,
                detail,
            },
        }
    }
}

pub trait FuseExportBackend: Send + Sync {
    fn stat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError>;
    fn lstat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError>;
    fn list_dir(&self, relpath: &str) -> Result<Vec<FuseBackendDirEntry>, FuseBackendError>;
    fn readlink(&self, relpath: &str) -> Result<String, FuseBackendError>;
    fn setxattr(
        &self,
        relpath: &str,
        name: &str,
        value: Vec<u8>,
        flags: i32,
    ) -> Result<(), FuseBackendError> {
        let _ = (relpath, name, value, flags);
        Err(FuseBackendError::NotImplemented {
            op: "setxattr",
            detail: "backend does not implement setxattr".to_string(),
        })
    }
    fn getxattr(&self, relpath: &str, name: &str) -> Result<Vec<u8>, FuseBackendError> {
        let _ = (relpath, name);
        Err(FuseBackendError::NotImplemented {
            op: "getxattr",
            detail: "backend does not implement getxattr".to_string(),
        })
    }
    fn listxattr(&self, relpath: &str) -> Result<Vec<u8>, FuseBackendError> {
        let _ = relpath;
        Err(FuseBackendError::NotImplemented {
            op: "listxattr",
            detail: "backend does not implement listxattr".to_string(),
        })
    }
    fn removexattr(&self, relpath: &str, name: &str) -> Result<(), FuseBackendError> {
        let _ = (relpath, name);
        Err(FuseBackendError::NotImplemented {
            op: "removexattr",
            detail: "backend does not implement removexattr".to_string(),
        })
    }
    fn read_committed(
        &self,
        relpath: &str,
        offset: u64,
        size: u32,
        committed_size: u64,
        committed_mtime_ns: i64,
    ) -> Result<Vec<u8>, FuseBackendError>;
    fn write_chunk(&self, relpath: &str, offset: u64, data: Vec<u8>) -> Result<(), FuseBackendError>;
    fn truncate(&self, relpath: &str, size: u64) -> Result<(), FuseBackendError>;
    fn fallocate(
        &self,
        relpath: &str,
        mode: i32,
        offset: u64,
        length: u64,
    ) -> Result<(), FuseBackendError> {
        let _ = (relpath, mode, offset, length);
        Err(FuseBackendError::NotImplemented {
            op: "fallocate",
            detail: "backend does not implement fallocate".to_string(),
        })
    }
    fn fiemap(
        &self,
        relpath: &str,
        request: &[u8],
        out_size: u32,
    ) -> Result<Vec<u8>, FuseBackendError> {
        let _ = (relpath, request, out_size);
        Err(FuseBackendError::NotImplemented {
            op: "fiemap",
            detail: "backend does not implement fiemap".to_string(),
        })
    }
    fn mkdir(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError>;
    fn mkfifo(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError>;
    fn mknod(&self, relpath: &str, mode: u32, rdev: u32) -> Result<(), FuseBackendError>;
    fn rmdir(&self, relpath: &str) -> Result<(), FuseBackendError>;
    fn unlink(&self, relpath: &str) -> Result<(), FuseBackendError>;
    fn link(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError>;
    fn rename(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError>;
    fn chmod(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError>;
    fn chown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError>;
    fn lchown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError>;
    fn utimens(
        &self,
        relpath: &str,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<(), FuseBackendError>;
    fn lutimens(
        &self,
        relpath: &str,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<(), FuseBackendError> {
        self.utimens(relpath, atime_ns, mtime_ns)
    }
    fn symlink(&self, linkname: &str, relpath: &str) -> Result<(), FuseBackendError>;
    fn statfs(&self, relpath: &str) -> Result<FuseBackendStatFs, FuseBackendError>;
}

#[cfg(feature = "fsagent_backend")]
pub struct FluxonFsAgentBackend {
    agent: Arc<fluxon_fs::agent::FluxonFsAgent>,
    path_projection: FuseMountPathProjection,
}

#[cfg(feature = "fsagent_backend")]
impl FluxonFsAgentBackend {
    pub fn new(
        agent: Arc<fluxon_fs::agent::FluxonFsAgent>,
        mountpoint_dir_abs: String,
    ) -> Result<Self, FuseAdapterError> {
        Ok(Self {
            agent,
            path_projection: FuseMountPathProjection::new(mountpoint_dir_abs)?,
        })
    }

    fn path_for_relpath(&self, relpath: &str) -> Result<String, FuseBackendError> {
        self.path_projection
            .abs_path_for_relpath(relpath)
            .map_err(|err| FuseBackendError::InvalidArgument {
                detail: err.to_string(),
            })
    }
}

#[cfg(feature = "fsagent_backend")]
impl FuseExportBackend for FluxonFsAgentBackend {
    fn stat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        let stat = self.agent.path_stat(file_abs.as_str()).map_err(fsagent_err_to_backend)?;
        Ok(remote_stat_to_backend(stat))
    }

    fn lstat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        let stat = self
            .agent
            .path_lstat(file_abs.as_str())
            .map_err(fsagent_err_to_backend)?;
        Ok(remote_stat_to_backend(stat))
    }

    fn list_dir(&self, relpath: &str) -> Result<Vec<FuseBackendDirEntry>, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        let entries = self
            .agent
            .path_list_dir(file_abs.as_str())
            .map_err(fsagent_err_to_backend)?;
        Ok(entries
            .into_iter()
            .map(|entry| FuseBackendDirEntry {
                name: entry.name,
                is_file: entry.is_file,
                is_dir: entry.is_dir,
                mode: entry.mode.max(0) as u32,
                ino: entry.ino.max(0) as u64,
            })
            .collect())
    }

    fn readlink(&self, relpath: &str) -> Result<String, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_readlink(file_abs.as_str())
            .map_err(fsagent_err_to_backend)
    }

    fn setxattr(
        &self,
        relpath: &str,
        name: &str,
        value: Vec<u8>,
        flags: i32,
    ) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_setxattr(file_abs.as_str(), name, value, i64::from(flags))
            .map_err(fsagent_err_to_backend)
    }

    fn getxattr(&self, relpath: &str, name: &str) -> Result<Vec<u8>, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_getxattr(file_abs.as_str(), name)
            .map_err(fsagent_err_to_backend)
    }

    fn listxattr(&self, relpath: &str) -> Result<Vec<u8>, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_listxattr(file_abs.as_str())
            .map_err(fsagent_err_to_backend)
    }

    fn removexattr(&self, relpath: &str, name: &str) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_removexattr(file_abs.as_str(), name)
            .map_err(fsagent_err_to_backend)
    }

    fn read_committed(
        &self,
        relpath: &str,
        offset: u64,
        size: u32,
        _committed_size: u64,
        _committed_mtime_ns: i64,
    ) -> Result<Vec<u8>, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        let session = self
            .agent
            .open_file_session_with_options(
                file_abs.as_str(),
                fluxon_fs::agent::FluxonFsFileSessionOpenOptions {
                    readable: true,
                    writable: false,
                    create: false,
                    create_new: false,
                    truncate: false,
                    append: false,
                    create_mode: None,
                    create_uid: None,
                    create_gid: None,
                },
            )
            .map_err(fsagent_err_to_backend)?;
        let bytes = session
            .read(
                i64::try_from(offset).map_err(|_| FuseBackendError::InvalidArgument {
                    detail: format!("read offset out of range: {}", offset),
                })?,
                i64::from(size),
            )
            .map_err(fsagent_err_to_backend)?;
        session.close().map_err(fsagent_err_to_backend)?;
        Ok(bytes)
    }

    fn write_chunk(&self, relpath: &str, offset: u64, data: Vec<u8>) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        let session = self
            .agent
            .open_file_session_with_options(
                file_abs.as_str(),
                fluxon_fs::agent::FluxonFsFileSessionOpenOptions {
                    readable: false,
                    writable: true,
                    create: true,
                    create_new: false,
                    truncate: false,
                    append: false,
                    create_mode: None,
                    create_uid: None,
                    create_gid: None,
                },
            )
            .map_err(fsagent_err_to_backend)?;
        session
            .write(
                i64::try_from(offset).map_err(|_| FuseBackendError::InvalidArgument {
                    detail: format!("write offset out of range: {}", offset),
                })?,
                data.as_slice(),
            )
            .map_err(fsagent_err_to_backend)?;
        session.flush().map_err(fsagent_err_to_backend)?;
        session.close().map_err(fsagent_err_to_backend)?;
        Ok(())
    }

    fn truncate(&self, relpath: &str, size: u64) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_truncate(
                file_abs.as_str(),
                i64::try_from(size).map_err(|_| FuseBackendError::InvalidArgument {
                    detail: format!("truncate size out of range: {}", size),
                })?,
            )
            .map_err(fsagent_err_to_backend)
    }

    fn fallocate(
        &self,
        relpath: &str,
        mode: i32,
        offset: u64,
        length: u64,
    ) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_fallocate(
                file_abs.as_str(),
                i64::from(mode),
                i64::try_from(offset).map_err(|_| FuseBackendError::InvalidArgument {
                    detail: format!("fallocate offset out of range: {}", offset),
                })?,
                i64::try_from(length).map_err(|_| FuseBackendError::InvalidArgument {
                    detail: format!("fallocate length out of range: {}", length),
                })?,
            )
            .map_err(fsagent_err_to_backend)
    }

    fn fiemap(
        &self,
        relpath: &str,
        request: &[u8],
        out_size: u32,
    ) -> Result<Vec<u8>, FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_fiemap(file_abs.as_str(), request.to_vec(), i64::from(out_size))
            .map_err(fsagent_err_to_backend)
    }

    fn mkdir(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_mkdir(file_abs.as_str(), i64::from(mode))
            .map_err(fsagent_err_to_backend)
    }

    fn mkfifo(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_mkfifo(file_abs.as_str(), i64::from(mode))
            .map_err(fsagent_err_to_backend)
    }

    fn mknod(&self, relpath: &str, mode: u32, rdev: u32) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_mknod(file_abs.as_str(), i64::from(mode), i64::from(rdev))
            .map_err(fsagent_err_to_backend)
    }

    fn rmdir(&self, relpath: &str) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_rmdir(file_abs.as_str())
            .map_err(fsagent_err_to_backend)
    }

    fn unlink(&self, relpath: &str) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_unlink(file_abs.as_str())
            .map_err(fsagent_err_to_backend)
    }

    fn link(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError> {
        let src_abs = self.path_for_relpath(src_relpath)?;
        let dst_abs = self.path_for_relpath(dst_relpath)?;
        self.agent
            .path_link(src_abs.as_str(), dst_abs.as_str())
            .map_err(fsagent_err_to_backend)
    }

    fn rename(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError> {
        let src_abs = self.path_for_relpath(src_relpath)?;
        let dst_abs = self.path_for_relpath(dst_relpath)?;
        self.agent
            .path_rename(src_abs.as_str(), dst_abs.as_str())
            .map_err(fsagent_err_to_backend)
    }

    fn chmod(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_chmod(file_abs.as_str(), i64::from(mode))
            .map_err(fsagent_err_to_backend)
    }

    fn chown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_chown(file_abs.as_str(), i64::from(uid), i64::from(gid))
            .map_err(fsagent_err_to_backend)
    }

    fn lchown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_lchown(file_abs.as_str(), i64::from(uid), i64::from(gid))
            .map_err(fsagent_err_to_backend)
    }

    fn utimens(
        &self,
        relpath: &str,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_utime(file_abs.as_str(), atime_ns, mtime_ns)
            .map_err(fsagent_err_to_backend)
    }

    fn symlink(&self, linkname: &str, relpath: &str) -> Result<(), FuseBackendError> {
        let file_abs = self.path_for_relpath(relpath)?;
        self.agent
            .path_symlink(linkname, file_abs.as_str())
            .map_err(fsagent_err_to_backend)
    }

    fn statfs(&self, relpath: &str) -> Result<FuseBackendStatFs, FuseBackendError> {
        Err(FuseBackendError::Os {
            errno: libc::ENOTSUP,
            path: relpath.to_string(),
            detail: format!("fluxon fs agent backend does not expose statfs for relpath={}", relpath),
        })
    }
}

pub struct FluxonRpcKvExportBackend {
    rpc_client: Arc<dyn UserRpcClient>,
    path_projection: FuseMountPathProjection,
    export_name: String,
    rpc_paths: FluxonFsExportRpcPaths,
    fallocate_rpc_path: String,
    fiemap_rpc_path: String,
    _request_identity: Option<FluxonFsRequestIdentity>,
}

impl FluxonRpcKvExportBackend {
    pub fn new(
        rpc_client: Arc<dyn UserRpcClient>,
        mountpoint_dir_abs: String,
        export_name: String,
        request_identity: Option<FluxonFsRequestIdentity>,
    ) -> Result<Self, FuseAdapterError> {
        if export_name.trim().is_empty() {
            return Err(FuseAdapterError::InvalidArgument {
                detail: "export_name must be non-empty".to_string(),
            });
        }
        Ok(Self {
            rpc_client,
            path_projection: FuseMountPathProjection::new(mountpoint_dir_abs)?,
            rpc_paths: export_rpc_paths_for_export_name_v1(export_name.as_str()),
            fallocate_rpc_path: export_fallocate_rpc_path_for_export_name_v1(export_name.as_str()),
            fiemap_rpc_path: export_fiemap_rpc_path_for_export_name_v1(export_name.as_str()),
            export_name,
            _request_identity: request_identity,
        })
    }

    fn path_for_relpath(&self, relpath: &str) -> Result<String, FuseBackendError> {
        self.path_projection
            .abs_path_for_relpath(relpath)
            .map_err(|err| FuseBackendError::InvalidArgument {
                detail: err.to_string(),
            })
    }

    fn call_rpc(
        &self,
        rpc_path: &str,
        payload: FlatDict,
        op: &'static str,
        path_for_err: &str,
    ) -> Result<FlatDict, FuseBackendError> {
        self.rpc_client
            .call("", rpc_path, payload, None)
            .map_err(|err| FuseBackendError::Os {
                errno: libc::EIO,
                path: path_for_err.to_string(),
                detail: format!(
                    "in-process rpc call failed: op={} export={} rpc_path={} err={}",
                    op, self.export_name, rpc_path, err
                ),
            })
    }

    fn ok_response(
        &self,
        rpc_path: &str,
        payload: FlatDict,
        op: &'static str,
        path_for_err: &str,
    ) -> Result<FlatDict, FuseBackendError> {
        let response = self.call_rpc(rpc_path, payload, op, path_for_err)?;
        let ok = match response.get("ok") {
            Some(FlatValue::Bool(value)) => *value,
            _ => {
                return Err(FuseBackendError::InvalidArgument {
                    detail: format!("{} response missing ok", op),
                });
            }
        };
        if ok {
            return Ok(response);
        }
        Err(err_from_resp(&response, path_for_err))
    }

    fn base_relpath_payload(&self, relpath: &str) -> FlatDict {
        let relpath_rpc = normalize_relpath_rpc(relpath);
        FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(self.export_name.clone()),
            ),
            (
                "relpath".to_string(),
                FlatValue::String(relpath_rpc.into_owned()),
            ),
        ])
    }
}

impl FuseExportBackend for FluxonRpcKvExportBackend {
    fn stat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let response = self.ok_response(
            self.rpc_paths.stat.as_str(),
            self.base_relpath_payload(relpath),
            "stat",
            path_for_err.as_str(),
        )?;
        Ok(FuseBackendStat {
            exists: matches!(response.get("exists"), Some(FlatValue::Bool(true))),
            is_file: matches!(response.get("is_file"), Some(FlatValue::Bool(true))),
            is_dir: matches!(response.get("is_dir"), Some(FlatValue::Bool(true))),
            size: get_i64(&response, "size").unwrap_or(0).max(0) as u64,
            ctime_ns: get_i64(&response, "ctime_ns").unwrap_or(0),
            atime_ns: get_i64(&response, "atime_ns").unwrap_or(0),
            mtime_ns: get_i64(&response, "mtime_ns").unwrap_or(0),
            mode: get_i64(&response, "mode").unwrap_or(0).max(0) as u32,
            uid: get_i64(&response, "uid").unwrap_or(0).max(0) as u32,
            gid: get_i64(&response, "gid").unwrap_or(0).max(0) as u32,
            nlink: get_i64(&response, "nlink").unwrap_or(0).max(0) as u64,
            ino: get_i64(&response, "ino").unwrap_or(0).max(0) as u64,
            rdev: get_i64(&response, "rdev").unwrap_or(0).max(0) as u32,
        })
    }

    fn lstat(&self, relpath: &str) -> Result<FuseBackendStat, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let response = self.ok_response(
            self.rpc_paths.lstat.as_str(),
            self.base_relpath_payload(relpath),
            "lstat",
            path_for_err.as_str(),
        )?;
        Ok(FuseBackendStat {
            exists: matches!(response.get("exists"), Some(FlatValue::Bool(true))),
            is_file: matches!(response.get("is_file"), Some(FlatValue::Bool(true))),
            is_dir: matches!(response.get("is_dir"), Some(FlatValue::Bool(true))),
            size: get_i64(&response, "size").unwrap_or(0).max(0) as u64,
            ctime_ns: get_i64(&response, "ctime_ns").unwrap_or(0),
            atime_ns: get_i64(&response, "atime_ns").unwrap_or(0),
            mtime_ns: get_i64(&response, "mtime_ns").unwrap_or(0),
            mode: get_i64(&response, "mode").unwrap_or(0).max(0) as u32,
            uid: get_i64(&response, "uid").unwrap_or(0).max(0) as u32,
            gid: get_i64(&response, "gid").unwrap_or(0).max(0) as u32,
            nlink: get_i64(&response, "nlink").unwrap_or(0).max(0) as u64,
            ino: get_i64(&response, "ino").unwrap_or(0).max(0) as u64,
            rdev: get_i64(&response, "rdev").unwrap_or(0).max(0) as u32,
        })
    }

    fn list_dir(&self, relpath: &str) -> Result<Vec<FuseBackendDirEntry>, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let response = self.ok_response(
            self.rpc_paths.list_dir.as_str(),
            self.base_relpath_payload(relpath),
            "list_dir",
            path_for_err.as_str(),
        )?;
        let entries_json = match response.get("entries_json") {
            Some(FlatValue::String(value)) => value,
            _ => {
                return Err(FuseBackendError::InvalidArgument {
                    detail: "list_dir response missing entries_json".to_string(),
                });
            }
        };
        let json_value: serde_json::Value =
            serde_json::from_str(entries_json).map_err(|err| FuseBackendError::InvalidArgument {
                detail: format!("entries_json parse failed: {}", err),
            })?;
        let items = json_value.as_array().ok_or_else(|| FuseBackendError::InvalidArgument {
            detail: "entries_json must decode to list".to_string(),
        })?;
        let mut out = Vec::new();
        for item in items {
            let Some(obj) = item.as_object() else {
                continue;
            };
            let Some(name) = obj.get("name").and_then(|value| value.as_str()) else {
                continue;
            };
            out.push(FuseBackendDirEntry {
                name: name.to_string(),
                is_file: obj
                    .get("is_file")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false),
                is_dir: obj
                    .get("is_dir")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false),
                mode: obj
                    .get("mode")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0)
                    .max(0) as u32,
                ino: obj
                    .get("ino")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0)
                    .max(0) as u64,
            });
        }
        Ok(out)
    }

    fn readlink(&self, relpath: &str) -> Result<String, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let response = self.ok_response(
            self.rpc_paths.readlink.as_str(),
            self.base_relpath_payload(relpath),
            "readlink",
            path_for_err.as_str(),
        )?;
        match response.get("target") {
            Some(FlatValue::String(value)) => Ok(value.clone()),
            _ => Err(FuseBackendError::InvalidArgument {
                detail: "readlink response missing target".to_string(),
            }),
        }
    }

    fn setxattr(
        &self,
        relpath: &str,
        name: &str,
        value: Vec<u8>,
        flags: i32,
    ) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("name".to_string(), FlatValue::String(name.to_string()));
        payload.insert("value".to_string(), FlatValue::Bytes(value));
        payload.insert("flags".to_string(), FlatValue::Int64(i64::from(flags)));
        self.ok_response(
            self.rpc_paths.setxattr.as_str(),
            payload,
            "setxattr",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn getxattr(&self, relpath: &str, name: &str) -> Result<Vec<u8>, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("name".to_string(), FlatValue::String(name.to_string()));
        let response = self.ok_response(
            self.rpc_paths.getxattr.as_str(),
            payload,
            "getxattr",
            path_for_err.as_str(),
        )?;
        match response.get("data") {
            Some(FlatValue::Bytes(value)) => Ok(value.clone()),
            _ => Err(FuseBackendError::InvalidArgument {
                detail: "getxattr response missing data".to_string(),
            }),
        }
    }

    fn listxattr(&self, relpath: &str) -> Result<Vec<u8>, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let response = self.ok_response(
            self.rpc_paths.listxattr.as_str(),
            self.base_relpath_payload(relpath),
            "listxattr",
            path_for_err.as_str(),
        )?;
        match response.get("data") {
            Some(FlatValue::Bytes(value)) => Ok(value.clone()),
            _ => Err(FuseBackendError::InvalidArgument {
                detail: "listxattr response missing data".to_string(),
            }),
        }
    }

    fn removexattr(&self, relpath: &str, name: &str) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("name".to_string(), FlatValue::String(name.to_string()));
        self.ok_response(
            self.rpc_paths.removexattr.as_str(),
            payload,
            "removexattr",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn read_committed(
        &self,
        relpath: &str,
        offset: u64,
        size: u32,
        _committed_size: u64,
        _committed_mtime_ns: i64,
    ) -> Result<Vec<u8>, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let offset_i64 = i64::try_from(offset).map_err(|_| FuseBackendError::InvalidArgument {
            detail: format!("read offset out of range: {}", offset),
        })?;
        let size_i64 = i64::from(size);
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("offset".to_string(), FlatValue::Int64(offset_i64));
        payload.insert("length".to_string(), FlatValue::Int64(size_i64));
        let response = self.ok_response(
            self.rpc_paths.read_chunk.as_str(),
            payload,
            "read_chunk",
            path_for_err.as_str(),
        )?;
        match response.get("data") {
            Some(FlatValue::Bytes(value)) => Ok(value.clone()),
            _ => Err(FuseBackendError::InvalidArgument {
                detail: "read_chunk response missing data".to_string(),
            }),
        }
    }

    fn write_chunk(&self, relpath: &str, offset: u64, data: Vec<u8>) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let offset_i64 = i64::try_from(offset).map_err(|_| FuseBackendError::InvalidArgument {
            detail: format!("write offset out of range: {}", offset),
        })?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("offset".to_string(), FlatValue::Int64(offset_i64));
        payload.insert("data".to_string(), FlatValue::Bytes(data));
        self.ok_response(
            self.rpc_paths.write_chunk.as_str(),
            payload,
            "write_chunk",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn truncate(&self, relpath: &str, size: u64) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let size_i64 = i64::try_from(size).map_err(|_| FuseBackendError::InvalidArgument {
            detail: format!("truncate size out of range: {}", size),
        })?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("size".to_string(), FlatValue::Int64(size_i64));
        self.ok_response(
            self.rpc_paths.truncate.as_str(),
            payload,
            "truncate",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn fallocate(
        &self,
        relpath: &str,
        mode: i32,
        offset: u64,
        length: u64,
    ) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let offset_i64 = i64::try_from(offset).map_err(|_| FuseBackendError::InvalidArgument {
            detail: format!("fallocate offset out of range: {}", offset),
        })?;
        let length_i64 = i64::try_from(length).map_err(|_| FuseBackendError::InvalidArgument {
            detail: format!("fallocate length out of range: {}", length),
        })?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("mode".to_string(), FlatValue::Int64(i64::from(mode)));
        payload.insert("offset".to_string(), FlatValue::Int64(offset_i64));
        payload.insert("length".to_string(), FlatValue::Int64(length_i64));
        self.ok_response(
            self.fallocate_rpc_path.as_str(),
            payload,
            "fallocate",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn fiemap(
        &self,
        relpath: &str,
        request: &[u8],
        out_size: u32,
    ) -> Result<Vec<u8>, FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("data".to_string(), FlatValue::Bytes(request.to_vec()));
        payload.insert("out_size".to_string(), FlatValue::Int64(i64::from(out_size)));
        let response = self.ok_response(
            self.fiemap_rpc_path.as_str(),
            payload,
            "fiemap",
            path_for_err.as_str(),
        )?;
        match response.get("data") {
            Some(FlatValue::Bytes(data)) => Ok(data.clone()),
            _ => Err(FuseBackendError::InvalidArgument {
                detail: "fiemap response missing data".to_string(),
            }),
        }
    }

    fn mkdir(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("mode".to_string(), FlatValue::Int64(i64::from(mode)));
        self.ok_response(
            self.rpc_paths.mkdir.as_str(),
            payload,
            "mkdir",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn mkfifo(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("mode".to_string(), FlatValue::Int64(i64::from(mode)));
        self.ok_response(
            self.rpc_paths.mkfifo.as_str(),
            payload,
            "mkfifo",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn mknod(&self, relpath: &str, mode: u32, rdev: u32) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("mode".to_string(), FlatValue::Int64(i64::from(mode)));
        payload.insert("rdev".to_string(), FlatValue::Int64(i64::from(rdev)));
        self.ok_response(
            self.rpc_paths.mknod.as_str(),
            payload,
            "mknod",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn rmdir(&self, relpath: &str) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        self.ok_response(
            self.rpc_paths.rmdir.as_str(),
            self.base_relpath_payload(relpath),
            "rmdir",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn unlink(&self, relpath: &str) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        self.ok_response(
            self.rpc_paths.unlink.as_str(),
            self.base_relpath_payload(relpath),
            "unlink",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn link(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(src_relpath)?;
        let src_relpath_rpc = normalize_relpath_rpc(src_relpath);
        let dst_relpath_rpc = normalize_relpath_rpc(dst_relpath);
        let payload = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(self.export_name.clone()),
            ),
            (
                "src_relpath".to_string(),
                FlatValue::String(src_relpath_rpc.into_owned()),
            ),
            (
                "dst_relpath".to_string(),
                FlatValue::String(dst_relpath_rpc.into_owned()),
            ),
        ]);
        self.ok_response(
            self.rpc_paths.link.as_str(),
            payload,
            "link",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn rename(&self, src_relpath: &str, dst_relpath: &str) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(src_relpath)?;
        let src_relpath_rpc = normalize_relpath_rpc(src_relpath);
        let dst_relpath_rpc = normalize_relpath_rpc(dst_relpath);
        let payload = FlatDict::from([
            (
                "export".to_string(),
                FlatValue::String(self.export_name.clone()),
            ),
            (
                "src_relpath".to_string(),
                FlatValue::String(src_relpath_rpc.into_owned()),
            ),
            (
                "dst_relpath".to_string(),
                FlatValue::String(dst_relpath_rpc.into_owned()),
            ),
        ]);
        self.ok_response(
            self.rpc_paths.rename.as_str(),
            payload,
            "rename",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn chmod(&self, relpath: &str, mode: u32) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("mode".to_string(), FlatValue::Int64(i64::from(mode)));
        self.ok_response(
            self.rpc_paths.chmod.as_str(),
            payload,
            "chmod",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn chown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("uid".to_string(), FlatValue::Int64(i64::from(uid)));
        payload.insert("gid".to_string(), FlatValue::Int64(i64::from(gid)));
        self.ok_response(
            self.rpc_paths.chown.as_str(),
            payload,
            "chown",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn lchown(&self, relpath: &str, uid: u32, gid: u32) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("uid".to_string(), FlatValue::Int64(i64::from(uid)));
        payload.insert("gid".to_string(), FlatValue::Int64(i64::from(gid)));
        self.ok_response(
            self.rpc_paths.lchown.as_str(),
            payload,
            "lchown",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn utimens(
        &self,
        relpath: &str,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        if let Some(atime_ns) = atime_ns {
            payload.insert("atime_ns".to_string(), FlatValue::Int64(atime_ns));
        }
        if let Some(mtime_ns) = mtime_ns {
            payload.insert("mtime_ns".to_string(), FlatValue::Int64(mtime_ns));
        }
        self.ok_response(
            self.rpc_paths.utime.as_str(),
            payload,
            "utime",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn lutimens(
        &self,
        relpath: &str,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        if let Some(atime_ns) = atime_ns {
            payload.insert("atime_ns".to_string(), FlatValue::Int64(atime_ns));
        }
        if let Some(mtime_ns) = mtime_ns {
            payload.insert("mtime_ns".to_string(), FlatValue::Int64(mtime_ns));
        }
        payload.insert("nofollow".to_string(), FlatValue::Bool(true));
        self.ok_response(
            self.rpc_paths.utime.as_str(),
            payload,
            "lutime",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn symlink(&self, linkname: &str, relpath: &str) -> Result<(), FuseBackendError> {
        let path_for_err = self.path_for_relpath(relpath)?;
        let mut payload = self.base_relpath_payload(relpath);
        payload.insert("target".to_string(), FlatValue::String(linkname.to_string()));
        self.ok_response(
            self.rpc_paths.symlink.as_str(),
            payload,
            "symlink",
            path_for_err.as_str(),
        )?;
        Ok(())
    }

    fn statfs(&self, relpath: &str) -> Result<FuseBackendStatFs, FuseBackendError> {
        Err(FuseBackendError::Os {
            errno: libc::ENOTSUP,
            path: relpath.to_string(),
            detail: format!(
                "fluxon rpc kv backend does not expose statfs for relpath={}",
                relpath
            ),
        })
    }
}

fn get_i64(payload: &FlatDict, key: &str) -> Option<i64> {
    match payload.get(key) {
        Some(FlatValue::Int64(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(feature = "fsagent_backend")]
fn remote_stat_to_backend(stat: fluxon_fs::agent::RemoteStat) -> FuseBackendStat {
    FuseBackendStat {
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

#[cfg(feature = "fsagent_backend")]
fn fsagent_err_to_backend(err: fluxon_fs::agent::FsAgentError) -> FuseBackendError {
    match err {
        fluxon_fs::agent::FsAgentError::InvalidArgument { detail } => {
            FuseBackendError::InvalidArgument { detail }
        }
        fluxon_fs::agent::FsAgentError::AccessDenied { path, detail } => {
            FuseBackendError::AccessDenied { path, detail }
        }
        fluxon_fs::agent::FsAgentError::Os {
            errno,
            path,
            detail,
        } => FuseBackendError::Os {
            errno,
            path,
            detail,
        },
        fluxon_fs::agent::FsAgentError::Io { path, detail } => FuseBackendError::Os {
            errno: libc::EIO,
            path,
            detail,
        },
        fluxon_fs::agent::FsAgentError::Shutdown { detail } => FuseBackendError::Os {
            errno: libc::EIO,
            path: ".".to_string(),
            detail,
        },
        fluxon_fs::agent::FsAgentError::Kv(err) => FuseBackendError::Os {
            errno: libc::EIO,
            path: ".".to_string(),
            detail: format!("kv error: {}", err),
        },
    }
}

fn err_from_resp(resp: &FlatDict, path_for_err: &str) -> FuseBackendError {
    let err_detail = match resp.get("err") {
        Some(FlatValue::String(value)) => value.to_string(),
        _ => "remote error".to_string(),
    };
    let Some(err_kind_value) = get_i64(resp, FLUXON_FS_RPC_ERR_KIND_KEY) else {
        return FuseBackendError::InvalidArgument {
            detail: format!(
                "remote error response missing err_kind: path={} detail={}",
                path_for_err, err_detail
            ),
        };
    };
    let Some(err_kind) = FluxonFsRpcErrorKind::from_i64(err_kind_value) else {
        return FuseBackendError::InvalidArgument {
            detail: format!(
                "remote error response has unknown err_kind={} path={} detail={}",
                err_kind_value, path_for_err, err_detail
            ),
        };
    };
    match err_kind {
        FluxonFsRpcErrorKind::InvalidArgument => FuseBackendError::InvalidArgument {
            detail: err_detail,
        },
        FluxonFsRpcErrorKind::Os => {
            let Some(errno_value) = get_i64(resp, "errno") else {
                return FuseBackendError::InvalidArgument {
                    detail: format!(
                        "remote os error missing errno: path={} detail={}",
                        path_for_err, err_detail
                    ),
                };
            };
            map_os_error(errno_value as i32, path_for_err, err_detail)
        }
        FluxonFsRpcErrorKind::AccessDenied => FuseBackendError::AccessDenied {
            path: path_for_err.to_string(),
            detail: err_detail,
        },
        FluxonFsRpcErrorKind::Internal => FuseBackendError::Os {
            errno: libc::EIO,
            path: path_for_err.to_string(),
            detail: format!(
                "remote internal error: path={} detail={}",
                path_for_err, err_detail
            ),
        },
    }
}

fn map_os_error(errno: i32, path_for_err: &str, detail: String) -> FuseBackendError {
    match errno {
        x if x == libc::ENOENT => FuseBackendError::NotFound {
            path: path_for_err.to_string(),
        },
        x if x == libc::EEXIST => FuseBackendError::AlreadyExists {
            path: path_for_err.to_string(),
        },
        x if x == libc::EISDIR => FuseBackendError::IsDirectory {
            path: path_for_err.to_string(),
        },
        x if x == libc::ENOTDIR => FuseBackendError::NotDirectory {
            path: path_for_err.to_string(),
        },
        _ => FuseBackendError::Os {
            errno,
            path: path_for_err.to_string(),
            detail,
        },
    }
}

fn normalize_relpath_rpc(relpath: &str) -> Cow<'_, str> {
    if relpath.is_empty() {
        Cow::Borrowed(".")
    } else {
        Cow::Borrowed(relpath)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{FluxonRpcKvExportBackend, FuseBackendError, FuseExportBackend};
    use crate::fluxon_rpc_kv::{FluxonInProcessFsExportMock, FluxonInProcessRpcKvApi};

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
                "fluxon_fs_fuse_backend_{}_{}_{}",
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

    fn new_backend(export_root_dir_abs: String, mountpoint_dir_abs: String) -> FluxonRpcKvExportBackend {
        let api = FluxonInProcessRpcKvApi::new();
        let _mock = FluxonInProcessFsExportMock::new(
            api.clone(),
            "demo".to_string(),
            export_root_dir_abs,
        )
        .unwrap();
        FluxonRpcKvExportBackend::new(api.rpc_client(), mountpoint_dir_abs, "demo".to_string(), None)
            .unwrap()
    }

    #[test]
    fn round_trips_authoritative_rpc_ops() {
        let temp = TestDir::new("rpc_ops");
        let export_root = temp.join("export");
        let mount_root = temp.join("mount");
        fs::create_dir_all(&export_root).unwrap();
        fs::create_dir_all(&mount_root).unwrap();
        fs::write(format!("{}/alpha.txt", export_root), b"hello").unwrap();

        let backend = new_backend(export_root.clone(), mount_root);

        let root_stat = backend.stat(".").unwrap();
        assert!(root_stat.exists);
        assert!(root_stat.is_dir);
        assert!(root_stat.atime_ns >= 0);

        let root_entries = backend.list_dir(".").unwrap();
        assert!(root_entries.iter().any(|entry| entry.name == "alpha.txt" && entry.is_file));

        let alpha_stat = backend.stat("alpha.txt").unwrap();
        let alpha_bytes = backend
            .read_committed("alpha.txt", 0, 5, alpha_stat.size, alpha_stat.mtime_ns)
            .unwrap();
        assert_eq!(alpha_bytes, b"hello");

        backend.mkdir("dir", 0o755).unwrap();
        backend.truncate("dir/new.txt", 0).unwrap();
        backend.write_chunk("dir/new.txt", 0, b"abcdef".to_vec()).unwrap();

        let new_file_stat = backend.stat("dir/new.txt").unwrap();
        assert_eq!(new_file_stat.size, 6);
        let new_file_bytes = backend
            .read_committed(
                "dir/new.txt",
                0,
                6,
                new_file_stat.size,
                new_file_stat.mtime_ns,
            )
            .unwrap();
        assert_eq!(new_file_bytes, b"abcdef");

        backend.truncate("dir/new.txt", 3).unwrap();
        let truncated_stat = backend.stat("dir/new.txt").unwrap();
        assert_eq!(truncated_stat.size, 3);

        backend.chmod("dir/new.txt", 0o600).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(format!("{}/dir/new.txt", export_root))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        backend
            .utimens(
                "dir/new.txt",
                Some(1_700_000_000_000_000_000),
                None,
            )
            .unwrap();
        let utime_atime_only = backend.stat("dir/new.txt").unwrap();
        assert_eq!(utime_atime_only.atime_ns, 1_700_000_000_000_000_000);

        backend
            .utimens("dir/new.txt", None, Some(1_800_000_000_000_000_000))
            .unwrap();
        let utime_both_fields = backend.stat("dir/new.txt").unwrap();
        assert_eq!(utime_both_fields.atime_ns, 1_700_000_000_000_000_000);
        assert_eq!(utime_both_fields.mtime_ns, 1_800_000_000_000_000_000);

        backend.rename("dir/new.txt", "dir/renamed.txt").unwrap();
        assert!(!Path::new(format!("{}/dir/new.txt", export_root).as_str()).exists());
        assert!(Path::new(format!("{}/dir/renamed.txt", export_root).as_str()).exists());

        backend.unlink("dir/renamed.txt").unwrap();
        backend.rmdir("dir").unwrap();
    }

    #[test]
    fn rpc_backend_only_statfs_is_explicitly_unsupported() {
        let temp = TestDir::new("unsupported");
        let export_root = temp.join("export");
        let mount_root = temp.join("mount");
        fs::create_dir_all(&export_root).unwrap();
        fs::create_dir_all(&mount_root).unwrap();

        let backend = new_backend(export_root, mount_root);

        let statfs_err = backend.statfs(".").unwrap_err();
        match statfs_err {
            FuseBackendError::Os { errno, .. } => assert_eq!(errno, libc::ENOTSUP),
            other => panic!("unexpected error: {:?}", other),
        }
    }
}
