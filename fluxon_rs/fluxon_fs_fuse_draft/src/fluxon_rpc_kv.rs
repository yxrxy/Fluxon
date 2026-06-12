use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "fsagent_backend")]
use fluxon_fs::agent::{
    FluxonFsAgentRpcKv, FluxonFsAgentRpcKvApiError, FluxonFsAgentRpcKvError,
    FluxonFsAgentRpcKvFlatDict, FluxonFsAgentRpcKvFlatValue, FluxonFsAgentRpcKvResult,
};
use fluxon_fs_core::config::{
    FluxonFsExportRpcPaths, export_fallocate_rpc_path_for_export_name_v1,
    export_fiemap_rpc_path_for_export_name_v1,
    export_rpc_paths_for_export_name_v1,
};
use parking_lot::Mutex;
use serde_json::json;
use thiserror::Error;

const CHUNK_BYTES: usize = fluxon_fs_core::s3_gateway::FS_S3_OBJECT_PIECE_BYTES;

#[derive(Debug, Clone, PartialEq)]
pub enum FlatValue {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
}

pub type FlatDict = BTreeMap<String, FlatValue>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FluxonRpcKvError {
    #[error("invalid argument: {detail}")]
    InvalidArgument { detail: String },

    #[error("rpc path already registered: {path}")]
    RpcPathAlreadyRegistered { path: String },

    #[error("rpc path not registered: {path}")]
    RpcPathNotRegistered { path: String },

    #[error("internal error: {detail}")]
    Internal { detail: String },
}

pub type FluxonRpcKvResult<T> = Result<T, FluxonRpcKvError>;

pub trait UserRpcClient: Send + Sync {
    fn call(
        &self,
        node_id: &str,
        path: &str,
        payload: FlatDict,
        timeout_ms: Option<u64>,
    ) -> FluxonRpcKvResult<FlatDict>;
}

pub trait UserRpcServer: Send + Sync {
    fn register(
        &self,
        path: &str,
        handler: Arc<dyn Fn(String, FlatDict) -> FluxonRpcKvResult<FlatDict> + Send + Sync + 'static>,
    ) -> FluxonRpcKvResult<()>;
}

pub trait KvClient: Send + Sync {
    fn get(&self, key: &str) -> FluxonRpcKvResult<Option<FlatDict>>;
    fn put(&self, key: &str, value: FlatDict) -> FluxonRpcKvResult<()>;
    fn delete(&self, key: &str) -> FluxonRpcKvResult<()>;
    fn is_exist(&self, key: &str) -> FluxonRpcKvResult<bool>;
}

type RpcHandler =
    Arc<dyn Fn(String, FlatDict) -> FluxonRpcKvResult<FlatDict> + Send + Sync + 'static>;

#[derive(Default)]
struct InProcessRpcKvState {
    rpc_handlers: BTreeMap<String, RpcHandler>,
    kv_entries: BTreeMap<String, FlatDict>,
}

#[derive(Default)]
struct InProcessRpcKvInner {
    state: Mutex<InProcessRpcKvState>,
}

#[derive(Clone, Default)]
pub struct FluxonInProcessRpcKvApi {
    inner: Arc<InProcessRpcKvInner>,
}

impl FluxonInProcessRpcKvApi {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rpc_client(&self) -> Arc<dyn UserRpcClient> {
        Arc::new(self.clone())
    }

    pub fn rpc_server(&self) -> Arc<dyn UserRpcServer> {
        Arc::new(self.clone())
    }

    pub fn kv_client(&self) -> Arc<dyn KvClient> {
        Arc::new(self.clone())
    }
}

impl UserRpcClient for FluxonInProcessRpcKvApi {
    fn call(
        &self,
        node_id: &str,
        path: &str,
        payload: FlatDict,
        _timeout_ms: Option<u64>,
    ) -> FluxonRpcKvResult<FlatDict> {
        validate_rpc_path(path)?;
        let handler = {
            let state = self.inner.state.lock();
            state
                .rpc_handlers
                .get(path)
                .cloned()
                .ok_or_else(|| FluxonRpcKvError::RpcPathNotRegistered {
                    path: path.to_string(),
                })?
        };
        handler(node_id.to_string(), payload)
    }
}

impl UserRpcServer for FluxonInProcessRpcKvApi {
    fn register(
        &self,
        path: &str,
        handler: Arc<dyn Fn(String, FlatDict) -> FluxonRpcKvResult<FlatDict> + Send + Sync + 'static>,
    ) -> FluxonRpcKvResult<()> {
        validate_rpc_path(path)?;
        let mut state = self.inner.state.lock();
        if state.rpc_handlers.contains_key(path) {
            return Err(FluxonRpcKvError::RpcPathAlreadyRegistered {
                path: path.to_string(),
            });
        }
        state.rpc_handlers.insert(path.to_string(), handler);
        Ok(())
    }
}

impl KvClient for FluxonInProcessRpcKvApi {
    fn get(&self, key: &str) -> FluxonRpcKvResult<Option<FlatDict>> {
        let state = self.inner.state.lock();
        Ok(state.kv_entries.get(key).cloned())
    }

    fn put(&self, key: &str, value: FlatDict) -> FluxonRpcKvResult<()> {
        let mut state = self.inner.state.lock();
        state.kv_entries.insert(key.to_string(), value);
        Ok(())
    }

    fn delete(&self, key: &str) -> FluxonRpcKvResult<()> {
        let mut state = self.inner.state.lock();
        state.kv_entries.remove(key);
        Ok(())
    }

    fn is_exist(&self, key: &str) -> FluxonRpcKvResult<bool> {
        let state = self.inner.state.lock();
        Ok(state.kv_entries.contains_key(key))
    }
}

#[cfg(feature = "fsagent_backend")]
impl FluxonFsAgentRpcKv for FluxonInProcessRpcKvApi {
    fn rpc_call(
        &self,
        node_id: &str,
        path: &str,
        payload: FluxonFsAgentRpcKvFlatDict,
        timeout_ms: Option<u64>,
    ) -> FluxonFsAgentRpcKvResult<FluxonFsAgentRpcKvFlatDict> {
        let resp = UserRpcClient::call(self, node_id, path, agent_flat_dict_to_draft(payload), timeout_ms)
            .map_err(draft_err_to_agent)?;
        Ok(draft_flat_dict_to_agent(resp))
    }

    fn kv_get(&self, key: &str) -> FluxonFsAgentRpcKvResult<Option<FluxonFsAgentRpcKvFlatDict>> {
        let got = KvClient::get(self, key).map_err(draft_err_to_agent)?;
        Ok(got.map(draft_flat_dict_to_agent))
    }

    fn kv_put(
        &self,
        key: &str,
        value: FluxonFsAgentRpcKvFlatDict,
    ) -> FluxonFsAgentRpcKvResult<()> {
        KvClient::put(self, key, agent_flat_dict_to_draft(value)).map_err(draft_err_to_agent)
    }
}

#[cfg(feature = "fsagent_backend")]
fn agent_flat_dict_to_draft(value: FluxonFsAgentRpcKvFlatDict) -> FlatDict {
    value
        .into_iter()
        .map(|(key, value)| (key, agent_flat_value_to_draft(value)))
        .collect()
}

#[cfg(feature = "fsagent_backend")]
fn draft_flat_dict_to_agent(value: FlatDict) -> FluxonFsAgentRpcKvFlatDict {
    value
        .into_iter()
        .map(|(key, value)| (key, draft_flat_value_to_agent(value)))
        .collect()
}

#[cfg(feature = "fsagent_backend")]
fn agent_flat_value_to_draft(value: FluxonFsAgentRpcKvFlatValue) -> FlatValue {
    match value {
        FluxonFsAgentRpcKvFlatValue::Int64(v) => FlatValue::Int64(v),
        FluxonFsAgentRpcKvFlatValue::Float64(v) => FlatValue::Float64(v),
        FluxonFsAgentRpcKvFlatValue::Bool(v) => FlatValue::Bool(v),
        FluxonFsAgentRpcKvFlatValue::String(v) => FlatValue::String(v),
        FluxonFsAgentRpcKvFlatValue::Bytes(v) => FlatValue::Bytes(v),
    }
}

#[cfg(feature = "fsagent_backend")]
fn draft_flat_value_to_agent(value: FlatValue) -> FluxonFsAgentRpcKvFlatValue {
    match value {
        FlatValue::Int64(v) => FluxonFsAgentRpcKvFlatValue::Int64(v),
        FlatValue::Float64(v) => FluxonFsAgentRpcKvFlatValue::Float64(v),
        FlatValue::Bool(v) => FluxonFsAgentRpcKvFlatValue::Bool(v),
        FlatValue::String(v) => FluxonFsAgentRpcKvFlatValue::String(v),
        FlatValue::Bytes(v) => FluxonFsAgentRpcKvFlatValue::Bytes(v),
    }
}

#[cfg(feature = "fsagent_backend")]
fn draft_err_to_agent(err: FluxonRpcKvError) -> FluxonFsAgentRpcKvError {
    match err {
        FluxonRpcKvError::InvalidArgument { detail } => {
            FluxonFsAgentRpcKvApiError::InvalidArgument { detail }.into()
        }
        FluxonRpcKvError::RpcPathAlreadyRegistered { path } => FluxonFsAgentRpcKvApiError::Unknown {
            detail: format!("rpc path already registered: {}", path),
        }
        .into(),
        FluxonRpcKvError::RpcPathNotRegistered { path } => FluxonFsAgentRpcKvApiError::Unknown {
            detail: format!("rpc path not registered: {}", path),
        }
        .into(),
        FluxonRpcKvError::Internal { detail } => {
            FluxonFsAgentRpcKvApiError::Unknown { detail }.into()
        }
    }
}

pub(crate) const FLUXON_FS_RPC_ERR_KIND_KEY: &str = "err_kind";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub(crate) enum FluxonFsRpcErrorKind {
    InvalidArgument = 1,
    Os = 2,
    AccessDenied = 3,
    Internal = 4,
}

impl FluxonFsRpcErrorKind {
    pub(crate) fn as_i64(self) -> i64 {
        self as i64
    }

    pub(crate) fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(Self::InvalidArgument),
            2 => Some(Self::Os),
            3 => Some(Self::AccessDenied),
            4 => Some(Self::Internal),
            _ => None,
        }
    }
}

struct FsExportMockState {
    export_name: String,
    export_root_dir: fs::File,
}

#[derive(Debug)]
pub struct FluxonInProcessFsExportMock {
    export_name: String,
    export_root_dir_abs: String,
    rpc_paths: FluxonFsExportRpcPaths,
}

impl FluxonInProcessFsExportMock {
    pub fn new(
        api: FluxonInProcessRpcKvApi,
        export_name: String,
        export_root_dir_abs: String,
    ) -> FluxonRpcKvResult<Self> {
        if export_name.trim().is_empty() {
            return Err(FluxonRpcKvError::InvalidArgument {
                detail: "export_name must be non-empty".to_string(),
            });
        }
        let export_root_dir_abs = canonicalize_export_root(export_root_dir_abs.as_str())?;
        let export_root_dir = fs::File::open(export_root_dir_abs.as_str()).map_err(|err| {
            FluxonRpcKvError::InvalidArgument {
                detail: format!("open export root failed: {}", err),
            }
        })?;
        let rpc_paths = export_rpc_paths_for_export_name_v1(export_name.as_str());
        let state = Arc::new(FsExportMockState {
            export_name: export_name.clone(),
            export_root_dir,
        });
        let rpc_server = api.rpc_server();

        register_fs_handler(rpc_server.clone(), rpc_paths.stat.as_str(), state.clone(), handle_stat)?;
        register_fs_handler(rpc_server.clone(), rpc_paths.lstat.as_str(), state.clone(), handle_lstat)?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.list_dir.as_str(),
            state.clone(),
            handle_list_dir,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.readlink.as_str(),
            state.clone(),
            handle_readlink,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.setxattr.as_str(),
            state.clone(),
            handle_setxattr,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.getxattr.as_str(),
            state.clone(),
            handle_getxattr,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.listxattr.as_str(),
            state.clone(),
            handle_listxattr,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.removexattr.as_str(),
            state.clone(),
            handle_removexattr,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.read_chunk.as_str(),
            state.clone(),
            handle_read_chunk,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.write_chunk.as_str(),
            state.clone(),
            handle_write_chunk,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.truncate.as_str(),
            state.clone(),
            handle_truncate,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            export_fallocate_rpc_path_for_export_name_v1(export_name.as_str()).as_str(),
            state.clone(),
            handle_fallocate,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            export_fiemap_rpc_path_for_export_name_v1(export_name.as_str()).as_str(),
            state.clone(),
            handle_fiemap,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.mkdir.as_str(),
            state.clone(),
            handle_mkdir,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.mkfifo.as_str(),
            state.clone(),
            handle_mkfifo,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.mknod.as_str(),
            state.clone(),
            handle_mknod,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.rmdir.as_str(),
            state.clone(),
            handle_rmdir,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.unlink.as_str(),
            state.clone(),
            handle_unlink,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.link.as_str(),
            state.clone(),
            handle_link,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.symlink.as_str(),
            state.clone(),
            handle_symlink,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.rename.as_str(),
            state.clone(),
            handle_rename,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.chmod.as_str(),
            state.clone(),
            handle_chmod,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.chown.as_str(),
            state.clone(),
            handle_chown,
        )?;
        register_fs_handler(
            rpc_server.clone(),
            rpc_paths.lchown.as_str(),
            state.clone(),
            handle_lchown,
        )?;
        register_fs_handler(rpc_server, rpc_paths.utime.as_str(), state, handle_utime)?;

        Ok(Self {
            export_name,
            export_root_dir_abs,
            rpc_paths,
        })
    }

    pub fn export_name(&self) -> &str {
        self.export_name.as_str()
    }

    pub fn export_root_dir_abs(&self) -> &str {
        self.export_root_dir_abs.as_str()
    }

    pub fn rpc_paths(&self) -> &FluxonFsExportRpcPaths {
        &self.rpc_paths
    }
}

fn validate_rpc_path(path: &str) -> FluxonRpcKvResult<()> {
    if path.trim().is_empty() {
        return Err(FluxonRpcKvError::InvalidArgument {
            detail: "rpc path must be non-empty".to_string(),
        });
    }
    if !path.starts_with('/') {
        return Err(FluxonRpcKvError::InvalidArgument {
            detail: format!("rpc path must be absolute: {}", path),
        });
    }
    Ok(())
}

fn canonicalize_export_root(export_root_dir_abs: &str) -> FluxonRpcKvResult<String> {
    if export_root_dir_abs.trim().is_empty() {
        return Err(FluxonRpcKvError::InvalidArgument {
            detail: "export_root_dir_abs must be non-empty".to_string(),
        });
    }
    let root = Path::new(export_root_dir_abs);
    if !root.is_absolute() {
        return Err(FluxonRpcKvError::InvalidArgument {
            detail: format!(
                "export_root_dir_abs must be absolute: {}",
                export_root_dir_abs
            ),
        });
    }
    let canonical = root.canonicalize().map_err(|err| FluxonRpcKvError::InvalidArgument {
        detail: format!("canonicalize export root failed: {}", err),
    })?;
    Ok(canonical.to_string_lossy().to_string())
}

fn register_fs_handler(
    rpc_server: Arc<dyn UserRpcServer>,
    path: &str,
    state: Arc<FsExportMockState>,
    handler: fn(&FsExportMockState, FlatDict) -> FlatDict,
) -> FluxonRpcKvResult<()> {
    rpc_server.register(
        path,
        Arc::new(move |_from_node, payload| Ok(handler(state.as_ref(), payload))),
    )
}

fn handle_stat(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    handle_stat_impl(state, payload, true)
}

fn handle_lstat(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    handle_stat_impl(state, payload, false)
}

fn handle_stat_impl(state: &FsExportMockState, payload: FlatDict, follow_symlink: bool) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let stat = match stat_at(&state.export_root_dir, relpath.as_str(), follow_symlink) {
            Ok(value) => value,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return resp_ok(BTreeMap::from([
                        ("exists".to_string(), FlatValue::Bool(false)),
                        ("is_file".to_string(), FlatValue::Bool(false)),
                        ("is_dir".to_string(), FlatValue::Bool(false)),
                        ("size".to_string(), FlatValue::Int64(0)),
                        ("ctime_ns".to_string(), FlatValue::Int64(0)),
                        ("atime_ns".to_string(), FlatValue::Int64(0)),
                        ("mtime_ns".to_string(), FlatValue::Int64(0)),
                        ("mode".to_string(), FlatValue::Int64(0)),
                        ("uid".to_string(), FlatValue::Int64(0)),
                        ("gid".to_string(), FlatValue::Int64(0)),
                        ("nlink".to_string(), FlatValue::Int64(0)),
                        ("ino".to_string(), FlatValue::Int64(0)),
                        ("rdev".to_string(), FlatValue::Int64(0)),
                    ]));
                }
                return resp_err_io(err);
            }
        };
        return resp_ok(BTreeMap::from([
            ("exists".to_string(), FlatValue::Bool(true)),
            ("is_file".to_string(), FlatValue::Bool(stat_is_file(&stat))),
            ("is_dir".to_string(), FlatValue::Bool(stat_is_dir(&stat))),
            ("size".to_string(), FlatValue::Int64(stat_size(&stat))),
            ("ctime_ns".to_string(), FlatValue::Int64(stat_ctime_ns(&stat))),
            ("atime_ns".to_string(), FlatValue::Int64(stat_atime_ns(&stat))),
            ("mtime_ns".to_string(), FlatValue::Int64(stat_mtime_ns(&stat))),
            ("mode".to_string(), FlatValue::Int64(stat_mode(&stat))),
            ("uid".to_string(), FlatValue::Int64(stat_uid(&stat))),
            ("gid".to_string(), FlatValue::Int64(stat_gid(&stat))),
            ("nlink".to_string(), FlatValue::Int64(stat_nlink(&stat))),
            ("ino".to_string(), FlatValue::Int64(stat_ino(&stat))),
            ("rdev".to_string(), FlatValue::Int64(stat_rdev(&stat))),
        ]));
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let metadata = match if follow_symlink {
            fs::metadata(&path)
        } else {
            fs::symlink_metadata(&path)
        } {
            Ok(value) => value,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return resp_ok(BTreeMap::from([
                        ("exists".to_string(), FlatValue::Bool(false)),
                        ("is_file".to_string(), FlatValue::Bool(false)),
                        ("is_dir".to_string(), FlatValue::Bool(false)),
                        ("size".to_string(), FlatValue::Int64(0)),
                        ("ctime_ns".to_string(), FlatValue::Int64(0)),
                        ("atime_ns".to_string(), FlatValue::Int64(0)),
                        ("mtime_ns".to_string(), FlatValue::Int64(0)),
                        ("mode".to_string(), FlatValue::Int64(0)),
                        ("uid".to_string(), FlatValue::Int64(0)),
                        ("gid".to_string(), FlatValue::Int64(0)),
                        ("nlink".to_string(), FlatValue::Int64(0)),
                        ("ino".to_string(), FlatValue::Int64(0)),
                        ("rdev".to_string(), FlatValue::Int64(0)),
                    ]));
                }
                return resp_err_io(err);
            }
        };
        let file_type = metadata.file_type();
        return resp_ok(BTreeMap::from([
            ("exists".to_string(), FlatValue::Bool(true)),
            ("is_file".to_string(), FlatValue::Bool(file_type.is_file())),
            ("is_dir".to_string(), FlatValue::Bool(file_type.is_dir())),
            ("size".to_string(), FlatValue::Int64(metadata.len() as i64)),
            (
                "ctime_ns".to_string(),
                FlatValue::Int64(metadata_ctime_ns(&metadata)),
            ),
            (
                "atime_ns".to_string(),
                FlatValue::Int64(metadata_atime_ns(&metadata)),
            ),
            (
                "mtime_ns".to_string(),
                FlatValue::Int64(metadata_mtime_ns(&metadata)),
            ),
            ("mode".to_string(), FlatValue::Int64(metadata_mode(&metadata))),
            ("uid".to_string(), FlatValue::Int64(metadata_uid(&metadata))),
            ("gid".to_string(), FlatValue::Int64(metadata_gid(&metadata))),
            ("nlink".to_string(), FlatValue::Int64(metadata_nlink(&metadata))),
            ("ino".to_string(), FlatValue::Int64(metadata_ino(&metadata))),
            ("rdev".to_string(), FlatValue::Int64(metadata_rdev(&metadata))),
        ]));
    }
}

fn handle_list_dir(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let dir = match open_at(
            &state.export_root_dir,
            relpath.as_str(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            None,
        ) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let read_dir = match fs::read_dir(fd_view_path(&dir)) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };

        let mut items: Vec<(String, serde_json::Value)> = Vec::new();
        for entry in read_dir {
            let entry = match entry {
                Ok(value) => value,
                Err(err) => return resp_err_io(err),
            };
            let metadata = match fs::symlink_metadata(entry.path()) {
                Ok(value) => value,
                Err(err) => return resp_err_io(err),
            };
            let name = entry.file_name().to_string_lossy().to_string();
            let file_type = metadata.file_type();
            items.push((
                name.clone(),
                json!({
                    "name": name,
                    "is_file": file_type.is_file(),
                    "is_dir": file_type.is_dir(),
                    "size": metadata.len() as i64,
                    "mtime_ns": metadata_mtime_ns(&metadata),
                    "mode": metadata_mode(&metadata),
                    "ino": metadata_ino(&metadata),
                }),
            ));
        }
        items.sort_by(|left, right| left.0.cmp(&right.0));
        let entries_json = match serde_json::to_string(
            &items
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<serde_json::Value>>(),
        ) {
            Ok(value) => value,
            Err(err) => {
                return resp_err(
                    FluxonFsRpcErrorKind::Internal,
                    format!("json encode failed: {}", err),
                    None,
                );
            }
        };
        return resp_ok(BTreeMap::from([(
            "entries_json".to_string(),
            FlatValue::String(entries_json),
        )]));
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let read_dir = match fs::read_dir(&path) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };

        let mut items: Vec<(String, serde_json::Value)> = Vec::new();
        for entry in read_dir {
            let entry = match entry {
                Ok(value) => value,
                Err(err) => return resp_err_io(err),
            };
            let metadata = match fs::symlink_metadata(entry.path()) {
                Ok(value) => value,
                Err(err) => return resp_err_io(err),
            };
            let name = entry.file_name().to_string_lossy().to_string();
            let file_type = metadata.file_type();
            items.push((
                name.clone(),
                json!({
                    "name": name,
                    "is_file": file_type.is_file(),
                    "is_dir": file_type.is_dir(),
                    "size": metadata.len() as i64,
                    "mtime_ns": metadata_mtime_ns(&metadata),
                    "mode": metadata_mode(&metadata),
                    "ino": metadata_ino(&metadata),
                }),
            ));
        }
        items.sort_by(|left, right| left.0.cmp(&right.0));
        let entries_json = match serde_json::to_string(
            &items
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<serde_json::Value>>(),
        ) {
            Ok(value) => value,
            Err(err) => {
                return resp_err(
                    FluxonFsRpcErrorKind::Internal,
                    format!("json encode failed: {}", err),
                    None,
                );
            }
        };
        return resp_ok(BTreeMap::from([(
            "entries_json".to_string(),
            FlatValue::String(entries_json),
        )]));
    }
}

fn handle_readlink(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let target = match readlink_at(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        return resp_ok(BTreeMap::from([(
            "target".to_string(),
            FlatValue::String(target),
        )]));
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let target = match fs::read_link(&path) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let target = match target.into_os_string().into_string() {
            Ok(value) => value,
            Err(_) => {
                return resp_err(
                    FluxonFsRpcErrorKind::InvalidArgument,
                    format!("readlink target is not valid UTF-8: {}", relpath),
                    None,
                );
            }
        };
        return resp_ok(BTreeMap::from([(
            "target".to_string(),
            FlatValue::String(target),
        )]));
    }
}

fn handle_setxattr(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let name = match require_str(&payload, "name") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let value = match payload.get("value") {
        Some(FlatValue::Bytes(value)) => value.clone(),
        _ => {
            return resp_err(
                FluxonFsRpcErrorKind::InvalidArgument,
                "value must be bytes".to_string(),
                None,
            );
        }
    };
    let flags = match require_i64(&payload, "flags") {
        Ok(value) => value,
        Err(resp) => return resp,
    };

    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = setxattr_at(
            &state.export_root_dir,
            relpath.as_str(),
            name.as_str(),
            value.as_slice(),
            flags,
        ) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }

    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = (path, name, value, flags);
        resp_err(
            FluxonFsRpcErrorKind::Internal,
            "setxattr is not implemented on non-unix".to_string(),
            None,
        )
    }
}

fn handle_getxattr(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let name = match require_str(&payload, "name") {
        Ok(value) => value,
        Err(resp) => return resp,
    };

    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let data =
            match getxattr_at(&state.export_root_dir, relpath.as_str(), name.as_str()) {
                Ok(value) => value,
                Err(err) => return resp_err_io(err),
            };
        return resp_ok(BTreeMap::from([("data".to_string(), FlatValue::Bytes(data))]));
    }

    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = (path, name);
        resp_err(
            FluxonFsRpcErrorKind::Internal,
            "getxattr is not implemented on non-unix".to_string(),
            None,
        )
    }
}

fn handle_listxattr(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };

    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let data = match listxattr_at(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        return resp_ok(BTreeMap::from([("data".to_string(), FlatValue::Bytes(data))]));
    }

    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = path;
        resp_err(
            FluxonFsRpcErrorKind::Internal,
            "listxattr is not implemented on non-unix".to_string(),
            None,
        )
    }
}

fn handle_removexattr(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let name = match require_str(&payload, "name") {
        Ok(value) => value,
        Err(resp) => return resp,
    };

    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = removexattr_at(&state.export_root_dir, relpath.as_str(), name.as_str()) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }

    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = (path, name);
        resp_err(
            FluxonFsRpcErrorKind::Internal,
            "removexattr is not implemented on non-unix".to_string(),
            None,
        )
    }
}

fn handle_read_chunk(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let offset = match require_i64(&payload, "offset") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let length = match require_i64(&payload, "length") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if offset < 0 || length < 0 || (length as usize) > CHUNK_BYTES {
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "offset/length out of range".to_string(),
            None,
        );
    }
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let mut file = match open_at(
            &state.export_root_dir,
            relpath.as_str(),
            libc::O_RDONLY | libc::O_CLOEXEC,
            None,
        ) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let metadata = match file.metadata() {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let file_size = metadata.len() as i64;
        if offset > file_size {
            return resp_ok(BTreeMap::from([(
                "data".to_string(),
                FlatValue::Bytes(Vec::new()),
            )]));
        }
        let to_read = std::cmp::min(length, file_size - offset) as usize;
        if let Err(err) = file.seek(SeekFrom::Start(offset as u64)) {
            return resp_err_io(err);
        }
        let mut data = vec![0u8; to_read];
        if let Err(err) = file.read_exact(&mut data) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::from([(
            "data".to_string(),
            FlatValue::Bytes(data),
        )]));
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let mut file = match fs::File::open(&path) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let metadata = match file.metadata() {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let file_size = metadata.len() as i64;
        if offset > file_size {
            return resp_ok(BTreeMap::from([(
                "data".to_string(),
                FlatValue::Bytes(Vec::new()),
            )]));
        }
        let to_read = std::cmp::min(length, file_size - offset) as usize;
        if let Err(err) = file.seek(SeekFrom::Start(offset as u64)) {
            return resp_err_io(err);
        }
        let mut data = vec![0u8; to_read];
        if let Err(err) = file.read_exact(&mut data) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::from([(
            "data".to_string(),
            FlatValue::Bytes(data),
        )]));
    }
}

fn handle_write_chunk(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let offset = match require_i64(&payload, "offset") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if offset < 0 {
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "offset must be non-negative".to_string(),
            None,
        );
    }
    let data = match payload.get("data") {
        Some(FlatValue::Bytes(value)) => value.clone(),
        _ => {
            return resp_err(
                FluxonFsRpcErrorKind::InvalidArgument,
                "data must be bytes".to_string(),
                None,
            );
        }
    };
    if data.len() > CHUNK_BYTES {
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "chunk too large".to_string(),
            None,
        );
    }
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let mut file = match open_at(
            &state.export_root_dir,
            relpath.as_str(),
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC,
            Some(0o666),
        ) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        if let Err(err) = file.seek(SeekFrom::Start(offset as u64)) {
            return resp_err_io(err);
        }
        if let Err(err) = file.write_all(&data) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let mut file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
        {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        if let Err(err) = file.seek(SeekFrom::Start(offset as u64)) {
            return resp_err_io(err);
        }
        if let Err(err) = file.write_all(&data) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_truncate(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let size = match require_i64(&payload, "size") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if size < 0 {
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "size must be non-negative".to_string(),
            None,
        );
    }
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let file = match open_at(
            &state.export_root_dir,
            relpath.as_str(),
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC,
            Some(0o666),
        ) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        if let Err(err) = file.set_len(size as u64) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
        {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        if let Err(err) = file.set_len(size as u64) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_fallocate(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let mode = match require_i64(&payload, "mode") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let mode = match i32::try_from(mode) {
        Ok(value) => value,
        Err(_) => {
            return resp_err(
                FluxonFsRpcErrorKind::InvalidArgument,
                format!("fallocate mode out of range: {}", mode),
                None,
            );
        }
    };
    let offset = match require_i64(&payload, "offset") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let length = match require_i64(&payload, "length") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if offset < 0 {
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "offset must be non-negative".to_string(),
            None,
        );
    }
    if length < 0 {
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "length must be non-negative".to_string(),
            None,
        );
    }
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let file = match open_at(
            &state.export_root_dir,
            relpath.as_str(),
            libc::O_RDWR | libc::O_CLOEXEC,
            None,
        ) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let rc = unsafe { libc::fallocate(file.as_raw_fd(), mode, offset, length) };
        if rc != 0 {
            return resp_err_io(std::io::Error::last_os_error());
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = (mode, offset, length);
        let _file = match fs::OpenOptions::new().read(true).write(true).open(&path) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "fallocate not supported on non-unix".to_string(),
            None,
        );
    }
}

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct MockFiemapHeader {
    fm_start: u64,
    fm_length: u64,
    fm_flags: u32,
    fm_mapped_extents: u32,
    fm_extent_count: u32,
    fm_reserved: u32,
}

#[cfg(target_os = "linux")]
#[repr(C)]
#[derive(Clone, Copy)]
struct MockFiemapExtent {
    fe_logical: u64,
    fe_physical: u64,
    fe_length: u64,
    fe_reserved64: [u64; 2],
    fe_flags: u32,
    fe_reserved: [u32; 3],
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
    std::mem::size_of::<MockFiemapHeader>() as u32,
);

#[cfg(target_os = "linux")]
fn prepare_fiemap_ioctl_buffer(
    request: &[u8],
    out_size: i64,
) -> Result<Vec<u8>, FluxonRpcKvError> {
    if request.len() < std::mem::size_of::<MockFiemapHeader>() {
        return Err(FluxonRpcKvError::InvalidArgument {
            detail: format!(
                "fiemap request too small: got {} need at least {}",
                request.len(),
                std::mem::size_of::<MockFiemapHeader>()
            ),
        });
    }
    let header = unsafe { std::ptr::read_unaligned(request.as_ptr().cast::<MockFiemapHeader>()) };
    let extent_bytes = usize::try_from(header.fm_extent_count)
        .unwrap_or(usize::MAX)
        .saturating_mul(std::mem::size_of::<MockFiemapExtent>());
    let want_len = std::mem::size_of::<MockFiemapHeader>()
        .saturating_add(extent_bytes)
        .max(request.len())
        .max(out_size.max(0) as usize);
    let mut buf = vec![0u8; want_len];
    buf[..request.len()].copy_from_slice(request);
    Ok(buf)
}

fn handle_fiemap(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let request = match payload.get("data") {
        Some(FlatValue::Bytes(value)) => value.clone(),
        _ => {
            return resp_err(
                FluxonFsRpcErrorKind::InvalidArgument,
                "fiemap request missing data bytes".to_string(),
                None,
            );
        }
    };
    let out_size = match require_i64(&payload, "out_size") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if out_size < 0 {
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "fiemap out_size must be non-negative".to_string(),
            None,
        );
    }
    #[cfg(target_os = "linux")]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let mut buf = match prepare_fiemap_ioctl_buffer(request.as_slice(), out_size) {
            Ok(value) => value,
            Err(err) => {
                return resp_err(
                    FluxonFsRpcErrorKind::InvalidArgument,
                    err.to_string(),
                    None,
                );
            }
        };
        let file = match fs::OpenOptions::new().read(true).open(&path) {
            Ok(value) => value,
            Err(err) => return resp_err_io(err),
        };
        let rc = unsafe {
            libc::ioctl(
                file.as_raw_fd(),
                FS_IOC_FIEMAP_CMD as libc::c_ulong,
                buf.as_mut_ptr(),
            )
        };
        if rc != 0 {
            return resp_err_io(std::io::Error::last_os_error());
        }
        return resp_ok(BTreeMap::from([(
            "data".to_string(),
            FlatValue::Bytes(buf),
        )]));
    }
    #[cfg(not(target_os = "linux"))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = (path, request, out_size);
        return resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "fiemap not supported on non-linux".to_string(),
            None,
        );
    }
}

fn handle_mkdir(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let mode = match require_i64(&payload, "mode") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = mkdir_at(&state.export_root_dir, relpath.as_str(), mode) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = mode;
        if let Err(err) = fs::create_dir(&path) {
            return resp_err_io(err);
        }
        resp_ok(BTreeMap::new())
    }
}

fn handle_mkfifo(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let mode = match require_i64(&payload, "mode") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = mknod_at_path(&state.export_root_dir, relpath.as_str(), mode, 0) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = create_fifo_at_path(&path, mode) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_mknod(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let mode = match require_i64(&payload, "mode") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let rdev = match require_i64(&payload, "rdev") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) =
            mknod_at_path(&state.export_root_dir, relpath.as_str(), mode, rdev)
        {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = create_node_at_path(&path, mode, rdev) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_rmdir(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = unlink_at(&state.export_root_dir, relpath.as_str(), true) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = fs::remove_dir(&path) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_unlink(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = unlink_at(&state.export_root_dir, relpath.as_str(), false) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = fs::remove_file(&path) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_link(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let src_relpath = match require_str(&payload, "src_relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let dst_relpath = match require_str(&payload, "dst_relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let src_relpath = match normalize_relpath(src_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let dst_relpath = match normalize_relpath(dst_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) =
            link_at(&state.export_root_dir, src_relpath.as_str(), dst_relpath.as_str())
        {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let src = match safe_join_root(&state.export_root_dir, src_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let dst = match safe_join_root(&state.export_root_dir, dst_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = fs::hard_link(&src, &dst) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_symlink(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let target = match require_str(&payload, "target") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = symlink_at(&state.export_root_dir, target.as_str(), relpath.as_str()) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = (target, path);
        resp_err(
            FluxonFsRpcErrorKind::Internal,
            "symlink is not implemented on non-unix".to_string(),
            None,
        )
    }
}

fn handle_rename(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let src_relpath = match require_str(&payload, "src_relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let dst_relpath = match require_str(&payload, "dst_relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let src_relpath = match normalize_relpath(src_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let dst_relpath = match normalize_relpath(dst_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) =
            rename_at(&state.export_root_dir, src_relpath.as_str(), dst_relpath.as_str())
        {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let src = match safe_join_root(&state.export_root_dir, src_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let dst = match safe_join_root(&state.export_root_dir, dst_relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = fs::rename(&src, &dst) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_chmod(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let mode = match require_i64(&payload, "mode") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = chmod_at(&state.export_root_dir, relpath.as_str(), mode) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let _ = (path, mode);
        resp_err(
            FluxonFsRpcErrorKind::Internal,
            "chmod is not implemented on non-unix".to_string(),
            None,
        )
    }
}

fn handle_chown(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    handle_chown_impl(state, payload, false)
}

fn handle_lchown(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    handle_chown_impl(state, payload, true)
}

fn handle_chown_impl(state: &FsExportMockState, payload: FlatDict, nofollow: bool) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let uid = match require_i64(&payload, "uid") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    let gid = match require_i64(&payload, "gid") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) =
            chown_at(&state.export_root_dir, relpath.as_str(), uid, gid, nofollow)
        {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        if let Err(err) = set_ownership_at_path(&path, uid, gid, nofollow) {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
}

fn handle_utime(state: &FsExportMockState, payload: FlatDict) -> FlatDict {
    let export = match require_str(&payload, "export") {
        Ok(value) => value,
        Err(resp) => return resp,
    };
    if let Err(resp) = require_export_name(state, export.as_str()) {
        return resp;
    }
    let relpath = match require_str(&payload, "relpath") {
        Ok(value) => value,
        Err(resp) => return resp,
    };

    #[cfg(unix)]
    {
        let relpath = match normalize_relpath(relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let atime_ns = match parse_utime_ns_field(&payload, "atime_ns") {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let mtime_ns = match parse_utime_ns_field(&payload, "mtime_ns") {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let nofollow = match payload.get("nofollow") {
            Some(FlatValue::Bool(value)) => *value,
            Some(_) => {
                return resp_err(
                    FluxonFsRpcErrorKind::InvalidArgument,
                    "nofollow must be bool".to_string(),
                    None,
                );
            }
            None => false,
        };
        if let Err(err) =
            utime_at(&state.export_root_dir, relpath.as_str(), atime_ns, mtime_ns, nofollow)
        {
            return resp_err_io(err);
        }
        return resp_ok(BTreeMap::new());
    }
    #[cfg(not(unix))]
    {
        let path = match safe_join_root(&state.export_root_dir, relpath.as_str()) {
            Ok(value) => value,
            Err(resp) => return resp,
        };
        let atime_ns = payload.get("atime_ns");
        let mtime_ns = payload.get("mtime_ns");
        let _ = (path, atime_ns, mtime_ns);
        resp_err(
            FluxonFsRpcErrorKind::Internal,
            "utime is not implemented on non-unix".to_string(),
            None,
        )
    }
}

fn require_export_name(state: &FsExportMockState, export_name: &str) -> Result<(), FlatDict> {
    if export_name == state.export_name {
        return Ok(());
    }
    Err(resp_err(
        FluxonFsRpcErrorKind::InvalidArgument,
        format!("unknown export: {}", export_name),
        None,
    ))
}

fn normalize_relpath(relpath: &str) -> Result<String, FlatDict> {
    let mut normalized_relpath = relpath.replace('\\', "/");
    while normalized_relpath.starts_with('/') {
        normalized_relpath = normalized_relpath[1..].to_string();
    }
    let parts: Vec<&str> = normalized_relpath
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    if parts.iter().any(|part| *part == "..") {
        return Err(resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "relpath contains '..'".to_string(),
            None,
        ));
    }
    if parts.is_empty() {
        return Ok(".".to_string());
    }
    Ok(parts.join("/"))
}

#[cfg(unix)]
fn relpath_cstring(relpath: &str) -> std::io::Result<std::ffi::CString> {
    std::ffi::CString::new(relpath.as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))
}

#[cfg(unix)]
fn fd_view_path(file: &fs::File) -> PathBuf {
    use std::os::fd::AsRawFd;

    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

#[cfg(unix)]
fn stat_at(export_root_dir: &fs::File, relpath: &str, follow_symlink: bool) -> std::io::Result<libc::stat> {
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;

    let c_relpath = relpath_cstring(relpath)?;
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    let flags = if follow_symlink {
        0
    } else {
        libc::AT_SYMLINK_NOFOLLOW
    };
    let rc = unsafe {
        libc::fstatat(
            export_root_dir.as_raw_fd(),
            c_relpath.as_ptr(),
            stat.as_mut_ptr(),
            flags,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { stat.assume_init() })
}

#[cfg(unix)]
fn open_at(
    export_root_dir: &fs::File,
    relpath: &str,
    flags: i32,
    mode: Option<libc::mode_t>,
) -> std::io::Result<fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let c_relpath = relpath_cstring(relpath)?;
    let rc = unsafe {
        libc::openat(
            export_root_dir.as_raw_fd(),
            c_relpath.as_ptr(),
            flags,
            mode.unwrap_or(0),
        )
    };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { fs::File::from_raw_fd(rc) })
}

#[cfg(unix)]
fn mkdir_at(export_root_dir: &fs::File, relpath: &str, mode: i64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    if mode < 0 {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    }
    let c_relpath = relpath_cstring(relpath)?;
    let rc =
        unsafe { libc::mkdirat(export_root_dir.as_raw_fd(), c_relpath.as_ptr(), mode as libc::mode_t) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    chmod_at(export_root_dir, relpath, mode & 0o7777)?;
    Ok(())
}

#[cfg(unix)]
fn mknod_at_path(
    export_root_dir: &fs::File,
    relpath: &str,
    mode: i64,
    rdev: i64,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    if mode < 0 || rdev < 0 {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    }
    let c_relpath = relpath_cstring(relpath)?;
    let rc = unsafe {
        libc::mknodat(
            export_root_dir.as_raw_fd(),
            c_relpath.as_ptr(),
            mode as libc::mode_t,
            rdev as libc::dev_t,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    chmod_at(export_root_dir, relpath, mode & 0o7777)?;
    Ok(())
}

#[cfg(unix)]
fn unlink_at(export_root_dir: &fs::File, relpath: &str, dir: bool) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let c_relpath = relpath_cstring(relpath)?;
    let flags = if dir { libc::AT_REMOVEDIR } else { 0 };
    let rc =
        unsafe { libc::unlinkat(export_root_dir.as_raw_fd(), c_relpath.as_ptr(), flags) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn link_at(export_root_dir: &fs::File, src_relpath: &str, dst_relpath: &str) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let c_src = relpath_cstring(src_relpath)?;
    let c_dst = relpath_cstring(dst_relpath)?;
    let rc = unsafe {
        libc::linkat(
            export_root_dir.as_raw_fd(),
            c_src.as_ptr(),
            export_root_dir.as_raw_fd(),
            c_dst.as_ptr(),
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn symlink_at(export_root_dir: &fs::File, target: &str, relpath: &str) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let c_target = std::ffi::CString::new(target.as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let c_relpath = relpath_cstring(relpath)?;
    let rc =
        unsafe { libc::symlinkat(c_target.as_ptr(), export_root_dir.as_raw_fd(), c_relpath.as_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn rename_at(export_root_dir: &fs::File, src_relpath: &str, dst_relpath: &str) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let c_src = relpath_cstring(src_relpath)?;
    let c_dst = relpath_cstring(dst_relpath)?;
    let rc = unsafe {
        libc::renameat(
            export_root_dir.as_raw_fd(),
            c_src.as_ptr(),
            export_root_dir.as_raw_fd(),
            c_dst.as_ptr(),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn chmod_at(export_root_dir: &fs::File, relpath: &str, mode: i64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    if mode < 0 {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    }
    if relpath == "." {
        let rc = unsafe { libc::fchmod(export_root_dir.as_raw_fd(), mode as libc::mode_t) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }
    let c_relpath = relpath_cstring(relpath)?;
    let rc = unsafe {
        libc::fchmodat(
            export_root_dir.as_raw_fd(),
            c_relpath.as_ptr(),
            mode as libc::mode_t,
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn chown_at(
    export_root_dir: &fs::File,
    relpath: &str,
    uid: i64,
    gid: i64,
    nofollow: bool,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let c_relpath = relpath_cstring(relpath)?;
    let uid = chown_id_from_i64(uid)?;
    let gid = chown_id_from_i64(gid)?;
    let flags = if nofollow {
        libc::AT_SYMLINK_NOFOLLOW
    } else {
        0
    };
    let rc = unsafe {
        libc::fchownat(
            export_root_dir.as_raw_fd(),
            c_relpath.as_ptr(),
            uid,
            gid,
            flags,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn readlink_at(export_root_dir: &fs::File, relpath: &str) -> std::io::Result<String> {
    use std::os::fd::AsRawFd;

    let c_relpath = relpath_cstring(relpath)?;
    let mut buf = vec![0u8; 256];
    loop {
        let rc = unsafe {
            libc::readlinkat(
                export_root_dir.as_raw_fd(),
                c_relpath.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
            )
        };
        if rc < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let rc = rc as usize;
        if rc < buf.len() {
            buf.truncate(rc);
            return String::from_utf8(buf).map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        buf.resize(buf.len().saturating_mul(2), 0);
    }
}

#[cfg(unix)]
fn setxattr_at(
    export_root_dir: &fs::File,
    relpath: &str,
    name: &str,
    value: &[u8],
    flags: i64,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let c_relpath = relpath_cstring(relpath)?;
    let c_name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let flags = i32::try_from(flags).map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let proc_path = fd_view_path(export_root_dir).join(relpath);
    let c_proc_path = std::ffi::CString::new(proc_path.to_string_lossy().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let _ = export_root_dir.as_raw_fd();
    let rc = unsafe {
        libc::lsetxattr(
            c_proc_path.as_ptr(),
            c_name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            flags,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let _ = c_relpath;
    Ok(())
}

#[cfg(unix)]
fn getxattr_at(export_root_dir: &fs::File, relpath: &str, name: &str) -> std::io::Result<Vec<u8>> {
    let proc_path = fd_view_path(export_root_dir).join(relpath);
    let c_proc_path = std::ffi::CString::new(proc_path.to_string_lossy().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let c_name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let size = unsafe { libc::lgetxattr(c_proc_path.as_ptr(), c_name.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let size = usize::try_from(size).map_err(|_| std::io::Error::from_raw_os_error(libc::EOVERFLOW))?;
    let mut data = vec![0u8; size];
    let rc = unsafe {
        libc::lgetxattr(
            c_proc_path.as_ptr(),
            c_name.as_ptr(),
            data.as_mut_ptr().cast(),
            data.len(),
        )
    };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let rc = usize::try_from(rc).map_err(|_| std::io::Error::from_raw_os_error(libc::EOVERFLOW))?;
    data.truncate(rc);
    Ok(data)
}

#[cfg(unix)]
fn listxattr_at(export_root_dir: &fs::File, relpath: &str) -> std::io::Result<Vec<u8>> {
    let proc_path = fd_view_path(export_root_dir).join(relpath);
    let c_proc_path = std::ffi::CString::new(proc_path.to_string_lossy().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let size = unsafe { libc::llistxattr(c_proc_path.as_ptr(), std::ptr::null_mut(), 0) };
    if size < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let size = usize::try_from(size).map_err(|_| std::io::Error::from_raw_os_error(libc::EOVERFLOW))?;
    let mut data = vec![0u8; size];
    let rc = unsafe { libc::llistxattr(c_proc_path.as_ptr(), data.as_mut_ptr().cast(), data.len()) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let rc = usize::try_from(rc).map_err(|_| std::io::Error::from_raw_os_error(libc::EOVERFLOW))?;
    data.truncate(rc);
    Ok(data)
}

#[cfg(unix)]
fn removexattr_at(export_root_dir: &fs::File, relpath: &str, name: &str) -> std::io::Result<()> {
    let proc_path = fd_view_path(export_root_dir).join(relpath);
    let c_proc_path = std::ffi::CString::new(proc_path.to_string_lossy().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let c_name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let rc = unsafe { libc::lremovexattr(c_proc_path.as_ptr(), c_name.as_ptr()) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn utime_at(
    export_root_dir: &fs::File,
    relpath: &str,
    atime_ns: Option<i64>,
    mtime_ns: Option<i64>,
    nofollow: bool,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let c_relpath = relpath_cstring(relpath)?;
    let flags = if nofollow {
        libc::AT_SYMLINK_NOFOLLOW
    } else {
        0
    };
    let rc = if atime_ns.is_none() && mtime_ns.is_none() {
        unsafe {
            libc::utimensat(
                export_root_dir.as_raw_fd(),
                c_relpath.as_ptr(),
                std::ptr::null(),
                flags,
            )
        }
    } else {
        let times = [
            atime_ns.map(timespec_from_ns).unwrap_or_else(utime_omit_timespec),
            mtime_ns.map(timespec_from_ns).unwrap_or_else(utime_omit_timespec),
        ];
        unsafe {
            libc::utimensat(
                export_root_dir.as_raw_fd(),
                c_relpath.as_ptr(),
                times.as_ptr(),
                flags,
            )
        }
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn stat_size(stat: &libc::stat) -> i64 {
    stat.st_size
}

#[cfg(unix)]
fn stat_ctime_ns(stat: &libc::stat) -> i64 {
    unix_time_parts_to_ns(stat.st_ctime, stat.st_ctime_nsec)
}

#[cfg(unix)]
fn stat_atime_ns(stat: &libc::stat) -> i64 {
    unix_time_parts_to_ns(stat.st_atime, stat.st_atime_nsec)
}

#[cfg(unix)]
fn stat_mtime_ns(stat: &libc::stat) -> i64 {
    unix_time_parts_to_ns(stat.st_mtime, stat.st_mtime_nsec)
}

#[cfg(unix)]
fn stat_mode(stat: &libc::stat) -> i64 {
    stat.st_mode as i64
}

#[cfg(unix)]
fn stat_uid(stat: &libc::stat) -> i64 {
    stat.st_uid as i64
}

#[cfg(unix)]
fn stat_gid(stat: &libc::stat) -> i64 {
    stat.st_gid as i64
}

#[cfg(unix)]
fn stat_nlink(stat: &libc::stat) -> i64 {
    (stat.st_nlink as u64).min(i64::MAX as u64) as i64
}

#[cfg(unix)]
fn stat_ino(stat: &libc::stat) -> i64 {
    (stat.st_ino as u64).min(i64::MAX as u64) as i64
}

#[cfg(unix)]
fn stat_rdev(stat: &libc::stat) -> i64 {
    (stat.st_rdev as u64).min(i64::MAX as u64) as i64
}

#[cfg(unix)]
fn stat_is_file(stat: &libc::stat) -> bool {
    (stat.st_mode & libc::S_IFMT) == libc::S_IFREG
}

#[cfg(unix)]
fn stat_is_dir(stat: &libc::stat) -> bool {
    (stat.st_mode & libc::S_IFMT) == libc::S_IFDIR
}

fn safe_join_root(export_root_dir: &fs::File, relpath: &str) -> Result<PathBuf, FlatDict> {
    #[cfg(unix)]
    use std::os::fd::AsRawFd;

    let mut normalized_relpath = relpath.replace('\\', "/");
    while normalized_relpath.starts_with('/') {
        normalized_relpath = normalized_relpath[1..].to_string();
    }
    let parts: Vec<&str> = normalized_relpath
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    if parts.iter().any(|part| *part == "..") {
        return Err(resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            "relpath contains '..'".to_string(),
            None,
        ));
    }
    let fd_path = PathBuf::from(format!("/proc/self/fd/{}", export_root_dir.as_raw_fd()));
    if parts.is_empty() {
        return Ok(fd_path.join("."));
    }
    Ok(fd_path.join(parts.join("/")))
}

fn metadata_mtime_ns(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return unix_time_parts_to_ns(metadata.mtime(), metadata.mtime_nsec());
    }
    #[cfg(not(unix))]
    {
        metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| (value.as_nanos() as i128).min(i64::MAX as i128) as i64)
            .unwrap_or(0)
    }
}

fn metadata_atime_ns(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return unix_time_parts_to_ns(metadata.atime(), metadata.atime_nsec());
    }
    #[cfg(not(unix))]
    {
        metadata
            .accessed()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| (value.as_nanos() as i128).min(i64::MAX as i128) as i64)
            .unwrap_or(0)
    }
}

fn metadata_ctime_ns(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return unix_time_parts_to_ns(metadata.ctime(), metadata.ctime_nsec());
    }
    #[cfg(not(unix))]
    {
        metadata
            .created()
            .ok()
            .or_else(|| metadata.modified().ok())
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| (value.as_nanos() as i128).min(i64::MAX as i128) as i64)
            .unwrap_or(0)
    }
}

fn unix_time_parts_to_ns(seconds: i64, nanos: i64) -> i64 {
    if seconds < 0 || nanos < 0 {
        return 0;
    }
    ((seconds as i128) * 1_000_000_000 + nanos as i128).min(i64::MAX as i128) as i64
}

fn metadata_uid(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return metadata.uid() as i64;
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn metadata_gid(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return metadata.gid() as i64;
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn metadata_nlink(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return metadata.nlink().min(i64::MAX as u64) as i64;
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        1
    }
}

fn metadata_ino(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return metadata.ino().min(i64::MAX as u64) as i64;
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn metadata_rdev(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return metadata.rdev().min(i64::MAX as u64) as i64;
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

#[cfg(unix)]
fn parse_utime_ns_field(payload: &FlatDict, key: &str) -> Result<Option<i64>, FlatDict> {
    match payload.get(key) {
        None => Ok(None),
        Some(FlatValue::Int64(value)) if *value >= 0 => Ok(Some(*value)),
        Some(FlatValue::Int64(_)) => Err(resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            format!("{key} must be non-negative"),
            None,
        )),
        Some(_) => Err(resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            format!("{key} must be int64"),
            None,
        )),
    }
}

#[cfg(unix)]
fn timespec_from_ns(value: i64) -> libc::timespec {
    libc::timespec {
        tv_sec: (value / 1_000_000_000) as libc::time_t,
        tv_nsec: (value % 1_000_000_000) as libc::c_long,
    }
}

#[cfg(unix)]
fn utime_omit_timespec() -> libc::timespec {
    libc::timespec {
        tv_sec: 0,
        tv_nsec: libc::UTIME_OMIT as libc::c_long,
    }
}

fn metadata_mode(metadata: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.mode() as i64
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn create_fifo_at_path(path: &Path, mode: i64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::ffi::CString;

        if mode < 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let c_path = CString::new(path.to_string_lossy().as_bytes())
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        let mode = if (mode as u32) & libc::S_IFMT == libc::S_IFIFO {
            mode as libc::mode_t
        } else {
            (mode as libc::mode_t) | libc::S_IFIFO
        };
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), mode) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
    }
}

fn create_node_at_path(path: &Path, mode: i64, rdev: i64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::ffi::CString;

        if mode < 0 || rdev < 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let c_path = CString::new(path.to_string_lossy().as_bytes())
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        let rc =
            unsafe { libc::mknod(c_path.as_ptr(), mode as libc::mode_t, rdev as libc::dev_t) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = (path, mode, rdev);
        Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
    }
}

fn set_ownership_at_path(path: &Path, uid: i64, gid: i64, nofollow: bool) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::ffi::CString;

        let uid = chown_id_from_i64(uid)?;
        let gid = chown_id_from_i64(gid)?;
        let c_path = CString::new(path.to_string_lossy().as_bytes())
            .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        let rc = if nofollow {
            unsafe { libc::lchown(c_path.as_ptr(), uid, gid) }
        } else {
            unsafe { libc::chown(c_path.as_ptr(), uid, gid) }
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        let _ = (path, uid, gid, nofollow);
        Err(std::io::Error::from_raw_os_error(libc::ENOTSUP))
    }
}

#[cfg(unix)]
fn chown_id_from_i64(value: i64) -> std::io::Result<u32> {
    if value == -1 {
        return Ok(u32::MAX);
    }
    if value < 0 || value > u32::MAX as i64 {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(value as u32)
}

fn resp_ok(mut extra: FlatDict) -> FlatDict {
    extra.insert("ok".to_string(), FlatValue::Bool(true));
    extra
}

fn resp_err(kind: FluxonFsRpcErrorKind, detail: String, errno: Option<i32>) -> FlatDict {
    let mut payload = BTreeMap::from([
        ("ok".to_string(), FlatValue::Bool(false)),
        ("err".to_string(), FlatValue::String(detail)),
        (
            FLUXON_FS_RPC_ERR_KIND_KEY.to_string(),
            FlatValue::Int64(kind.as_i64()),
        ),
    ]);
    if let Some(errno) = errno {
        payload.insert("errno".to_string(), FlatValue::Int64(i64::from(errno)));
    }
    payload
}

fn resp_err_io(err: std::io::Error) -> FlatDict {
    let errno = err.raw_os_error().unwrap_or(libc::EIO);
    resp_err(FluxonFsRpcErrorKind::Os, err.to_string(), Some(errno))
}

fn require_str(payload: &FlatDict, key: &str) -> Result<String, FlatDict> {
    match payload.get(key) {
        Some(FlatValue::String(value)) => {
            if value.is_empty() {
                return Err(resp_err(
                    FluxonFsRpcErrorKind::InvalidArgument,
                    format!("{} must be non-empty", key),
                    None,
                ));
            }
            Ok(value.clone())
        }
        _ => Err(resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            format!("{} must be string", key),
            None,
        )),
    }
}

fn require_i64(payload: &FlatDict, key: &str) -> Result<i64, FlatDict> {
    match payload.get(key) {
        Some(FlatValue::Int64(value)) => Ok(*value),
        _ => Err(resp_err(
            FluxonFsRpcErrorKind::InvalidArgument,
            format!("{} must be int64", key),
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use fluxon_fs_core::config::export_rpc_paths_for_export_name_v1;

    use super::{FlatDict, FlatValue, FluxonInProcessFsExportMock, FluxonInProcessRpcKvApi};

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
                "fluxon_fs_fuse_draft_{}_{}_{}",
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

    #[test]
    fn kv_round_trip_in_process() {
        let api = FluxonInProcessRpcKvApi::new();
        let kv = api.kv_client();

        let value = FlatDict::from([("answer".to_string(), FlatValue::Int64(42))]);
        kv.put("/demo/key", value.clone()).unwrap();

        assert!(kv.is_exist("/demo/key").unwrap());
        assert_eq!(kv.get("/demo/key").unwrap(), Some(value));

        kv.delete("/demo/key").unwrap();
        assert!(!kv.is_exist("/demo/key").unwrap());
        assert_eq!(kv.get("/demo/key").unwrap(), None);
    }

    #[test]
    fn rpc_register_and_call_in_process() {
        let api = FluxonInProcessRpcKvApi::new();
        let rpc_server = api.rpc_server();
        let rpc_client = api.rpc_client();

        rpc_server
            .register(
                "/demo/echo",
                Arc::new(move |from_node, mut payload: FlatDict| {
                    payload.insert("from".to_string(), FlatValue::String(from_node));
                    Ok(payload)
                }),
            )
            .unwrap();

        let resp = rpc_client
            .call(
                "node-a",
                "/demo/echo",
                FlatDict::from([("ping".to_string(), FlatValue::Bool(true))]),
                None,
            )
            .unwrap();

        assert_eq!(
            resp.get("from"),
            Some(&FlatValue::String("node-a".to_string()))
        );
        assert_eq!(resp.get("ping"), Some(&FlatValue::Bool(true)));
    }

    #[test]
    fn export_root_must_exist_before_mock_registration() {
        let api = FluxonInProcessRpcKvApi::new();
        let temp = TestDir::new("missing_root");
        let missing = format!("{}/missing", temp.path);

        let err = super::FluxonInProcessFsExportMock::new(
            api,
            "demo".to_string(),
            missing,
        )
        .unwrap_err();

        match err {
            super::FluxonRpcKvError::InvalidArgument { detail } => {
                assert!(detail.contains("canonicalize export root failed"));
            }
            other => panic!("unexpected error: {}", other),
        }
    }

    #[test]
    fn export_mock_utimens_supports_omit_and_stat_reports_atime() {
        let api = FluxonInProcessRpcKvApi::new();
        let rpc_client = api.rpc_client();
        let rpc_paths = export_rpc_paths_for_export_name_v1("demo");
        let temp = TestDir::new("utimens");
        let export_root = temp.join("export");
        fs::create_dir_all(&export_root).unwrap();
        fs::write(temp.join("export/file.txt"), b"time").unwrap();
        let _mock = FluxonInProcessFsExportMock::new(
            api.clone(),
            "demo".to_string(),
            export_root,
        )
        .unwrap();

        let stat_payload = FlatDict::from([
            ("export".to_string(), FlatValue::String("demo".to_string())),
            ("relpath".to_string(), FlatValue::String("file.txt".to_string())),
        ]);
        let stat_before = rpc_client
            .call("", rpc_paths.stat.as_str(), stat_payload.clone(), None)
            .unwrap();
        let mtime_before = match stat_before.get("mtime_ns") {
            Some(FlatValue::Int64(value)) => *value,
            other => panic!("unexpected mtime response: {:?}", other),
        };
        assert!(matches!(stat_before.get("atime_ns"), Some(FlatValue::Int64(_))));

        rpc_client
            .call(
                "",
                rpc_paths.utime.as_str(),
                FlatDict::from([
                    ("export".to_string(), FlatValue::String("demo".to_string())),
                    ("relpath".to_string(), FlatValue::String("file.txt".to_string())),
                    (
                        "atime_ns".to_string(),
                        FlatValue::Int64(1_900_000_000_000_000_000),
                    ),
                ]),
                None,
            )
            .unwrap();
        let stat_after_atime = rpc_client
            .call("", rpc_paths.stat.as_str(), stat_payload.clone(), None)
            .unwrap();
        assert_eq!(
            stat_after_atime.get("atime_ns"),
            Some(&FlatValue::Int64(1_900_000_000_000_000_000))
        );
        assert_eq!(
            stat_after_atime.get("mtime_ns"),
            Some(&FlatValue::Int64(mtime_before))
        );

        rpc_client
            .call(
                "",
                rpc_paths.utime.as_str(),
                FlatDict::from([
                    ("export".to_string(), FlatValue::String("demo".to_string())),
                    ("relpath".to_string(), FlatValue::String("file.txt".to_string())),
                    (
                        "mtime_ns".to_string(),
                        FlatValue::Int64(2_000_000_000_000_000_000),
                    ),
                ]),
                None,
            )
            .unwrap();
        let stat_after_mtime = rpc_client
            .call("", rpc_paths.stat.as_str(), stat_payload, None)
            .unwrap();
        assert_eq!(
            stat_after_mtime.get("atime_ns"),
            Some(&FlatValue::Int64(1_900_000_000_000_000_000))
        );
        assert_eq!(
            stat_after_mtime.get("mtime_ns"),
            Some(&FlatValue::Int64(2_000_000_000_000_000_000))
        );
    }

    #[test]
    fn export_mock_chmod_supports_root_relpath_dot() {
        #[cfg(not(unix))]
        {
            return;
        }

        let api = FluxonInProcessRpcKvApi::new();
        let rpc_client = api.rpc_client();
        let rpc_paths = export_rpc_paths_for_export_name_v1("demo");
        let temp = TestDir::new("chmod_root");
        let export_root = temp.join("export");
        fs::create_dir_all(&export_root).unwrap();
        let _mock = FluxonInProcessFsExportMock::new(
            api.clone(),
            "demo".to_string(),
            export_root.clone(),
        )
        .unwrap();

        rpc_client
            .call(
                "",
                rpc_paths.chmod.as_str(),
                FlatDict::from([
                    ("export".to_string(), FlatValue::String("demo".to_string())),
                    ("relpath".to_string(), FlatValue::String(".".to_string())),
                    ("mode".to_string(), FlatValue::Int64(0o777)),
                ]),
                None,
            )
            .unwrap();

        let metadata = fs::metadata(export_root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            assert_eq!(metadata.mode() & 0o7777, 0o777);
        }
    }
}
