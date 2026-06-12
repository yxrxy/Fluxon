// English note:
// - These are UI-local response and payload types extracted from ui_ssr.rs.
// - Keep them close together so helper logic and handlers share one compact type surface.

use std::fmt;

#[derive(Debug)]
enum UiHandlerError {
    BadRequest(String),
    Forbidden(String),
    NotFound(String),
    Conflict(String),
    BadGateway(String),
}

impl fmt::Display for UiHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UiHandlerError::BadRequest(msg) => write!(f, "BadRequest: {}", msg),
            UiHandlerError::Forbidden(msg) => write!(f, "Forbidden: {}", msg),
            UiHandlerError::NotFound(msg) => write!(f, "NotFound: {}", msg),
            UiHandlerError::Conflict(msg) => write!(f, "Conflict: {}", msg),
            UiHandlerError::BadGateway(msg) => write!(f, "BadGateway: {}", msg),
        }
    }
}

impl UiHandlerError {
    fn status(&self) -> StatusCode {
        match self {
            UiHandlerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            UiHandlerError::Forbidden(_) => StatusCode::FORBIDDEN,
            UiHandlerError::NotFound(_) => StatusCode::NOT_FOUND,
            UiHandlerError::Conflict(_) => StatusCode::CONFLICT,
            UiHandlerError::BadGateway(_) => StatusCode::BAD_GATEWAY,
        }
    }

    fn message(self) -> String {
        match self {
            UiHandlerError::BadRequest(msg)
            | UiHandlerError::Forbidden(msg)
            | UiHandlerError::NotFound(msg)
            | UiHandlerError::Conflict(msg)
            | UiHandlerError::BadGateway(msg) => msg,
        }
    }

    fn into_text_response(self) -> Response {
        let status = self.status();
        text_response(status, self.message())
    }

    fn into_json_response(self) -> Response {
        let status = self.status();
        json_response(status, &UiApiErrorBody { error: self.message() })
    }
}

#[derive(serde::Serialize)]
struct UiApiErrorBody {
    error: String,
}

#[derive(serde::Serialize)]
struct UiApiOkBody {
    ok: bool,
}

#[derive(serde::Serialize)]
struct UiTransferTaskListBody {
    ok: bool,
    tasks: Vec<UiTransferTaskSnapshot>,
}

#[derive(Clone, serde::Serialize)]
struct UiDirItem {
    name: String,
    mtime_ns: i64,
}

#[derive(Clone, serde::Serialize)]
struct UiFileItem {
    name: String,
    key: String,
    size: i64,
    mtime_ns: i64,
}

#[derive(Clone, serde::Serialize)]
struct UiBrowseProviderItem {
    agent_instance_key: String,
    remote_root_dir_abs: String,
}

#[derive(Clone, serde::Serialize)]
struct UiBrowsePayload {
    bucket: String,
    mount_path: String,
    provider_items: Vec<UiBrowseProviderItem>,
    prefix: String,
    parent_prefix: Option<String>,
    dirs: Vec<UiDirItem>,
    files: Vec<UiFileItem>,
}

#[derive(serde::Serialize)]
struct UiKeyResultBody {
    ok: bool,
    bucket: String,
    key: String,
    prefix: String,
}

#[derive(serde::Serialize)]
struct UiMultipartCreateBody {
    ok: bool,
    key: String,
    prefix: String,
    upload_id: String,
}

#[derive(serde::Serialize)]
struct UiWorkspaceBootstrap {
    initial_tab: UiBrowsePayload,
    available_buckets: Vec<String>,
    transfer_enabled: bool,
}

#[derive(serde::Serialize)]
struct UiMultipartPartBody {
    ok: bool,
    etag: String,
}

#[derive(Clone, serde::Serialize)]
struct UiFsMasterBrowseEntry {
    name: String,
    path_abs: String,
    is_dir: bool,
    is_file: bool,
}

#[derive(serde::Serialize)]
struct UiFsMasterBrowseBody {
    ok: bool,
    agent_instance_key: String,
    dir_abs: String,
    parent_dir_abs: Option<String>,
    entries: Vec<UiFsMasterBrowseEntry>,
}
