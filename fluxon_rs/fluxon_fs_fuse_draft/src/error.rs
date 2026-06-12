use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FuseAdapterError {
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

    #[error("bad file descriptor: fh={fh}")]
    BadFileDescriptor { fh: u64 },

    #[error("busy: path={path} detail={detail}")]
    Busy { path: String, detail: String },

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

impl FuseAdapterError {
    pub fn errno(&self) -> i32 {
        match self {
            Self::InvalidArgument { .. } => libc::EINVAL,
            Self::NotFound { .. } => libc::ENOENT,
            Self::AlreadyExists { .. } => libc::EEXIST,
            Self::IsDirectory { .. } => libc::EISDIR,
            Self::NotDirectory { .. } => libc::ENOTDIR,
            Self::BadFileDescriptor { .. } => libc::EBADF,
            Self::Busy { .. } => libc::EBUSY,
            Self::AccessDenied { .. } => libc::EACCES,
            Self::NotImplemented { .. } => libc::ENOSYS,
            Self::Os { errno, .. } => *errno,
        }
    }
}
