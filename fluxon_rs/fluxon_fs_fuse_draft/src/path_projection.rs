use fluxon_fs_core::path::{PathError, safe_abs_dirpath, safe_relpath};

use crate::error::FuseAdapterError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedPath {
    callback_path: String,
    relpath: String,
}

impl ProjectedPath {
    pub fn callback_path(&self) -> &str {
        self.callback_path.as_str()
    }

    pub fn relpath(&self) -> &str {
        self.relpath.as_str()
    }

    pub fn lock_key(&self) -> &str {
        self.relpath()
    }
}

#[derive(Debug, Clone)]
pub struct FuseMountPathProjection {
    mountpoint_dir_abs: String,
}

impl FuseMountPathProjection {
    pub fn new(mountpoint_dir_abs: String) -> Result<Self, FuseAdapterError> {
        let normalized = safe_abs_dirpath(mountpoint_dir_abs.as_str()).map_err(path_err_to_fuse)?;
        Ok(Self {
            mountpoint_dir_abs: normalized,
        })
    }

    pub fn mountpoint_dir_abs(&self) -> &str {
        self.mountpoint_dir_abs.as_str()
    }

    pub fn project(&self, callback_path: &str) -> Result<ProjectedPath, FuseAdapterError> {
        if !callback_path.starts_with('/') {
            return Err(FuseAdapterError::InvalidArgument {
                detail: format!("callback path must be absolute: {}", callback_path),
            });
        }
        if callback_path == "/" {
            return Ok(ProjectedPath {
                callback_path: "/".to_string(),
                relpath: ".".to_string(),
            });
        }
        let rel = safe_relpath(callback_path.trim_start_matches('/')).map_err(path_err_to_fuse)?;
        Ok(ProjectedPath {
            callback_path: callback_path.to_string(),
            relpath: rel,
        })
    }

    pub fn abs_path_for_relpath(&self, relpath: &str) -> Result<String, FuseAdapterError> {
        if relpath == "." {
            return Ok(self.mountpoint_dir_abs.clone());
        }
        let rel = safe_relpath(relpath).map_err(path_err_to_fuse)?;
        Ok(format!(
            "{}/{}",
            self.mountpoint_dir_abs.trim_end_matches('/'),
            rel
        ))
    }
}

fn path_err_to_fuse(err: PathError) -> FuseAdapterError {
    match err {
        PathError::Empty => FuseAdapterError::InvalidArgument {
            detail: "path must not be empty".to_string(),
        },
        PathError::Invalid { detail } => FuseAdapterError::InvalidArgument { detail },
    }
}

#[cfg(test)]
mod tests {
    use super::FuseMountPathProjection;

    #[test]
    fn root_projects_to_dot() {
        let projection = FuseMountPathProjection::new("/tmp/fuse".to_string()).unwrap();
        let projected = projection.project("/").unwrap();
        assert_eq!(projected.relpath(), ".");
    }

    #[test]
    fn rejects_escape_path() {
        let projection = FuseMountPathProjection::new("/tmp/fuse".to_string()).unwrap();
        let err = projection.project("/../../etc/passwd").unwrap_err();
        assert_eq!(err.errno(), libc::EINVAL);
    }
}
