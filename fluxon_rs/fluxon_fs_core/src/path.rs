use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error("empty relpath")]
    Empty,
    #[error("invalid relpath: {detail}")]
    Invalid { detail: String },
}

/// Normalize a user-provided relative path into a safe POSIX-style relpath.
///
/// Rules:
/// - No leading slash
/// - No `..` segments
/// - Collapses repeated separators and `.`
///
/// This intentionally matches the semantics used by the Python demo receiver.
pub fn safe_relpath(input: &str) -> Result<String, PathError> {
    let mut p = input.replace("\\", "/");
    while p.starts_with('/') {
        p = p[1..].to_string();
    }
    if p.is_empty() || p == "." {
        return Err(PathError::Empty);
    }

    let parts: Vec<&str> = p
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    if parts.iter().any(|s| *s == "..") {
        return Err(PathError::Invalid {
            detail: format!("contains '..': {input:?}"),
        });
    }

    let out = parts.join("/");
    if out.is_empty() {
        return Err(PathError::Empty);
    }
    Ok(out)
}

pub fn safe_abs_dirpath(input: &str) -> Result<String, PathError> {
    let raw = input.trim().replace("\\", "/");
    if raw.is_empty() {
        return Err(PathError::Empty);
    }
    if !raw.starts_with('/') {
        return Err(PathError::Invalid {
            detail: format!("must start with '/': {input:?}"),
        });
    }
    let parts: Vec<&str> = raw
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    if parts.iter().any(|s| *s == "..") {
        return Err(PathError::Invalid {
            detail: format!("contains '..': {input:?}"),
        });
    }
    if parts.is_empty() {
        return Ok("/".to_string());
    }
    Ok(format!("/{}", parts.join("/")))
}

pub fn relpath_from_abs_dirpath(input: &str) -> Result<String, PathError> {
    let dir_abs = safe_abs_dirpath(input)?;
    if dir_abs == "/" {
        return Ok(".".to_string());
    }
    Ok(dir_abs.trim_start_matches('/').to_string())
}
