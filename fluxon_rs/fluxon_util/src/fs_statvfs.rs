use std::ffi::CString;
use std::io;
use std::path::Path;

/// Return `(used_bytes, total_bytes)` for the filesystem containing `path` (statvfs).
pub fn statvfs_used_total(path: &str) -> io::Result<(u64, u64)> {
    let c_path = CString::new(path)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut vfs as *mut libc::statvfs) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let bsize = vfs.f_frsize as u64;
    let total = bsize.saturating_mul(vfs.f_blocks as u64);
    let free = bsize.saturating_mul(vfs.f_bfree as u64);
    let used = total.saturating_sub(free);
    Ok((used, total))
}

fn unescape_mountinfo_path(s: &str) -> io::Result<String> {
    // English note:
    // - `/proc/self/mountinfo` escapes whitespace and backslash using octal sequences (`\040`, `\011`, ...).
    // - We decode `\XYZ` (3 octal digits) into a single byte and keep all other bytes verbatim.
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i: usize = 0;
    while i < b.len() {
        if b[i] == b'\\' {
            if i + 3 < b.len()
                && b[i + 1].is_ascii_digit()
                && b[i + 2].is_ascii_digit()
                && b[i + 3].is_ascii_digit()
            {
                let d1 = (b[i + 1] - b'0') as u16;
                let d2 = (b[i + 2] - b'0') as u16;
                let d3 = (b[i + 3] - b'0') as u16;
                let v = d1 * 64 + d2 * 8 + d3;
                out.push((v & 0xff) as u8);
                i += 4;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "mountinfo path is not utf-8"))
}

/// Find the mount point directory (as shown by `df -h`) for an absolute directory path.
///
/// This is derived from `/proc/self/mountinfo` and uses "most specific prefix" matching.
pub fn mount_point_for_abs_dir(path: &str) -> io::Result<String> {
    let p0 = normalize_abs_dir_label(path);
    if p0.is_empty() || !p0.starts_with('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path must be an absolute directory",
        ));
    }

    let raw = std::fs::read_to_string("/proc/self/mountinfo")?;
    let mut best: Option<String> = None;

    for line in raw.lines() {
        let (pre, _post) = match line.split_once(" - ") {
            Some(v) => v,
            None => continue,
        };
        let mut it = pre.split_whitespace();
        // Fields: mount_id parent_id major:minor root mount_point options ...
        let _mount_id = it.next();
        let _parent_id = it.next();
        let _major_minor = it.next();
        let _root = it.next();
        let mount_point_esc = match it.next() {
            Some(v) => v,
            None => continue,
        };
        let mount_point = unescape_mountinfo_path(mount_point_esc)?;
        if mount_point.is_empty() || !mount_point.starts_with('/') {
            continue;
        }
        let mp = normalize_abs_dir_label(&mount_point);
        if mp.is_empty() {
            continue;
        }

        let is_match = if mp == "/" {
            true
        } else if p0 == mp {
            true
        } else {
            p0.starts_with(mp.as_str()) && p0.as_bytes().get(mp.len()).copied() == Some(b'/')
        };
        if !is_match {
            continue;
        }
        match best.as_deref() {
            None => best = Some(mp),
            Some(cur) => {
                if mp.len() > cur.len() {
                    best = Some(mp);
                }
            }
        }
    }

    best.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("mount point not found for dir: {p0}"),
        )
    })
}

/// Normalize an absolute directory path for use as a stable metric label value.
///
/// Contract:
/// - trims whitespace
/// - strips trailing slashes while preserving "/" itself
pub fn normalize_abs_dir_label(p: &str) -> String {
    let s0 = p.trim();
    if s0.is_empty() {
        return String::new();
    }
    // English note:
    // - We only normalize for metric label stability and topology display.
    // - We intentionally do NOT canonicalize (no fs access, no symlink resolution).
    let mut s = s0.to_string();
    while s.len() > 1 && s.ends_with('/') {
        s.pop();
    }
    // Keep this strict: only accept absolute paths (including "/").
    if s != "/" && !Path::new(&s).is_absolute() {
        return String::new();
    }
    s
}
