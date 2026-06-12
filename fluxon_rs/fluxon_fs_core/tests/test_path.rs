use fluxon_fs_core::path::{PathError, safe_relpath};

#[test]
fn safe_relpath_rejects_empty_and_dot() {
    assert!(matches!(safe_relpath(""), Err(PathError::Empty)));
    assert!(matches!(safe_relpath("."), Err(PathError::Empty)));
    assert!(matches!(safe_relpath("/"), Err(PathError::Empty)));
}

#[test]
fn safe_relpath_rejects_parent_segments() {
    let err = safe_relpath("../a").unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("contains '..'"));

    let err = safe_relpath("a/../b").unwrap_err();
    let msg = format!("{}", err);
    assert!(msg.contains("contains '..'"));
}

#[test]
fn safe_relpath_normalizes_separators_and_curdir() {
    assert_eq!(safe_relpath("a/b").unwrap(), "a/b");
    assert_eq!(safe_relpath("a//b").unwrap(), "a/b");
    assert_eq!(safe_relpath("a/./b").unwrap(), "a/b");
    assert_eq!(safe_relpath("\\a\\b").unwrap(), "a/b");
}

#[test]
fn safe_relpath_strips_leading_slashes() {
    assert_eq!(safe_relpath("/a").unwrap(), "a");
    assert_eq!(safe_relpath("//a/b").unwrap(), "a/b");
}
