pub fn get_current_git_commitid() -> Option<String> {
    let hash = env!("GIT_COMMIT_HASH");
    if hash == "unknown" || hash.is_empty() {
        None
    } else {
        Some(hash.to_string())
    }
}
