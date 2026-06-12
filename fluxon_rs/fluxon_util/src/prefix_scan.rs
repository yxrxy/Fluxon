#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefixScanAction {
    Continue,
    Break,
}

pub fn prefix_scan_key_after(key: &[u8]) -> Vec<u8> {
    let mut next_key = Vec::with_capacity(key.len() + 1);
    next_key.extend_from_slice(key);
    next_key.push(0);
    next_key
}

pub fn prefix_scan_range_end_exclusive(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    for idx in (0..out.len()).rev() {
        if out[idx] != 0xFF {
            out[idx] = out[idx].saturating_add(1);
            out.truncate(idx + 1);
            return Some(out);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{prefix_scan_key_after, prefix_scan_range_end_exclusive};

    #[test]
    fn prefix_scan_range_end_matches_ascii_prefix_ordering() {
        assert_eq!(
            prefix_scan_range_end_exclusive(b"/cluster/transfer_link/p2p"),
            Some(b"/cluster/transfer_link/p2q".to_vec())
        );
        assert_eq!(
            prefix_scan_range_end_exclusive(b"/cluster/transfer_link/p2p/"),
            Some(b"/cluster/transfer_link/p2p0".to_vec())
        );
    }

    #[test]
    fn prefix_scan_range_end_is_none_for_all_ff_prefix() {
        assert_eq!(prefix_scan_range_end_exclusive(&[0xFF, 0xFF]), None);
    }

    #[test]
    fn prefix_scan_key_after_resumes_after_last_seen_key() {
        assert_eq!(prefix_scan_key_after(b"/prefix/a"), b"/prefix/a\0");
    }
}
