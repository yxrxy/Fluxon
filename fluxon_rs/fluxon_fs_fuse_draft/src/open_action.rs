#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAction {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

impl OpenAction {
    pub fn is_writable(self) -> bool {
        matches!(self, Self::WriteOnly | Self::ReadWrite)
    }

    pub fn is_readable(self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadWrite)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlagsView {
    bits: i32,
}

impl OpenFlagsView {
    pub fn new(bits: i32) -> Self {
        Self { bits }
    }

    pub fn bits(self) -> i32 {
        self.bits
    }

    pub fn contains_create(self) -> bool {
        (self.bits & libc::O_CREAT) != 0
    }

    pub fn contains_truncate(self) -> bool {
        (self.bits & libc::O_TRUNC) != 0
    }

    pub fn contains_exclusive(self) -> bool {
        (self.bits & libc::O_EXCL) != 0
    }

    pub fn contains_append(self) -> bool {
        (self.bits & libc::O_APPEND) != 0
    }

    pub fn access_mode_bits(self) -> i32 {
        self.bits & libc::O_ACCMODE
    }
}

pub fn classify_open_action(flags: i32) -> Option<OpenAction> {
    match OpenFlagsView::new(flags).access_mode_bits() {
        libc::O_RDONLY => Some(OpenAction::ReadOnly),
        libc::O_WRONLY => Some(OpenAction::WriteOnly),
        libc::O_RDWR => Some(OpenAction::ReadWrite),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{OpenAction, OpenFlagsView, classify_open_action};

    #[test]
    fn classify_access_modes() {
        assert_eq!(classify_open_action(libc::O_RDONLY), Some(OpenAction::ReadOnly));
        assert_eq!(classify_open_action(libc::O_WRONLY), Some(OpenAction::WriteOnly));
        assert_eq!(classify_open_action(libc::O_RDWR), Some(OpenAction::ReadWrite));
    }

    #[test]
    fn detect_flag_bits() {
        let flags = OpenFlagsView::new(libc::O_RDWR | libc::O_CREAT | libc::O_APPEND);
        assert!(flags.contains_create());
        assert!(flags.contains_append());
        assert!(!flags.contains_truncate());
    }
}
