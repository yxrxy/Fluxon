use crate::file_stream::{FuseDetachedFileState, FuseFileStream};
use crate::open_action::OpenAction;

pub struct FuseFileEntry {
    id: u64,
    projected_relpath: String,
    open_action: OpenAction,
    stream: Box<dyn FuseFileStream>,
    detached: bool,
    detached_state: Option<FuseDetachedFileState>,
}

impl FuseFileEntry {
    pub fn new(
        id: u64,
        projected_relpath: String,
        open_action: OpenAction,
        stream: Box<dyn FuseFileStream>,
    ) -> Self {
        Self {
            id,
            projected_relpath,
            open_action,
            stream,
            detached: false,
            detached_state: None,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn projected_relpath(&self) -> &str {
        self.projected_relpath.as_str()
    }

    pub fn open_action(&self) -> OpenAction {
        self.open_action
    }

    pub fn stream(&self) -> &dyn FuseFileStream {
        self.stream.as_ref()
    }

    pub fn stream_mut(&mut self) -> &mut dyn FuseFileStream {
        self.stream.as_mut()
    }

    pub fn is_detached(&self) -> bool {
        self.detached
    }

    pub fn mark_detached(&mut self) {
        self.detached = true;
        if self.detached_state.is_none() {
            self.detached_state = self.stream.detached_state();
        }
    }

    pub fn detached_state(&self) -> Option<&FuseDetachedFileState> {
        self.detached_state.as_ref()
    }
}
