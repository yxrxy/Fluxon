use dashmap::DashMap;

#[derive(Debug)]
pub struct ClientTestRecordOneTransfer {
    _key: String,
    _value_size: u32,
    /// (put_time_ms, put_version) - only for put operations
    _put_id: Option<(u64, u32)>,
    /// get_id - only for get operations
    _get_id: Option<u64>,
    _target_node_id: String,
    _target_address: String,
}

pub struct ClientTestRecord {
    /// (key, put_time_ms, put_version) -> ClientTestRecordOneTransfer
    transfering_puts: DashMap<(String, u64, u32), ClientTestRecordOneTransfer>,
    /// get_id -> ClientTestRecordOneTransfer
    transfering_gets: DashMap<u64, ClientTestRecordOneTransfer>,
}

impl ClientTestRecord {
    pub fn new() -> Self {
        Self {
            transfering_puts: DashMap::new(),
            transfering_gets: DashMap::new(),
        }
    }

    pub fn add_transfering_put(
        &self,
        key: String,
        value_size: u32,
        put_time_ms: u64,
        put_version: u32,
        target_node_id: String,
        target_address: String,
    ) {
        self.transfering_puts.insert(
            (key.clone(), put_time_ms, put_version),
            ClientTestRecordOneTransfer {
                _key: key,
                _value_size: value_size,
                _put_id: Some((put_time_ms, put_version)),
                _get_id: None,
                _target_node_id: target_node_id,
                _target_address: target_address,
            },
        );
    }

    pub fn remove_transfering_put(&self, key: String, put_id: (u64, u32)) {
        self.transfering_puts.remove(&(key, put_id.0, put_id.1));
    }

    pub fn add_transfering_get(
        &self,
        get_id: u64,
        key: String,
        value_size: u32,
        target_addr: u64,
        node_id: String,
        _peer_is_src_or_target: bool,
    ) {
        self.transfering_gets.insert(
            get_id,
            ClientTestRecordOneTransfer {
                _key: key,
                _value_size: value_size,
                _put_id: None,
                _get_id: Some(get_id),
                _target_node_id: node_id,
                _target_address: format!("{:#x}", target_addr),
            },
        );
    }

    pub fn remove_transfering_get(&self, get_id: u64) {
        self.transfering_gets.remove(&get_id);
    }

    pub fn debug_transfering(&self) {
        tracing::info!("--------------------------------");
        tracing::info!("transfering puts count: {:?}", self.transfering_puts.len());

        // holding the lock in the whole iteration
        for entry in self.transfering_puts.iter() {
            tracing::info!("- transfering put: {:?}", entry.value());
        }

        tracing::info!("transfering gets count: {:?}", self.transfering_gets.len());

        for entry in self.transfering_gets.iter() {
            tracing::info!("- transfering get: {:?}", entry.value());
        }
        tracing::info!("--------------------------------");
    }
}
