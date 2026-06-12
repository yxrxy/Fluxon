use std::sync::Arc;

use tokio::runtime::Handle;

use crate::client_kv_api::PutOptionalArgs;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use crate::user_api::codec_flat_dict::{decode_flat_dict_bytes, encode_flat_dict_bytes};
use crate::user_api::flat_dict::FlatDict;
use crate::{Framework, KvClientTrait, KvGetResult};
use fluxon_util::run_async_from_sync::{SyncAsyncBridge, borrow_stable_owner};

pub trait KvClient: Send + Sync {
    fn get(&self, key: &str) -> KvResult<Option<FlatDict>>;
    fn put(&self, key: &str, value: FlatDict) -> KvResult<()>;
    fn delete(&self, key: &str) -> KvResult<()>;
    fn is_exist(&self, key: &str) -> KvResult<bool>;
}

pub(crate) struct UserKvApi {
    pub(crate) framework: Arc<Framework>,
    pub(crate) runtime: Handle,
}

impl KvClient for UserKvApi {
    fn get(&self, key: &str) -> KvResult<Option<FlatDict>> {
        let framework = borrow_stable_owner(&self.framework);
        let res = self
            .runtime
            .run_async_from_sync(async { framework.kv_get(key).await });
        let got = match res {
            Ok(v) => v?,
            Err(e) => {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!("runtime bridge failed: {}", e),
                }));
            }
        };
        let bytes: Option<Vec<u8>> = match got {
            KvGetResult::Owner(opt) => opt.map(|h| h.bytes().to_vec()),
            KvGetResult::External(opt) => opt.map(|h| h.bytes().to_vec()),
        };
        match bytes {
            None => Ok(None),
            Some(b) => Ok(Some(decode_flat_dict_bytes(&b)?)),
        }
    }

    fn put(&self, key: &str, value: FlatDict) -> KvResult<()> {
        let payload = encode_flat_dict_bytes(&value)?;
        let framework = borrow_stable_owner(&self.framework);
        let opts = PutOptionalArgs::new();
        let res = self
            .runtime
            .run_async_from_sync(async { framework.kv_put(key, &payload, opts).await });
        match res {
            Ok(v) => v,
            Err(e) => Err(KvError::Api(ApiError::Unknown {
                detail: format!("runtime bridge failed: {}", e),
            })),
        }
    }

    fn delete(&self, key: &str) -> KvResult<()> {
        let framework = borrow_stable_owner(&self.framework);
        let res = self
            .runtime
            .run_async_from_sync(async { framework.kv_delete(key).await });
        match res {
            Ok(v) => v,
            Err(e) => Err(KvError::Api(ApiError::Unknown {
                detail: format!("runtime bridge failed: {}", e),
            })),
        }
    }

    fn is_exist(&self, key: &str) -> KvResult<bool> {
        let framework = borrow_stable_owner(&self.framework);
        let res = self
            .runtime
            .run_async_from_sync(async { framework.kv_is_exist(key).await });
        match res {
            Ok(v) => v,
            Err(e) => Err(KvError::Api(ApiError::Unknown {
                detail: format!("runtime bridge failed: {}", e),
            })),
        }
    }
}
