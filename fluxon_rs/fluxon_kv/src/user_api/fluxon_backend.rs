use std::sync::Arc;

use tokio::runtime::Handle;

use crate::Framework;
use crate::rpcresp_kvresult_convert::msg_and_error::KvResult;
use crate::user_api::kv::{KvClient, UserKvApi};
use crate::user_api::rpc::{FluxonUserRpcImpl, UserRpcClient, UserRpcServer};
use crate::{ClusterMember, MembershipEventReceiver};

pub struct FluxonUserApi {
    framework: Arc<Framework>,
    kv: UserKvApi,
    rpc: FluxonUserRpcImpl,
}

impl FluxonUserApi {
    pub fn new(framework: Arc<Framework>, runtime: Handle) -> KvResult<Self> {
        Ok(Self {
            framework: framework.clone(),
            kv: UserKvApi {
                framework: framework.clone(),
                runtime: runtime.clone(),
            },
            rpc: FluxonUserRpcImpl { framework, runtime },
        })
    }

    pub fn kv(&self) -> &dyn KvClient {
        &self.kv
    }

    pub fn rpc_client(&self) -> &dyn UserRpcClient {
        &self.rpc
    }

    pub fn rpc_server(&self) -> &dyn UserRpcServer {
        &self.rpc
    }

    pub fn framework(&self) -> &Arc<Framework> {
        &self.framework
    }

    pub fn runtime_handle(&self) -> Handle {
        self.rpc.runtime.clone()
    }

    pub fn membership_snapshot(&self) -> Vec<ClusterMember> {
        let cm_view = self.framework.cluster_manager_view();
        let cm = cm_view.cluster_manager();
        assert!(
            cm.is_watching(),
            "membership_snapshot requires ClusterManager watching started"
        );
        let mut out = cm.get_members();
        out.push(cm.get_self_info());
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    pub fn membership_listen(&self) -> MembershipEventReceiver {
        let cm_view = self.framework.cluster_manager_view();
        let cm = cm_view.cluster_manager();
        assert!(
            cm.is_watching(),
            "membership_listen requires ClusterManager watching started"
        );
        cm.listen()
    }
}
