use super::{ClusterEvent, ClusterManager};
use async_trait::async_trait;
// Reuse unified error group type defined in rpcresp_kvresult_convert
pub use crate::rpcresp_kvresult_convert::msg_and_error::ClusterManagerExtError;

const NODE_TAG_MASTER: &str = "master";

pub type ClusterManagerExtResult<T> = Result<T, ClusterManagerExtError>;

#[async_trait]
pub trait ClusterManagerAppLogicExt {
    async fn find_or_wait_master_node(&self) -> ClusterManagerExtResult<String>;
}

#[async_trait]
impl ClusterManagerAppLogicExt for ClusterManager {
    async fn find_or_wait_master_node(&self) -> ClusterManagerExtResult<String> {
        let members = self.get_members();
        let master_nodes: Vec<_> = members
            .iter()
            .filter(|m| m.metadata.contains_key(NODE_TAG_MASTER))
            .collect();
        if master_nodes.len() == 1 {
            Ok(master_nodes[0].id.clone())
        } else if master_nodes.len() == 0 {
            let mut rx = self.listen();
            tracing::info!(
                "no master found, current members: {:?}, waiting for master node to join",
                members
            );
            while let Ok(event) = rx.recv().await {
                if let ClusterEvent::MemberJoined(member) = event {
                    if member.metadata.contains_key(NODE_TAG_MASTER) {
                        return Ok(member.id);
                    }
                }
            }
            Err(ClusterManagerExtError::MasterNotFound {})
        } else {
            Err(ClusterManagerExtError::MultipleMasters {
                nodes: master_nodes
                    .into_iter()
                    .map(|m| m.id.to_owned())
                    .collect::<Vec<_>>(),
            })
        }
    }

    // fn refind_master_node_or_wait(&self) -> ClusterManagerExtResult<String> {
    //     self.
    // }
}
