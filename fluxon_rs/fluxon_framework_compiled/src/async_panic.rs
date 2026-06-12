use std::sync::atomic::AtomicU64;

pub struct AsyncPanicSend {
    next_panic_id: AtomicU64,
    sender: limit_thirdparty::tokio::sync::ampsc::Sender<String>,
}

impl AsyncPanicSend {
    async fn send_and_panic(
        sender: limit_thirdparty::tokio::sync::ampsc::Sender<String>,
        panic_id: u64,
        msg: String,
    ) {
        sender
            .send(format!("async_panic_{}: {}", panic_id, msg))
            .await
            .unwrap();
        panic!("async_panic_{}: {}", panic_id, msg);
    }

    pub fn new(sender: limit_thirdparty::tokio::sync::ampsc::Sender<String>) -> Self {
        Self {
            next_panic_id: AtomicU64::new(0),
            sender,
        }
    }

    pub async fn async_panic(&self, msg: String) {
        let panic_id = self
            .next_panic_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self::send_and_panic(self.sender.clone(), panic_id, msg).await;
    }

    pub fn spawn_on(
        &self,
        runtime: &tokio::runtime::Handle,
        msg: String,
    ) -> tokio::task::JoinHandle<()> {
        let panic_id = self
            .next_panic_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        runtime.spawn(Self::send_and_panic(self.sender.clone(), panic_id, msg))
    }
}

pub struct AsyncPanicRecv {
    receiver: limit_thirdparty::tokio::sync::ampsc::Receiver<String>,
}

impl AsyncPanicRecv {
    pub fn new(receiver: limit_thirdparty::tokio::sync::ampsc::Receiver<String>) -> Self {
        Self { receiver }
    }

    pub async fn recv_and_panic(&mut self) -> String {
        let received = self.receiver.recv().await.unwrap();
        panic!("async_panic received: {}", received);
    }
}

pub fn new_async_panic() -> (AsyncPanicSend, AsyncPanicRecv) {
    let (tx, rx) = limit_thirdparty::tokio::sync::ampsc::channel(1000);
    (AsyncPanicSend::new(tx), AsyncPanicRecv::new(rx))
}

pub trait AsyncPanicSendExt {
    fn async_panic(&self, msg: String);
}
