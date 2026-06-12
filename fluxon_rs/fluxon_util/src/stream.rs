use futures::Stream;
use limit_thirdparty::tokio::sync::ampsc;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct AMpscUnboundedReceiverStreamWrapper<T> {
    pub inner: ampsc::UnboundedReceiver<T>,
}

impl<T> Stream for AMpscUnboundedReceiverStreamWrapper<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

pub struct AMpscReceiverStreamWrapper<T> {
    pub inner: ampsc::Receiver<T>,
}

impl<T> Stream for AMpscReceiverStreamWrapper<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}
