use super::*;
use crate::server::tcp_pool_tests::TestTransport;
use std::collections::VecDeque;

fn blocked_control() -> (
    ControlChannel<TestTransport>,
    tokio::io::DuplexStream,
    broadcast::Sender<bool>,
    DataChannelRequestSender,
) {
    let (conn, peer) = tokio::io::duplex(1);
    let (sender, receiver) = data_channel_requests(ServiceType::Tcp);
    tcp_pool::replenish(0, &mut VecDeque::new(), &sender).unwrap();
    let (shutdown, shutdown_rx) = broadcast::channel(1);
    (
        ControlChannel {
            conn,
            shutdown_rx,
            data_ch_req_rx: receiver,
            heartbeat_interval: 0,
        },
        peer,
        shutdown,
        sender,
    )
}

#[tokio::test(start_paused = true)]
async fn pending_partial_control_write_is_shutdown_aware() {
    let (control, mut peer, shutdown, _requests) = blocked_control();
    let writer = tokio::spawn(control.run());
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    shutdown.send(true).unwrap();
    time::timeout(Duration::from_millis(100), writer)
        .await
        .expect("shutdown must interrupt a pending control write")
        .unwrap()
        .unwrap();
    let mut partial = Vec::new();
    peer.read_to_end(&mut partial).await.unwrap();
    assert!(!partial.is_empty());
    assert!(
        partial.len()
            < bincode::serialize(&ControlChannelCmd::CreateDataChannel)
                .unwrap()
                .len()
    );
}

#[tokio::test(start_paused = true)]
async fn pending_control_write_expires_and_closes_connection() {
    let (control, mut peer, _shutdown, requests) = blocked_control();
    let writer = tokio::spawn(control.run());
    time::timeout(Duration::from_secs(12), writer)
        .await
        .expect("a blocked control writer must expire before reserve retry")
        .unwrap()
        .unwrap();
    assert!(requests.is_closed());
    let mut partial = Vec::new();
    peer.read_to_end(&mut partial).await.unwrap();
    assert!(!partial.is_empty());
    assert!(
        partial.len()
            < bincode::serialize(&ControlChannelCmd::CreateDataChannel)
                .unwrap()
                .len()
    );
}
