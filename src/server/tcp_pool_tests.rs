use super::*;
use crate::config::TransportConfig;
use crate::transport::AddrMaybeCached;
use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::io::DuplexStream;
use tokio::net::ToSocketAddrs;

#[derive(Debug)]
pub(crate) struct TestTransport;

#[async_trait]
impl Transport for TestTransport {
    type Acceptor = ();
    type RawStream = DuplexStream;
    type Stream = DuplexStream;

    fn new(_: &TransportConfig) -> Result<Self> {
        Ok(Self)
    }

    fn hint(_: &Self::Stream, _: SocketOpts) {}

    async fn bind<Address: ToSocketAddrs + Send + Sync>(
        &self,
        _: Address,
    ) -> Result<Self::Acceptor> {
        unreachable!()
    }

    async fn accept(&self, _: &Self::Acceptor) -> Result<(Self::RawStream, SocketAddr)> {
        unreachable!()
    }

    async fn handshake(&self, stream: Self::RawStream) -> Result<Self::Stream> {
        Ok(stream)
    }

    async fn connect(&self, _: &AddrMaybeCached) -> Result<Self::Stream> {
        std::future::pending().await
    }
}

async fn unused_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

async fn connect(address: &str) -> TcpStream {
    time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(stream) = TcpStream::connect(address).await {
                return stream;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

struct Pool {
    address: String,
    data: mpsc::Sender<DuplexStream>,
    requests: DataChannelRequestReceiver,
    shutdown: broadcast::Sender<bool>,
    task: tokio::task::JoinHandle<Result<()>>,
}

impl Pool {
    async fn new() -> Self {
        let address = unused_address().await;
        let (data, receiver) = mpsc::channel(32);
        let (request_sender, requests) = data_channel_requests(ServiceType::Tcp);
        let (shutdown, shutdown_receiver) = broadcast::channel(1);
        let task = tokio::spawn(run_tcp_connection_pool::<TestTransport>(
            address.clone(),
            String::new(),
            receiver,
            request_sender,
            shutdown_receiver,
        ));
        Self {
            address,
            data,
            requests,
            shutdown,
            task,
        }
    }

    async fn channel(&self) -> DuplexStream {
        let (server, client) = tokio::io::duplex(1024);
        self.data.send(server).await.unwrap();
        client
    }

    async fn stop(self) {
        self.shutdown.send(true).unwrap();
        time::timeout(Duration::from_secs(1), self.task)
            .await
            .expect("pool must stop even without a data channel")
            .unwrap()
            .unwrap();
    }
}

async fn started(channel: &mut DuplexStream) {
    let command = bincode::serialize(&DataChannelCmd::StartForwardTcp).unwrap();
    let mut received = vec![0; command.len()];
    time::timeout(Duration::from_secs(1), channel.read_exact(&mut received))
        .await
        .expect("healthy channel must start independently")
        .unwrap();
    assert_eq!(received, command);
}

#[tokio::test]
async fn idle_pool_retries_lost_creation_requests() {
    let mut pool = Pool::new().await;
    for _ in 0..8 {
        time::timeout(Duration::from_secs(1), pool.requests.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(
        time::timeout(Duration::from_millis(200), pool.requests.recv())
            .await
            .is_err()
    );
    let requests = time::timeout(Duration::from_secs(32), async {
        for _ in 0..8 {
            pool.requests.recv().await.unwrap();
        }
    })
    .await;
    assert!(
        requests.is_ok(),
        "missing reserve must be requested again without visitors"
    );
    let mut channel = pool.channel().await;
    let mut visitor = connect(&pool.address).await;
    visitor.write_all(b"recovered").await.unwrap();
    started(&mut channel).await;
    let mut payload = [0; 9];
    time::timeout(Duration::from_secs(1), channel.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"recovered");
    pool.stop().await;
}

#[tokio::test]
async fn visitor_startup_expires_without_a_data_channel() {
    let pool = Pool::new().await;
    let mut visitor = connect(&pool.address).await;
    let mut byte = [0];
    let result = time::timeout(Duration::from_secs(32), visitor.read(&mut byte)).await;
    assert!(
        matches!(result, Ok(Ok(0)) | Ok(Err(_))),
        "startup must be bounded"
    );
    pool.stop().await;
}

#[tokio::test]
async fn stalled_start_does_not_block_other_visitors() {
    let pool = Pool::new().await;
    let _first = connect(&pool.address).await;
    let (stalled_server, _stalled_client) = tokio::io::duplex(1);
    pool.data.send(stalled_server).await.unwrap();
    time::sleep(Duration::from_millis(50)).await;
    let _second = connect(&pool.address).await;
    let mut healthy = pool.channel().await;
    started(&mut healthy).await;
    pool.stop().await;
}

#[tokio::test]
async fn shutdown_interrupts_waiting_visitor() {
    let pool = Pool::new().await;
    let _visitor = connect(&pool.address).await;
    time::sleep(Duration::from_millis(50)).await;
    pool.stop().await;
}

#[tokio::test]
async fn queued_half_close_keeps_payload_and_response_and_session_lifetime() {
    let pool = Pool::new().await;
    let mut visitor = connect(&pool.address).await;
    visitor.write_all(b"request").await.unwrap();
    visitor.shutdown().await.unwrap();
    time::sleep(Duration::from_millis(100)).await;
    let mut channel = pool.channel().await;
    started(&mut channel).await;
    let mut request = Vec::new();
    time::timeout(Duration::from_secs(1), channel.read_to_end(&mut request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request, b"request");
    time::sleep(Duration::from_secs(31)).await;
    channel.write_all(b"response").await.unwrap();
    channel.shutdown().await.unwrap();
    let mut response = Vec::new();
    time::timeout(Duration::from_secs(1), visitor.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, b"response");
    pool.stop().await;
}

#[tokio::test]
async fn full_visitor_queue_rejects_instead_of_blocking_accept() {
    let address = unused_address().await;
    let (shutdown, shutdown_receiver) = broadcast::channel(1);
    let _queue = tcp_listen_and_send(address.clone(), shutdown_receiver);
    let mut visitors = Vec::new();
    for _ in 0..128 {
        visitors.push(connect(&address).await);
    }
    let mut overflow = connect(&address).await;
    let mut byte = [0];
    let result = time::timeout(Duration::from_secs(1), overflow.read(&mut byte)).await;
    assert!(
        matches!(result, Ok(Ok(0)) | Ok(Err(_))),
        "overflow must be explicitly rejected"
    );
    shutdown.send(true).unwrap();
}

#[tokio::test]
async fn reset_visitor_does_not_consume_next_channel() {
    let pool = Pool::new().await;
    let reset = connect(&pool.address).await.into_std().unwrap();
    let reset = socket2::Socket::from(reset);
    reset.set_linger(Some(Duration::ZERO)).unwrap();
    drop(reset);
    time::sleep(Duration::from_millis(200)).await;
    let mut live = connect(&pool.address).await;
    live.write_all(b"live").await.unwrap();
    let mut channel = pool.channel().await;
    started(&mut channel).await;
    let mut payload = [0; 4];
    time::timeout(Duration::from_secs(1), channel.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"live");
    pool.stop().await;
}

#[tokio::test]
async fn broken_channel_retries_same_visitor() {
    let pool = Pool::new().await;
    let (broken, peer) = tokio::io::duplex(1024);
    drop(peer);
    pool.data.send(broken).await.unwrap();
    let mut visitor = connect(&pool.address).await;
    visitor.write_all(b"retry").await.unwrap();
    let mut channel = pool.channel().await;
    started(&mut channel).await;
    let mut payload = [0; 5];
    time::timeout(Duration::from_secs(1), channel.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"retry");
    pool.stop().await;
}

#[tokio::test]
async fn queued_empty_half_close_can_receive_response() {
    let pool = Pool::new().await;
    let mut visitor = connect(&pool.address).await;
    visitor.shutdown().await.unwrap();
    time::sleep(Duration::from_millis(200)).await;
    let mut channel = pool.channel().await;
    started(&mut channel).await;
    channel.write_all(b"greeting").await.unwrap();
    channel.shutdown().await.unwrap();
    let mut response = Vec::new();
    time::timeout(Duration::from_secs(1), visitor.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, b"greeting");
    pool.stop().await;
}

#[tokio::test]
async fn stalled_startup_write_expires_and_releases_admission() {
    let pool = Pool::new().await;
    let mut visitor = connect(&pool.address).await;
    let (stalled_server, _stalled_client) = tokio::io::duplex(1);
    pool.data.send(stalled_server).await.unwrap();
    let mut byte = [0];
    let result = time::timeout(Duration::from_secs(32), visitor.read(&mut byte)).await;
    assert!(matches!(result, Ok(Ok(0)) | Ok(Err(_))));
    let mut next = connect(&pool.address).await;
    next.write_all(b"next").await.unwrap();
    let mut channel = pool.channel().await;
    started(&mut channel).await;
    let mut payload = [0; 4];
    time::timeout(Duration::from_secs(1), channel.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"next");
    pool.stop().await;
}

#[tokio::test]
async fn late_surplus_channels_are_closed_and_reserve_stays_usable() {
    let pool = Pool::new().await;
    let mut channels = Vec::new();
    for _ in 0..16 {
        channels.push(pool.channel().await);
    }
    for mut surplus in channels.drain(8..) {
        let mut byte = [0];
        assert_eq!(
            time::timeout(Duration::from_secs(1), surplus.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
    let mut visitor = connect(&pool.address).await;
    visitor.write_all(b"late").await.unwrap();
    started(&mut channels[0]).await;
    let mut payload = [0; 4];
    time::timeout(Duration::from_secs(1), channels[0].read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"late");
    pool.stop().await;
}
