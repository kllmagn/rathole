use super::*;
use tokio::io::DuplexStream;

#[tokio::test(start_paused = true)]
async fn stalled_backend_connect_has_its_own_deadline() {
    let (tunnel, _visitor) = tokio::io::duplex(64);
    let connector = std::future::pending::<io::Result<DuplexStream>>();
    let result = time::timeout(Duration::from_secs(12), forward_tcp(tunnel, connector)).await;
    assert!(
        matches!(&result, Ok(Err(error)) if error.to_string().contains("Backend TCP connection timed out")),
        "stalled backend connection must expire independently: {result:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn backend_connect_deadline_does_not_limit_forwarded_session() {
    let (tunnel, mut visitor) = tokio::io::duplex(64);
    let (backend, mut service) = tokio::io::duplex(64);
    let forwarding = tokio::spawn(forward_tcp(tunnel, async { Ok(backend) }));
    tokio::task::yield_now().await;
    time::advance(Duration::from_secs(60)).await;
    assert!(!forwarding.is_finished());
    visitor.write_all(b"request").await.unwrap();
    let mut payload = [0; 7];
    service.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"request");
    service.write_all(b"response").await.unwrap();
    let mut response = [0; 8];
    visitor.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"response");
    visitor.shutdown().await.unwrap();
    service.shutdown().await.unwrap();
    time::timeout(Duration::from_secs(1), forwarding)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
