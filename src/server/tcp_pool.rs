use super::*;
use std::collections::VecDeque;
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;
use tokio::time::Instant;

pub(super) const VISITOR_LIMIT: usize = 128;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(100);

pub(super) struct Visitor {
    stream: TcpStream,
    deadline: Instant,
    permit: OwnedSemaphorePermit,
}

impl Visitor {
    pub(super) fn new(stream: TcpStream, permit: OwnedSemaphorePermit) -> Self {
        Self {
            stream,
            deadline: Instant::now() + STARTUP_TIMEOUT,
            permit,
        }
    }

    fn is_waiting(&self) -> bool {
        if Instant::now() >= self.deadline {
            warn!("Rejecting TCP visitor: startup deadline expired");
            return false;
        }
        match self.stream.take_error() {
            Ok(None) => true,
            error => {
                debug!(?error, "Dropping disconnected TCP visitor");
                false
            }
        }
    }
}

pub(super) fn replenish(
    idle: usize,
    requests: &mut VecDeque<Instant>,
    sender: &DataChannelRequestSender,
) -> Result<()> {
    let now = Instant::now();
    while requests.front().is_some_and(|deadline| *deadline <= now) {
        requests.pop_front();
        warn!("TCP data channel request expired; replenishing reserve");
    }
    while idle + requests.len() < TCP_POOL_SIZE {
        match sender.try_send(true) {
            Ok(()) => requests.push_back(now + REQUEST_TIMEOUT),
            Err(mpsc::error::TrySendError::Full(_)) => break,
            Err(error) => {
                return Err(error).with_context(|| "Failed to replenish TCP data channel reserve")
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pending_creation_commands_stay_bounded_across_expiry_cycles() {
        let (sender, mut receiver) = data_channel_requests(ServiceType::Tcp);
        let mut outstanding = VecDeque::new();
        replenish(0, &mut outstanding, &sender).unwrap();
        receiver.recv().await.unwrap();
        for _ in 0..4 {
            time::advance(REQUEST_TIMEOUT + Duration::from_secs(1)).await;
            replenish(0, &mut outstanding, &sender).unwrap();
            assert!(
                receiver.len() <= TCP_POOL_SIZE,
                "a blocked writer must not accumulate creation commands: {}",
                receiver.len()
            );
        }
        assert_eq!(receiver.len(), TCP_POOL_SIZE);
    }
}

async fn start<Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static>(
    visitor: Visitor,
    mut channel: Stream,
    proxy_protocol: String,
) -> Option<Visitor> {
    let command = bincode::serialize(&DataChannelCmd::StartForwardTcp).unwrap();
    let result = time::timeout_at(visitor.deadline, async {
        write_and_flush(&mut channel, &command).await?;
        if !proxy_protocol.is_empty() {
            let header = generate_proxy_protocol_header(&visitor.stream, &proxy_protocol)?;
            write_and_flush(&mut channel, &header).await?;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {
            let Visitor {
                mut stream, permit, ..
            } = visitor;
            drop(permit);
            tokio::spawn(
                async move {
                    let _ = copy_bidirectional(&mut channel, &mut stream).await;
                }
                .instrument(Span::current()),
            );
            None
        }
        Ok(Err(error)) => {
            debug!(%error, "TCP data channel startup failed");
            visitor.is_waiting().then_some(visitor)
        }
        Err(_) => {
            warn!("Rejecting TCP visitor: data channel startup timed out");
            None
        }
    }
}

pub(super) async fn run<T: Transport>(
    bind_addr: String,
    proxy_protocol: String,
    mut channels: mpsc::Receiver<T::Stream>,
    requests: DataChannelRequestSender,
    mut shutdown: broadcast::Receiver<bool>,
) -> Result<()> {
    let mut visitors = tcp_listen_and_send(bind_addr, shutdown.resubscribe());
    let mut waiting = VecDeque::<Visitor>::new();
    let mut idle = VecDeque::new();
    let mut outstanding = VecDeque::new();
    let mut starting = JoinSet::new();
    let mut maintenance = time::interval(MAINTENANCE_INTERVAL);
    maintenance.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        waiting.retain(Visitor::is_waiting);
        while !waiting.is_empty() && !idle.is_empty() && starting.len() < TCP_POOL_SIZE {
            let visitor = waiting.pop_front().unwrap();
            let channel = idle.pop_front().unwrap();
            starting
                .spawn(start(visitor, channel, proxy_protocol.clone()).instrument(Span::current()));
        }
        replenish(idle.len(), &mut outstanding, &requests)?;

        tokio::select! {
            _ = shutdown.recv() => break,
            visitor = visitors.recv() => {
                let Some(visitor) = visitor else { break };
                waiting.push_back(visitor);
            }
            channel = channels.recv() => {
                let Some(channel) = channel else { break };
                outstanding.pop_front();
                if idle.len() < TCP_POOL_SIZE {
                    idle.push_back(channel);
                } else {
                    debug!("Discarding excess TCP data channel");
                }
            }
            result = starting.join_next(), if !starting.is_empty() => {
                match result {
                    Some(Ok(Some(visitor))) => waiting.push_front(visitor),
                    Some(Err(error)) => warn!(%error, "TCP startup task failed"),
                    _ => (),
                }
            }
            _ = maintenance.tick() => (),
        }
    }

    info!("TCP pool shutdown");
    Ok(())
}
