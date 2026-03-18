use super::BcSubscription;
use crate::{bc::model::*, Error, Result};
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use log::*;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use tokio::{sync::RwLock, task::JoinSet};

// =============================================================================
// Channel Capacity Constants
// =============================================================================
// These control buffering between camera protocol and consumers (RTSP, etc.)
//
// Sizing rationale:
// - At 30fps H.264, each frame is a Bc message (~1-10KB depending on I/P frame)
// - 500 messages ≈ 16 seconds of video at 30fps
// - Larger buffers prevent frame drops during transient client slowdowns
// - Memory cost: ~500 × avg_frame_size ≈ 2-5MB per active stream
//
// Trade-offs:
// - Larger = more resilient to jitter, but higher memory and latency
// - Smaller = lower latency, but more prone to frame drops

/// Capacity for outgoing message channel (camera → subscribers)
/// Sized for ~16 seconds of video frames at 30fps
const MESSAGE_CHANNEL_CAPACITY: usize = 500;

/// Capacity for internal poll command routing
/// Higher than message capacity to prevent command starvation during high video traffic
const POLL_COMMAND_CAPACITY: usize = 1000;

/// Capacity for individual subscriber channels
/// Matches message channel to prevent asymmetric backpressure
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 500;

/// Interval between subscriber cleanup passes (in seconds)
///
/// Cleanup removes closed subscriber channels to free resources.
/// Running every 5 seconds instead of per-message reduces CPU overhead
/// from O(n) per frame to O(n) per interval at 30fps video streams.
const SUBSCRIBER_CLEANUP_INTERVAL_SECS: u64 = 5;

type MsgHandler = dyn 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>;

#[derive(Default)]
struct Subscriber {
    /// Subscribers based on their ID and their num
    /// First filtered by ID then number
    /// If num is None it will be upgraded to a Some based on the number the
    /// camera assigns
    num: BTreeMap<u32, BTreeMap<Option<u16>, Sender<Result<Bc>>>>,
    /// Subscribers based on their ID
    id: BTreeMap<u32, Arc<MsgHandler>>,
}

pub(crate) type BcConnSink = Box<dyn Sink<Bc, Error = Error> + Send + Sync + Unpin>;
pub(crate) type BcConnSource = Box<dyn Stream<Item = Result<Bc>> + Send + Sync + Unpin>;

/// A shareable connection to a camera.  Handles serialization of messages.  To send/receive, call
/// .[subscribe()] with a message number.  You can use the BcSubscription to send or receive only
/// messages with that number; each incoming message is routed to its appropriate subscriber.
///
/// There can be only one subscriber per kind of message at a time.
pub struct BcConnection {
    sink: Sender<Result<Bc>>,
    poll_commander: Sender<PollCommand>,
    rx_thread: RwLock<JoinSet<Result<()>>>,
    cancel: CancellationToken,
}

impl BcConnection {
    pub async fn new(mut sink: BcConnSink, mut source: BcConnSource) -> Result<BcConnection> {
        let (sinker, sinker_rx) = channel::<Result<Bc>>(MESSAGE_CHANNEL_CAPACITY);
        let cancel = CancellationToken::new();

        let (poll_commander, poll_commanded) = channel(POLL_COMMAND_CAPACITY);
        let mut poller = Poller {
            subscribers: Default::default(),
            sink: sinker.clone(),
            reciever: ReceiverStream::new(poll_commanded),
            last_cleanup: Instant::now(),
        };

        let mut rx_thread = JoinSet::<Result<()>>::new();
        let thread_poll_commander = poll_commander.clone();
        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    let sender = thread_poll_commander;
                    while let Some(bc) = source.next().await {
                        sender.send(PollCommand::Bc(Box::new(bc))).await?;
                    }
                    Result::Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    let mut stream = ReceiverStream::new(sinker_rx);
                    while let Some(packet) = stream.next().await {
                        sink.send(packet?).await?;
                    }
                    Ok(())
                } => v
            }
        });

        let thread_cancel = cancel.clone();
        rx_thread.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    loop {
                        if let n @ Err(_) = poller.run().await {
                            trace!("Polling has ended");
                            return n;
                        }
                    }
                }=> v
            }
        });

        Ok(BcConnection {
            sink: sinker,
            poll_commander,
            rx_thread: RwLock::new(rx_thread),
            cancel,
        })
    }

    pub(super) async fn send(&self, bc: Bc) -> crate::Result<()> {
        self.sink.send(Ok(bc)).await?;
        Ok(())
    }

    pub async fn subscribe(&self, msg_id: u32, msg_num: u16) -> Result<BcSubscription> {
        let (tx, rx) = channel(SUBSCRIBER_CHANNEL_CAPACITY);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, Some(msg_num), tx))
            .await?;
        Ok(BcSubscription::new(rx, Some(msg_num as u32), self))
    }

    /// Some messages are initiated by the camera. This creates a handler for them
    /// It requires a closure that will be used to handle the message
    /// and return either None or Some(Bc) reply
    pub async fn handle_msg<T>(&self, msg_id: u32, handler: T) -> Result<()>
    where
        T: 'static + Send + Sync + for<'a> Fn(&'a Bc) -> BoxFuture<'a, Option<Bc>>,
    {
        self.poll_commander
            .send(PollCommand::AddHandler(msg_id, Arc::new(handler)))
            .await?;
        Ok(())
    }

    /// Stop a message handler created using [`handle_msg`]
    #[allow(dead_code)] // Currently unused but added for future use
    pub async fn unhandle_msg(&self, msg_id: u32) -> Result<()> {
        self.poll_commander
            .send(PollCommand::RemoveHandler(msg_id))
            .await?;
        Ok(())
    }

    /// Some times we want to wait for a reply on a new message ID
    /// to do this we wait for the next packet with a certain ID
    /// grab it's message ID and then subscribe to that ID
    ///
    /// The command Snap that grabs a jpeg payload is an example of this
    ///
    /// This function creates a temporary handle to grab this single message
    pub async fn subscribe_to_id(&self, msg_id: u32) -> Result<BcSubscription> {
        let (tx, rx) = channel(SUBSCRIBER_CHANNEL_CAPACITY);
        self.poll_commander
            .send(PollCommand::AddSubscriber(msg_id, None, tx))
            .await?;
        Ok(BcSubscription::new(rx, None, self))
    }

    pub(crate) async fn join(&self) -> Result<()> {
        let mut locked_threads = self.rx_thread.write().await;
        while let Some(res) = locked_threads.join_next().await {
            match res {
                Err(e) => {
                    locked_threads.abort_all();
                    return Err(e.into());
                }
                Ok(Err(e)) => {
                    locked_threads.abort_all();
                    return Err(e);
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.poll_commander.send(PollCommand::Disconnect).await;
        self.cancel.cancel();
        let mut locked_threads = self.rx_thread.write().await;
        while locked_threads.join_next().await.is_some() {}
        Ok(())
    }
}

impl Drop for BcConnection {
    fn drop(&mut self) {
        log::trace!("Drop BcConnection");
        self.cancel.cancel();

        let poll_commander = self.poll_commander.clone();
        let _gt = tokio::runtime::Handle::current().enter();
        let mut threads = std::mem::take(&mut self.rx_thread);
        tokio::task::spawn(async move {
            let _ = poll_commander.send(PollCommand::Disconnect).await;
            let locked_threads = threads.get_mut();
            while locked_threads.join_next().await.is_some() {}
            log::trace!("Dropped BcConnection");
        });
    }
}

enum PollCommand {
    Bc(Box<Result<Bc>>),
    AddHandler(u32, Arc<MsgHandler>),
    RemoveHandler(u32),
    AddSubscriber(u32, Option<u16>, Sender<Result<Bc>>),
    Disconnect,
}

impl std::fmt::Debug for PollCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollCommand::Bc(_) => f.write_str("PollCommand::Bc"),
            PollCommand::AddHandler(_, _) => f.write_str("PollCommand::AddHandler"),
            PollCommand::RemoveHandler(_) => f.write_str("PollCommand::RemoveHandler"),
            PollCommand::AddSubscriber(_, _, _) => f.write_str("PollCommand::AddSubscriber"),
            PollCommand::Disconnect => f.write_str("PollCommand::Disconnect"),
        }
    }
}

struct Poller {
    subscribers: Subscriber,
    sink: Sender<Result<Bc>>,
    reciever: ReceiverStream<PollCommand>,
    last_cleanup: Instant,
}

impl Poller {
    async fn run(&mut self) -> Result<()> {
        let cancel = CancellationToken::new();
        let _dropguard = cancel.clone().drop_guard();
        let cleanup_interval = std::time::Duration::from_secs(SUBSCRIBER_CLEANUP_INTERVAL_SECS);

        while let Some(command) = self.reciever.next().await {
            // Periodic cleanup of closed subscribers (every N seconds instead of per-message)
            // This reduces CPU overhead from O(n) per frame to O(n) per interval
            if self.last_cleanup.elapsed() >= cleanup_interval {
                let before = self
                    .subscribers
                    .num
                    .values()
                    .map(|v| v.len())
                    .sum::<usize>();
                self.subscribers
                    .num
                    .iter_mut()
                    .for_each(|(_, channels)| channels.retain(|_, channel| !channel.is_closed()));
                self.subscribers
                    .num
                    .retain(|_, channels| !channels.is_empty());
                let after = self
                    .subscribers
                    .num
                    .values()
                    .map(|v| v.len())
                    .sum::<usize>();
                if before != after {
                    debug!("Cleaned up {} closed subscriber channels", before - after);
                }
                self.last_cleanup = Instant::now();
            }
            // Handle the command
            match command {
                PollCommand::Bc(boxed_response) => {
                    match *boxed_response {
                        Ok(response) => {
                            let msg_id = response.meta.msg_id;
                            let msg_num = response.meta.msg_num;
                            log::trace!(
                                "Looking for ID: {} with num: {}, in {:?} and {:?}",
                                msg_id,
                                msg_num,
                                self.subscribers.id.keys().to_owned(),
                                self.subscribers
                                    .num
                                    .iter()
                                    .map(|(k, v)| (k, v.keys()))
                                    .collect::<Vec<_>>(),
                            );
                            match (
                                self.subscribers.id.get(&msg_id),
                                self.subscribers.num.get_mut(&msg_id), // Both filter first on ID
                            ) {
                                (Some(occ), _) => {
                                    log::trace!("Calling ID callback");
                                    let occ = occ.clone();
                                    let sink = self.sink.clone();
                                    // Move this on another thread coz I have NO idea
                                    // how long the callback will run for
                                    // and we must NOT hang
                                    let cancel = cancel.clone();
                                    tokio::task::spawn(async move {
                                        tokio::select! {
                                            _ = cancel.cancelled() => Result::Ok(()),
                                            v = occ(&response) => {
                                                if let Some(reply) = v {
                                                    // Verify message number matches - log error instead of panic
                                                    // to keep camera connection alive for ALPR reliability
                                                    if reply.meta.msg_num != response.meta.msg_num {
                                                        error!(
                                                            "Message handler returned wrong msg_num: expected {}, got {}",
                                                            response.meta.msg_num, reply.meta.msg_num
                                                        );
                                                        return Result::Ok(());
                                                    }
                                                    if let Err(e) = sink.send(Ok(reply)).await {
                                                        warn!("Failed to send handler reply: {}", e);
                                                    }
                                                }
                                                Result::Ok(())
                                            }
                                        }
                                    });
                                    log::trace!("Called ID callback");
                                }
                                (None, Some(occ)) => {
                                    let sender = if let Some(sender) =
                                        occ.get(&Some(msg_num)).filter(|a| !a.is_closed()).cloned()
                                    {
                                        // Connection with id exists and is not closed
                                        Some(sender)
                                    } else if let Some(sender) = occ.get(&None).cloned() {
                                        // Upgrade a None to a known MsgID
                                        occ.remove(&None);
                                        occ.insert(Some(msg_num), sender.clone());
                                        Some(sender)
                                    } else if occ
                                        .get(&Some(msg_num))
                                        .map(|a| a.is_closed())
                                        .unwrap_or(false)
                                    {
                                        // Connection is closed and there is no None to replace it
                                        // Remove it for cleanup and report no sender
                                        occ.remove(&Some(msg_num));
                                        None
                                    } else {
                                        None
                                    };
                                    if let Some(sender) = sender {
                                        let capacity = sender.capacity();
                                        let max_capacity = sender.max_capacity();
                                        let threshold = max_capacity / 10; // 10% remaining

                                        if capacity == 0 {
                                            warn!(
                                                "OBSERVE: Subscriber saturated for msg {} (ID: {}), dropping subscriber to preserve stream integrity",
                                                msg_num, msg_id
                                            );
                                            occ.remove(&Some(msg_num));
                                            continue;
                                        }

                                        if capacity <= threshold {
                                            debug!(
                                                "Channel low: {}/{} for msg {} (ID: {})",
                                                capacity, max_capacity, &msg_num, &msg_id
                                            );
                                        } else {
                                            trace!(
                                                "Channel: {}/{} for msg {} (ID: {})",
                                                capacity,
                                                max_capacity,
                                                &msg_num,
                                                &msg_id
                                            );
                                        }

                                        match sender.try_send(Ok(response)) {
                                            Ok(()) => {}
                                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                                warn!(
                                                    "OBSERVE: Subscriber full for msg {} (ID: {}), dropping subscriber to avoid backpressure",
                                                    msg_num, msg_id
                                                );
                                                occ.remove(&Some(msg_num));
                                            }
                                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                warn!(
                                                    "Subscriber closed for msg {} (ID: {}), dropping subscriber",
                                                    msg_num, msg_id
                                                );
                                                occ.remove(&Some(msg_num));
                                            }
                                        }
                                    } else {
                                        trace!(
                                            "Ignoring uninteresting message id {} (number: {})",
                                            msg_id,
                                            msg_num
                                        );
                                        trace!("Contents: {:?}", response);
                                    }
                                }
                                (None, None) => {
                                    trace!(
                                        "Ignoring uninteresting message id {} (number: {})",
                                        msg_id,
                                        msg_num
                                    );
                                    trace!("Contents: {:?}", response);
                                }
                            }
                        }
                        Err(e) => {
                            for sub in self.subscribers.num.values() {
                                for sender in sub.values() {
                                    let _ = sender.send(Err(e.clone())).await;
                                }
                            }
                            self.subscribers.num.clear();
                            self.subscribers.id.clear();
                            return Err(e);
                        }
                    }
                }
                PollCommand::AddHandler(msg_id, handler) => {
                    match self.subscribers.id.entry(msg_id) {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(handler);
                        }
                        Entry::Occupied(_) => {
                            return Err(Error::SimultaneousSubscriptionId { msg_id });
                        }
                    };
                }
                PollCommand::RemoveHandler(msg_id) => {
                    self.subscribers.id.remove(&msg_id);
                }
                PollCommand::AddSubscriber(msg_id, msg_num, tx) => {
                    match self
                        .subscribers
                        .num
                        .entry(msg_id)
                        .or_default()
                        .entry(msg_num)
                    {
                        Entry::Vacant(vac_entry) => {
                            vac_entry.insert(tx);
                        }
                        Entry::Occupied(mut occ_entry) => {
                            if occ_entry.get().is_closed() {
                                occ_entry.insert(tx);
                            } else {
                                // log::error!("Failed to subscribe in bcconn to {:?} for {:?}", msg_num, msg_id);
                                let _ = tx
                                    .send(Err(Error::SimultaneousSubscription { msg_num }))
                                    .await;
                            }
                        }
                    };
                }
                PollCommand::Disconnect => {
                    return Err(Error::ConnectionShutdown);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify channel capacity constants are sized appropriately
    /// These values affect memory usage and buffering behavior
    #[test]
    fn test_channel_capacities_are_reasonable() {
        // Message channel should hold several seconds of video at 30fps
        // 500 / 30fps = ~16 seconds
        assert!(
            MESSAGE_CHANNEL_CAPACITY >= 100,
            "Message channel too small for video buffering"
        );
        assert!(
            MESSAGE_CHANNEL_CAPACITY <= 2000,
            "Message channel too large, excessive memory use"
        );

        // Poll command channel should be larger than message channel
        // to prevent command starvation
        assert!(
            POLL_COMMAND_CAPACITY > MESSAGE_CHANNEL_CAPACITY,
            "Poll command channel should be larger than message channel"
        );

        // Subscriber channel should match message channel
        // to prevent asymmetric backpressure
        assert_eq!(
            SUBSCRIBER_CHANNEL_CAPACITY, MESSAGE_CHANNEL_CAPACITY,
            "Subscriber and message channels should match"
        );
    }

    /// Verify that capacity values haven't accidentally regressed
    /// to old smaller values
    #[test]
    fn test_channel_capacities_not_regressed() {
        // Pre-fix values were 100/200/100 - ensure we haven't regressed
        assert!(
            MESSAGE_CHANNEL_CAPACITY >= 500,
            "Message channel capacity regressed below 500"
        );
        assert!(
            POLL_COMMAND_CAPACITY >= 1000,
            "Poll command capacity regressed below 1000"
        );
        assert!(
            SUBSCRIBER_CHANNEL_CAPACITY >= 500,
            "Subscriber channel capacity regressed below 500"
        );
    }
}
