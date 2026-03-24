use super::*;

use crate::common::UseCounter;
use neolink_core::{bc_protocol::StreamKind, bcmedia::model::BcMedia, Error as CoreError};
use tokio::sync::mpsc::{error::TrySendError, Receiver as MpscReceiver};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "pushnoti")]
use crate::common::PushNoti;

/// Channel capacity for buffering video frames between camera and RTSP server
/// At 30fps, 100 frames ≈ 3.3 seconds of buffering capacity
/// This handles client-side consumption delays and network jitter
/// while preventing excessive memory usage (~10 MB per stream at 8 Mbps)
const MEDIA_CHANNEL_CAPACITY: usize = 100;

impl NeoInstance {
    /// Streams a camera source while not paused
    pub(crate) async fn stream_while_live(
        &self,
        stream: StreamKind,
    ) -> AnyResult<MpscReceiver<BcMedia>> {
        let config = self.config().await?.borrow().clone();
        let name = config.name.clone();

        let media_rx = if config.pause.on_motion {
            let (media_tx, media_rx) = tokio::sync::mpsc::channel(MEDIA_CHANNEL_CAPACITY);
            let counter = UseCounter::new().await;
            let cleanup_cancel = CancellationToken::new();

            let mut md = self.motion().await?;
            let mut tasks = JoinSet::new();
            // Stream for 5s on a new client always
            // This lets us negotiate the camera stream type
            let init_permit = counter.create_activated().await?;
            let init_cancel = cleanup_cancel.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = init_cancel.cancelled() => {}
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                        drop(init_permit);
                    }
                }
            });

            // Create the permit for controlling the motion
            let mut md_permit = {
                let md_state = md.borrow_and_update().clone();
                match md_state {
                    MdState::Start(_) => {
                        log::info!("{name}::{stream:?}: Starting with Motion");
                        counter.create_activated().await?
                    }
                    MdState::Stop(_) | MdState::Unknown => {
                        log::info!("{name}::{stream:?}: Waiting with Motion");
                        counter.create_deactivated().await?
                    }
                }
            };
            // Now listen to the motion
            let thread_name = name.clone();
            let motion_cancel = cleanup_cancel.clone();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = motion_cancel.cancelled() => {
                            break AnyResult::Ok(());
                        }
                        changed = md.changed() => {
                            match changed {
                                Ok(_) => {
                                    let md_state: MdState = md.borrow_and_update().clone();
                                    match md_state {
                                        MdState::Start(_) => {
                                            log::info!("{thread_name}::{stream:?}: Motion Started");
                                            md_permit.activate().await?;
                                        }
                                        MdState::Stop(_) => {
                                            log::info!("{thread_name}::{stream:?}: Motion Stopped");
                                            md_permit.deactivate().await?;
                                        }
                                        MdState::Unknown => {}
                                    }
                                }
                                Err(e) => {
                                    break AnyResult::Err(e.into());
                                }
                            }
                        }
                    }
                }
            });

            #[cfg(feature = "pushnoti")]
            {
                // Creates a permit for controlling based on the PN
                let pn_permit = counter.create_deactivated().await?;
                let mut pn = self.push_notifications().await?;
                pn.borrow_and_update(); // Ignore any PNs that have already been sent before this
                let thread_name = name.clone();
                let pn_cancel = cleanup_cancel.clone();
                tasks.spawn(async move {
                    loop {
                        tokio::select! {
                            _ = pn_cancel.cancelled() => {
                                break AnyResult::Ok(());
                            }
                            changed = pn.changed() => {
                                if let Err(e) = changed {
                                    break Err(e);
                                }
                                let noti: Option<PushNoti> = pn.borrow_and_update().clone();
                                if let Some(noti) = noti {
                                    if noti.message.contains("Motion Alert from") {
                                        log::info!(
                                            "{thread_name}::{stream:?}: Push Notification Received"
                                        );
                                        let mut new_pn_permit = pn_permit.subscribe();
                                        new_pn_permit.activate().await?;
                                        let sleeper_cancel = cleanup_cancel.clone();
                                        tokio::spawn(async move {
                                            tokio::select! {
                                                _ = sleeper_cancel.cancelled() => {}
                                                _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                                                    drop(new_pn_permit);
                                                }
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                });
            }

            // Send the camera when the pemit is active
            let camera_permit = counter.create_deactivated().await?;
            let thread_camera = self.clone();
            tokio::spawn(async move {
                let result: AnyResult<()> = loop {
                    tokio::select! {
                        _ = media_tx.closed() => {
                            break AnyResult::Ok(());
                        }
                        res = camera_permit.aquired_users() => {
                            if let Err(e) = res {
                                break AnyResult::Err(e);
                            }
                        }
                    }

                    log::debug!("Starting stream");
                    let step = tokio::select! {
                        _ = media_tx.closed() => {
                            AnyResult::Ok(())
                        }
                        v = camera_permit.dropped_users() => {
                            log::debug!("Dropped users: {v:?}");
                            v
                        },
                        v = async {
                            log::debug!("Getting stream");
                            let mut stream = thread_camera.stream(stream).await?;
                            log::debug!("Got stream");
                            while let Some(media) = stream.recv().await {
                                if let Err(err) = media_tx.try_send(media) {
                                    match err {
                                        TrySendError::Full(m) => {
                                            let media_type = match &m {
                                                BcMedia::Iframe(_) => "Iframe",
                                                BcMedia::Pframe(_) => "Pframe",
                                                BcMedia::Aac(_) => "Aac",
                                                BcMedia::Adpcm(_) => "Adpcm",
                                                _ => "Other",
                                            };
                                            log::warn!("OBSERVE: Queue-full drop stream={:?} media={}", stream, media_type);
                                        }
                                        TrySendError::Closed(_) => {
                                            return AnyResult::Ok(());
                                        }
                                    }
                                }
                            }
                            AnyResult::Ok(())
                        } => {
                            log::debug!("Stopped stream: {v:?}");
                            v
                        },
                        v = tasks.join_next() => {
                            match v {
                                Some(Ok(Ok(()))) => {
                                    log::debug!("Task finished unexpectedly");
                                    Err(anyhow!("Task ended prematurely"))
                                }
                                Some(Ok(Err(e))) => {
                                    log::debug!("Task failed: {e:?}");
                                    Err(e)
                                }
                                Some(Err(e)) => {
                                    log::debug!("Task join failed: {e:?}");
                                    Err(e.into())
                                }
                                None => {
                                    log::debug!("Task set exhausted");
                                    Err(anyhow!("Task set exhausted"))
                                }
                            }
                        }
                    };

                    if let Err(e) = step {
                        break AnyResult::Err(e);
                    }

                    log::debug!("Pausing stream");
                };

                cleanup_cancel.cancel();
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                drop(counter);
                log::debug!("Stream thread stopped {result:?}");
                result
            });

            Ok(media_rx)
        } else {
            self.stream(stream).await
        }?;

        Ok(media_rx)
    }

    /// Streams a camera source
    pub(crate) async fn stream(&self, stream: StreamKind) -> AnyResult<MpscReceiver<BcMedia>> {
        let (media_tx, media_rx) = tokio::sync::mpsc::channel(MEDIA_CHANNEL_CAPACITY);
        let config = self.config().await?.borrow().clone();
        let strict = config.strict;
        let thread_camera = self.clone();
        tokio::task::spawn(async move {
            let result = thread_camera
                .run_task(move |cam| {
                    let media_tx = media_tx.clone();
                    Box::pin(async move {
                        let mut media_stream = cam.start_video(stream, 0, strict).await?;
                        log::trace!("Camera started");
                        loop {
                            match media_stream.get_data().await {
                                Ok(Ok(media)) => match media_tx.try_send(media) {
                                    Ok(()) => {}
                                    Err(TrySendError::Full(m)) => {
                                        let media_type = match &m {
                                            BcMedia::Iframe(_) => "Iframe",
                                            BcMedia::Pframe(_) => "Pframe",
                                            BcMedia::Aac(_) => "Aac",
                                            BcMedia::Adpcm(_) => "Adpcm",
                                            _ => "Other",
                                        };
                                        log::warn!(
                                            "OBSERVE: Queue-full drop stream={:?} media={}",
                                            stream,
                                            media_type
                                        );
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        log::trace!("Stream consumer dropped");
                                        return AnyResult::Ok(());
                                    }
                                },
                                Ok(Err(e)) => {
                                    log::debug!("Recovered from stream error: {:?}", e);
                                }
                                Err(CoreError::StreamFinished) => {
                                    return Err(CoreError::DroppedConnection.into());
                                }
                                Err(e) => {
                                    return Err(e.into());
                                }
                            }
                        }
                    })
                })
                .await;

            log::debug!("Camera finished streaming: {result:?}");
        });

        Ok(media_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify media channel capacity is sized for video buffering
    #[test]
    fn test_media_channel_capacity() {
        // At 30fps, should buffer a few seconds of video
        let buffer_seconds = MEDIA_CHANNEL_CAPACITY as f64 / 30.0;

        assert!(
            buffer_seconds >= 2.0,
            "Media channel only buffers {:.1}s of video at 30fps",
            buffer_seconds
        );
        assert!(
            buffer_seconds <= 15.0,
            "Media channel buffers {:.1}s, may use excessive memory",
            buffer_seconds
        );
    }

    /// Verify capacity hasn't regressed to old value
    #[test]
    fn test_media_channel_not_regressed() {
        assert!(
            MEDIA_CHANNEL_CAPACITY >= 100,
            "Media channel capacity regressed below 100"
        );
    }
}
