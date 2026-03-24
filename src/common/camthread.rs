use std::sync::{Arc, Weak};
use tokio::{
    sync::watch::{Receiver as WatchReceiver, Sender as WatchSender},
    time::{interval, sleep, timeout, Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use crate::{config::CameraConfig, utils::connect_and_login, AnyResult};
use neolink_core::bc_protocol::BcCamera;

// =============================================================================
// Camera Keepalive Configuration
// =============================================================================
// These control how we detect if a camera connection is still alive.
//
// Detection time calculation:
//   Total = PING_INTERVAL × MAX_MISSED_PINGS = 15s × 10 = 150 seconds
//
// This is intentionally longer than TCP keepalive (120s) because:
// - TCP keepalive detects network-level failures (cable unplugged, router down)
// - Ping detects application-level failures (camera firmware hang, protocol error)
// - During high video traffic, pings compete with video frames for channel space
//
// Trade-offs:
// - Shorter intervals = faster detection, but more load during streaming
// - Longer timeouts = more resilient to transient delays, but slower detection

/// How often to send a ping to check if the camera is responsive
const PING_INTERVAL: Duration = Duration::from_secs(15);

/// How long to wait for a ping response before counting it as missed
/// Longer than ping interval to handle message channel backpressure during video streaming
const PING_TIMEOUT: Duration = Duration::from_secs(30);

/// Number of consecutive missed pings before declaring the connection dead
const MAX_MISSED_PINGS: u32 = 10;

/// Delay after connection to allow camera firmware to fully initialize
/// Some cameras return errors if queried too quickly after login
const CAMERA_WAKEUP_DELAY: Duration = Duration::from_secs(2);

#[derive(Default, Debug, Clone)]
struct PingLatencyStats {
    count: u64,
    min_ms: f64,
    max_ms: f64,
    sum_ms: f64,
    sum_sq_ms: f64,
}

impl PingLatencyStats {
    fn record(&mut self, elapsed: Duration) {
        let ms = elapsed.as_secs_f64() * 1000.0;
        if self.count == 0 {
            self.min_ms = ms;
            self.max_ms = ms;
        } else {
            self.min_ms = self.min_ms.min(ms);
            self.max_ms = self.max_ms.max(ms);
        }
        self.count += 1;
        self.sum_ms += ms;
        self.sum_sq_ms += ms * ms;
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn format_ms(&self) -> String {
        if self.is_empty() {
            "n/a".to_string()
        } else {
            let avg_ms = self.sum_ms / self.count as f64;
            let variance = (self.sum_sq_ms / self.count as f64) - (avg_ms * avg_ms);
            let mdev_ms = variance.max(0.0).sqrt();
            format!(
                "min={:.1} max={:.1} avg={:.1} mdev={:.1}",
                self.min_ms, self.max_ms, avg_ms, mdev_ms
            )
        }
    }
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum NeoCamThreadState {
    Connected,
    Disconnected,
}

pub(crate) struct NeoCamThread {
    state: WatchReceiver<NeoCamThreadState>,
    config: WatchReceiver<CameraConfig>,
    cancel: CancellationToken,
    camera_watch: WatchSender<Weak<BcCamera>>,
}

impl NeoCamThread {
    pub(crate) async fn new(
        watch_state_rx: WatchReceiver<NeoCamThreadState>,
        watch_config_rx: WatchReceiver<CameraConfig>,
        camera_watch_tx: WatchSender<Weak<BcCamera>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            state: watch_state_rx,
            config: watch_config_rx,
            cancel,
            camera_watch: camera_watch_tx,
        }
    }
    async fn run_camera(&mut self, config: &CameraConfig) -> AnyResult<()> {
        let name = config.name.clone();
        log::debug!("{}: Attempting connection", name);
        let connect_start = std::time::Instant::now();
        let camera = Arc::new(connect_and_login(config).await?);
        let connect_elapsed = connect_start.elapsed();
        log::info!(
            "{}: Connected to camera in {:.1}s",
            name,
            connect_elapsed.as_secs_f64()
        );
        log::info!(
            "{}: Connection status=healthy, idle_disconnect={}, update_time={}, update_to={}s",
            name,
            config.idle_disconnect,
            config.update_time,
            config.pause.motion_timeout
        );

        sleep(CAMERA_WAKEUP_DELAY).await;
        if let Err(e) = update_camera_time(&camera, &name, config.update_time).await {
            log::warn!(
                "{}: Could not set camera time (perhaps your login is not an admin): {e:?}",
                name
            );
        }
        sleep(CAMERA_WAKEUP_DELAY).await;

        self.camera_watch.send_replace(Arc::downgrade(&camera));

        let cancel_check = self.cancel.clone();
        let mut health_tick = interval(Duration::from_secs(60));
        health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut ping_ok = 0usize;
        let mut ping_timeout = 0usize;
        let mut ping_unintelligible = 0usize;
        let mut ping_other = 0usize;
        let mut ping_latency = PingLatencyStats::default();
        let session_start = Instant::now();
        // Now we wait for a disconnect
        tokio::select! {
            _ = cancel_check.cancelled() => {
                AnyResult::Ok(())
            }
            v = camera.join() => {
                v?;
                Ok(())
            },
            v = async {
                let mut ping_interval = interval(PING_INTERVAL);
                ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut missed_pings = 0;
                loop {
                    tokio::select! {
                        _ = health_tick.tick() => {
                            log::info!(
                                "{}: Connection status=healthy, uptime={:?}, ping_ok={}, ping_timeout={}, ping_bad_reply={}, ping_ms={}, consecutive_failures={}/{}",
                                name,
                                session_start.elapsed(),
                                ping_ok,
                                ping_timeout,
                                ping_unintelligible + ping_other,
                                ping_latency.format_ms(),
                                missed_pings,
                                MAX_MISSED_PINGS
                            );
                        }
                        _ = ping_interval.tick() => {
                            log::trace!("Sending ping");
                            let ping_start = Instant::now();
                            match timeout(PING_TIMEOUT, camera.get_linktype()).await {
                                Ok(Ok(_)) => {
                                    ping_ok += 1;
                                    ping_latency.record(ping_start.elapsed());
                                    log::trace!("Ping reply received");
                                    missed_pings = 0;
                                    continue
                                },
                                Ok(Err(neolink_core::Error::UnintelligibleReply { reply, why })) => {
                                    ping_unintelligible += 1;
                                    log::warn!(
                                        "{}: Unintelligible ping reply from camera: {reply:?}: {why}",
                                        name
                                    );
                                    missed_pings += 1;
                                    if missed_pings < MAX_MISSED_PINGS {
                                        continue;
                                    } else {
                                        log::error!(
                                            "{}: ping health failed after {} consecutive bad replies (ok={}, timeout={}, bad_reply={})",
                                            name,
                                            missed_pings,
                                            ping_ok,
                                            ping_timeout,
                                            ping_unintelligible + ping_other
                                        );
                                        break Err(anyhow::anyhow!(
                                            "Timed out waiting for camera ping reply"
                                        ));
                                    }
                                },
                                Ok(Err(e)) => {
                                    ping_other += 1;
                                    log::warn!(
                                        "{}: Ping failed with camera error: {:?}",
                                        name,
                                        e
                                    );
                                    break Err(e.into());
                                },
                                Err(_) => {
                                    // Timeout
                                    ping_timeout += 1;
                                    missed_pings += 1;
                                    if missed_pings < MAX_MISSED_PINGS {
                                        log::debug!(
                                            "{}: Ping timeout ({}/{}), will retry",
                                            name,
                                            missed_pings,
                                            MAX_MISSED_PINGS
                                        );
                                        continue;
                                    } else {
                                        log::error!(
                                            "{}: ping health failed after {} consecutive timeouts (ok={}, timeout={}, bad_reply={})",
                                            name,
                                            missed_pings,
                                            ping_ok,
                                            ping_timeout,
                                            ping_unintelligible + ping_other
                                        );
                                        break Err(anyhow::anyhow!("Timed out waiting for camera ping reply"));
                                    }
                                }
                            }
                        }
                    }
                }
            } => {
                if let Err(ref e) = v {
                    log::warn!(
                        "{}: Connection status=degraded, uptime={:?}, ping_ok={}, ping_timeout={}, ping_bad_reply={}, ping_ms={}",
                        name,
                        session_start.elapsed(),
                        ping_ok,
                        ping_timeout,
                        ping_unintelligible + ping_other,
                        ping_latency.format_ms()
                    );
                    log::debug!("{}: camera session ended with error: {:?}", name, e);
                } else {
                    log::info!(
                        "{}: Connection status=ended normally, uptime={:?}, ping_ok={}, ping_timeout={}, ping_bad_reply={}, ping_ms={}",
                        name,
                        session_start.elapsed(),
                        ping_ok,
                        ping_timeout,
                        ping_unintelligible + ping_other,
                        ping_latency.format_ms()
                    );
                }
                v
            },
        }?;

        log::info!(
            "{}: Camera session closed cleanly after {:?} (ping_ok={}, ping_timeout={}, ping_bad_reply={}, ping_ms={})",
            name,
            session_start.elapsed(),
            ping_ok,
            ping_timeout,
            ping_unintelligible + ping_other,
            ping_latency.format_ms()
        );
        let _ = camera.logout().await;
        let _ = camera.shutdown().await;

        Ok(())
    }

    // Will run and attempt to maintain the connection
    //
    // A watch sender is used to send the new camera
    // whenever it changes
    pub(crate) async fn run(&mut self) -> AnyResult<()> {
        // Reconnect backoff covers camera reboot time (30-90s).
        // Geometric series: 0.5 + 1 + 2 + 4 + 8 + 15 = 30.5s to cover the window.
        const MAX_BACKOFF: Duration = Duration::from_secs(15);
        const MIN_BACKOFF: Duration = Duration::from_millis(500);

        let mut backoff = MIN_BACKOFF;

        loop {
            self.state
                .clone()
                .wait_for(|state| matches!(state, NeoCamThreadState::Connected))
                .await?;
            let mut config_rec = self.config.clone();

            let config = config_rec.borrow_and_update().clone();
            let now = Instant::now();
            let name = config.name.clone();

            let mut state = self.state.clone();

            let res = tokio::select! {
                Ok(_) = config_rec.changed() => {
                    None
                }
                Ok(_) = state.wait_for(|state| matches!(state, NeoCamThreadState::Disconnected)) => {
                    log::trace!("State changed to disconnect");
                    None
                }
                v = self.run_camera(&config) => {
                    Some(v)
                }
            };
            self.camera_watch.send_replace(Weak::new());

            if res.is_none() {
                // If None go back and reload NOW
                //
                // This occurs if there was a config change
                log::trace!("Config change or Manual disconnect");
                continue;
            }

            // Else we see what the result actually was
            let result = res.unwrap();

            if now.elapsed() > Duration::from_secs(60) {
                // Command ran long enough to be considered a success
                backoff = MIN_BACKOFF;
            }
            if backoff > MAX_BACKOFF {
                backoff = MAX_BACKOFF;
            }

            match result {
                Ok(()) => {
                    // Normal shutdown
                    log::trace!("Normal camera shutdown");
                    self.cancel.cancel();
                    return Ok(());
                }
                Err(e) => {
                    // An error
                    // Check if it is non-retry
                    let e_inner = e.downcast_ref::<neolink_core::Error>();
                    match e_inner {
                        Some(neolink_core::Error::CameraLoginFail) => {
                            // Fatal
                            log::error!("{name}: Login credentials were not accepted");
                            self.cancel.cancel();
                            return Err(e);
                        }
                        _ => {
                            // Non fatal
                            log::warn!("{name}: Connection Lost: {:?}", e);
                            log::info!("{name}: Attempt reconnect in {:?}", backoff);
                            sleep(backoff).await;
                            backoff *= 2;
                        }
                    }
                }
            }
        }
    }
}

impl Drop for NeoCamThread {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn update_camera_time(camera: &BcCamera, name: &str, update_time: bool) -> AnyResult<()> {
    let cam_time = camera.get_time().await?;
    let mut update = false;
    if let Some(time) = cam_time {
        log::info!("{}: Camera time is already set: {}", name, time);
        if update_time {
            update = true;
        }
    } else {
        update = true;
        log::warn!("{}: Camera has no time set, Updating", name);
    }
    if update {
        use std::time::SystemTime;
        let new_time = SystemTime::now();

        log::info!("{}: Setting time to {:?}", name, new_time);
        match camera.set_time(new_time.into()).await {
            Ok(_) => {
                let cam_time = camera.get_time().await?;
                if let Some(time) = cam_time {
                    log::info!("{}: Camera time is now set: {}", name, time);
                }
            }
            Err(e) => {
                log::error!(
                    "{}: Camera did not accept new time (is user an admin?): Error: {:?}",
                    name,
                    e
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify ping timing constants are configured correctly
    #[test]
    fn test_ping_timing_constants() {
        // Ping interval should be reasonable (not too aggressive, not too slow)
        assert!(
            PING_INTERVAL >= Duration::from_secs(5),
            "Ping interval too aggressive, will cause excessive traffic"
        );
        assert!(
            PING_INTERVAL <= Duration::from_secs(60),
            "Ping interval too slow, detection will take too long"
        );

        // Ping timeout should be longer than ping interval
        // to handle backpressure during high traffic
        assert!(
            PING_TIMEOUT >= PING_INTERVAL,
            "Ping timeout should be >= ping interval"
        );

        // Max missed pings should allow for transient failures
        assert!(
            MAX_MISSED_PINGS >= 3,
            "Too few missed pings allowed, will cause spurious disconnects"
        );
        assert!(
            MAX_MISSED_PINGS <= 20,
            "Too many missed pings allowed, detection too slow"
        );
    }

    /// Verify total detection time is reasonable
    #[test]
    fn test_total_detection_time() {
        // Total detection time = PING_INTERVAL * MAX_MISSED_PINGS
        let total_detection = PING_INTERVAL.as_secs() * MAX_MISSED_PINGS as u64;

        // Expected: 15s × 10 = 150s
        // Bounds tightened to catch regressions while allowing some flexibility
        assert!(
            total_detection <= 180,
            "Total detection time too long: {}s (expected ~150s)",
            total_detection
        );

        // Should be >= TCP keepalive (120s) to avoid duplicate detection
        // TCP keepalive handles network failures; ping handles application issues
        assert!(
            total_detection >= 120,
            "Detection time {}s shorter than TCP keepalive (120s)",
            total_detection
        );
    }

    /// Verify camera wakeup delay is reasonable
    #[test]
    fn test_camera_wakeup_delay() {
        assert!(
            CAMERA_WAKEUP_DELAY >= Duration::from_secs(1),
            "Camera wakeup delay too short"
        );
        assert!(
            CAMERA_WAKEUP_DELAY <= Duration::from_secs(10),
            "Camera wakeup delay too long"
        );
    }
}
