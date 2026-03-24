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
const RECONNECT_MIN_BACKOFF: Duration = Duration::from_millis(500);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(15);
const RECONNECT_JITTER_MAX: Duration = Duration::from_millis(250);
const RECONNECT_STABLE_RESET: Duration = Duration::from_secs(60);

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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ReconnectFailureKind {
    Transient,
    Noisy,
    Fatal,
}

fn classify_reconnect_failure(error: &anyhow::Error) -> ReconnectFailureKind {
    match error.downcast_ref::<neolink_core::Error>() {
        Some(neolink_core::Error::CameraLoginFail) => ReconnectFailureKind::Fatal,
        Some(
            neolink_core::Error::TimeoutDisconnected
            | neolink_core::Error::Timeout(_)
            | neolink_core::Error::StreamFinished
            | neolink_core::Error::DroppedConnection
            | neolink_core::Error::DroppedConnectionTry(_)
            | neolink_core::Error::BroadcastDroppedConnectionTry(_)
            | neolink_core::Error::TokioBcSendError
            | neolink_core::Error::ConnectionUnavailable
            | neolink_core::Error::BcUdpTimeout
            | neolink_core::Error::BcUdpReconnectTimeout,
        ) => ReconnectFailureKind::Noisy,
        Some(neolink_core::Error::UnintelligibleReply { .. }) => ReconnectFailureKind::Noisy,
        Some(neolink_core::Error::CameraServiceUnavailable { .. }) => {
            ReconnectFailureKind::Transient
        }
        _ => ReconnectFailureKind::Transient,
    }
}

fn format_ping_latency_metrics(stats: &PingLatencyStats) -> String {
    stats.format_ms()
}

fn jittered_backoff(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    let jitter_cap = RECONNECT_JITTER_MAX.as_millis() as u64;
    let spread = std::cmp::max(base_ms / 4, jitter_cap / 2);
    let low = base_ms.saturating_sub(spread);
    let high = base_ms.saturating_add(spread);
    let chosen = if low >= high {
        base_ms
    } else {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        low + (seed % (high - low + 1))
    };
    Duration::from_millis(chosen.clamp(
        RECONNECT_MIN_BACKOFF.as_millis() as u64,
        RECONNECT_MAX_BACKOFF.as_millis() as u64,
    ))
}

fn next_backoff_delay(
    current: Duration,
    kind: ReconnectFailureKind,
    session_uptime: Duration,
) -> Duration {
    if session_uptime >= RECONNECT_STABLE_RESET {
        return RECONNECT_MIN_BACKOFF;
    }

    let multiplier: u64 = match kind {
        ReconnectFailureKind::Transient => 2,
        ReconnectFailureKind::Noisy => 3,
        ReconnectFailureKind::Fatal => 1,
    };

    let current_ms = current.as_millis() as u64;
    let mut next_ms = current_ms.saturating_mul(multiplier);
    if next_ms == 0 {
        next_ms = RECONNECT_MIN_BACKOFF.as_millis() as u64;
    }
    let capped = Duration::from_millis(next_ms).min(RECONNECT_MAX_BACKOFF);
    let jittered = jittered_backoff(capped);
    if jittered < RECONNECT_MIN_BACKOFF {
        RECONNECT_MIN_BACKOFF
    } else {
        jittered
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
        log::info!("METRIC camera={} event=connect_attempt", name);
        let connect_start = std::time::Instant::now();
        let camera = Arc::new(connect_and_login(config).await?);
        let connect_elapsed = connect_start.elapsed();
        log::info!(
            "{}: Connected to camera in {:.1}s",
            name,
            connect_elapsed.as_secs_f64()
        );
        log::info!(
            "METRIC camera={} event=connect_success connect_ms={:.1}",
            name,
            connect_elapsed.as_secs_f64() * 1000.0
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
                                format_ping_latency_metrics(&ping_latency),
                                missed_pings,
                                MAX_MISSED_PINGS
                            );
                            log::info!(
                                "METRIC camera={} event=health uptime_ms={} ping_ok={} ping_timeout={} ping_bad_reply={} ping_rtt_ms={} consecutive_failures={} max_missed_pings={}",
                                name,
                                session_start.elapsed().as_millis(),
                                ping_ok,
                                ping_timeout,
                                ping_unintelligible + ping_other,
                                format_ping_latency_metrics(&ping_latency),
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
                        format_ping_latency_metrics(&ping_latency)
                    );
                    log::info!(
                        "METRIC camera={} event=session_end outcome=degraded uptime_ms={} ping_ok={} ping_timeout={} ping_bad_reply={} ping_rtt_ms={}",
                        name,
                        session_start.elapsed().as_millis(),
                        ping_ok,
                        ping_timeout,
                        ping_unintelligible + ping_other,
                        format_ping_latency_metrics(&ping_latency)
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
                        format_ping_latency_metrics(&ping_latency)
                    );
                    log::info!(
                        "METRIC camera={} event=session_end outcome=normal uptime_ms={} ping_ok={} ping_timeout={} ping_bad_reply={} ping_rtt_ms={}",
                        name,
                        session_start.elapsed().as_millis(),
                        ping_ok,
                        ping_timeout,
                        ping_unintelligible + ping_other,
                        format_ping_latency_metrics(&ping_latency)
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
            format_ping_latency_metrics(&ping_latency)
        );
        log::info!(
            "METRIC camera={} event=session_closed uptime_ms={} ping_ok={} ping_timeout={} ping_bad_reply={} ping_rtt_ms={}",
            name,
            session_start.elapsed().as_millis(),
            ping_ok,
            ping_timeout,
            ping_unintelligible + ping_other,
            format_ping_latency_metrics(&ping_latency)
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
        // Geometric series with jitter prevents reconnect storms when cameras flap.
        let mut backoff = RECONNECT_MIN_BACKOFF;

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

            let session_uptime = now.elapsed();

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
                            let kind = classify_reconnect_failure(&e);
                            backoff = next_backoff_delay(backoff, kind, session_uptime);
                            log::info!(
                                "METRIC camera={} event=reconnect failure_kind={:?} session_uptime_ms={} backoff_ms={}",
                                name,
                                kind,
                                session_uptime.as_millis(),
                                backoff.as_millis()
                            );
                            log::info!("{name}: Attempt reconnect in {:?}", backoff);
                            sleep(backoff).await;
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
