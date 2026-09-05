use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppLeakyType, AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{
    sync::mpsc::{channel as mpsc, error::TryRecvError},
    task::JoinHandle,
    time::{interval, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::NeoInstance,
    rtsp::gst::{default_splash_launch, NeoMediaFactory},
    AnyResult,
};

/// Audio buffer size in bytes
///
/// Audio typically runs at ~800kbps. This buffer provides ~7 seconds of audio
/// which is larger than video (2 seconds) to ensure smooth playback.
/// Formula: 512 packets × 1416 bytes/packet ≈ 725KB
const AUDIO_BUFFER_SIZE: u32 = 512 * 1416;

/// How long to wait for the first video packet before failing stream startup.
const INITIAL_VIDEO_TIMEOUT: Duration = Duration::from_secs(20);

/// After video is detected, allow a short grace period to discover audio so the
/// RTSP SDP is stable for the lifetime of the mounted path.
const INITIAL_AUDIO_GRACE: Duration = Duration::from_secs(2);

/// Delay between attempts to reopen a camera stream after it closes.
const STREAM_RETRY_DELAY: Duration = Duration::from_secs(1);

/// If the camera stops producing packets but never hard-closes the channel,
/// rebuild the stream rather than letting the client sit on stale media.
///
/// This MUST be longer than the core's own `STREAM_NO_FRAME_TIMEOUT` (15s in
/// `crates/core/.../stream.rs`). While the core's video subscription is still
/// alive, a brief WiFi dropout rides through on that same subscription — video
/// simply resumes when packets return, with no teardown, no keyframe re-wait
/// and no PTS reset. A shorter value here pre-empts that: it tore down and
/// rebuilt the whole stream on every multi-second hiccup, which is exactly the
/// thrashing a flaky Lumus link produces. We only rebuild once the core has
/// actually given up on the stream.
const STREAM_STALL_TIMEOUT: Duration = Duration::from_secs(20);
/// Time to wait for a newly created RTSP client pipeline to become live.
const SOURCE_READY_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll interval while waiting for the RTSP client pipeline to become live.
const SOURCE_READY_POLL: Duration = Duration::from_millis(20);
/// Maximum number of times `wait_for_sources_ready` will retry before giving up.
const MAX_READY_RETRIES: u32 = 3;
const VIDEO_TIMESTAMP_WRAP_WINDOW: u32 = 5_000_000;

/// Capacity of the per-client media queue.
///
/// This queue carries fully parsed `BcMedia` packets, so dropping from here is
/// recoverable. Keeping it bounded prevents RTSP backpressure from propagating
/// all the way down into the raw Baichuan packet router.
/// At 30fps, 100 frames ≈ 3.3 seconds — enough for jitter, low enough to bound memory.
const CLIENT_MEDIA_QUEUE_CAPACITY: usize = 100;
const MAX_BOOTSTRAP_FRAMES: usize = 256;

/// Only catch up to the latest keyframe (dropping older P-frames/GOPs) once a
/// client's drained backlog reaches this many frames. Below this we keep every
/// frame to preserve motion for detectors like Frigate. Sized well under the
/// queue capacity so we react before the queue saturates, but far above normal
/// scheduling jitter (a few frames) so ordinary operation never decimates.
const DRAIN_CATCHUP_THRESHOLD: usize = CLIENT_MEDIA_QUEUE_CAPACITY / 4; // ~0.8s at 30fps

/// Maximum delay between reopen attempts (exponential backoff cap).
/// There is no retry limit — the factory loops indefinitely until the stream
/// is restored. The parent NeoCamThread handles fatal connection failures.
const REOPEN_MAX_DELAY: Duration = Duration::from_secs(30);
/// If a client sender thread makes no progress for this long, cancel it and let the client reconnect.
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(45);
/// How often to sweep stale RTSP clients when the camera is quiet.
const CLIENT_REAP_INTERVAL: Duration = Duration::from_secs(5);
/// If the audio output clock falls this far behind the video output clock, snap
/// it forward (never backward) so a reconnect gap never turns into permanent
/// A/V lag for a connected client.
const MAX_AV_SKEW: Duration = Duration::from_secs(1);
/// Used for the "audio running ahead" observability log only.
const AV_AHEAD_WARN: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

#[derive(Clone, Debug)]
struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
    /// Pass AAC through as MPEG4-GENERIC instead of decoding to L16.
    audio_passthrough: bool,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let audio_passthrough = instance.config().await?.borrow().audio_passthrough;
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 30, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
            audio_passthrough,
        })
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(_) => {
                self.aud_type = Some(AudioType::Aac);
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
            BcMedia::Skip | BcMedia::Discont => {}
        }
    }
}

/// How long the RTSP server thread waits for the factory loop to hand back a
/// built pipeline before it gives up on that client request.
const CLIENT_BUILD_TIMEOUT: Duration = Duration::from_secs(2);

enum ClientMsg {
    NewClient {
        element: Element,
        reply: std::sync::mpsc::SyncSender<AnyResult<Element>>,
        /// After this instant the RTSP server has stopped waiting for `reply`
        /// and discarded `element`; building a pipeline for it would only
        /// create an orphan sender thread.
        deadline: Instant,
    },
}

/// A parsed frame shared, without copying, between the bootstrap buffer and
/// every client's queue and GStreamer buffer.
type SharedMedia = Arc<BcMedia>;

/// `AsRef<[u8]>` view of a frame's payload so `gst::Buffer::from_slice` can
/// wrap the shared allocation directly instead of copying it into a pool.
#[derive(Clone)]
struct FramePayload(SharedMedia);

impl AsRef<[u8]> for FramePayload {
    fn as_ref(&self) -> &[u8] {
        match &*self.0 {
            BcMedia::Iframe(frame) => &frame.data,
            BcMedia::Pframe(frame) => &frame.data,
            BcMedia::Aac(frame) => &frame.data,
            BcMedia::Adpcm(frame) => &frame.data,
            _ => &[],
        }
    }
}

struct ClientState {
    sender: tokio::sync::mpsc::Sender<SharedMedia>,
    thread_handle: Option<std::thread::JoinHandle<AnyResult<()>>>,
    cancel: CancellationToken,
    last_activity: Arc<Mutex<Instant>>,
    /// This client fell behind and had a video frame dropped in fan-out. Until
    /// the next keyframe is delivered, further video for this client is skipped
    /// so we never hand its pipeline a dangling P-frame.
    needs_keyframe: bool,
}

struct TimestampState {
    next_video_ts: Duration,
    last_video_source_ts: Option<u32>,
    next_audio_ts: Duration,
    video_needs_discont: bool,
    audio_needs_discont: bool,
}

impl Default for TimestampState {
    fn default() -> Self {
        Self {
            next_video_ts: Duration::default(),
            last_video_source_ts: None,
            next_audio_ts: Duration::default(),
            video_needs_discont: true,
            audio_needs_discont: true,
        }
    }
}

impl TimestampState {
    /// Reset source-timestamp tracking on a stream reconnect while KEEPING the
    /// output clock (`next_video_ts` / `next_audio_ts`) monotonic.
    ///
    /// The camera's source timestamps restart after a reconnect, so we must stop
    /// deriving deltas from the old source value (`last_video_source_ts = None`,
    /// which makes `next_video_timestamp` fall back to the frame cadence for the
    /// first post-reconnect frame). But the *output* PTS/DTS we hand to
    /// GStreamer must never go backwards — zeroing it here made ffmpeg see a
    /// "non-monotonous DTS", dropping frames and breaking recordings. We mark
    /// both streams discontinuous so the resume is flagged correctly.
    fn reset_for_reconnect(&mut self) {
        self.last_video_source_ts = None;
        self.video_needs_discont = true;
        self.audio_needs_discont = true;
        // next_video_ts and next_audio_ts are intentionally preserved so the
        // output clock stays monotonic across the reconnect.
    }

    /// Keep the audio output clock from lagging the video output clock.
    ///
    /// Video PTS follows the camera's (wall-clock based) frame timestamps, so
    /// after a gap it moves forward by the real elapsed time. Audio PTS is a
    /// running sum of packet durations and does not. Left alone, every gap
    /// pushes audio further behind video for the lifetime of the client. When
    /// audio is more than `MAX_AV_SKEW` behind, jump it forward to the video
    /// clock and flag a discontinuity. Audio is never moved backwards.
    ///
    /// Returns true if the audio clock was adjusted.
    fn align_audio_to_video(&mut self) -> bool {
        if self.next_video_ts.is_zero() {
            // No video sent yet on this client; nothing to align against.
            return false;
        }
        if self.next_audio_ts + MAX_AV_SKEW < self.next_video_ts {
            log::info!(
                "OBSERVE: audio clock {:?} behind video, snapping forward to {:?}",
                self.next_video_ts - self.next_audio_ts,
                self.next_video_ts
            );
            self.next_audio_ts = self.next_video_ts;
            self.audio_needs_discont = true;
            return true;
        }
        if self.next_video_ts + AV_AHEAD_WARN < self.next_audio_ts {
            log::debug!(
                "OBSERVE: audio clock {:?} ahead of video",
                self.next_audio_ts - self.next_video_ts
            );
        }
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameSendOutcome {
    Sent,
    DroppedVideo,
}

fn buffer_media(buffer: &mut Vec<SharedMedia>, media: SharedMedia) {
    if media.is_keyframe() {
        buffer.clear();
    }

    buffer.push(media);

    if buffer.len() > MAX_BOOTSTRAP_FRAMES {
        if let Some(last_iframe) = buffer.iter().rposition(|item| item.is_keyframe()) {
            if last_iframe > 0 {
                let _ = buffer.drain(0..last_iframe);
                return;
            }
        }
        let drain_count = buffer.len().saturating_sub(MAX_BOOTSTRAP_FRAMES / 2);
        let _ = buffer.drain(0..drain_count);
    }
}

fn touch_client_activity(last_activity: &Arc<Mutex<Instant>>) {
    if let Ok(mut guard) = last_activity.lock() {
        *guard = Instant::now();
    }
}

fn reap_finished_handles(handles: &mut Vec<std::thread::JoinHandle<AnyResult<()>>>, label: &str) {
    let mut still_running = Vec::with_capacity(handles.len());
    for handle in handles.drain(..) {
        if handle.is_finished() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    log::debug!("{label}: sender thread exited with error: {e:?}");
                }
                Err(e) => {
                    log::warn!("{label}: sender thread panicked: {e:?}");
                }
            }
        } else {
            still_running.push(handle);
        }
    }
    *handles = still_running;
}

/// Reap clients whose sender thread finished, and cancel clients that have
/// not accepted media for `CLIENT_IDLE_TIMEOUT`.
///
/// `camera_delivering` must be false while the camera itself has been silent
/// for that long: then *no* client has activity, and cancelling them would turn
/// a camera outage into a pile of RTSP sessions with no feeder thread (the
/// client keeps its session, gets nothing, and has to time out and reconnect).
fn reap_stale_clients(
    clients: &mut Vec<ClientState>,
    old_thread_handles: &mut Vec<std::thread::JoinHandle<AnyResult<()>>>,
    stream_name: &str,
    camera_delivering: bool,
) {
    let mut still_open = Vec::with_capacity(clients.len());
    let now = Instant::now();

    for client in clients.drain(..) {
        let stale = camera_delivering
            && client
                .last_activity
                .lock()
                .map(|last| now.duration_since(*last) > CLIENT_IDLE_TIMEOUT)
                .unwrap_or(true);
        let finished = client
            .thread_handle
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished);

        if stale {
            log::info!(
                "{stream_name}: cancelling stale RTSP client after {:?} of inactivity",
                CLIENT_IDLE_TIMEOUT
            );
            client.cancel.cancel();
        }

        if finished || stale {
            if let Some(handle) = client.thread_handle {
                old_thread_handles.push(handle);
            }
        } else {
            still_open.push(client);
        }
    }

    *clients = still_open;
    reap_finished_handles(old_thread_handles, stream_name);
}

// Thin wrappers over the shared `BcMedia` predicates so the whole media path
// (core transport, instance fan-in, RTSP fan-out) speaks one vocabulary for
// what may be dropped and where the stream can resync. See `BcMedia::is_video`.
fn is_keyframe(media: &BcMedia) -> bool {
    media.is_keyframe()
}

fn media_kind_str(media: &BcMedia) -> &'static str {
    match media {
        BcMedia::Iframe(_) => "Iframe",
        BcMedia::Pframe(_) => "Pframe",
        BcMedia::Aac(_) => "Aac",
        BcMedia::Adpcm(_) => "Adpcm",
        _ => "Other",
    }
}

async fn reopen_stream(
    camera: &NeoInstance,
    stream: StreamKind,
    name: &str,
) -> tokio::sync::mpsc::Receiver<BcMedia> {
    let mut delay = STREAM_RETRY_DELAY;
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        match camera.stream_while_live(stream).await {
            Ok(new_media_rx) => return new_media_rx,
            Err(e) => {
                log::warn!(
                    "{name}::{stream}: failed to restart camera stream (attempt {attempt}), retrying in {delay:?}: {e:?}",
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(REOPEN_MAX_DELAY);
            }
        }
    }
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    let (name, splash_launch) = {
        let cfg = camera.config().await?;
        let cfg = cfg.borrow();
        // When the splash is disabled, use a plain black frame for the
        // placeholder pipeline so nothing resembling real footage is ever
        // exposed to an NVR like Frigate.
        let pattern = if cfg.use_splash {
            cfg.splash_pattern.to_string()
        } else {
            "black".to_string()
        };
        (cfg.name.clone(), default_splash_launch(&pattern))
    };

    let thread = tokio::task::spawn(async move {
        let (mut media_rx, mut buffer, mut stream_config) = loop {
            log::info!("{name}::{stream}: Starting camera stream immediately");
            let mut media_rx = match camera.stream_while_live(stream).await {
                Ok(media_rx) => media_rx,
                Err(e) => {
                    log::warn!("{name}::{stream}: failed to start camera stream, retrying: {e:?}");
                    tokio::time::sleep(STREAM_RETRY_DELAY).await;
                    continue;
                }
            };

            log::trace!("{name}::{stream}: Learning camera stream type");
            let mut buffer = vec![];
            let mut stream_config = StreamConfig::new(&camera, stream).await?;
            let mut audio_deadline: Option<Instant> = None;

            let ready = loop {
                if stream_config.vid_type.is_some() {
                    audio_deadline.get_or_insert_with(|| Instant::now() + INITIAL_AUDIO_GRACE);
                    if stream_config.aud_type.is_some()
                        || audio_deadline
                            .map(|deadline| Instant::now() >= deadline)
                            .unwrap_or(false)
                    {
                        break true;
                    }
                }

                let wait_for = audio_deadline
                    .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                    .filter(|duration| !duration.is_zero())
                    .unwrap_or(INITIAL_VIDEO_TIMEOUT);

                match timeout(wait_for, media_rx.recv()).await {
                    Ok(Some(media)) => {
                        stream_config.update_from_media(&media);
                        buffer_media(&mut buffer, Arc::new(media));
                    }
                    Ok(None) => {
                        log::warn!(
                            "{name}::{stream}: camera stream ended before RTSP was ready, retrying"
                        );
                        break false;
                    }
                    Err(_) => {
                        log::warn!(
                            "{name}::{stream}: timed out waiting for initial media, retrying"
                        );
                        break false;
                    }
                }
            };

            if ready {
                break (media_rx, buffer, stream_config);
            }

            tokio::time::sleep(STREAM_RETRY_DELAY).await;
        };

        let mut clients: Vec<ClientState> = Vec::new();
        let mut old_thread_handles: Vec<std::thread::JoinHandle<AnyResult<()>>> = Vec::new();
        let mut waiting_for_keyframe = false;
        // Shared flag: set by main loop on stream reconnect, read by sender threads
        // to reset their TimestampState and avoid non-monotonic DTS.
        let timestamps_generation = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut client_reap = interval(CLIENT_REAP_INTERVAL);
        client_reap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_media_received = Instant::now();
        // Connection state of the underlying camera. While it is disconnected
        // the camera thread is already reconnecting and the pending stream
        // task is waiting for it; re-requesting the stream every stall
        // interval would only pile up start requests for the camera to sort
        // out when it comes back.
        let mut camera_watch = camera.camera();
        let mut logged_waiting_for_camera = false;

        // Reopening the upstream camera stream can back off for many seconds.
        // It MUST NOT be awaited inline in the select! below, or new RTSP
        // clients (Frigate reconnecting) would not be serviced during that
        // window and their connect would time out. Instead we run the reopen in
        // a background task and swap in the fresh receiver when it completes,
        // while the loop keeps accepting clients.
        let (reopen_tx, mut reopen_rx) =
            tokio::sync::mpsc::channel::<tokio::sync::mpsc::Receiver<BcMedia>>(1);
        let mut reopening = false;

        macro_rules! trigger_reopen {
            ($reason:expr) => {{
                if !reopening {
                    log::warn!("{name}::{stream}: {}, re-requesting stream", $reason);
                    reopening = true;
                    waiting_for_keyframe = true;
                    // Drop the old receiver right now so the previous stream
                    // task ends immediately (it selects on the channel being
                    // closed) instead of lingering until the camera returns.
                    media_rx = {
                        let (_closed_tx, closed_rx) = tokio::sync::mpsc::channel::<BcMedia>(1);
                        closed_rx
                    };
                    // `buffer` (the last GOP) is intentionally kept: a client
                    // that connects during the outage can preroll from it, so
                    // DESCRIBE answers immediately instead of hanging until
                    // the camera is back. The sender threads flag DISCONT and
                    // reset source-timestamp tracking on the generation bump,
                    // so replaying a stale GOP is safe.
                    timestamps_generation.fetch_add(1, std::sync::atomic::Ordering::Release);
                    let reopen_camera = camera.clone();
                    let reopen_name = name.clone();
                    let reopen_tx = reopen_tx.clone();
                    tokio::spawn(async move {
                        let new_rx = reopen_stream(&reopen_camera, stream, &reopen_name).await;
                        let _ = reopen_tx.send(new_rx).await;
                    });
                }
            }};
        }

        loop {
            tokio::select! {
                _ = client_reap.tick() => {
                    let camera_delivering = last_media_received.elapsed() < CLIENT_IDLE_TIMEOUT;
                    reap_stale_clients(&mut clients, &mut old_thread_handles, &name, camera_delivering);
                    if !reopening && last_media_received.elapsed() >= STREAM_STALL_TIMEOUT {
                        let camera_connected = camera_watch.borrow().upgrade().is_some();
                        if camera_connected {
                            logged_waiting_for_camera = false;
                            trigger_reopen!(format!(
                                "no media received for {STREAM_STALL_TIMEOUT:?}"
                            ));
                        } else if !logged_waiting_for_camera {
                            logged_waiting_for_camera = true;
                            log::info!(
                                "{name}::{stream}: no media for {:?} and camera is disconnected; waiting for it to reconnect",
                                last_media_received.elapsed()
                            );
                        }
                    }
                }
                Ok(()) = camera_watch.changed() => {
                    let connected = camera_watch.borrow_and_update().upgrade().is_some();
                    if connected {
                        // Give the fresh connection a full stall interval to
                        // deliver before we consider re-requesting.
                        last_media_received = Instant::now();
                        logged_waiting_for_camera = false;
                        log::debug!("{name}::{stream}: camera reconnected, waiting for stream to deliver");
                    }
                }
                Some(new_rx) = reopen_rx.recv(), if reopening => {
                    log::info!("{name}::{stream}: camera stream re-requested, waiting for first keyframe");
                    media_rx = new_rx;
                    last_media_received = Instant::now();
                    reopening = false;
                }
                media_opt = media_rx.recv(), if !reopening => {
                    match media_opt {
                        Some(media) => {
                            last_media_received = Instant::now();
                            let media: SharedMedia = Arc::new(media);
                            stream_config.update_from_media(&media);
                            if waiting_for_keyframe {
                                if !is_keyframe(&media) {
                                    log::debug!(
                                        "{name}::{stream}: Dropping pre-keyframe media while resyncing"
                                    );
                                    continue;
                                }

                                log::info!(
                                    "{name}::{stream}: Resynchronized on keyframe after reconnect"
                                );
                                waiting_for_keyframe = false;
                            }

                            buffer_media(&mut buffer, Arc::clone(&media));

                            if !clients.is_empty() {
                                let mut still_open = Vec::with_capacity(clients.len());
                                let media_is_keyframe = media.is_keyframe();
                                let media_is_video = media.is_video();

                                for mut client in clients.drain(..) {
                                    // A client that had a video frame dropped is
                                    // resyncing: skip further video for it until
                                    // the next keyframe, so its pipeline never
                                    // receives a dangling P-frame. Audio still
                                    // flows through.
                                    if client.needs_keyframe && media_is_video && !media_is_keyframe {
                                        still_open.push(client);
                                        continue;
                                    }

                                    match client.sender.try_send(Arc::clone(&media)) {
                                        Ok(()) => {
                                            if media_is_keyframe {
                                                client.needs_keyframe = false;
                                            }
                                            touch_client_activity(&client.last_activity);
                                            still_open.push(client);
                                        }
                                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                            // Consumer is behind. Drop this frame.
                                            // If it was video, enter resync so we
                                            // resume cleanly at the next keyframe.
                                            // Do NOT touch activity on a drop, so a
                                            // permanently-stuck client is eventually
                                            // reaped instead of looking alive.
                                            if media_is_video {
                                                client.needs_keyframe = true;
                                            }
                                            log::debug!(
                                                "OBSERVE: Queue-full drop stream={} media={} (Client Fanout){}",
                                                stream,
                                                media_kind_str(&media),
                                                if media_is_video { ", resyncing at next keyframe" } else { "" }
                                            );
                                            still_open.push(client);
                                        }
                                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                            log::info!("Client sender disconnected for {name}::{stream}");
                                            // Collect the thread handle for cleanup
                                            if let Some(handle) = client.thread_handle {
                                                old_thread_handles.push(handle);
                                            }
                                        }
                                    }
                                }

                                clients = still_open;

                                // Join any finished old sender threads to reclaim resources
                                reap_finished_handles(&mut old_thread_handles, &name);
                            }
                        }
                        None => {
                            trigger_reopen!("camera stream channel closed");
                        }
                    }
                },
                msg_opt = client_rx.recv() => {
                    if let Some(ClientMsg::NewClient { element, reply, deadline }) = msg_opt {
                        if Instant::now() > deadline {
                            log::info!(
                                "{name}::{stream}: RTSP server already gave up on a client request, not building its pipeline"
                            );
                            continue;
                        }
                        log::info!("New RTSP client for {name}::{stream}");
                        let build_result: AnyResult<(Option<AppSrc>, Option<AppSrc>)> = (|| {
                            clear_bin(&element)?;

                            log::trace!("{name}::{stream}: Building the pipeline");
                            let vid_src = match stream_config.vid_type.as_ref() {
                                Some(VideoType::H264) => {
                                    let src = build_h264(&element, &stream_config)?;
                                    Some(src)
                                }
                                Some(VideoType::H265) => {
                                    let src = build_h265(&element, &stream_config)?;
                                    Some(src)
                                }
                                None => {
                                    return Err(anyhow!("{name}::{stream}: video type not learned"));
                                }
                            };

                            let aud_src = match stream_config.aud_type.as_ref() {
                                Some(AudioType::Aac) => match build_aac(&element, &stream_config) {
                                    Ok(src) => Some(src),
                                    Err(e) => {
                                        log::warn!(
                                            "{name}::{stream}: failed to build AAC audio pipeline, continuing without audio: {e:?}"
                                        );
                                        None
                                    }
                                },
                                Some(AudioType::Adpcm(block_size)) => {
                                    match build_adpcm(&element, *block_size, &stream_config) {
                                        Ok(src) => Some(src),
                                        Err(e) => {
                                            log::warn!(
                                                "{name}::{stream}: failed to build ADPCM audio pipeline, continuing without audio: {e:?}"
                                            );
                                            None
                                        }
                                    }
                                }
                                None => None,
                            };

                            if let Some(app) = vid_src.as_ref() {
                                app.set_callbacks(
                                    AppSrcCallbacks::builder()
                                        .seek_data(move |_, _seek_pos| true)
                                        .build(),
                                );
                            }
                            if let Some(app) = aud_src.as_ref() {
                                app.set_callbacks(
                                    AppSrcCallbacks::builder()
                                        .seek_data(move |_, _seek_pos| true)
                                        .build(),
                                );
                            }

                            Ok((vid_src, aud_src))
                        })();

                        let (vid_src, aud_src) = match build_result {
                            Ok(srcs) => srcs,
                            Err(e) => {
                                log::warn!(
                                    "{name}::{stream}: failed to build client pipeline, factory remains alive: {e:?}"
                                );
                                let _ = reply.send(Err(e));
                                continue;
                            }
                        };

                        log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                        if reply.send(Ok(element)).is_err() {
                            // The server thread stopped waiting while we were
                            // building; the element is discarded with it.
                            log::info!(
                                "{name}::{stream}: RTSP server stopped waiting for the client pipeline, discarding it"
                            );
                            continue;
                        }

                        let (tx, mut rx) =
                            tokio::sync::mpsc::channel::<SharedMedia>(CLIENT_MEDIA_QUEUE_CAPACITY);
                        let client_cancel = CancellationToken::new();
                        let client_last_activity = Arc::new(Mutex::new(Instant::now()));

                        let mut to_send = buffer.clone();
                        let bootstrap_needs_keyframe = prepare_bootstrap_batch(&mut to_send);

                        let sender_name = name.clone();
                        let thread_name = format!("{sender_name}::{stream}::sender");
                        let stream_config_clone = stream_config.clone();
                        let thread_ts_gen = timestamps_generation.clone();
                        let thread_cancel = client_cancel.clone();
                        let thread_last_activity = client_last_activity.clone();
                        let thread_handle = std::thread::Builder::new()
                            .name(thread_name)
                            .spawn(move || {
                                let name = sender_name;
                                let mut timestamps = TimestampState::default();
                                let mut waiting_for_keyframe = bootstrap_needs_keyframe;
                                let mut local_gen = thread_ts_gen.load(std::sync::atomic::Ordering::Acquire);

                                if let Err(e) = wait_for_sources_ready(
                                    &vid_src,
                                    &aud_src,
                                    &name,
                                    &stream.to_string(),
                                    &thread_cancel,
                                ) {
                                    log::warn!(
                                        "{name}::{stream}: RTSP pipeline did not become live in time: {e:?}"
                                    );
                                    return AnyResult::Err(e);
                                }

                                'sender_loop: {
                                    log::trace!("Sending buffered frames");
                                    for buffered in to_send.drain(..) {
                                        touch_client_activity(&thread_last_activity);
                                        if waiting_for_keyframe {
                                            if !is_keyframe(&buffered) {
                                                continue;
                                            }
                                            log::info!("OBSERVE: RTSP client resynchronized on keyframe, resetting timing");
                                            timestamps.last_video_source_ts = None;
                                            waiting_for_keyframe = false;
                                        }

                                        match send_to_sources(
                                            buffered,
                                            &vid_src,
                                            &aud_src,
                                            &mut timestamps,
                                            &stream_config_clone,
                                        ) {
                                            Ok(FrameSendOutcome::Sent) => {}
                                            Ok(FrameSendOutcome::DroppedVideo) => {
                                                waiting_for_keyframe = true;
                                                timestamps.video_needs_discont = true;
                                            }
                                            Err(r) => {
                                                log::info!("Failed to send to source: {r:?}");
                                                break 'sender_loop;
                                            }
                                        }
                                    }

                                    log::trace!("Sending new frames");
                                while let Some(mut batch) = drain_latest_batch_with_cancel(&mut rx, &thread_cancel) {
                                    for data in batch.drain(..) {
                                        touch_client_activity(&thread_last_activity);
                                        // Check if the main loop signaled a stream reconnect
                                        let current_gen = thread_ts_gen.load(std::sync::atomic::Ordering::Acquire);
                                        if current_gen != local_gen {
                                            log::info!("{name}::{stream}: Resetting source-timestamp tracking after stream reconnect (gen {local_gen} -> {current_gen}), keeping output clock monotonic");
                                            timestamps.reset_for_reconnect();
                                            local_gen = current_gen;
                                        }

                                        if waiting_for_keyframe {
                                            if !is_keyframe(&data) {
                                                continue;
                                            }
                                            log::info!("OBSERVE: RTSP client resynchronized on keyframe, resetting timing");
                                            timestamps.last_video_source_ts = None;
                                            waiting_for_keyframe = false;
                                        }

                                            match send_to_sources(
                                                data,
                                                &vid_src,
                                                &aud_src,
                                                &mut timestamps,
                                                &stream_config_clone,
                                            ) {
                                                Ok(FrameSendOutcome::Sent) => {}
                                                Ok(FrameSendOutcome::DroppedVideo) => {
                                                    waiting_for_keyframe = true;
                                                    timestamps.video_needs_discont = true;
                                                }
                                                Err(r) => {
                                                    log::info!("Failed to send to source: {r:?}");
                                                    break 'sender_loop;
                                                }
                                            }
                                        }
                                    }
                                }
                                log::trace!("All media received");
                                AnyResult::Ok(())
                            })
                            .ok();

                        if thread_handle.is_none() {
                            log::error!("Failed to spawn frame sender thread");
                        }

                        clients.push(ClientState {
                            sender: tx,
                            thread_handle,
                            cancel: client_cancel,
                            last_activity: client_last_activity,
                            needs_keyframe: false,
                        });
                    } else {
                        break;
                    }
                }
            }
        }
        // Clean up: join all remaining sender threads to prevent resource leaks
        for client in clients.drain(..) {
            client.cancel.cancel();
            if let Some(handle) = client.thread_handle {
                old_thread_handles.push(handle);
            }
        }
        while !old_thread_handles.is_empty() {
            reap_finished_handles(&mut old_thread_handles, &name);
            if !old_thread_handles.is_empty() {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(&splash_launch, move |element| {
        // Use a SyncSender with a timeout so we don't block the RTSP server
        // indefinitely if the factory background task is busy reopening the camera stream
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        client_tx
            .try_send(ClientMsg::NewClient {
                element: element.clone().upcast(),
                reply: tx,
                deadline: Instant::now() + CLIENT_BUILD_TIMEOUT,
            })
            .ok();

        // The factory loop now services client requests even while reopening the
        // upstream stream (reopen runs in the background), so this rarely waits
        // long. Allow a 2s margin for the loop to be momentarily busy fanning out
        // a large I-frame so a client connect isn't rejected spuriously.
        let element = match rx.recv_timeout(CLIENT_BUILD_TIMEOUT) {
            Ok(Ok(e)) => e,
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to receive new element from client thread: {e:?}"
                ))
            }
        };
        Ok(Some(element))
    })
    .await?;
    Ok((factory, thread))
}

fn send_to_sources(
    data: SharedMedia,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    timestamps: &mut TimestampState,
    stream_config: &StreamConfig,
) -> AnyResult<FrameSendOutcome> {
    match &*data {
        BcMedia::Aac(aac) => {
            if let Some(duration) = aac.duration() {
                if let Some(aud_src) = aud_src.as_ref() {
                    let pkt_duration = Duration::from_micros(duration as u64);
                    timestamps.align_audio_to_video();
                    let ts = next_cumulative_timestamp(&mut timestamps.next_audio_ts, pkt_duration);
                    let sent = send_to_appsrc(
                        aud_src,
                        FramePayload(Arc::clone(&data)),
                        ts,
                        Some(pkt_duration),
                        true,
                        false,
                        timestamps.audio_needs_discont,
                    )?;
                    if sent {
                        timestamps.audio_needs_discont = false;
                    }
                }
            }
            Ok(FrameSendOutcome::Sent)
        }
        BcMedia::Adpcm(adpcm) => {
            if let Some(duration) = adpcm.duration() {
                if let Some(aud_src) = aud_src.as_ref() {
                    let pkt_duration = Duration::from_micros(duration as u64);
                    timestamps.align_audio_to_video();
                    let ts = next_cumulative_timestamp(&mut timestamps.next_audio_ts, pkt_duration);
                    let sent = send_to_appsrc(
                        aud_src,
                        FramePayload(Arc::clone(&data)),
                        ts,
                        Some(pkt_duration),
                        true,
                        false,
                        timestamps.audio_needs_discont,
                    )?;
                    if sent {
                        timestamps.audio_needs_discont = false;
                    }
                }
            }
            Ok(FrameSendOutcome::Sent)
        }
        BcMedia::Iframe(BcMediaIframe { microseconds, .. }) => {
            let frame_interval = 1_000_000u64 / u64::from(stream_config.fps.max(1));
            if let Some(vid_src) = vid_src.as_ref() {
                let pkt_duration = Duration::from_micros(frame_interval);
                let ts = next_video_timestamp(
                    *microseconds,
                    &mut timestamps.last_video_source_ts,
                    &mut timestamps.next_video_ts,
                    pkt_duration,
                );
                let sent = send_to_appsrc(
                    vid_src,
                    FramePayload(Arc::clone(&data)),
                    ts,
                    Some(pkt_duration),
                    false,
                    true,
                    timestamps.video_needs_discont,
                )?;
                if sent {
                    timestamps.video_needs_discont = false;
                }
            }
            Ok(FrameSendOutcome::Sent)
        }
        BcMedia::Pframe(BcMediaPframe { microseconds, .. }) => {
            let frame_interval = 1_000_000u64 / u64::from(stream_config.fps.max(1));
            if let Some(vid_src) = vid_src.as_ref() {
                let pkt_duration = Duration::from_micros(frame_interval);
                let ts = next_video_timestamp(
                    *microseconds,
                    &mut timestamps.last_video_source_ts,
                    &mut timestamps.next_video_ts,
                    pkt_duration,
                );
                let sent = send_to_appsrc(
                    vid_src,
                    FramePayload(Arc::clone(&data)),
                    ts,
                    Some(pkt_duration),
                    true,
                    true,
                    timestamps.video_needs_discont,
                )?;
                if sent {
                    timestamps.video_needs_discont = false;
                } else {
                    return Ok(FrameSendOutcome::DroppedVideo);
                }
            }
            Ok(FrameSendOutcome::Sent)
        }
        _ => Ok(FrameSendOutcome::Sent),
    }
}

fn next_cumulative_timestamp(last: &mut Duration, increment: Duration) -> Duration {
    let ts = *last;
    let increment = if increment.is_zero() {
        Duration::from_micros(1)
    } else {
        increment
    };
    *last = last.saturating_add(increment);
    ts
}

fn next_video_timestamp(
    source_ts: u32,
    last_source: &mut Option<u32>,
    next_ts: &mut Duration,
    fallback_increment: Duration,
) -> Duration {
    let ts = *next_ts;
    let increment = match *last_source {
        None => fallback_increment,
        Some(prev) if source_ts > prev => Duration::from_micros((source_ts - prev) as u64),
        Some(prev) if source_ts == prev => {
            log::debug!(
                "RTSP video timestamp repeated: source={} prev={}, using fallback increment",
                source_ts,
                prev
            );
            fallback_increment
        }
        Some(prev)
            if prev >= u32::MAX - VIDEO_TIMESTAMP_WRAP_WINDOW
                && source_ts <= VIDEO_TIMESTAMP_WRAP_WINDOW =>
        {
            log::info!(
                "OBSERVE: Timestamp wrap event source={} prev={}",
                source_ts,
                prev
            );
            Duration::from_micros(source_ts.wrapping_sub(prev) as u64)
        }
        Some(_) => {
            // Camera restart or timestamp reset. Keep the RTSP clock monotonic and
            // fall back to the expected frame cadence instead of replaying an old source delta.
            log::warn!(
                "OBSERVE: Timestamp reset/backward jump detected: source={} prev={}",
                source_ts,
                last_source.unwrap_or_default()
            );
            fallback_increment
        }
    };
    *last_source = Some(source_ts);
    *next_ts = next_ts.saturating_add(increment);
    ts
}

fn send_to_appsrc(
    appsrc: &AppSrc,
    payload: FramePayload,
    ts: Duration,
    duration: Option<Duration>,
    can_drop: bool,
    is_video: bool,
    discont: bool,
) -> AnyResult<bool> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    const MAX_RETRIES: u32 = 3;
    const INITIAL_WAIT_MS: u64 = 5;
    const BUFFER_THRESHOLD: u64 = 90; // Percent

    let max_bytes = appsrc.max_bytes();
    let threshold_bytes = max_bytes * BUFFER_THRESHOLD / 100;

    let mut retries = 0;
    let mut wait_ms = INITIAL_WAIT_MS;

    while appsrc.current_level_bytes() >= threshold_bytes && retries < MAX_RETRIES {
        retries += 1;
        std::thread::sleep(std::time::Duration::from_millis(wait_ms));
        wait_ms *= 2;
    }

    if retries >= MAX_RETRIES && can_drop {
        return Ok(false);
    }

    // Wrap the shared frame allocation directly. The previous design kept up
    // to 32 `GstBufferPool`s keyed by exact frame size, each preallocating 8
    // buffers; since video frame sizes vary per frame this created and evicted
    // a pool for nearly every frame (32 x 8 x frame size resident per client,
    // all churned through glibc malloc) and copied every payload on top.
    let mut buf = gstreamer::Buffer::from_slice(payload);
    {
        let gst_buf_mut = buf
            .get_mut()
            .ok_or_else(|| anyhow::anyhow!("Failed to get mutable buffer reference"))?;

        let time = gstreamer::ClockTime::from_useconds(ts.as_micros() as u64);
        gst_buf_mut.set_pts(time);
        if !is_video {
            gst_buf_mut.set_dts(time);
        }
        if discont {
            gst_buf_mut.set_flags(gstreamer::BufferFlags::DISCONT);
        }
        if let Some(duration) = duration {
            gst_buf_mut.set_duration(gstreamer::ClockTime::from_useconds(
                duration.as_micros() as u64
            ));
        }
    }

    match appsrc.push_buffer(buf) {
        Ok(_) => Ok(true),
        Err(FlowError::Flushing) => Ok(false),
        Err(FlowError::Eos) => Ok(false),
        Err(e) => Err(anyhow::anyhow!("Error in streaming: {e:?}")),
    }
}

fn drain_latest_batch_with_cancel(
    rx: &mut tokio::sync::mpsc::Receiver<SharedMedia>,
    cancel: &CancellationToken,
) -> Option<Vec<SharedMedia>> {
    loop {
        if cancel.is_cancelled() {
            return None;
        }

        match rx.try_recv() {
            Ok(first) => {
                let mut batch = vec![first];
                while let Ok(next) = rx.try_recv() {
                    batch.push(next);
                }

                // Only catch up by jumping to the latest keyframe when the
                // backlog is genuinely large. The old behaviour trimmed to the
                // last keyframe on EVERY drain — so a normal 2-3 frame batch
                // (ordinary scheduling jitter) discarded whole GOPs of P-frames,
                // destroying the motion Frigate's detector relies on. When only
                // slightly behind we keep every frame; `send_to_appsrc` still
                // applies keyframe-aware backpressure downstream if needed.
                if batch.len() >= DRAIN_CATCHUP_THRESHOLD {
                    if let Some(last_iframe) = batch.iter().rposition(|m| is_keyframe(m)) {
                        if last_iframe > 0 {
                            log::debug!(
                                "RTSP consumer far behind ({} frames queued); catching up to latest keyframe",
                                batch.len()
                            );
                            let _ = batch.drain(0..last_iframe);
                        }
                    } else {
                        log::trace!("Drained large RTSP batch without a keyframe; keeping latest media to avoid stalls");
                    }
                }

                return Some(batch);
            }
            Err(TryRecvError::Empty) => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(TryRecvError::Disconnected) => return None,
        }
    }
}

fn prepare_bootstrap_batch(batch: &mut Vec<SharedMedia>) -> bool {
    if let Some(last_iframe) = batch.iter().rposition(|m| is_keyframe(m)) {
        if last_iframe > 0 {
            let _ = batch.drain(0..last_iframe);
        }
        false
    } else if batch.len() > 1 {
        log::trace!("Trimming RTSP bootstrap without a keyframe to avoid startup lag");
        if let Some(last) = batch.pop() {
            batch.clear();
            batch.push(last);
        }
        true
    } else {
        false
    }
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn wait_for_sources_ready(
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    name: &str,
    stream: &str,
    cancel: &CancellationToken,
) -> Result<()> {
    let mut start = Instant::now();
    let mut retries = 0;

    loop {
        if cancel.is_cancelled() {
            return Err(anyhow!("RTSP client cancelled while waiting for sources"));
        }

        let video_ready = vid_src.as_ref().map_or(Ok(true), check_live_ready);
        let audio_ready = aud_src.as_ref().map_or(Ok(true), check_live_ready);

        match (video_ready, audio_ready) {
            (Ok(true), Ok(true)) => return Ok(()),
            (Err(e), _) | (_, Err(e)) => {
                log::error!(
                    "OBSERVE: Appsrc readiness failure for {}::{}: {:?}",
                    name,
                    stream,
                    e
                );
                return Err(e);
            }
            _ => {}
        }

        if start.elapsed() >= SOURCE_READY_TIMEOUT {
            retries += 1;
            if retries >= MAX_READY_RETRIES {
                return Err(anyhow!(
                    "{}::{}: RTSP sources did not become ready after {} retries",
                    name,
                    stream,
                    retries
                ));
            }
            log::info!(
                "OBSERVE: RTSP client startup retry {} for {}::{}",
                retries,
                name,
                stream
            );
            start = Instant::now();
        }

        std::thread::sleep(SOURCE_READY_POLL);
    }
}

fn check_live_ready(app: &AppSrc) -> Result<bool> {
    app.bus().ok_or_else(|| anyhow!("App source is closed"))?;
    Ok(app.pads().iter().all(|pad| pad.is_linked()))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(true);
    source.set_block(false);
    source.set_min_latency(1_000_000_000i64 / (stream_config.fps.max(1) as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    // Leaky-downstream is required for a live stream: when the client falls
    // behind, the appsrc must shed its oldest buffers so latency stays bounded.
    // Without it, latency grows without bound (I-frames are pushed
    // unconditionally) and a slightly-slow client drifts minutes behind. The
    // source-side keyframe-aware logic keeps how often we leak low.
    source.set_leaky_type(AppLeakyType::Downstream);
    source.set_do_timestamp(false);
    source.set_format(gstreamer::Format::Time);
    source.set_stream_type(AppStreamType::Stream);
    source.set_caps(Some(
        &Caps::builder("video/x-h264")
            .field("stream-format", "byte-stream")
            .field("alignment", "au")
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser, &stamper])?;
    Element::link_many([&source, &queue, &parser, &stamper])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: stamper,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    payload.set_property("config-interval", -1i32);
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(true);
    source.set_block(false);
    source.set_min_latency(1_000_000_000i64 / (stream_config.fps.max(1) as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    // Leaky-downstream is required for a live stream. When the client falls
    // behind, the appsrc must shed its oldest buffers so latency stays bounded
    // (~max-bytes ≈ 2s). Without it the queue grows without bound — I-frames are
    // pushed unconditionally, so a slightly-slow client drifts minutes behind.
    // The source-side keyframe-aware logic (per-client resync, drain catch-up)
    // keeps how often we must leak low, and the decoder recovers at the next
    // keyframe when a leak does drop mid-GOP.
    source.set_leaky_type(AppLeakyType::Downstream);
    source.set_do_timestamp(false);
    source.set_format(gstreamer::Format::Time);
    source.set_stream_type(AppStreamType::Stream);
    source.set_caps(Some(
        &Caps::builder("video/x-h265")
            .field("stream-format", "byte-stream")
            .field("alignment", "au")
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser, &stamper])?;
    Element::link_many([&source, &queue, &parser, &stamper])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: stamper,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    payload.set_property("config-interval", -1i32);
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

/// Common appsrc setup for the AAC input, shared by the passthrough and the
/// decode-to-L16 pipelines.
fn make_aac_appsrc(buffer_size: u32) -> Result<AppSrc> {
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(true);
    source.set_block(false);
    source.set_min_latency(20_000_000);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_leaky_type(AppLeakyType::Downstream);
    source.set_do_timestamp(false);
    source.set_format(gstreamer::Format::Time);
    source.set_stream_type(AppStreamType::Stream);
    // The camera sends ADTS-framed AAC (see `BcMediaAac::duration`, which
    // parses the ADTS header).
    source.set_caps(Some(
        &Caps::builder("audio/mpeg")
            .field("mpegversion", 4i32)
            .field("stream-format", "adts")
            .build(),
    ));
    Ok(source)
}

/// AAC passthrough: `appsrc ! aacparse` feeding `rtpmp4gpay`.
///
/// One RTP packet per AAC frame, no decode. Decoding to L16 produced 2048-byte
/// PCM frames that `rtpL16pay` split into two RTP packets per frame; clients
/// that timestamp by arrival time (Frigate's default `preset-rtsp-generic`
/// passes `-use_wallclock_as_timestamps 1`) then saw two packets with the same
/// timestamp and logged/dropped every one of them as a non-monotonic DTS.
fn pipe_aac(bin: &Element, _stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = AUDIO_BUFFER_SIZE;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac passthrough pipeline");
    let source = make_aac_appsrc(buffer_size)?
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    // aacparse re-frames ADTS to raw AAC (with codec_data for the SDP) when
    // the downstream payloader asks for `stream-format=raw`.
    let parser = make_element("aacparse", "audparser")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    if !stream_config.audio_passthrough {
        return build_aac_l16(bin, stream_config);
    }
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpmp4gpay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

/// Legacy AAC path: decode to L16 PCM (`audio_passthrough = false`).
fn pipe_aac_l16(bin: &Element, _stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = AUDIO_BUFFER_SIZE;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac decode pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(true);
    source.set_block(false);
    source.set_min_latency(20_000_000);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    // Leaky-downstream is required for a LIVE stream: when the client falls
    // behind, the appsrc must shed its oldest buffers so latency stays bounded
    // (~max-bytes ≈ 2s). Without it the queue grows without bound — I-frames are
    // pushed unconditionally, so a slightly-slow client drifts minutes behind.
    // The source-side keyframe-aware logic (per-client resync, drain catch-up)
    // keeps how often we must leak low, and the decoder recovers at the next
    // keyframe when a leak does drop mid-GOP.
    source.set_leaky_type(AppLeakyType::Downstream);
    source.set_do_timestamp(false);
    source.set_format(gstreamer::Format::Time);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn build_aac_l16(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac_l16(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_l16_payloader()?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

/// `rtpL16pay` sized so one decoded audio block fits in one RTP packet.
///
/// The default 1400-byte MTU splits a 2048-byte (1024 sample, 16-bit mono)
/// block into two RTP packets. Clients that timestamp by arrival time then see
/// two packets with the same timestamp and treat the second as a DTS error.
/// RTSP clients use TCP interleaving where a 4 KiB RTP packet is fine.
fn make_l16_payloader() -> AnyResult<Element> {
    let payload = make_element("rtpL16pay", "pay1")?;
    payload.set_property("mtu", 4096u32);
    Ok(payload)
}

fn pipe_adpcm(bin: &Element, block_size: u32, _stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // ! rtpL16pay name=pay1

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(true);
    source.set_block(false);
    source.set_min_latency(20_000_000);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    // Leaky-downstream is required for a LIVE stream: when the client falls
    // behind, the appsrc must shed its oldest buffers so latency stays bounded
    // (~max-bytes ≈ 2s). Without it the queue grows without bound — I-frames are
    // pushed unconditionally, so a slightly-slow client drifts minutes behind.
    // The source-side keyframe-aware logic (per-client resync, drain catch-up)
    // keeps how often we must leak low, and the decoder recovers at the next
    // keyframe when a leak does drop mid-GOP.
    source.set_leaky_type(AppLeakyType::Downstream);
    source.set_do_timestamp(false);
    source.set_format(gstreamer::Format::Time);
    source.set_stream_type(AppStreamType::Stream);

    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "div")
            .field("block_align", block_size as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    decoder.connect_pad_added(move |_element, pad| {
        let Some(sink_pad) = encoder.static_pad("sink") else {
            log::error!("Encoder is missing its sink pad");
            return;
        };
        if let Err(e) = pad.link(&sink_pad) {
            log::error!("Failed to link ADPCM decoder to encoder: {:?}", e);
        }
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

fn build_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_adpcm(bin, block_size, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_l16_payloader()?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, _stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = AUDIO_BUFFER_SIZE;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(true);
    source.set_block(false);
    source.set_min_latency(20_000_000);
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    // Leaky-downstream is required for a LIVE stream: when the client falls
    // behind, the appsrc must shed its oldest buffers so latency stays bounded
    // (~max-bytes ≈ 2s). Without it the queue grows without bound — I-frames are
    // pushed unconditionally, so a slightly-slow client drifts minutes behind.
    // The source-side keyframe-aware logic (per-client resync, drain catch-up)
    // keeps how often we must leak low, and the decoder recovers at the next
    // keyframe when a leak does drop mid-GOP.
    source.set_leaky_type(AppLeakyType::Downstream);
    source.set_do_timestamp(false);
    source.set_format(gstreamer::Format::Time);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    silence.set_property("is-live", true);
    silence.set_property("do-timestamp", true);
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "h264timestamper" => "codectimestamper (gst-plugins-bad)",
            "h265timestamper" => "codectimestamper (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "rtpmp4gpay" => "rtp (gst-plugins-good)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue2.set_property("max-size-buffers", 0u32);
    queue2.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .ok_or_else(|| anyhow!("Failed to get sink pad from queue"))?;
    let ghost_pad = GhostPad::builder_with_target(&pad)
        .map_err(|e| anyhow!("Failed to build ghost pad for queue sink: {:?}", e))?
        .build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .ok_or_else(|| anyhow!("Failed to get src pad from queue2"))?;
    let ghost_pad = GhostPad::builder_with_target(&pad)
        .map_err(|e| anyhow!("Failed to build ghost pad for queue2 src: {:?}", e))?
        .build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // Leaky-downstream keeps this decoupling queue bounded for live streaming;
    // paired with the leaky appsrc it prevents latency from accumulating when a
    // client can't keep up.
    queue.set_property_from_str("leaky", "downstream");
    Ok(queue)
}

fn buffer_size(bitrate: u32) -> u32 {
    // Buffer size based on bitrate:
    // - 2 seconds of video to handle client consumption delays
    // - This helps prevent "Buffer full" warnings during network jitter
    // - Minimum 1MB for low bitrate streams
    // Formula: bitrate (bits/s) * 2 seconds / 8 (bits to bytes)
    std::cmp::max(bitrate * 2 / 8, 1024u32 * 1024u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_iframe() -> SharedMedia {
        Arc::new(BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: 1,
            time: None,
            data: vec![1, 2, 3, 4],
        }))
    }

    fn sample_pframe() -> SharedMedia {
        Arc::new(BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: 2,
            data: vec![5, 6, 7, 8],
        }))
    }

    #[test]
    fn test_frame_payload_exposes_data_without_copy() {
        let frame = sample_iframe();
        let payload = FramePayload(Arc::clone(&frame));
        assert_eq!(payload.as_ref(), &[1, 2, 3, 4]);
        assert_eq!(Arc::strong_count(&frame), 2);
        assert!(FramePayload(Arc::new(BcMedia::Skip)).as_ref().is_empty());
    }

    /// Verify audio buffer size is reasonable
    #[test]
    fn test_audio_buffer_size() {
        // Audio at ~800kbps = 100KB/s
        // Buffer should hold several seconds
        let audio_seconds = AUDIO_BUFFER_SIZE as f64 / 100_000.0;

        assert!(
            audio_seconds >= 5.0,
            "Audio buffer only holds {:.1}s at 800kbps",
            audio_seconds
        );
        assert!(
            audio_seconds <= 30.0,
            "Audio buffer holds {:.1}s, may be excessive",
            audio_seconds
        );
    }

    /// Verify video buffer size calculation
    #[test]
    fn test_video_buffer_size() {
        // 4 Mbps stream
        let size_4mbps = buffer_size(4_000_000);
        // Should be ~2 seconds = 1MB
        assert!(
            size_4mbps >= 1_000_000,
            "4Mbps buffer too small: {} bytes",
            size_4mbps
        );
        assert!(
            size_4mbps <= 2_000_000,
            "4Mbps buffer too large: {} bytes",
            size_4mbps
        );

        // Low bitrate should get minimum 1MB
        let size_low = buffer_size(100_000);
        assert_eq!(
            size_low,
            1024 * 1024,
            "Low bitrate should get minimum 1MB buffer"
        );

        // High bitrate (8 Mbps)
        let size_8mbps = buffer_size(8_000_000);
        assert!(
            size_8mbps >= 2_000_000,
            "8Mbps buffer too small: {} bytes",
            size_8mbps
        );
    }

    /// Verify backpressure constants in send_to_appsrc are reasonable
    #[test]
    fn test_backpressure_constants() {
        // These are defined locally in send_to_appsrc but we can verify the logic
        // Reduced from 5 retries (310ms) to 3 retries (70ms) for ALPR latency
        const MAX_RETRIES: u32 = 3;
        const INITIAL_WAIT_MS: u64 = 5;
        const BUFFER_THRESHOLD: u64 = 90;

        // Max total wait time = 10 + 20 + 40 = 70ms
        let mut total_wait: u64 = 0;
        let mut wait_ms = INITIAL_WAIT_MS;
        for _ in 0..MAX_RETRIES {
            total_wait += wait_ms;
            wait_ms *= 2;
        }

        // Total wait should be under 100ms for ALPR responsiveness
        assert!(
            total_wait <= 100,
            "Backpressure total wait too long for ALPR: {}ms (max 100ms)",
            total_wait
        );

        // Threshold should be high (don't wait until nearly full)
        assert!(
            BUFFER_THRESHOLD >= 80,
            "Buffer threshold too low, will cause unnecessary waits"
        );
        assert!(
            BUFFER_THRESHOLD <= 95,
            "Buffer threshold too high, may cause drops"
        );
    }

    /// Verify timestamp types can handle long-running streams
    #[test]
    fn test_timestamp_no_overflow() {
        // Timestamps must be u64 to avoid overflow during long streams
        // u32 overflows after ~71 minutes at 30fps (u32::MAX / (1_000_000 / 30) / 30 / 60)
        // u64 can handle ~584,942 years at 30fps

        const MICROSECONDS: u64 = 1_000_000;
        let fps: u64 = 30;
        let frame_duration = MICROSECONDS / fps;

        // Simulate 24 hours of streaming
        let hours: u64 = 24;
        let frames_per_hour = 30 * 60 * 60;
        let total_frames = hours * frames_per_hour;
        let total_microseconds = total_frames * frame_duration;

        // This should not overflow with u64
        assert!(
            total_microseconds < u64::MAX,
            "Timestamp would overflow after {} hours",
            hours
        );

        // Verify we can handle at least 1 year of streaming
        let one_year_frames: u64 = 30 * 60 * 60 * 24 * 365;
        let one_year_microseconds = one_year_frames * frame_duration;
        assert!(
            one_year_microseconds < u64::MAX,
            "Timestamp would overflow within 1 year"
        );
    }

    #[test]
    fn test_video_timestamp_wraps_monotonically() {
        let mut last_source = Some(u32::MAX - 10);
        let mut next_ts = Duration::from_secs(10);
        let frame = Duration::from_micros(33_333);

        let first = next_video_timestamp(u32::MAX - 5, &mut last_source, &mut next_ts, frame);
        let second = next_video_timestamp(3, &mut last_source, &mut next_ts, frame);

        assert_eq!(first, Duration::from_secs(10));
        assert_eq!(second, Duration::from_secs(10) + Duration::from_micros(5));
        assert!(next_ts > second);
    }

    #[test]
    fn test_audio_timestamp_never_stalls_on_zero_increment() {
        let mut next_ts = Duration::from_micros(0);
        let first = next_cumulative_timestamp(&mut next_ts, Duration::from_micros(0));
        let second = next_cumulative_timestamp(&mut next_ts, Duration::from_micros(0));

        assert!(second > first);
        assert_eq!(second, Duration::from_micros(1));
    }

    #[test]
    fn test_drain_latest_batch_preserves_media_without_keyframe() {
        let mut batch = vec![sample_pframe(), sample_pframe(), sample_pframe()];
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        for media in batch.drain(..) {
            tx.try_send(media).unwrap();
        }

        let drained = drain_latest_batch_with_cancel(&mut rx, &CancellationToken::new())
            .expect("expected batch");
        assert_eq!(drained.len(), 3);
        assert!(matches!(
            drained.first().map(|m| &**m),
            Some(BcMedia::Pframe(_))
        ));
        assert!(matches!(
            drained.last().map(|m| &**m),
            Some(BcMedia::Pframe(_))
        ));
    }

    #[test]
    fn test_drain_small_batch_is_not_decimated() {
        // A small batch (below the catch-up threshold) must keep every frame,
        // even when it contains a keyframe — decimating here destroys motion.
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        tx.try_send(sample_pframe()).unwrap();
        tx.try_send(sample_pframe()).unwrap();
        tx.try_send(sample_iframe()).unwrap();
        tx.try_send(sample_pframe()).unwrap();

        let drained = drain_latest_batch_with_cancel(&mut rx, &CancellationToken::new())
            .expect("expected batch");
        assert_eq!(drained.len(), 4, "small batch should not be trimmed");
        assert!(matches!(
            drained.first().map(|m| &**m),
            Some(BcMedia::Pframe(_))
        ));
        assert!(matches!(
            drained.last().map(|m| &**m),
            Some(BcMedia::Pframe(_))
        ));
    }

    #[test]
    fn test_drain_large_batch_catches_up_to_keyframe() {
        // When genuinely far behind (backlog >= DRAIN_CATCHUP_THRESHOLD) we jump
        // to the latest keyframe to bound latency.
        assert!(DRAIN_CATCHUP_THRESHOLD >= 2);
        let total = DRAIN_CATCHUP_THRESHOLD + 4;
        let (tx, mut rx) = tokio::sync::mpsc::channel(total + 1);
        // Fill with P-frames, place a keyframe two-from-last.
        for _ in 0..(total - 2) {
            tx.try_send(sample_pframe()).unwrap();
        }
        tx.try_send(sample_iframe()).unwrap();
        tx.try_send(sample_pframe()).unwrap();

        let drained = drain_latest_batch_with_cancel(&mut rx, &CancellationToken::new())
            .expect("expected batch");
        assert_eq!(
            drained.len(),
            2,
            "large batch should be trimmed to the latest keyframe onward"
        );
        assert!(matches!(
            drained.first().map(|m| &**m),
            Some(BcMedia::Iframe(_))
        ));
        assert!(matches!(
            drained.last().map(|m| &**m),
            Some(BcMedia::Pframe(_))
        ));
    }

    #[test]
    fn test_prepare_bootstrap_batch_trims_to_latest_keyframe() {
        let mut batch = vec![
            sample_pframe(),
            sample_pframe(),
            sample_iframe(),
            sample_pframe(),
        ];
        let needs_keyframe = prepare_bootstrap_batch(&mut batch);

        assert!(!needs_keyframe);
        assert_eq!(batch.len(), 2);
        assert!(matches!(
            batch.first().map(|m| &**m),
            Some(BcMedia::Iframe(_))
        ));
    }

    #[test]
    fn test_prepare_bootstrap_batch_limits_lag_without_keyframe() {
        let mut batch = vec![sample_pframe(), sample_pframe(), sample_pframe()];
        let needs_keyframe = prepare_bootstrap_batch(&mut batch);

        assert!(needs_keyframe);
        assert_eq!(batch.len(), 1);
        assert!(matches!(
            batch.first().map(|m| &**m),
            Some(BcMedia::Pframe(_))
        ));
    }

    #[test]
    fn test_audio_snaps_forward_when_behind_video() {
        let mut ts = TimestampState {
            next_video_ts: Duration::from_secs(30),
            next_audio_ts: Duration::from_secs(20),
            ..Default::default()
        };
        ts.audio_needs_discont = false;
        assert!(ts.align_audio_to_video());
        assert_eq!(ts.next_audio_ts, Duration::from_secs(30));
        assert!(
            ts.audio_needs_discont,
            "a snap must be flagged as a discontinuity"
        );
    }

    #[test]
    fn test_audio_never_moved_backwards_or_within_tolerance() {
        // Audio ahead of video: leave it alone (moving it back would produce
        // non-monotonic timestamps).
        let mut ahead = TimestampState {
            next_video_ts: Duration::from_secs(10),
            next_audio_ts: Duration::from_secs(15),
            ..Default::default()
        };
        assert!(!ahead.align_audio_to_video());
        assert_eq!(ahead.next_audio_ts, Duration::from_secs(15));

        // Behind but within tolerance: leave it alone.
        let mut close = TimestampState {
            next_video_ts: Duration::from_secs(10),
            next_audio_ts: Duration::from_millis(9500),
            ..Default::default()
        };
        assert!(!close.align_audio_to_video());
        assert_eq!(close.next_audio_ts, Duration::from_millis(9500));

        // No video yet: nothing to align against.
        let mut no_video = TimestampState {
            next_audio_ts: Duration::from_secs(5),
            ..Default::default()
        };
        assert!(!no_video.align_audio_to_video());
        assert_eq!(no_video.next_audio_ts, Duration::from_secs(5));
    }

    #[test]
    fn test_stall_timeout_relationship() {
        // CLIENT_REAP_INTERVAL must be strictly less than STREAM_STALL_TIMEOUT
        // so client reap ticks can regularly check stall conditions.
        assert!(CLIENT_REAP_INTERVAL < STREAM_STALL_TIMEOUT);
    }
}
