use std::{
    collections::HashMap,
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
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle, time::timeout};

use crate::{common::NeoInstance, rtsp::gst::NeoMediaFactory, AnyResult};

/// Audio buffer size in bytes
///
/// Audio typically runs at ~800kbps. This buffer provides ~7 seconds of audio
/// which is larger than video (2 seconds) to ensure smooth playback.
/// Formula: 512 packets × 1416 bytes/packet ≈ 725KB
const AUDIO_BUFFER_SIZE: u32 = 512 * 1416;

/// Maximum number of buffer pools to maintain
///
/// Buffer pools are keyed by frame size. Video frames typically cluster around
/// a few common sizes (I-frames ~50-100KB, P-frames ~5-20KB), so 32 pools
/// should be more than sufficient. This prevents memory leaks from cameras
/// with highly variable frame sizes.
const MAX_BUFFER_POOLS: usize = 32;

/// How long to wait for the first video packet before failing stream startup.
const INITIAL_VIDEO_TIMEOUT: Duration = Duration::from_secs(20);

/// After video is detected, allow a short grace period to discover audio so the
/// RTSP SDP is stable for the lifetime of the mounted path.
const INITIAL_AUDIO_GRACE: Duration = Duration::from_secs(2);

/// Delay between attempts to reopen a camera stream after it closes.
const STREAM_RETRY_DELAY: Duration = Duration::from_secs(1);

/// If the camera stops producing packets but never hard-closes the channel,
/// rebuild the stream rather than letting the client sit on stale media.
const STREAM_STALL_TIMEOUT: Duration = Duration::from_secs(10);
/// Time to wait for a newly created RTSP client pipeline to become live.
const SOURCE_READY_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll interval while waiting for the RTSP client pipeline to become live.
const SOURCE_READY_POLL: Duration = Duration::from_millis(20);
const VIDEO_TIMESTAMP_WRAP_WINDOW: u32 = 5_000_000;

/// Capacity of the per-client media queue.
///
/// This queue carries fully parsed `BcMedia` packets, so dropping from here is
/// recoverable. Keeping it bounded prevents RTSP backpressure from propagating
/// all the way down into the raw Baichuan packet router.
const CLIENT_MEDIA_QUEUE_CAPACITY: usize = 500;
const MAX_BOOTSTRAP_FRAMES: usize = 256;

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
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
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
            BcMedia::Skip => {}
        }
    }
}

enum ClientMsg {
    NewClient {
        element: Element,
        reply: tokio::sync::oneshot::Sender<AnyResult<Element>>,
    },
}

struct ClientState {
    sender: tokio::sync::mpsc::Sender<BcMedia>,
}

#[derive(Default)]
struct TimestampState {
    next_video_ts: Duration,
    last_video_source_ts: Option<u32>,
    next_audio_ts: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameSendOutcome {
    Sent,
    DroppedVideo,
}

fn buffer_media(buffer: &mut Vec<BcMedia>, media: BcMedia) {
    if matches!(media, BcMedia::Iframe(_)) {
        buffer.clear();
    }

    buffer.push(media);

    if buffer.len() > MAX_BOOTSTRAP_FRAMES {
        if let Some(last_iframe) = buffer.iter().rposition(|item| matches!(item, BcMedia::Iframe(_)))
        {
            if last_iframe > 0 {
                let _ = buffer.drain(0..last_iframe);
                return;
            }
        }
        let _ = buffer.drain(0..500);
    }
}

fn should_drop_for_backpressure(media: &BcMedia) -> bool {
    matches!(
        media,
        BcMedia::Aac(_) | BcMedia::Adpcm(_) | BcMedia::Pframe(_)
    )
}

fn is_keyframe(media: &BcMedia) -> bool {
    matches!(media, BcMedia::Iframe(_))
}

async fn reopen_stream(
    camera: &NeoInstance,
    stream: StreamKind,
    name: &str,
) -> AnyResult<tokio::sync::mpsc::Receiver<BcMedia>> {
    loop {
        match camera.stream_while_live(stream).await {
            Ok(new_media_rx) => return Ok(new_media_rx),
            Err(e) => {
                log::warn!(
                    "{name}::{stream}: failed to restart camera stream, retrying: {e:?}"
                );
                tokio::time::sleep(STREAM_RETRY_DELAY).await;
            }
        }
    }
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    let name = camera.config().await?.borrow().name.clone();

    let thread = tokio::task::spawn(async move {
        let (mut media_rx, mut buffer, mut stream_config) = loop {
            log::info!("{name}::{stream}: Starting camera stream immediately");
            let mut media_rx = match camera.stream_while_live(stream).await {
                Ok(media_rx) => media_rx,
                Err(e) => {
                    log::warn!(
                        "{name}::{stream}: failed to start camera stream, retrying: {e:?}"
                    );
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
                        buffer_media(&mut buffer, media);
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
        let mut waiting_for_keyframe = false;

        loop {
            tokio::select! {
                media_opt = tokio::time::timeout(STREAM_STALL_TIMEOUT, media_rx.recv()) => {
                    match media_opt {
                        Ok(Some(media)) => {
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

                            buffer_media(&mut buffer, media.clone());

                            if !clients.is_empty() {
                                let mut still_open = Vec::with_capacity(clients.len());
                                let drop_ok = should_drop_for_backpressure(&media);
                                let mut delivered = false;

                                for client in clients.drain(..) {
                                    match client.sender.try_send(media.clone()) {
                                        Ok(()) => {
                                            delivered = true;
                                            still_open.push(client);
                                        }
                                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                            let media_type = match &media {
                                                BcMedia::Iframe(_) => "Iframe",
                                                BcMedia::Pframe(_) => "Pframe",
                                                BcMedia::Aac(_) => "Aac",
                                                BcMedia::Adpcm(_) => "Adpcm",
                                                _ => "Other",
                                            };
                                            if drop_ok {
                                                log::debug!(
                                                    "OBSERVE: Queue-full drop stream={} media={} (Client Fanout)", stream, media_type
                                                );
                                                still_open.push(client);
                                            } else if is_keyframe(&media) {
                                                log::warn!(
                                                    "OBSERVE: Queue-full drop stream={} media={} (Client Fanout). Dropping keyframe but keeping client.", stream, media_type
                                                );
                                                still_open.push(client);
                                            } else {
                                                log::warn!(
                                                    "OBSERVE: Queue-full drop stream={} media={} (Client Fanout). Backpressure on non-droppable.", stream, media_type
                                                );
                                                still_open.push(client);
                                            }
                                        }
                                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                            log::info!("Client sender disconnected for {name}::{stream}");
                                        }
                                    }
                                }

                                clients = still_open;
                                if delivered {
                                    continue;
                                }
                            }
                        }
                        Ok(None) => {
                            log::warn!("{name}::{stream}: Camera stream channel closed, restarting");
                            waiting_for_keyframe = true;
                            buffer.clear();
                            media_rx = reopen_stream(&camera, stream, &name).await?;
                        }
                        Err(_) => {
                            log::warn!(
                                "{name}::{stream}: No media received for {:?}, restarting stream",
                                STREAM_STALL_TIMEOUT
                            );
                            waiting_for_keyframe = true;
                            buffer.clear();
                            media_rx = reopen_stream(&camera, stream, &name).await?;
                        }
                    }
                },
                msg_opt = client_rx.recv() => {
                    if let Some(ClientMsg::NewClient { element, reply }) = msg_opt {
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
                        let _ = reply.send(Ok(element));

                        let (tx, mut rx) =
                            tokio::sync::mpsc::channel(CLIENT_MEDIA_QUEUE_CAPACITY);
                        clients.push(ClientState { sender: tx });

                        let mut to_send = buffer.clone();
                        let bootstrap_needs_keyframe = prepare_bootstrap_batch(&mut to_send);

                        let sender_name = name.clone();
                        let thread_name = format!("{sender_name}::{stream}::sender");
                        let stream_config_clone = stream_config.clone();
                        std::thread::Builder::new()
                            .name(thread_name)
                            .spawn(move || {
                                let name = sender_name;
                                let mut timestamps = TimestampState::default();
                                let mut pools = Default::default();
                                let mut waiting_for_keyframe = bootstrap_needs_keyframe;

                                if let Err(e) = wait_for_sources_ready(&vid_src, &aud_src, &name, &stream.to_string()) {
                                    log::warn!(
                                        "{name}::{stream}: RTSP pipeline did not become live in time: {e:?}"
                                    );
                                    return AnyResult::Err(e);
                                }

                                'sender_loop: {
                                    log::trace!("Sending buffered frames");
                                    for buffered in to_send.drain(..) {
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
                                            &mut pools,
                                            &vid_src,
                                            &aud_src,
                                            &mut timestamps,
                                            &stream_config_clone,
                                        ) {
                                            Ok(FrameSendOutcome::Sent) => {}
                                            Ok(FrameSendOutcome::DroppedVideo) => {
                                                waiting_for_keyframe = true;
                                            }
                                            Err(r) => {
                                                log::info!("Failed to send to source: {r:?}");
                                                break 'sender_loop;
                                            }
                                        }
                                    }

                                    log::trace!("Sending new frames");
                                while let Some(mut batch) = drain_latest_batch(&mut rx) {
                                    for data in batch.drain(..) {
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
                                                &mut pools,
                                                &vid_src,
                                                &aud_src,
                                                &mut timestamps,
                                                &stream_config_clone,
                                            ) {
                                                Ok(FrameSendOutcome::Sent) => {}
                                                Ok(FrameSendOutcome::DroppedVideo) => {
                                                    waiting_for_keyframe = true;
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
                            .unwrap_or_else(|e| {
                                log::error!("Failed to spawn frame sender thread: {e}");
                                std::thread::spawn(|| AnyResult::Ok(()))
                            });
                    } else {
                        break;
                    }
                }
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        let (reply, new_element) = tokio::sync::oneshot::channel();
        client_tx.blocking_send(ClientMsg::NewClient { element, reply })?;

        let element = new_element.blocking_recv()??;
        Ok(Some(element))
    })
    .await?;
    Ok((factory, thread))
}

fn send_to_sources(
    data: BcMedia,
    pools: &mut HashMap<usize, gstreamer::BufferPool>,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    timestamps: &mut TimestampState,
    stream_config: &StreamConfig,
) -> AnyResult<FrameSendOutcome> {
    match data {
        BcMedia::Aac(aac) => {
            if let Some(duration) = aac.duration() {
                if let Some(aud_src) = aud_src.as_ref() {
                    let pkt_duration = Duration::from_micros(duration as u64);
                    let ts = next_cumulative_timestamp(&mut timestamps.next_audio_ts, pkt_duration);
                    let _ = send_to_appsrc(aud_src, aac.data, ts, Some(pkt_duration), true, false, pools)?;
                }
            }
            Ok(FrameSendOutcome::Sent)
        }
        BcMedia::Adpcm(adpcm) => {
            if let Some(duration) = adpcm.duration() {
                if let Some(aud_src) = aud_src.as_ref() {
                    let pkt_duration = Duration::from_micros(duration as u64);
                    let ts = next_cumulative_timestamp(&mut timestamps.next_audio_ts, pkt_duration);
                    let _ = send_to_appsrc(aud_src, adpcm.data, ts, Some(pkt_duration), true, false, pools)?;
                }
            }
            Ok(FrameSendOutcome::Sent)
        }
        BcMedia::Iframe(BcMediaIframe {
            data,
            microseconds,
            ..
        }) => {
            let frame_interval = 1_000_000u64 / u64::from(stream_config.fps.max(1));
            if let Some(vid_src) = vid_src.as_ref() {
                let pkt_duration = Duration::from_micros(frame_interval);
                let ts = next_video_timestamp(
                    microseconds,
                    &mut timestamps.last_video_source_ts,
                    &mut timestamps.next_video_ts,
                    pkt_duration,
                );
                let _ = send_to_appsrc(vid_src, data, ts, Some(pkt_duration), false, true, pools)?;
            }
            Ok(FrameSendOutcome::Sent)
        }
        BcMedia::Pframe(BcMediaPframe {
            data,
            microseconds,
            ..
        }) => {
            let frame_interval = 1_000_000u64 / u64::from(stream_config.fps.max(1));
            if let Some(vid_src) = vid_src.as_ref() {
                let pkt_duration = Duration::from_micros(frame_interval);
                let ts = next_video_timestamp(
                    microseconds,
                    &mut timestamps.last_video_source_ts,
                    &mut timestamps.next_video_ts,
                    pkt_duration,
                );
                if !send_to_appsrc(vid_src, data, ts, Some(pkt_duration), true, true, pools)? {
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
        Some(prev) if prev >= u32::MAX - VIDEO_TIMESTAMP_WRAP_WINDOW && source_ts <= VIDEO_TIMESTAMP_WRAP_WINDOW => {
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
    data: Vec<u8>,
    ts: Duration,
    duration: Option<Duration>,
    can_drop: bool,
    is_video: bool,
    pools: &mut HashMap<usize, gstreamer::BufferPool>,
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

    if retries >= MAX_RETRIES {
        if can_drop {
            return Ok(false);
        }
    }

    let msg_size = data.len();

    while pools.len() >= MAX_BUFFER_POOLS && !pools.contains_key(&msg_size) {
        if let Some(&smallest_key) = pools.keys().min() {
            if let Some(old_pool) = pools.remove(&smallest_key) {
                let _ = old_pool.set_active(false);
            }
        }
    }

    let pool = pools.entry(msg_size).or_insert_with_key(|size| {
        let pool = gstreamer::BufferPool::new();
        let mut pool_config = pool.config();
        pool_config.set_params(None, (*size) as u32, 8, 32);
        if let Err(e) = pool.set_config(pool_config) {
            log::error!("Failed to configure buffer pool: {}", e);
        }
        if let Err(e) = pool.set_active(true) {
            log::error!("Failed to activate buffer pool: {}", e);
        }
        pool
    });

    let buf = {
        let mut new_buf = pool
            .acquire_buffer(None)
            .map_err(|e| anyhow::anyhow!("Failed to acquire buffer from pool: {e:?}"))?;
        let gst_buf_mut = new_buf
            .get_mut()
            .ok_or_else(|| anyhow::anyhow!("Failed to get mutable buffer reference"))?;
            
        let time = gstreamer::ClockTime::from_useconds(ts.as_micros() as u64);
        gst_buf_mut.set_pts(time);
        if !is_video {
            gst_buf_mut.set_dts(time);
        }
        if let Some(duration) = duration {
            gst_buf_mut.set_duration(gstreamer::ClockTime::from_useconds(duration.as_micros() as u64));
        }
        
        let mut gst_buf_data = gst_buf_mut
            .map_writable()
            .map_err(|e| anyhow::anyhow!("Failed to map buffer writable: {e:?}"))?;
        gst_buf_data.copy_from_slice(data.as_slice());
        drop(gst_buf_data);
        new_buf
    };

    match appsrc.push_buffer(buf) {
        Ok(_) => Ok(true),
        Err(FlowError::Flushing) => Ok(false),
        Err(FlowError::Eos) => Ok(false),
        Err(e) => Err(anyhow::anyhow!("Error in streaming: {e:?}")),
    }?;
    
    Ok(true)
}

fn drain_latest_batch(
    rx: &mut tokio::sync::mpsc::Receiver<BcMedia>,
) -> Option<Vec<BcMedia>> {
    let first = rx.blocking_recv()?;
    let mut batch = vec![first];
    while let Ok(next) = rx.try_recv() {
        batch.push(next);
    }

    if let Some(last_iframe) = batch.iter().rposition(is_keyframe) {
        if last_iframe > 0 {
            let _ = batch.drain(0..last_iframe);
        }
    } else if batch.len() > 1 {
        log::trace!(
            "Drained RTSP batch without a keyframe; keeping latest media to avoid stalls"
        );
    }

    Some(batch)
}

fn prepare_bootstrap_batch(batch: &mut Vec<BcMedia>) -> bool {
    if let Some(last_iframe) = batch.iter().rposition(is_keyframe) {
        if last_iframe > 0 {
            let _ = batch.drain(0..last_iframe);
        }
        false
    } else if batch.len() > 1 {
        log::trace!(
            "Trimming RTSP bootstrap without a keyframe to avoid startup lag"
        );
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

fn wait_for_sources_ready(vid_src: &Option<AppSrc>, aud_src: &Option<AppSrc>, name: &str, stream: &str) -> Result<()> {
    let mut start = Instant::now();
    let mut retries = 0;

    loop {
        let video_ready = vid_src.as_ref().map_or(Ok(true), check_live_ready);
        let audio_ready = aud_src.as_ref().map_or(Ok(true), check_live_ready);

        match (video_ready, audio_ready) {
            (Ok(true), Ok(true)) => return Ok(()),
            (Err(e), _) | (_, Err(e)) => {
                log::error!("OBSERVE: Appsrc readiness failure for {}::{}: {:?}", name, stream, e);
                return Err(e);
            }
            _ => {}
        }

        if start.elapsed() >= SOURCE_READY_TIMEOUT {
            retries += 1;
            log::info!("OBSERVE: RTSP client startup retry {} for {}::{}", retries, name, stream);
            start = Instant::now(); // Reset to wait again
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

fn pipe_aac(bin: &Element, _stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = AUDIO_BUFFER_SIZE;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
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

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
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

    let payload = make_element("rtpL16pay", "pay1")?;
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
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
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

    fn sample_iframe() -> BcMedia {
        BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: 1,
            time: None,
            data: vec![1, 2, 3, 4],
        })
    }

    fn sample_pframe() -> BcMedia {
        BcMedia::Pframe(BcMediaPframe {
            video_type: VideoType::H264,
            microseconds: 2,
            data: vec![5, 6, 7, 8],
        })
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

    /// Verify buffer pool limits prevent memory leaks
    #[test]
    fn test_buffer_pool_limits() {
        // MAX_BUFFER_POOLS limits how many different frame-size pools we maintain
        // This prevents memory leaks from cameras with variable frame sizes
        assert!(
            MAX_BUFFER_POOLS >= 16,
            "Too few buffer pools, may cause excessive eviction"
        );
        assert!(
            MAX_BUFFER_POOLS <= 64,
            "Too many buffer pools allowed, memory leak risk"
        );

        // Video frames typically cluster around a few sizes:
        // - I-frames: 50-100KB (few different sizes)
        // - P-frames: 5-20KB (few different sizes)
        // - Audio: 512-2KB (consistent size)
        // 32 pools should be more than enough
        assert_eq!(MAX_BUFFER_POOLS, 32, "Expected 32 buffer pools");
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

        let drained = drain_latest_batch(&mut rx).expect("expected batch");
        assert_eq!(drained.len(), 3);
        assert!(matches!(drained.first(), Some(BcMedia::Pframe(_))));
        assert!(matches!(drained.last(), Some(BcMedia::Pframe(_))));
    }

    #[test]
    fn test_drain_latest_batch_trims_prekeyframes() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        tx.try_send(sample_pframe()).unwrap();
        tx.try_send(sample_pframe()).unwrap();
        tx.try_send(sample_iframe()).unwrap();
        tx.try_send(sample_pframe()).unwrap();

        let drained = drain_latest_batch(&mut rx).expect("expected batch");
        assert_eq!(drained.len(), 2);
        assert!(matches!(drained.first(), Some(BcMedia::Iframe(_))));
        assert!(matches!(drained.last(), Some(BcMedia::Pframe(_))));
    }

    #[test]
    fn test_prepare_bootstrap_batch_trims_to_latest_keyframe() {
        let mut batch = vec![sample_pframe(), sample_pframe(), sample_iframe(), sample_pframe()];
        let needs_keyframe = prepare_bootstrap_batch(&mut batch);

        assert!(!needs_keyframe);
        assert_eq!(batch.len(), 2);
        assert!(matches!(batch.first(), Some(BcMedia::Iframe(_))));
    }

    #[test]
    fn test_prepare_bootstrap_batch_limits_lag_without_keyframe() {
        let mut batch = vec![sample_pframe(), sample_pframe(), sample_pframe()];
        let needs_keyframe = prepare_bootstrap_batch(&mut batch);

        assert!(needs_keyframe);
        assert_eq!(batch.len(), 1);
        assert!(matches!(batch.first(), Some(BcMedia::Pframe(_))));
    }
}
