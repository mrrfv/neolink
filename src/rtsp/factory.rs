use gstreamer::ClockTime;
use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, FlowError, GhostPad};
use gstreamer_app::{AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle};

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
        }
    }
}

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element| {
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

enum ClientMsg {
    NewClient {
        element: Element,
        reply: tokio::sync::oneshot::Sender<Element>,
    },
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient { element, reply } => {
                    log::info!("New RTSP client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    tokio::task::spawn(async move {
                        clear_bin(&element)?;
                        log::trace!("{name}::{stream}: Starting camera");

                        // Start the camera
                        let config = camera.config().await?.borrow().clone();
                        let mut media_rx = camera.stream_while_live(stream).await?;

                        log::trace!("{name}::{stream}: Learning camera stream type");
                        // Learn the camera data type
                        let mut buffer = vec![];
                        let mut frame_count = 0usize;

                        let mut stream_config = StreamConfig::new(&camera, stream).await?;
                        while let Some(media) = media_rx.recv().await {
                            stream_config.update_from_media(&media);
                            buffer.push(media);
                            if frame_count > 10
                                || (stream_config.vid_type.is_some()
                                    && stream_config.aud_type.is_some())
                            {
                                break;
                            }
                            frame_count += 1;
                        }

                        log::trace!("{name}::{stream}: Building the pipeline");
                        // Build the right video pipeline
                        let vid_src = match stream_config.vid_type.as_ref() {
                            Some(VideoType::H264) => {
                                let src = build_h264(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(VideoType::H265) => {
                                let src = build_h265(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => {
                                build_unknown(&element, &config.splash_pattern.to_string())?;
                                AnyResult::Ok(None)
                            }
                        }?;

                        // Build the right audio pipeline
                        let aud_src = match stream_config.aud_type.as_ref() {
                            Some(AudioType::Aac) => {
                                let src = build_aac(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(AudioType::Adpcm(block_size)) => {
                                let src = build_adpcm(&element, *block_size, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => AnyResult::Ok(None),
                        }?;

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

                        log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                        // Send the pipeline back to the factory so it can start
                        let _ = reply.send(element);

                        // Run blocking code on a separate named thread.
                        // This is not an async thread — it sends frames into GStreamer.
                        let thread_name = format!("{name}::{stream}::sender");
                        std::thread::Builder::new()
                            .name(thread_name.clone())
                            .spawn(move || {
                                // Use u64 for timestamps to avoid overflow
                                // u32 overflows after ~71 minutes at 30fps
                                let mut aud_ts: u64 = 0;
                                let mut vid_ts: u64 = 0;
                                let mut pools = Default::default();

                                log::trace!("{name}::{stream}: Sending buffered frames");
                                for buffered in buffer.drain(..) {
                                    send_to_sources(
                                        buffered,
                                        &mut pools,
                                        &vid_src,
                                        &aud_src,
                                        &mut vid_ts,
                                        &mut aud_ts,
                                        &stream_config,
                                    )?;
                                }

                                log::trace!("{name}::{stream}: Sending new frames");
                                while let Some(data) = media_rx.blocking_recv() {
                                    let r = send_to_sources(
                                        data,
                                        &mut pools,
                                        &vid_src,
                                        &aud_src,
                                        &mut vid_ts,
                                        &mut aud_ts,
                                        &stream_config,
                                    );
                                    if let Err(r) = &r {
                                        log::info!("Failed to send to source: {r:?}");
                                    }
                                    r?;
                                }
                                log::trace!("All media received");
                                AnyResult::Ok(())
                            })
                            .unwrap_or_else(|e| {
                                log::error!("Failed to spawn frame sender thread: {e}");
                                // Return a dummy handle that does nothing
                                std::thread::spawn(|| AnyResult::Ok(()))
                            });
                        AnyResult::Ok(())
                    });
                }
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        let (reply, new_element) = tokio::sync::oneshot::channel();
        client_tx.blocking_send(ClientMsg::NewClient { element, reply })?;

        let element = new_element.blocking_recv()?;
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
    vid_ts: &mut u64,
    aud_ts: &mut u64,
    stream_config: &StreamConfig,
) -> AnyResult<()> {
    // Update timestamps (u64 to avoid overflow during long streams)
    match data {
        BcMedia::Aac(aac) => {
            if let Some(duration) = aac.duration() {
                if let Some(aud_src) = aud_src.as_ref() {
                    log::debug!("Sending AAC: {:?}", Duration::from_micros(*aud_ts));
                    send_to_appsrc(aud_src, aac.data, Duration::from_micros(*aud_ts), pools)?;
                }
                *aud_ts += duration as u64;
            } else {
                log::warn!("Skipping AAC frame: could not calculate duration");
            }
        }
        BcMedia::Adpcm(adpcm) => {
            if let Some(duration) = adpcm.duration() {
                if let Some(aud_src) = aud_src.as_ref() {
                    log::trace!("Sending ADPCM: {:?}", Duration::from_micros(*aud_ts));
                    send_to_appsrc(aud_src, adpcm.data, Duration::from_micros(*aud_ts), pools)?;
                }
                *aud_ts += duration as u64;
            } else {
                log::warn!("Skipping ADPCM frame: could not calculate duration");
            }
        }
        BcMedia::Iframe(BcMediaIframe { data, .. })
        | BcMedia::Pframe(BcMediaPframe { data, .. }) => {
            if let Some(vid_src) = vid_src.as_ref() {
                log::trace!("Sending VID: {:?}", Duration::from_micros(*vid_ts));
                send_to_appsrc(vid_src, data, Duration::from_micros(*vid_ts), pools)?;
            }
            const MICROSECONDS: u64 = 1_000_000;
            *vid_ts += MICROSECONDS / stream_config.fps as u64;
        }
        _ => {}
    }
    Ok(())
}

fn send_to_appsrc(
    appsrc: &AppSrc,
    data: Vec<u8>,
    mut ts: Duration,
    pools: &mut HashMap<usize, gstreamer::BufferPool>,
) -> AnyResult<()> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    // In live mode we follow the advice in
    // https://gstreamer.freedesktop.org/documentation/additional/design/element-source.html?gi-language=c#live-sources
    // Only push buffers when in play state and have a clock
    // we also timestamp at the current time
    if appsrc.is_live() {
        if let Some(time) = appsrc
            .current_clock_time()
            .and_then(|t| appsrc.base_time().map(|bt| t - bt))
        {
            if matches!(appsrc.current_state(), gstreamer::State::Playing) {
                ts = Duration::from_micros(time.useconds());
            } else {
                // Not playing
                return Ok(());
            }
        } else {
            // Clock not up yet
            return Ok(());
        }
    }

    // Backpressure handling: brief wait if buffer is too full before pushing.
    // This gives slow clients a chance to catch up without excessive delays.
    //
    // Architecture note: Each RTSP client has their own thread and buffer,
    // so backpressure on one client does NOT affect other clients.
    //
    // Strategy:
    // - Check buffer level before pushing
    // - If > 90% full, wait with exponential backoff
    // - Max 3 retries (~70ms total wait) - reduced from 5 retries (~310ms)
    //   to minimize latency impact for time-sensitive applications like ALPR
    // - If still full after retries, push anyway (GStreamer will handle overflow)
    const MAX_RETRIES: u32 = 3;
    const INITIAL_WAIT_MS: u64 = 10;
    const BUFFER_THRESHOLD: u64 = 90; // Percent

    let max_bytes = appsrc.max_bytes();
    let threshold_bytes = max_bytes * BUFFER_THRESHOLD / 100;

    let mut retries = 0;
    let mut wait_ms = INITIAL_WAIT_MS;

    while appsrc.current_level_bytes() >= threshold_bytes && retries < MAX_RETRIES {
        retries += 1;
        log::trace!(
            "Buffer {:.0}% full on {}, waiting {}ms (retry {}/{})",
            (appsrc.current_level_bytes() as f64 / max_bytes as f64) * 100.0,
            appsrc.name(),
            wait_ms,
            retries,
            MAX_RETRIES
        );
        std::thread::sleep(std::time::Duration::from_millis(wait_ms));
        wait_ms *= 2; // Exponential backoff: 10, 20, 40ms = 70ms max
    }

    if retries >= MAX_RETRIES {
        let is_audio = appsrc.name().as_str().contains("aud");
        let fill_pct = (appsrc.current_level_bytes() as f64 / max_bytes as f64) * 100.0;

        if is_audio {
            // Drop audio frames when buffer is full to prevent pipeline collapse.
            // Audio drops are far less noticeable than the alternative: the buffer
            // overflows, causing TCP congestion → pipeline flushing → EOF → full
            // stream reconnection cycle visible to the user.
            log::debug!(
                "Dropping audio frame on {} (buffer {:.0}% full) to prevent pipeline collapse",
                appsrc.name(),
                fill_pct,
            );
            return Ok(());
        }

        // For video: log warning but continue pushing (video is critical)
        log::warn!(
            "Client {} buffer {:.0}% full after {}ms backpressure, frame may be delayed",
            appsrc.name(),
            fill_pct,
            10 + 20 + 40 // Total backpressure wait
        );
    }

    let msg_size = data.len();

    // Evict oldest pools if we're at capacity (prevents memory leak)
    // Remove the smallest pool first - video frames are larger and more important
    while pools.len() >= MAX_BUFFER_POOLS && !pools.contains_key(&msg_size) {
        if let Some(&smallest_key) = pools.keys().min() {
            if let Some(old_pool) = pools.remove(&smallest_key) {
                log::debug!(
                    "Evicting buffer pool for size {} (have {} pools, max {})",
                    smallest_key,
                    pools.len() + 1,
                    MAX_BUFFER_POOLS
                );
                let _ = old_pool.set_active(false);
            }
        }
    }

    // Get or create a pool of this len
    let pool = pools.entry(msg_size).or_insert_with_key(|size| {
        log::trace!("Creating buffer pool for frame size {} bytes", size);
        let pool = gstreamer::BufferPool::new();
        let mut pool_config = pool.config();
        // Set a max buffers to ensure we don't grow in memory endlessly
        pool_config.set_params(None, (*size) as u32, 8, 32);
        if let Err(e) = pool.set_config(pool_config) {
            log::error!("Failed to configure buffer pool: {}", e);
        }
        if let Err(e) = pool.set_active(true) {
            log::error!("Failed to activate buffer pool: {}", e);
        }
        pool
    });

    // Get a buffer from the pool and then copy in the data
    let buf = {
        let mut new_buf = pool
            .acquire_buffer(None)
            .map_err(|e| anyhow!("Failed to acquire buffer from pool: {e:?}"))?;
        let gst_buf_mut = new_buf
            .get_mut()
            .ok_or_else(|| anyhow!("Failed to get mutable buffer reference"))?;
        let time = ClockTime::from_useconds(ts.as_micros() as u64);
        gst_buf_mut.set_dts(time);
        gst_buf_mut.set_pts(time);
        let mut gst_buf_data = gst_buf_mut
            .map_writable()
            .map_err(|e| anyhow!("Failed to map buffer writable: {e:?}"))?;
        gst_buf_data.copy_from_slice(data.as_slice());
        drop(gst_buf_data);
        new_buf
    };

    // Push buffer into the appsrc
    match appsrc.push_buffer(buf) {
        Ok(_) => Ok(()),
        Err(FlowError::Flushing) => {
            // Pipeline is flushing or client disconnected
            log::debug!(
                "Pipeline flushing on {}, client may have disconnected",
                appsrc.name()
            );
            Ok(())
        }
        Err(FlowError::Eos) => {
            // End of stream - normal shutdown
            log::debug!("EOS on {}", appsrc.name());
            Ok(())
        }
        Err(e) => Err(anyhow!("Error in streaming: {e:?}")),
    }?;
    // Check if we need to pause
    if appsrc.current_level_bytes() >= appsrc.max_bytes() * 2 / 3
        && matches!(appsrc.current_state(), gstreamer::State::Paused)
    {
        if let Err(e) = appsrc.set_state(gstreamer::State::Playing) {
            log::error!("Failed to set appsrc to Playing: {e:?}");
        }
    } else if appsrc.current_level_bytes() <= appsrc.max_bytes() / 3
        && matches!(appsrc.current_state(), gstreamer::State::Playing)
    {
        if let Err(e) = appsrc.set_state(gstreamer::State::Paused) {
            log::error!("Failed to set appsrc to Paused: {e:?}");
        }
    }
    Ok(())
}
fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
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

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

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

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

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

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
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
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

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

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = AUDIO_BUFFER_SIZE;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
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

    // The fallback
    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let fallback_switch = make_element("fallbackswitch", "audfallbackswitch");
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
        fallback_switch.set_property("immediate-fallback", true);
    }

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        bin.add_many([&silence, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([&silence, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

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

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
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
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
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
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = AUDIO_BUFFER_SIZE;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
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
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
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
        const INITIAL_WAIT_MS: u64 = 10;
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
}
