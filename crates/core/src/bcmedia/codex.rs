//! Handles sending and recieving messages as packets
//!
//! BcMediaCodex is used with a `[tokio_util::codec::Framed]` to form complete packets
//!
use crate::bcmedia::model::*;
use crate::{Error, Result};
use bytes::{Buf, BytesMut};
use log::*;
use tokio_util::codec::{Decoder, Encoder};

pub struct BcMediaCodex {
    /// If true we will not search for the start of the next packet
    /// in the event that the stream appears to be corrupted
    strict: bool,
    amount_skipped: usize,
    /// Set once bytes have been skipped; the next good frame is preceded by a
    /// `BcMedia::Discont` so consumers resync at a keyframe.
    discont_pending: bool,
    /// Frame held back while `Discont` is emitted first.
    pending: Option<BcMedia>,
}

impl BcMediaCodex {
    pub(crate) fn new(strict: bool) -> Self {
        Self {
            strict,
            amount_skipped: 0,
            discont_pending: false,
            pending: None,
        }
    }
}

impl Encoder<BcMedia> for BcMediaCodex {
    type Error = Error;

    fn encode(&mut self, item: BcMedia, dst: &mut BytesMut) -> Result<()> {
        let buf: Vec<u8> = Default::default();
        let buf = item.serialize(buf)?;
        dst.extend_from_slice(buf.as_slice());
        Ok(())
    }
}

impl Decoder for BcMediaCodex {
    type Item = BcMedia;
    type Error = Error;

    /// Since frames can cross EOF boundaries we overload this so it doesn't error if
    /// there are bytes left on the stream
    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => Ok(None),
        }
    }

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if let Some(held) = self.pending.take() {
            return Ok(Some(held));
        }
        loop {
            match BcMedia::deserialize(src) {
                Ok(BcMedia::Skip) => {
                    if self.amount_skipped > 0 {
                        trace!("Amount skipped to restore stream: {}", self.amount_skipped);
                        self.amount_skipped = 0;
                    }
                    continue;
                }
                Ok(bc) => {
                    if self.amount_skipped > 0 {
                        trace!("Amount skipped to restore stream: {}", self.amount_skipped);
                        self.amount_skipped = 0;
                    }
                    if self.discont_pending {
                        self.discont_pending = false;
                        self.pending = Some(bc);
                        return Ok(Some(BcMedia::Discont));
                    }
                    return Ok(Some(bc));
                }
                Err(Error::NomIncomplete(_)) => {
                    if self.amount_skipped > 0 {
                        trace!("Amount skipped to restore stream: {}", self.amount_skipped);
                        self.amount_skipped = 0;
                    }
                    return Ok(None);
                }
                Err(e) => {
                    if self.strict {
                        return Err(e);
                    } else if src.is_empty() {
                        return Ok(None);
                    } else {
                        if self.amount_skipped == 0 {
                            debug!("Error in stream attempting to restore");
                            trace!("   Stream Error: {:?}", e);
                        }
                        self.discont_pending = true;
                        // Resync by advancing one byte and letting the parser
                        // re-validate at the next offset. The frame magics are
                        // strongly validated (I/P frames require a following
                        // "H264"/"H265" tag and bounded sizes), so a false match
                        // inside corrupted data fails the parse and we keep
                        // scanning. This finds the next real frame boundary
                        // instead of discarding the whole buffer, which would
                        // throw away the next valid frame if it had already
                        // arrived — turning brief packet loss into a multi-second
                        // gap. When the scan reaches a partial magic at the tail
                        // (< header size), deserialize reports NomIncomplete and
                        // those bytes are preserved for the next read.
                        self.amount_skipped += 1;
                        src.advance(1);
                        continue;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iframe_sample() -> Vec<u8> {
        [
            include_bytes!("samples/iframe_0.raw").as_ref(),
            include_bytes!("samples/iframe_1.raw").as_ref(),
            include_bytes!("samples/iframe_2.raw").as_ref(),
            include_bytes!("samples/iframe_3.raw").as_ref(),
            include_bytes!("samples/iframe_4.raw").as_ref(),
        ]
        .concat()
    }

    #[test]
    fn non_strict_emits_discont_then_frames_after_garbage() {
        let frame = iframe_sample();
        let mut raw = vec![0x42u8; 17];
        raw.extend_from_slice(&frame);
        raw.extend_from_slice(&frame);
        let mut buf = BytesMut::from(&raw[..]);
        let mut codex = BcMediaCodex::new(false);
        assert!(matches!(
            codex.decode(&mut buf).unwrap(),
            Some(BcMedia::Discont)
        ));
        assert!(matches!(
            codex.decode(&mut buf).unwrap(),
            Some(BcMedia::Iframe(_))
        ));
        assert!(matches!(
            codex.decode(&mut buf).unwrap(),
            Some(BcMedia::Iframe(_))
        ));
        assert!(codex.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn clean_stream_has_no_discont() {
        let frame = iframe_sample();
        let mut buf = BytesMut::from(&frame[..]);
        let mut codex = BcMediaCodex::new(false);
        assert!(matches!(
            codex.decode(&mut buf).unwrap(),
            Some(BcMedia::Iframe(_))
        ));
        assert!(codex.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn strict_errors_on_garbage() {
        let mut buf = BytesMut::from(&[0x42u8; 17][..]);
        let mut codex = BcMediaCodex::new(true);
        assert!(codex.decode(&mut buf).is_err());
    }
}
