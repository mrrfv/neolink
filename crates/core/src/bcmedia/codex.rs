//! Handles sending and recieving messages as packets
//!
//! BcMediaCodex is used with a `[tokio_util::codec::Framed]` to form complete packets
//!
use crate::bcmedia::model::*;
use crate::{Error, Result};
use bytes::{Buf, BytesMut};
use log::*;
use std::convert::TryInto;
use tokio_util::codec::{Decoder, Encoder};

pub struct BcMediaCodex {
    /// If true we will not search for the start of the next packet
    /// in the event that the stream appears to be corrupted
    strict: bool,
    amount_skipped: usize,
}

impl BcMediaCodex {
    pub(crate) fn new(strict: bool) -> Self {
        Self {
            strict,
            amount_skipped: 0,
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
                    match &bc {
                        BcMedia::Iframe(f) => debug!("Parsed IFrame: payload={}", f.data.len()),
                        BcMedia::Pframe(f) => debug!("Parsed PFrame: payload={}", f.data.len()),
                        BcMedia::Aac(f) => debug!("Parsed AAC: payload={}", f.data.len()),
                        BcMedia::Adpcm(f) => debug!("Parsed ADPCM: payload={}", f.data.len()),
                        BcMedia::InfoV1(_) => debug!("Parsed InfoV1"),
                        BcMedia::InfoV2(_) => debug!("Parsed InfoV2"),
                        _ => {}
                    }
                    if self.amount_skipped > 0 {
                        trace!("Amount skipped to restore stream: {}", self.amount_skipped);
                        self.amount_skipped = 0;
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
                            debug!("Error in stream attempting to restore: {:?}", e);
                        }
                        
                        // Instead of clearing the whole buffer, skip bytes until we find something
                        // that looks like a valid magic header.
                        let mut found_magic = false;
                        let mut skip_count = 0;
                        
                        for i in 1..src.len() {
                            if i + 4 > src.len() {
                                break;
                            }
                            let potential_magic = u32::from_le_bytes(src[i..i+4].try_into().unwrap());
                            
                            if (potential_magic == MAGIC_HEADER_BCMEDIA_INFO_V1)
                                || (potential_magic == MAGIC_HEADER_BCMEDIA_INFO_V2)
                                || ((potential_magic & 0x00FFFFFF) == 0x00306463) // cd0x style
                                || ((potential_magic & 0x00FFFFFF) == 0x00316463) // cd1x style
                                || ((potential_magic & 0xFFFFFF00) == 0x63643000) // 00dc style
                                || ((potential_magic & 0xFFFFFF00) == 0x63643100) // 01dc style
                                || (potential_magic == MAGIC_HEADER_BCMEDIA_AAC)
                                || (potential_magic == MAGIC_HEADER_BCMEDIA_ADPCM)
                                || (potential_magic == MAGIC_MARKER_REOLINK)
                                || (potential_magic == MAGIC_MARKER_BAICHUAN)
                                || (potential_magic == MAGIC_HEADER_BCMEDIA_NULL)
                                || (potential_magic == MAGIC_HEADER_BCMEDIA_INFO_ALT)
                            {
                                debug!("Found potential magic {:08x} at offset {}, skipping {} bytes", potential_magic, i, i);
                                trace!("Bytes at magic: {:02x?}", &src[i..src.len().min(i+32)]);

                                skip_count = i;
                                found_magic = true;
                                break;
                            }
                        }
                        
                        if found_magic {
                            self.amount_skipped += skip_count;
                            src.advance(skip_count);
                            // Loop again to try and parse from the new position
                            continue;
                        } else {
                            // No magic found in the entire buffer, but maybe it's incomplete
                            // We should only clear what we definitely can't use.
                            // Keep the last 3 bytes just in case a magic starts there.
                            let keep = std::cmp::min(src.len(), 3);
                            let discard = src.len() - keep;
                            self.amount_skipped += discard;
                            src.advance(discard);
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }
}
