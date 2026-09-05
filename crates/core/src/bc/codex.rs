//! Create a tokio encoder/decoder for turning a AsyncRead/Write stream into
//! a Bc packet
//!
//! BcCodex is used with a `[tokio_util::codec::Framed]` to form complete packets
//!
use crate::bc::model::*;
use crate::bc::xml::*;
use crate::{Credentials, Error, Result};
use bytes::{Buf, BytesMut};
use nom::AsBytes;
use tokio_util::codec::{Decoder, Encoder};

pub(crate) struct BcCodex {
    context: BcContext,
    /// Bytes discarded while hunting for the next message boundary after a
    /// framing error. Non-zero means we are mid-resync.
    skipped: usize,
}

/// How many trailing bytes to keep when no magic is found in a corrupt buffer:
/// they may be the first bytes of a magic split across two reads.
const MAGIC_TAIL_KEEP: usize = 3;

impl BcCodex {
    pub(crate) fn new_with_debug(credentials: Credentials) -> Self {
        let mut context = BcContext::new(credentials);

        context.debug_on();
        Self {
            context,
            skipped: 0,
        }
    }
    pub(crate) fn new(credentials: Credentials) -> Self {
        Self {
            context: BcContext::new(credentials),
            skipped: 0,
        }
    }
}

/// Offset of the next BC message magic in `buf`, if any.
fn find_next_magic(buf: &[u8]) -> Option<usize> {
    let magics = [MAGIC_HEADER.to_le_bytes(), MAGIC_HEADER_REV.to_le_bytes()];
    buf.windows(4)
        .position(|window| magics.iter().any(|magic| window == magic))
}

impl Encoder<Bc> for BcCodex {
    type Error = Error;

    fn encode(&mut self, item: Bc, dst: &mut BytesMut) -> Result<()> {
        // let context = self.context.read().unwrap();
        const BC_ENCRYPTED: EncryptionProtocol = EncryptionProtocol::BCEncrypt;
        let buf: Vec<u8> = Default::default();
        let enc_protocol: &EncryptionProtocol = match self.context.get_encrypted() {
            EncryptionProtocol::Aes { .. } | EncryptionProtocol::FullAes { .. }
                if item.meta.msg_id == 1 =>
            {
                // During login the encyption protocol cannot go higher than BCEncrypt
                // even if we support AES. (BUt it can go lower i.e. None)
                &BC_ENCRYPTED
            }
            n => n,
        };
        let buf = item.serialize(buf, enc_protocol)?;
        dst.extend_from_slice(buf.as_slice());
        Ok(())
    }
}

impl Decoder for BcCodex {
    type Item = Bc;
    type Error = Error;

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    log::debug!(
                        "bytes remaining on BC stream: {:X?}",
                        buf.as_bytes().chunks(25).next()
                    );
                    // Right after this we seem to get an issue with the camera dropping us
                    // Needs probing
                    // F0, DE, BC, A, 3, 0, 0, 0, 88, 6, 0, 0, 0, 1, 4, 0, C8, 0, 0, 0, 0, 0, 0, 0, 30, 31, 64, 63, 48,
                    // 32, 36, 34, 6A, 6, 0, 0, 0, 0, 0, 0, D8, F5, C7, 86, 56, 0, 0, 0, 0, 0, 0, 1, 21, 9A, FC, 22, 7F, 6, AE, F6, 15, FF, E5, 71, 4, 2F, 24, 61, 15, 96, F0, BF, 83, DE, 10, BE, B4, 2E, 3
                    // 9, 76, 56, 92, 7E, 48, 79, 20, 9A, DC, 1B, BB, AC, 22, 60, 5C, 72, B5, 3D, 8, E0, 34, 43, 3F, 2E, A7, 81, A8, 11, 75, 7F, 58, 3E, 8, 54, 91, 43, 21, EC, 6B, D6, 1A, D5, CB, D5, 6C,
                    // 8C, 2E, 6E, A3, 51, C3, A4, F0, CF, 2B, 61, 81, D0, 1C, A1, 76, EE, BF, 7A, D5, D8, D1, C4, D, B0, 45, EE, 3E, 93, 9A, CE, 5F, AB, 75, 55, AC, 9D, 66, DE, 23, 6D, 5F, 25, 57, DA, F5
                    //, E, 7F, 8D, 30, A7, 66, C4, 60, 76, 41, D0, 6A, 23, E, A9, C5, 51, EE, F6, DD, 19, E7, A8, 96, 9F, 2B, AF, 31, 90, 9D, FC, BE
                    Ok(None)
                }
            }
        }
        // match self.decode(buf)? {
        //     Some(frame) => Ok(Some(frame)),
        //     None => Ok(None),
        // }
    }

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let bc = loop {
            match Bc::deserialize(&self.context, src) {
                Ok(bc) => {
                    if self.skipped > 0 {
                        log::warn!(
                            "BC stream resynchronised after skipping {} bytes",
                            self.skipped
                        );
                        self.skipped = 0;
                    }
                    break bc;
                }
                Err(Error::NomIncomplete(_)) => return Ok(None),
                Err(e) => {
                    // Framing error. On the UDP transport this is what a
                    // skipped (lost, never retransmitted) packet looks like:
                    // we are somewhere inside a message with no way to know
                    // where it ends. Tearing the whole camera connection down
                    // for that costs a full reconnect (discovery, login,
                    // keyframe wait; on a Lumus 15-75s). Instead hunt for the
                    // next message magic and resume there. The damaged message
                    // is lost: video recovers at the next keyframe, control
                    // replies are covered by the caller's timeout/retry.
                    if self.skipped == 0 {
                        log::warn!(
                            "BC stream framing error, scanning for next message boundary: {:?}",
                            e
                        );
                    }
                    if src.is_empty() {
                        return Ok(None);
                    }
                    match find_next_magic(&src[1..]) {
                        Some(offset) => {
                            let drop = offset + 1;
                            src.advance(drop);
                            self.skipped += drop;
                            continue;
                        }
                        None => {
                            let keep = src.len().min(MAGIC_TAIL_KEEP);
                            let drop = src.len() - keep;
                            src.advance(drop);
                            self.skipped += drop;
                            return Ok(None);
                        }
                    }
                }
            }
        };
        // Update context
        if let Bc {
            meta:
                BcMeta {
                    msg_id: 1,
                    response_code,
                    ..
                },
            body:
                BcBody::ModernMsg(ModernMsg {
                    payload:
                        Some(BcPayloads::BcXml(BcXml {
                            encryption: Some(Encryption { nonce, .. }),
                            ..
                        })),
                    ..
                }),
        } = &bc
        {
            if response_code >> 8 == 0xdd {
                // Login reply has the encryption info
                // Set that the encryption type now
                let encryption_protocol_byte = (response_code & 0xff) as usize;
                match encryption_protocol_byte {
                    0x00 => self.context.set_encrypted(EncryptionProtocol::Unencrypted),
                    0x01 => self.context.set_encrypted(EncryptionProtocol::BCEncrypt),
                    0x02 => self.context.set_encrypted(EncryptionProtocol::aes(
                        self.context.credentials.make_aeskey(nonce),
                    )),
                    0x12 => self.context.set_encrypted(EncryptionProtocol::full_aes(
                        self.context.credentials.make_aeskey(nonce),
                    )),
                    _ => {
                        return Err(Error::UnknownEncryption(encryption_protocol_byte));
                    }
                }
            }
        }

        if let BcBody::ModernMsg(ModernMsg {
            extension:
                Some(Extension {
                    binary_data: Some(on_off),
                    ..
                }),
            ..
        }) = bc.body
        {
            if on_off == 0 {
                self.context.binary_off(bc.meta.msg_num);
            } else {
                self.context.binary_on(bc.meta.msg_num);
            }
        }

        Ok(Some(bc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bc::model::EncryptionProtocol;

    fn login_codex() -> BcCodex {
        BcCodex {
            context: BcContext::new_with_encryption(EncryptionProtocol::BCEncrypt),
            skipped: 0,
        }
    }

    fn login_sample() -> &'static [u8] {
        include_bytes!("samples/model_sample_modern_login.bin")
    }

    #[test]
    fn decodes_clean_message() {
        let mut buf = BytesMut::from(login_sample());
        let mut codex = login_codex();
        let bc = codex.decode(&mut buf).unwrap().expect("message");
        assert_eq!(bc.meta.msg_id, 1);
        assert!(buf.is_empty());
        assert_eq!(codex.skipped, 0);
    }

    #[test]
    fn resyncs_to_next_magic_after_garbage() {
        // Garbage, then a stray partial magic, then a real message: the codec
        // must skip to the real message instead of failing the connection.
        let mut raw = vec![0x11u8; 37];
        raw.extend_from_slice(&[0xf0, 0xde]);
        raw.extend_from_slice(login_sample());
        let mut buf = BytesMut::from(&raw[..]);
        let mut codex = login_codex();
        let bc = codex
            .decode(&mut buf)
            .unwrap()
            .expect("message after resync");
        assert_eq!(bc.meta.msg_id, 1);
        assert!(buf.is_empty());
        assert_eq!(codex.skipped, 0, "skip counter resets after a good frame");
    }

    #[test]
    fn garbage_without_magic_is_dropped_keeping_a_tail() {
        let mut buf = BytesMut::from(&[0x11u8; 100][..]);
        let mut codex = login_codex();
        assert!(codex.decode(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), MAGIC_TAIL_KEEP);
        assert_eq!(codex.skipped, 100 - MAGIC_TAIL_KEEP);
    }

    #[test]
    fn magic_split_across_reads_is_found() {
        let sample = login_sample();
        let mut buf = BytesMut::from(&[0x11u8; 20][..]);
        buf.extend_from_slice(&sample[..2]);
        let mut codex = login_codex();
        assert!(codex.decode(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&sample[2..]);
        let bc = codex.decode(&mut buf).unwrap().expect("message");
        assert_eq!(bc.meta.msg_id, 1);
        assert!(buf.is_empty());
    }
}
