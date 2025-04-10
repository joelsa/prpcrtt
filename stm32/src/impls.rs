use core::{fmt::Arguments, ops::DerefMut};
use cobs::{decode_in_place, encode};
use embassy_sync::{blocking_mutex::raw::RawMutex, mutex::Mutex};
use embassy_time::Timer;
use postcard_rpc::{
    header::{VarHeader, VarKeyKind},
    server::{
        AsWireRxErrorKind, AsWireTxErrorKind, WireRx, WireRxErrorKind, WireTx, WireTxErrorKind,
    },
};
use rtt_target::{DownChannel, UpChannel};
use serde::Serialize;

pub enum RttRxError {}

impl AsWireRxErrorKind for RttRxError {
    fn as_kind(&self) -> WireRxErrorKind {
        todo!()
    }
}

pub enum RttTxError {}

impl AsWireTxErrorKind for RttTxError {
    fn as_kind(&self) -> WireTxErrorKind {
        todo!()
    }
}

pub struct RttRx {
    pub channel: DownChannel,
    pub buf: &'static mut [u8],
    pub used: usize,
}

pub struct RttTx<R: RawMutex + 'static> {
    pub inner: &'static Mutex<R, RttTxInner>,
}

impl<R: RawMutex + 'static> Clone for RttTx<R> {
    fn clone(&self) -> Self {
        Self { inner: self.inner }
    }
}

pub struct RttTxInner {
    pub channel: UpChannel,
    pub buf1: &'static mut [u8],
    pub buf2: &'static mut [u8],
}

impl WireRx for RttRx {
    type Error = RttRxError;

    async fn receive<'a>(&mut self, outbuf: &'a mut [u8]) -> Result<&'a mut [u8], Self::Error> {
        let Self { channel, buf, used } = self;
        'frame: loop {
            let window = &mut buf[*used..];
            if window.is_empty() {
                todo!("Implement drain until 0");
            }
            let read_ct = channel.read(window);
            if read_ct == 0 {
                Timer::after_millis(1).await;
                continue 'frame;
            }
            // | used before | rx'd now        | later       |
            // | used before | used now | data | later       |
            //               |----------^ - pos
            // | to decode              | data | later       |
            // ^^^^^^^^^^^^^^^^^^^^^^^^^^ - passed to decoder
            // | after decoding     | x | data | later       |
            // ^^^^^^^^^^^^^^^^^^^^^^ - copied to caller
            // | data | unused                               |
            // ^^^^^^^^ - retained for next call
            let (now, _later) = window.split_at_mut(read_ct);
            if let Some(pos) = now.iter().position(|b| *b == 0) {
                // Okay, we DID have data, let's store off what we need for
                // copying back later. We need to start copying after old used
                // plus pos
                //
                // include the zero
                let pos = pos + 1;
                let copyback_start = *used + pos;
                let copyback_ct = now.len() - pos;

                // Now we need to get the slice of data to pass to the decoder
                let to_decode = *used + pos;
                let done = if let Ok(b) = decode_in_place(&mut buf[..to_decode]) {
                    // TODO bounds check
                    outbuf[..b].copy_from_slice(&buf[..b]);
                    Some(b)
                } else {
                    // bad frame, do we report this? or just move on?
                    None
                };

                // Success or not, copy back unused data
                let (dest, srcplus) = buf.split_at_mut(copyback_ct);
                dest.copy_from_slice(&srcplus[(copyback_start - copyback_ct)..][..copyback_ct]);
                *used = copyback_ct;

                if let Some(b) = done {
                    return Ok(&mut outbuf[..b]);
                }
            } else {
                *used += now.len();
            }
        }
    }
}

impl<R: RawMutex> WireTx for RttTx<R> {
    type Error = RttTxError;

    async fn send<T: Serialize + ?Sized>(
        &self,
        hdr: VarHeader,
        msg: &T,
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;
        let RttTxInner { channel, buf1, buf2 } = inner.deref_mut();
        let Some((hdr, later)) = hdr.write_to_slice(buf1) else {
            panic!();
        };
        let Ok(body) = postcard::to_slice(msg, later) else {
            panic!();
        };
        let used = hdr.len() + body.len();
        // TODO: check max size
        let used = encode(&buf1[..used], buf2);
        let window = &buf2[..used];
        // TODO: rtt-target doesn't really have any reasonable way to determine
        // the amount sent in order to properly fragment the writes, so we'll
        // need to just block if full, I guess :/
        channel.write(window);
        channel.write(&[0]);
        Ok(())
    }

    async fn send_raw(&self, buf: &[u8]) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().await;
        let RttTxInner { channel, buf1: _, buf2 } = inner.deref_mut();
        // TODO: check max size
        let used = encode(buf, buf2);
        let window = &buf2[..used];
        // TODO: rtt-target doesn't really have any reasonable way to determine
        // the amount sent in order to properly fragment the writes, so we'll
        // need to just block if full, I guess :/
        channel.write(window);
        channel.write(&[0]);
        Ok(())
    }

    async fn send_log_str(&self, _kkind: VarKeyKind, _s: &str) -> Result<(), Self::Error> {
        todo!()
    }

    async fn send_log_fmt<'a>(
        &self,
        _kkind: VarKeyKind,
        _a: Arguments<'a>,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}
