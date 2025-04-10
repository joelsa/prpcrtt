#![no_std]
#![no_main]

use app::AppTx;
use embassy_executor::Spawner;
use embassy_stm32::gpio::{Level, Output, Speed};
use embassy_sync::{blocking_mutex::raw::ThreadModeRawMutex, mutex::Mutex};
use embassy_time::{Duration, Instant, Ticker};
use impls::{RttRx, RttTx, RttTxInner};
use postcard_rpc::{header::VarSeq, server::{Dispatch, Sender, Server}};
use rtt_target::rtt_init; 
use static_cell::{ConstStaticCell, StaticCell};
use template_icd::{HelloTopic, HelloWorld};

use panic_probe as _;
use defmt::info;

pub mod app;
pub mod handlers;
pub mod impls;

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    use rtt_target::ChannelMode;
    let channels = rtt_init! {
        up: {
            0: {
                size: 1024,
                name: "defmt",
            }
            1: {
                size: 1024,
                mode: ChannelMode::BlockIfFull,
                name: "postcard-rpc uplink",
            }
        }
        down: {
            0: {
                size: 1024,
                name: "postcard-rpc downlink",
            }
        }
    };
    
    rtt_target::set_defmt_channel(channels.up.0);

    info!("Initializing Firmware...");

    // SYSTEM INIT
    let p = embassy_stm32::init(Default::default());
    // Obtain the flash ID
    let unique_id = unique_id::get_unique_id().unwrap();

    // USB/RPC INIT
    let pbufs = app::PBUFS.take();
    let led = Output::new(p.PB14, Level::Low, Speed::Low);

    let context = app::Context { unique_id, led };

    static BUF_TX_1: ConstStaticCell<[u8; 1024]> = ConstStaticCell::new([0u8; 1024]);
    static BUF_TX_2: ConstStaticCell<[u8; 1024]> = ConstStaticCell::new([0u8; 1024]);
    static BUF_RX: ConstStaticCell<[u8; 1024]> = ConstStaticCell::new([0u8; 1024]);
    static TX_STO: StaticCell<Mutex<ThreadModeRawMutex, RttTxInner>> = StaticCell::new();

    let tx_impl = RttTx {
        inner: TX_STO.init(Mutex::new(RttTxInner {
            channel: channels.up.1,
            buf1: BUF_TX_1.take(),
            buf2: BUF_TX_2.take(),
        })),
    };
    let rx_impl = RttRx {
        channel: channels.down.0,
        buf: BUF_RX.take(),
        used: 0,
    };

    let dispatcher = app::MyApp::new(context, spawner.into());
    let vkk = dispatcher.min_key_len();
    let mut server: app::AppServer = Server::new(
        tx_impl,
        rx_impl,
        pbufs.rx_buf.as_mut_slice(),
        dispatcher,
        vkk,
    );
    let sender = server.sender();
    // We need to spawn the USB task so that USB messages are handled by
    // embassy-usb
    spawner.must_spawn(logging_task(sender));

    // Begin running!
    loop {
        // If the host disconnects, we'll return an error here.
        // If this happens, just wait until the host reconnects
        let _ = server.run().await;
    }
}

/// This task is a "sign of life" logger
#[embassy_executor::task]
pub async fn logging_task(sender: Sender<AppTx>) {
    let mut ticker = Ticker::every(Duration::from_secs(3));
    let start = Instant::now();
    let mut ctr = 0u32;
    loop {
        ticker.next().await;
        let _ = sender.publish::<HelloTopic>(VarSeq::Seq4(ctr), &HelloWorld {
            uptime: start.elapsed().as_ticks(),
        }).await;
        ctr += 1;
    }
}

/// Helper to get unique ID from flash
mod unique_id {
    // use embassy_rp::{
    //     flash::{Blocking, Flash},
    //     peripherals::FLASH,
    // };

    /// This function retrieves the unique ID of the external flash memory.
    ///
    /// The RP2040 has no internal unique ID register, but most flash chips do,
    /// So we use that instead.
    //pub fn get_unique_id(flash: &mut FLASH) -> Option<u64> {
    pub fn get_unique_id() -> Option<u64> {
        // let mut flash: Flash<'_, FLASH, Blocking, { 16 * 1024 * 1024 }> =
        //     Flash::new_blocking(flash);
        // let mut id = [0u8; core::mem::size_of::<u64>()];
        // flash.blocking_unique_id(&mut id).ok()?;
        Some(u64::from_be_bytes([1,2,3,4,5,6,7,8]))
    }
}
