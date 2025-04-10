use std::time::Duration;

use cobs::{decode_vec, encode_vec};
use impls::{ProbeRttRx, ProbeRttTx, TokSpawn};
use postcard_rpc::{header::VarSeqKind, host_client::HostClient, standard_icd::WireError};
use probe_rs::{
    config::TargetSelector,
    probe::list::Lister,
    rtt::{Rtt, ScanRegion},
    Permissions, Session,
};
use template_icd::{HelloTopic, LedState, RadarEndpoint, RadarPoint, RadarPointSeq, SetLedEndpoint};
use tokio::{sync::mpsc, time::{sleep, timeout}};

pub mod impls;

#[tokio::main]
async fn main() {
    let lister = Lister::new();
    let probes = lister.list_all();
    for p in probes.iter() {
        println!("{p:?}");
    }
    let probe = probes[0].open().unwrap();
    let mut session = probe
        .attach(TargetSelector::from("STM32H723ZGTx"), Permissions::default())
        .unwrap();
    let rtt = {
        let mut core = session.core(0).unwrap();

        eprintln!("Attaching to RTT...");

        let rtt = Rtt::attach_region(&mut core, &ScanRegion::Ram).unwrap();

        sleep(Duration::from_millis(50)).await;
        rtt
    };

    let (out_tx, out_rx) = mpsc::channel(64);
    let (inc_tx, inc_rx) = mpsc::channel(64);

    let app_rx = ProbeRttRx { inc: inc_rx };
    let app_tx = ProbeRttTx { out: out_tx };

    std::thread::spawn(move || worker(session, rtt, inc_tx, out_rx));

    let client = HostClient::<WireError>::new_with_wire(
        app_tx,
        app_rx,
        TokSpawn,
        VarSeqKind::Seq2,
        "error",
        64,
    );

    let mut sub = client.subscribe_multi::<HelloTopic>(64).await.unwrap();
    tokio::task::spawn(async move {
        while let Ok(_) = sub.recv().await {}
    });

    let mut pointcloud = RadarPointSeq::new();
    pointcloud.push(RadarPoint { x: 1.0, y: 2.0, z: 3.0, snr_db: 4.0, noise_db: 5.0, v_doppler_mps: 6.0 }).unwrap();
    pointcloud.push(RadarPoint { x: 1.0, y: 2.0, z: 3.0, snr_db: 4.0, noise_db: 5.0, v_doppler_mps: 6.0 }).unwrap();
    let res = timeout(Duration::from_secs(1), client.send_resp::<RadarEndpoint>(&pointcloud)).await;
    match res {
        Ok(r) => {
            let got = r.unwrap();
            println!("{got:?}");
        },
        Err(_) => {
            println!("Error :(");
        }
    }

    loop {
        let res = timeout(Duration::from_secs(1), client.send_resp::<SetLedEndpoint>(&LedState::On)).await;
        match res {
            Ok(r) => {
                let _got = r.unwrap();
            },
            Err(_) => {
                println!("Timeout :(");
            }
        }

        sleep(Duration::from_secs(1)).await;


        let res = timeout(Duration::from_secs(1), client.send_resp::<SetLedEndpoint>(&LedState::Off)).await;
        match res {
            Ok(r) => {
                let _got = r.unwrap();
            },
            Err(_) => {
                println!("Timeout :(");
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

fn worker(
    mut session: Session,
    mut rtt: Rtt,
    inc_tx: mpsc::Sender<Vec<u8>>,
    mut out_rx: mpsc::Receiver<Vec<u8>>,
) {
    let mut core = session.core(0).unwrap();
    let mut buf = [0u8; 1024];
    let mut inc_staging = vec![];
    let mut pending_out = None;

    loop {
        let mut progress = false;
        let up = rtt.up_channel(0).unwrap();
        let got = up.read(&mut core, &mut buf).unwrap();
        if got != 0 {
            // println!("RX: Got {got} (staging: {})", inc_staging.len());
            progress = true;
            let mut window = &buf[..got];
            while !window.is_empty() {
                if let Some(pos) = window.iter().position(|b| *b == 0) {
                    let (now, later) = window.split_at(pos + 1);
                    inc_staging.extend_from_slice(now);
                    if let Ok(frame) = decode_vec(&inc_staging) {
                        // println!("RX: Got Frame {}", frame.len());
                        inc_tx.blocking_send(frame).unwrap();
                    } else {
                        // println!("RX: DECODE FAIL");
                    }
                    inc_staging.clear();
                    window = later;
                } else {
                    inc_staging.extend_from_slice(window);
                    window = &[];
                }
            }
        }

        if pending_out.is_none() {
            match out_rx.try_recv() {
                Ok(msg) => {
                    let mut out = encode_vec(&msg);
                    out.push(0);
                    // println!("TX: Got Frame {}", out.len());
                    pending_out = Some(out);
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => panic!(),
            }
        }

        if let Some(mut tx) = pending_out.take() {
            let out = rtt.down_channel(0).unwrap();
            let ct = out.write(&mut core, &tx).unwrap();
            if ct == tx.len() {
                // wrote all
                progress = true;
                // println!("TX: Sent {}", ct);
            } else if ct != 0 {
                // wrote some
                progress = true;
                let later = tx.split_off(ct);
                pending_out = Some(later);
                // println!("TX: Sent {}", ct);
            } else {
                pending_out = Some(tx);
            }
        }

        if !progress {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

