#![cfg_attr(not(feature = "use-std"), no_std)]

use postcard_rpc::{endpoints, topics, TopicDirection};
use postcard_schema::Schema;
use serde::{Deserialize, Serialize};
use heapless::Vec;

#[derive(Debug, Serialize, Deserialize, Schema)]
pub struct RadarPoint {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub snr_db: f64,
    pub noise_db: f64,
    pub v_doppler_mps: f64,
}

pub type RadarPointSeq = Vec<RadarPoint, 256>;

#[derive(Debug, Serialize, Deserialize, Schema)]
pub struct RadarResponse {
    pub v_r: [f64; 3],
    pub sigma: [f64; 3],
    pub time_us: u64,
}

#[derive(Debug, Serialize, Deserialize, Schema)]
pub enum LedState {
    Off,
    On,
}

#[derive(Debug, Serialize, Deserialize, Schema)]
pub struct HelloWorld {
    pub uptime: u64,
}

// ---
// Endpoints spoken by our device
endpoints! {
    list = ENDPOINT_LIST;
    | EndpointTy                | RequestTy       | ResponseTy    | Path                          |
    | ----------                | ---------       | ----------    | ----                          |
    | GetUniqueIdEndpoint       | ()              | u64           | "poststation/unique_id/get"   |
    | RadarEndpoint             | RadarPointSeq   | RadarResponse | "template/radar/process"      |
    | SetLedEndpoint            | LedState        | ()            | "template/led/set"            |
    | GetLedEndpoint            | ()              | LedState      | "template/led/get"            |
}

// incoming topics handled by our device
topics! {
    list = TOPICS_IN_LIST;
    direction = TopicDirection::ToServer;
    | TopicTy                   | MessageTy     | Path              |
    | -------                   | ---------     | ----              |
}

// outgoing topics handled by our device
topics! {
    list = TOPICS_OUT_LIST;
    direction = TopicDirection::ToClient;
    | TopicTy                   | MessageTy     | Path              | Cfg   |
    | -------                   | ---------     | ----              | ---   |
    | HelloTopic                | HelloWorld    | "hello"           |       |
}
