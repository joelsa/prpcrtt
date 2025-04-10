use postcard_rpc::{header::VarHeader, server::Sender};
use template_icd::{LedState, RadarPointSeq, RadarResponse, RadarEndpoint};

use crate::app::{AppTx, Context, TaskContext};

/// This is an example of a BLOCKING handler.
pub fn unique_id(context: &mut Context, _header: VarHeader, _arg: ()) -> u64 {
    context.unique_id
}

/// Also a BLOCKING handler
pub fn set_led(context: &mut Context, _header: VarHeader, arg: LedState) {
    match arg {
        LedState::Off => context.led.set_low(),
        LedState::On => context.led.set_high(),
    }
}

pub fn get_led(context: &mut Context, _header: VarHeader, _arg: ()) -> LedState {
    match context.led.is_set_low() {
        true => LedState::Off,
        false => LedState::On,
    }
}

pub fn radar_handler(_context: &mut Context, _header: VarHeader, points: RadarPointSeq) -> RadarResponse {
    let response = if points.len() >= 2 {
        // Take first point's coordinates as v_r
        let v_r = [
            points[0].x,
            points[0].y,
            points[0].z,
        ];
        
        // Take second point's coordinates as sigma
        let sigma = [
            points[1].x,
            points[1].y,
            points[1].z,
        ];

        RadarResponse { v_r, sigma, time_us: 0 }
    } else {
        // Default response if not enough points
        RadarResponse {
            v_r: [0.0, 0.0, 0.0],
            sigma: [0.0, 0.0, 0.0],
             time_us: 0,
        }
    };

    response
}
