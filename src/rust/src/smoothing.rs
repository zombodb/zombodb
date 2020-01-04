/*
DROP TYPE IF EXISTS smoothing CASCADE;

CREATE TYPE smoothing;
CREATE OR REPLACE FUNCTION smoothing_in(cstring) RETURNS smoothing LANGUAGE C As 'zombodb.so', 'smoothing_in';
CREATE OR REPLACE FUNCTION smoothing_out(smoothing) RETURNS cstring LANGUAGE C As 'zombodb.so', 'smoothing_out';
CREATE TYPE smoothing (
    internallength = 12,
    input = smoothing_in,
    output = smoothing_out
);

CREATE OR REPLACE FUNCTION smoothing_phase(smoothing) RETURNS float4 LANGUAGE  C As 'zombodb.so', 'smoothing_phase';
CREATE OR REPLACE FUNCTION smoothing_series(smoothing) RETURNS float4 LANGUAGE  C As 'zombodb.so', 'smoothing_series';
CREATE OR REPLACE FUNCTION smoothing_smooth(smoothing) RETURNS float4 LANGUAGE  C As 'zombodb.so', 'smoothing_smooth';
CREATE OR REPLACE FUNCTION smoothing_agg(smoothing, float4) RETURNS smoothing LANGUAGE C AS 'zombodb.so', 'smoothing_agg';

CREATE AGGREGATE smoothing(float4) (
    SFUNC = smoothing_agg,
    STYPE = smoothing
);
*/
use pg_bridge::stringinfo::StringInfo;
use pg_bridge::*;
use pg_bridge_macros::*;
use std::convert::TryInto;

#[derive(Debug, DatumCompatible)]
#[repr(C)]
struct Smoothing {
    smooth: f32,
    phase: f32,
    series: f32,
}

#[pg_extern]
fn smoothing_smooth(smoothing: PgBox<Smoothing>) -> f32 {
    smoothing.smooth
}

#[pg_extern]
fn smoothing_phase(smoothing: PgBox<Smoothing>) -> f32 {
    smoothing.phase
}

#[pg_extern]
fn smoothing_series(smoothing: PgBox<Smoothing>) -> f32 {
    smoothing.series
}

#[pg_extern]
fn smoothing_in(input: &std::ffi::CStr) -> PgBox<Smoothing> {
    let mut smoothing = PgBox::<Smoothing>::alloc();

    let input = input.to_str().unwrap();
    let vals: Vec<_> = input.split(",").collect();

    smoothing.smooth = vals.get(0).unwrap().parse().unwrap();
    smoothing.phase = vals.get(1).unwrap().parse().unwrap();
    smoothing.series = vals.get(2).unwrap().parse().unwrap();

    smoothing
}

#[pg_extern]
fn smoothing_out(input: PgBox<Smoothing>) -> &'static std::ffi::CStr {
    let mut output = StringInfo::new();

    output.push_str(&format!(
        "{},{},{}",
        input.smooth, input.phase, input.series
    ));

    output.into()
}

#[pg_extern]
fn smoothing_agg(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let mut smoothing = pg_getarg_boxed(fcinfo, 0).unwrap_or_else(|| {
        // do initial setup
        let mut smoothing = PgBox::<Smoothing>::alloc();
        smoothing.phase = 1.0;
        smoothing.smooth = 1.0;
        smoothing.series = 0f32;

        // ... do heavy-weight initialization of 'smoothing' here ...

        // swap in the datum for the first argument to the function
        // so it'll be available on subsequent calls of this group
        unsafe { fcinfo.as_mut() }.unwrap().arg[0] = smoothing.as_datum();

        smoothing
    });

    let b: f32 = pg_getarg::<f32>(fcinfo, 1)
        .try_into()
        .expect("arg2 should not be null");

    smoothing.series += b;

    smoothing.into()
}
