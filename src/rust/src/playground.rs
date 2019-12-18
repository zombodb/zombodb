use pg_bridge::pg_sys::pg11 as pg_sys;
use pg_bridge::*;

#[pg_extern]
pub unsafe extern "C" fn rust_add_two_numbers(fcinfo: pg_sys::FunctionCallInfo) -> i64 {
    let a = pg_getarg_i32(&*fcinfo, 0).unwrap();
    let b = pg_getarg_i32(&*fcinfo, 1).unwrap();

    a as i64 + b as i64
}
