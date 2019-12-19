use pg_bridge::*;

#[pg_extern]
pub unsafe extern "C" fn rust_add_two_numbers(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let a = pg_getarg_i32(&fcinfo, 0).unwrap();
    let b = pg_getarg_i32(&fcinfo, 1).unwrap();

    (a as i64 + b as i64) as pg_sys::Datum
}

#[pg_extern]
pub unsafe extern "C" fn rust_test_text(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let s = pg_getarg_text_pp_as_str(&fcinfo, 0).unwrap();

    info!("{}", s);

    rust_str_to_text_p("some return value") as pg_sys::Datum
}
