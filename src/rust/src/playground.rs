use pg_bridge::*;
use std::convert::TryInto;

#[pg_extern]
fn rust_add_two_numbers(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let a: i32 = pg_getarg::<i32>(fcinfo, 0).try_into().unwrap();
    let b: i32 = pg_getarg::<i32>(fcinfo, 1).try_into().unwrap();

    (a as i64 + b as i64) as pg_sys::Datum
}

#[pg_extern]
fn rust_test_text(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let s: &str = pg_getarg::<&str>(fcinfo, 0).try_into().unwrap();
    let _bar = unsafe { pg_sys::palloc(42) };

    PgMemoryContexts::Of(fcinfo as void_ptr).switch_to(|| {
        let _foo = unsafe { pg_sys::palloc(42) };
        info!("HERE: {}", s);
    });

    let result = Spi::connect(|client| {
        let table = client.select(
            "SELECT a, b, id FROM foo WHERE c = $1 ORDER BY random()",
            None,
            Some(vec![(
                PgOid::CommonBuiltIn(CommonBuiltInOids::TEXTOID),
                PgDatum::<pg_sys::Datum>::from("one"),
            )]),
        );

        let mut a: &str = "uninitialized";
        for row in table.into_iter() {
            a = row.get(0).unwrap().try_into().unwrap();
        }

        Ok(PgDatum::<&str>::from(a))
    });

    info!("unwrapping result");
    //    let result:  = result.try_into().unwrap();
    let result: &str = result.try_into().unwrap();
    info!("SPI RESULT={:?}", result);

    check_for_interrupts!();

    let result = Spi::connect(|mut client| {
        let rc = client.update("UPDATE foo set a = a", None, None);
        info!("rc={:?}", rc);

        let _tid = pg_sys::ItemPointerData {
            ip_blkid: pg_sys::BlockIdData {
                bi_hi: 12,
                bi_lo: 88,
            },
            ip_posid: 42,
        };
        Ok(PgDatum::from(42i32))
    });

    let result: i32 = result.try_into().unwrap();
    //    let tid: i32 = result.into_inner();
    info!("ItemPointerData from SPI result={:?}", result);
    info!("{}", s);

    let rc = rust_str_to_text_p("some return value");
    rc as pg_sys::Datum
}

#[pg_extern]
fn rust_get_tid(_fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    new_item_pointer(12, 42).into_pg() as pg_sys::Datum
}
