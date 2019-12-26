#[macro_use]
extern crate pg_guard_attr;

use pg_guard::register_panic_handler;

mod indexam;
mod playground;
mod rest;

#[no_mangle]
pub extern "C" fn rust_init() {
    register_panic_handler();
}
