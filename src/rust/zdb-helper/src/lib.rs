use pg_bridge::guard::register_panic_handler;

mod rest;

#[no_mangle]
pub extern "C" fn rust_init() {
    register_panic_handler();
}
