mod rest;

#[no_mangle]
pub extern "C" fn rust_init() {
    elog_wrapper::register_panic_handler();
}
