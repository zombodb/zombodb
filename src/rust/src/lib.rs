mod indexam;
mod playground;
mod rest;

#[no_mangle]
pub extern "C" fn rust_init() {
    pg_bridge::initialize();
}
