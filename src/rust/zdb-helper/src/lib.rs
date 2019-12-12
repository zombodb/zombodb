mod memcxt;
mod rest;
mod stringinfo;

#[no_mangle]
pub extern "C" fn rust_init() {
    println!("RUST_INIT()");
    pg_bridge::register_panic_handler();
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
