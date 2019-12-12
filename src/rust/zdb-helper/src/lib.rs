mod rest;

#[no_mangle]
pub extern "C" fn rust_init() {
    elog_wrapper::register_panic_handler();
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
