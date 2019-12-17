#[macro_export]
macro_rules! check_for_interrupts {
    () => {
        unsafe {
            if pg_sys::externs::InterruptPending {
                pg_sys::externs::ProcessInterrupts();
            }
        }
    };
}
