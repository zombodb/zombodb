#[macro_export]
macro_rules! check_for_interrupts {
    () => {
        unsafe {
            extern "C" {
                static InterruptPending: bool;
            }
            if InterruptPending {
                #[pg_guard]
                extern "C" {
                    fn ProcessInterrupts();
                }
                ProcessInterrupts();
            }
        }
    }
}

