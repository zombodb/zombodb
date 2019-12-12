use pg_bridge::*;
use std::os::raw::c_char;

#[longjmp_guard]
extern "C" {
    fn ex_fun();
    fn ex_fun2(a: i32, z: *const c_char);
}

fn main() {
//    guarded_fn()
}
