use pg_guard::*;
use std::os::raw::c_char;

#[pg_guard]
extern "C" {
    pub fn ex_fun();
    fn ex_fun2(a: i32, z: *const c_char);
}

fn main() {
    //    guarded_fn()
}
