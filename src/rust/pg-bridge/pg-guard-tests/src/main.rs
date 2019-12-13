use pg_guard::*;
use std::os::raw::c_char;

#[pg_guard]
extern "C" {
    pub fn ex_fun();
    fn ex_fun2(a: i32, z: *const c_char);
}

#[pg_guard]
pub fn foo(a: i32, b: Option<bool>) -> Result<String, Vec<bool>> {
    println!("HERE");
    Err(Vec::new())
}

fn main() {
    //    guarded_fn()
}
