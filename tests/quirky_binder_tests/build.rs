use std::path::Path;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    quirky_binder_tests_source::generate_tests(Path::new(&out_dir));
}
