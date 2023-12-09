use std::path::Path;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    datapet_tests_source::generate_tests(Path::new(&out_dir));
}
