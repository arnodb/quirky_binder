fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/quirky_binder.capnp")
        .run()
        .expect("schema compiler command");
}
