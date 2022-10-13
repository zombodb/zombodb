fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/zql/parser.lalrpop");
    built::write_built_file().expect("Failed to acquire build-time information");
    lalrpop::Configuration::new()
        .generate_in_source_tree()
        .process()
        .expect("failed to generate parser");
}
