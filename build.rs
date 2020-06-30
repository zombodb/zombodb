fn main() {
    lalrpop::Configuration::new()
        .generate_in_source_tree()
        .process()
        .expect("failed to generate parser");
}
