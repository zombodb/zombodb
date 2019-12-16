extern crate bindgen;
extern crate clang;

use bindgen::callbacks::MacroParsingBehavior;
use common::rewrite_extern_block;
use quote::quote;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use syn::export::{ToTokens, TokenStream2};
use syn::Item;

#[derive(Debug)]
struct IgnoredMacros(HashSet<String>);

impl IgnoredMacros {
    fn default() -> Self {
        // these cause duplicate definition problems on linux
        // see: https://github.com/rust-lang/rust-bindgen/issues/687
        IgnoredMacros(
            vec![
                "FP_INFINITE".into(),
                "FP_NAN".into(),
                "FP_NORMAL".into(),
                "FP_SUBNORMAL".into(),
                "FP_ZERO".into(),
                "IPPORT_RESERVED".into(),
            ]
            .into_iter()
            .collect(),
        )
    }
}

impl bindgen::callbacks::ParseCallbacks for IgnoredMacros {
    fn will_parse_macro(&self, name: &str) -> MacroParsingBehavior {
        if self.0.contains(name) {
            bindgen::callbacks::MacroParsingBehavior::Ignore
        } else {
            bindgen::callbacks::MacroParsingBehavior::Default
        }
    }
}

fn make_git_repo_path(out_dir: String) -> PathBuf {
    let mut pg_git_path = PathBuf::from(out_dir);
    // backup 3 directories
    pg_git_path.pop();
    pg_git_path.pop();
    pg_git_path.pop();

    // and a new dir named "pg_git_repo"
    pg_git_path.push("pg_git_repo");

    // return the path we built
    pg_git_path
}

fn main() -> Result<(), std::io::Error> {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let pg_git_path = make_git_repo_path(out_dir);
    let pg_git_repo_url = "git://git.postgresql.org/git/postgresql.git";

    eprintln!(
        "postgres checkout directory: {}",
        pg_git_path.as_os_str().to_str().unwrap()
    );
    let need_generate = git_clone_postgres(&pg_git_path, pg_git_repo_url)
        .expect(&format!("Unable to git clone {}", pg_git_repo_url));

    for v in &vec![
        ("pg10", "REL_10_STABLE"),
        ("pg11", "REL_11_STABLE"),
        ("pg12", "REL_12_STABLE"),
    ] {
        let version = v.0;
        let branch = v.1;
        let mut output_rs = PathBuf::new();
        output_rs.push(format!("src/{}.rs", version));

        if !need_generate
            && output_rs
                .metadata()
                .unwrap()
                .created()
                .unwrap()
                .lt(&pg_git_path.metadata().unwrap().modified().unwrap())
        {
            eprintln!("{} already exists:  skipping", output_rs.to_str().unwrap());
            continue;
        }

        git_switch_branch(&pg_git_path, branch)
            .expect(&format!("Unable to switch to branch {}", branch));

        clean_and_configure_and_make(&pg_git_path).expect(&format!(
            "Unable to make clean and configure postgres branch {}",
            branch
        ));

        let bindings = bindgen::Builder::default()
            .header(format!("include/{}.h", version))
            .clang_arg(&format!("-I{}/src/include", pg_git_path.to_str().unwrap()))
            .parse_callbacks(Box::new(IgnoredMacros::default()))
            .rustfmt_bindings(true)
            .derive_debug(false)
            .layout_tests(false)
            .generate()
            .expect(&format!("Unable to generate bindings for {}", version));

        let bindings = apply_pg_guard(bindings.to_string())?;
        std::fs::write(output_rs.clone(), bindings)
            .expect(&format!("Unable to save bindings for {}", version));

        rust_fmt(output_rs.as_path()).expect(&format!("Unable to run rustfmt for {}", version));
    }

    Ok(())
}

fn git_clone_postgres(path: &Path, repo_url: &str) -> Result<bool, std::io::Error> {
    if path.exists() {
        let mut gitdir = path.clone().to_path_buf();
        gitdir.push(Path::new(".git/config"));

        if gitdir.exists() && gitdir.is_file() {
            // we already have git cloned
            // do a fetch instead
            eprintln!("git fetch --all");
            let output = Command::new("git")
                .arg("fetch")
                .arg("--all")
                .current_dir(path)
                .output()?;

            // a status code of zero and more than 1 line on stdout means we fetched new stuff
            return Ok(output.status.code().unwrap() == 0
                && String::from_utf8(output.stdout).unwrap().lines().count() > 1);
        }
    }

    eprintln!("git clone {} {}", repo_url, path.to_str().unwrap());
    let output = Command::new("git")
        .arg("clone")
        .arg(repo_url)
        .arg(path)
        .output()?;

    // if the output status is zero, that means we cloned the repo
    Ok(output.status.code().unwrap() == 0)
}

fn git_switch_branch(path: &Path, branch_name: &str) -> Result<(), std::io::Error> {
    eprintln!("Switching to branch {}", branch_name);
    Command::new("git")
        .arg("checkout")
        .arg(branch_name)
        .current_dir(path)
        .output()?;

    eprintln!("git pull");
    Command::new("git").arg("pull").current_dir(path).output()?;

    Ok(())
}

fn clean_and_configure_and_make(path: &Path) -> Result<(), std::io::Error> {
    eprintln!("make distclean");
    Command::new("make")
        .arg("distclean")
        .current_dir(path)
        .output()?;

    eprintln!("./configure");
    Command::new("sh")
        .arg("-c")
        .arg("./configure")
        .current_dir(path)
        .output()?;

    eprintln!("make -j {}", num_cpus::get());
    Command::new("make")
        .arg("-j")
        .arg(&format!("{}", num_cpus::get()))
        .current_dir(path)
        .output()?;

    Ok(())
}

fn apply_pg_guard(input: String) -> Result<String, std::io::Error> {
    let mut stream = TokenStream2::new();
    let file = syn::parse_file(input.as_str()).unwrap();

    for item in file.items.into_iter() {
        match item {
            Item::ForeignMod(block) => {
                let block = rewrite_extern_block(block);
                stream.extend(quote! { #block });
            }
            _ => {
                stream.extend(quote! { #item });
            }
        }
    }

    Ok(format!("{}", stream.into_token_stream()))
}

fn rust_fmt(path: &Path) -> Result<(), std::io::Error> {
    eprintln!("rustfmt {}", path.to_str().unwrap());
    Command::new("rustfmt")
        .arg(path)
        .current_dir(".")
        .output()?;

    Ok(())
}
