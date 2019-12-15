extern crate bindgen;
extern crate clang;

use bindgen::callbacks::MacroParsingBehavior;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() -> Result<(), std::io::Error> {
    let pg_git_repo = "git://git.postgresql.org/git/postgresql.git";
    let pg_git_path = Path::new("pg_git/");

    git_clone_postgres(pg_git_path, pg_git_repo)
        .expect(&format!("Unable to git clone {}", pg_git_repo));

    for v in &vec![
        ("pg10", "REL_10_STABLE"),
        ("pg11", "REL_11_STABLE"),
        ("pg12", "REL_12_STABLE"),
    ] {
        let version = v.0;
        let branch = v.1;
        let mut output_rs = PathBuf::new();
        output_rs.push(format!("src/{}.rs", version));

        if output_rs.is_file() {
            eprintln!("{} already exists:  skipping", output_rs.to_str().unwrap());
            continue;
        }

        git_switch_branch(pg_git_path, branch)
            .expect(&format!("Unable to switch to branch {}", branch));

        clean_and_configure_and_make(pg_git_path).expect(&format!(
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

        bindings.write_to_file(output_rs).expect(&format!(
            "Unable to save bindings for {} to src/{}.rs",
            version, version
        ));
    }

    Ok(())
}

fn git_clone_postgres(path: &Path, repo_url: &str) -> Result<(), std::io::Error> {
    if path.exists() {
        let mut gitdir = path.clone().to_path_buf();
        gitdir.push(Path::new(".git/config"));

        if gitdir.exists() && gitdir.is_file() {
            // we already have git cloned
            // do a fetch instead
            eprintln!("git fetch --all");
            Command::new("git")
                .arg("fetch")
                .arg("--all")
                .current_dir(path)
                .output()?;

            return Ok(());
        }
    }

    eprintln!("git clone {} {}", repo_url, path.to_str().unwrap());
    Command::new("git")
        .arg("clone")
        .arg(repo_url)
        .arg(path)
        .output()?;

    Ok(())
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
