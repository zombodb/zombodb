use colored::Colorize;
use rayon::prelude::*;
use std::collections::HashSet;
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::str::FromStr;

macro_rules! exit_with_error {
    () => ({ exit_with_error!("explicit panic") });
    ($msg:expr) => ({ exit_with_error!("{}", $msg) });
    ($msg:expr,) => ({ exit_with_error!($msg) });
    ($fmt:expr, $($arg:tt)+) => ({
        use colored::Colorize;
        eprint!("{} ", "[error]".bold().red());
        eprintln!($fmt, $($arg)+);
        do_exit();
        unreachable!();
    });
}

macro_rules! handle_result {
    ($expr:expr, $message:expr) => {{
        match $expr {
            Ok(result) => result,
            Err(e) => exit_with_error!("{}:\n{}", $message, e),
        }
    }};
    ($expr:expr, $fmt:expr, $($arg:tt)+) => ({
        handle_result!($expr, format!($fmt, $($arg)+))
    });

}

static PGVERS: &[u16; 5] = &[11, 12, 13, 14, 15];

fn do_exit() {
    // best effort to kill the docker process
    if let Ok(output) = Command::new("docker").arg("ps").output() {
        let mut container_ids = Vec::new();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if line.contains("zombodb-build") {
                if let Some(container_id) = line.split_whitespace().next() {
                    container_ids.push(container_id.to_string())
                }
            }
        }

        if !container_ids.is_empty() {
            eprintln!("{} {:?}", "KILLING".bold().red(), container_ids);
            Command::new("docker")
                .arg("kill")
                .args(container_ids)
                .output()
                .ok();
        }
    }
    std::process::exit(1);
}

fn main() -> Result<(), std::io::Error> {
    let timer_start = std::time::Instant::now();
    ctrlc::set_handler(do_exit).expect("unable to set ^C handler");
    let max_cpus = std::env::var("CPUS").unwrap_or(num_cpus::get().to_string());
    rayon::ThreadPoolBuilder::new()
        .num_threads(max_cpus.parse().expect("`CPUS` envvar is invalid"))
        .build_global()
        .ok();

    println!(
        "{} `num_threads` to {}",
        "     Setting".bold().green(),
        max_cpus
    );
    let pgx_version = determine_pgx_version()?;
    println!(
        "{} pgx version to {}",
        "     Setting".bold().green(),
        pgx_version
    );

    let targetdir = PathBuf::from_str("./target/zdb-build/").unwrap();
    let artifactdir = PathBuf::from_str("./target/zdb-build/artifacts/").unwrap();
    let builddir = PathBuf::from_str("./target/zdb-build/build/").unwrap();
    let repodir = PathBuf::from_str("./target/zdb-build/zombodb/").unwrap();

    remove_dir(&targetdir);
    remove_dir(&artifactdir);
    std::fs::create_dir_all(&artifactdir).expect("failed to create artifactdir");
    std::fs::create_dir_all(&builddir).expect("failed to create builddir");
    std::fs::create_dir_all(&repodir).expect("failed to create repodir");

    let mut args = std::env::args();
    args.next(); // consume executable name
    let branch = args.next().unwrap_or_else(|| {
        exit_with_error!("usage:  cargo run <branch> [<docker-image-name> <pg major version>]")
    });
    let user_image = args.next();
    let user_pgver: Option<u16> = match args.next() {
        Some(pgver) => Some(pgver.parse().expect("pgver is not a valid number")),
        None => None,
    };
    let dockerfiles = find_dockerfiles()?;

    handle_result!(git_clone(&branch, &repodir), "failed to clone ZomboDB repo");

    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
        println!(
            "elapsed time: {}",
            durationfmt::to_string(timer_start.elapsed())
        );
    });

    dockerfiles
        .par_iter()
        .filter(|(image, _)| user_image.is_none() || user_image.as_ref().unwrap() == image)
        .for_each(|(image, file)| {
            let dockerfile = handle_result!(
                parse_dockerfile(&file),
                "failed to parse: {}",
                file.display().to_string().bold().yellow()
            );
            let args = parse_dockerfile_arg_names(&dockerfile);

            if args.contains("PGVER") {
                // gotta build a separate image for each pg version
                PGVERS
                    .par_iter()
                    .filter(|pgver| {
                        user_pgver.is_none() || **pgver == *user_pgver.as_ref().unwrap()
                    })
                    .for_each(|pgver| {
                        let start = std::time::Instant::now();
                        let image = handle_result!(
                            docker_build(image, Some(*pgver)),
                            "{}-pg{}:  failed to run `docker build`",
                            image.bold().red(),
                            pgver.to_string().bold().red()
                        );
                        println!(
                            "{} {} in {}",
                            "       Built".bold().cyan(),
                            image,
                            durationfmt::to_string(start.elapsed())
                        );

                        let start = std::time::Instant::now();
                        handle_result!(
                            docker_run(
                                &image,
                                *pgver,
                                &repodir,
                                &builddir,
                                &artifactdir,
                                &pgx_version
                            ),
                            "Failed to compile {} for {}",
                            image,
                            pgver
                        );
                        println!(
                            "{} {} for pg{} in {}",
                            "    Packaged".bold().blue(),
                            image,
                            pgver,
                            durationfmt::to_string(start.elapsed())
                        );
                    });
            } else {
                // can build it just once
                let start = std::time::Instant::now();
                let image = handle_result!(
                    docker_build(image, None),
                    "{}:  failed to run `docker build`",
                    image.bold().red()
                );
                println!(
                    "{} {} in {}",
                    "       Built".bold().cyan(),
                    image,
                    durationfmt::to_string(start.elapsed())
                );

                PGVERS
                    .par_iter()
                    .filter(|pgver| {
                        user_pgver.is_none() || **pgver == *user_pgver.as_ref().unwrap()
                    })
                    .for_each(|pgver| {
                        let start = std::time::Instant::now();
                        handle_result!(
                            docker_run(
                                &image,
                                *pgver,
                                &repodir,
                                &builddir,
                                &artifactdir,
                                &pgx_version
                            ),
                            "Failed to compile {} for {}",
                            image,
                            pgver
                        );
                        println!(
                            "{} {} for pg{} in {}",
                            "    Packaged".bold().blue(),
                            image,
                            pgver,
                            durationfmt::to_string(start.elapsed())
                        );
                    });
            }
        });
    println!(
        "{} in {}",
        "    Finished".bold().green(),
        durationfmt::to_string(timer_start.elapsed())
    );

    Ok(())
}

fn remove_dir(dir: &PathBuf) {
    if dir.exists() {
        println!("{} `{}`", "    Removing".bold().green(), dir.display());
        std::fs::remove_dir_all(&dir).expect("failed to remove existing targetdir");
    }
}

fn docker_build(base_image: &str, pgver: Option<u16>) -> Result<String, std::io::Error> {
    let image_name = format!(
        "{}{}",
        base_image,
        pgver
            .map(|ver| format!("-pg{}", ver))
            .unwrap_or_else(String::new)
    );

    let mut command = Command::new("docker");
    command
        .arg("build")
        .arg("--build-arg")
        .arg(&format!(
            "USER={}",
            users::get_current_username()
                .expect("no username")
                .to_str()
                .unwrap()
        ))
        .arg("--build-arg")
        .arg(&format!("UID={}", users::get_current_uid()))
        .arg("--build-arg")
        .arg(&format!("GID={}", users::get_current_gid()));

    if pgver.is_some() {
        command
            .arg("--build-arg")
            .arg(&format!("PGVER={}", pgver.unwrap()));
    }

    command.arg("-t").arg(&image_name).arg(base_image);

    println!("{} {}", " Dockerizing".bold().green(), image_name);
    let command_str = format!("{:?}", command);
    let output = command.output()?;
    handle_command_output(image_name, command_str, &output)
}

fn docker_run(
    image: &str,
    pgver: u16,
    repodir: &PathBuf,
    builddir: &PathBuf,
    artifactdir: &PathBuf,
    pgx_version: &str,
) -> Result<String, std::io::Error> {
    let mut builddir = builddir.clone();
    builddir.push(&format!("{}-{}", image, pgver));
    handle_result!(
        std::fs::create_dir_all(&builddir),
        "failed to create directory: {}",
        builddir.display().to_string().bold().yellow()
    );

    println!(
        "{} repository for {}:pg{}",
        "     Copying".bold().green(),
        image,
        pgver
    );
    let contents: Vec<_> = repodir
        .read_dir()
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    fs_extra::copy_items(&contents, &builddir, &fs_extra::dir::CopyOptions::default())
        .expect("failed to copy repository directory");

    let mut command = Command::new("docker");
    command
        .arg("run")
        .arg("-e")
        .arg(&format!("pgver={}", pgver))
        .arg("-e")
        .arg(&format!("image={}", image))
        .arg("-e")
        .arg(&format!("pgx_version={}", pgx_version))
        .arg("-w")
        .arg(&format!("/build"))
        .arg("--mount")
        .arg(&format!(
            "type=bind,source={},target=/build",
            builddir.canonicalize()?.display()
        ))
        .arg("--mount")
        .arg(&format!(
            "type=bind,source={},target=/artifacts",
            artifactdir.canonicalize()?.display()
        ))
        .arg("--rm")
        .arg("--user")
        .arg(&format!(
            "{}:{}",
            users::get_current_uid(),
            users::get_current_gid()
        ))
        .arg(image)
        .arg("bash")
        .arg("-c")
        .arg("./docker-build-system/package.sh ${pgver} ${image} ${pgx_version}");

    println!(
        "{} {} for pg{}",
        "   Packaging".bold().green(),
        image,
        pgver
    );

    let command_str = format!("{:?}", command);
    let output = command.output()?;
    handle_command_output(image.into(), command_str, &output)
}

fn git_clone(branch: &str, repodir: &PathBuf) -> Result<(), std::io::Error> {
    let mut command = Command::new("git");

    command
        .arg("clone")
        .arg("--depth")
        .arg("1")
        .arg("--single-branch")
        .arg("--branch")
        .arg(branch)
        .arg("https://github.com/zombodb/zombodb.git")
        .arg(repodir.canonicalize().unwrap());

    println!("{} `{}` branch", "     Cloning".bold().green(), branch);
    let command_str = format!("{:?}", command);
    let output = command.output()?;
    handle_command_output((), command_str, &output)?;

    // copy our "package.sh" script into the repodir so it'll use
    // what's related to us and not from whatever branch we cloned
    let mut package_sh_target = repodir.canonicalize().unwrap();
    package_sh_target.push("docker-build-system");
    package_sh_target.push("package.sh");
    std::fs::copy("./package.sh", package_sh_target)?;
    Ok(())
}

fn handle_command_output<T>(
    return_value: T,
    command_str: String,
    output: &Output,
) -> Result<T, std::io::Error> {
    if !output.status.success() {
        let mut log = command_str.yellow().to_string();
        log.push('\n');
        log.push('\n');
        log.push_str(&String::from_utf8_lossy(&output.stdout));
        log.push_str(&String::from_utf8_lossy(&output.stderr));
        Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, log))
    } else {
        Ok(return_value.into())
    }
}

fn find_dockerfiles() -> Result<Vec<(String, PathBuf)>, std::io::Error> {
    let mut files = Vec::new();
    for f in std::fs::read_dir(".")? {
        let f = f?;
        if f.file_type()?.is_dir()
            && f.file_name()
                .to_string_lossy()
                .starts_with("zombodb-build-")
        {
            let mut path = f.path();
            path.push("Dockerfile");
            if path.exists() {
                files.push((f.file_name().into_string().unwrap(), path));
            }
        }
    }

    files.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(files)
}

fn parse_dockerfile_arg_names(dockerfile: &Vec<(String, Option<String>)>) -> HashSet<String> {
    let mut args = HashSet::new();

    for (k, v) in dockerfile {
        if k == "ARG" {
            let parts: Vec<&str> = v
                .as_ref()
                .expect("no value for ARG")
                .splitn(2, '=')
                .collect();
            let mut parts = parts.into_iter();
            args.insert(parts.next().expect("no ARG value").into());
        }
    }

    args
}

fn parse_dockerfile(filename: &PathBuf) -> Result<Vec<(String, Option<String>)>, std::io::Error> {
    let file = std::fs::File::open(filename)?;
    let buffer = std::io::BufReader::new(file).lines();
    let mut map = Vec::new();
    for line in buffer {
        let line = line?.trim().to_string();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        let key = (*parts.get(0).expect("no key")).to_string();
        let value = (parts.get(1)).map(|v| v.to_string());
        map.push((key, value));
    }
    Ok(map)
}

fn determine_pgx_version() -> Result<String, std::io::Error> {
    let mut command = Command::new("cargo");

    command.current_dir("../").arg("tree").arg("-i").arg("pgx");

    let output = command.output()?;
    let output = String::from_utf8(output.stdout).expect("invalid UTF8 output from cargo tree");
    let first_line = output
        .lines()
        .next()
        .expect("no first line from cargo tree");
    let mut parts = first_line.split(" v");
    parts.next();
    Ok(parts
        .next()
        .expect("no version number found in cargo tree output")
        .into())
}
