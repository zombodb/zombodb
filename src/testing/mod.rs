use lazy_static::*;
use pgx_tests::add_shutdown_hook;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct State {
    es_started: bool,
}
lazy_static! {
    static ref INIT_LOCK: Arc<Mutex<State>> = Arc::new(Mutex::new(State { es_started: false }));
}

pub(crate) fn initialize_tests(options: Vec<&str>) {
    for option in options {
        match option {
            "(es = true)" => {
                let mut state = INIT_LOCK.lock().expect("initialization lock poisoned");
                if !state.es_started {
                    eprintln!("starting Elasticsearch...");
                    start_es();
                    state.es_started = true;
                }
            }
            _ => {}
        }
    }
}

fn start_es() {
    let mut data_dir = std::env::current_dir().unwrap();
    data_dir.push("elasticsearch");
    data_dir.push("data");

    if data_dir.exists() {
        eprintln!("removing previous Elasticsearch data directory");
        std::fs::remove_dir_all(data_dir).ok();
    }

    let mut bin_dir = std::env::current_dir().unwrap();
    bin_dir.push("elasticsearch");
    bin_dir.push("bin");

    let mut es = Command::new("elasticsearch");
    es.arg("-Ehttp.port=19200")
        .arg("-Ecluster.name=zombodb_test_framework")
        .arg("-Ediscovery.type=single-node")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env(
            "PATH",
            format!("{}:{}", bin_dir.display(), std::env::var("PATH").unwrap()),
        )
        .current_dir(bin_dir);

    monitor_es(es)
}

fn monitor_es(mut es: Command) {
    let (sender, receiver) = std::sync::mpsc::channel();
    let mut child = es.spawn().expect("failed to start Elasticsearch");
    std::thread::spawn(move || {
        let pid = child.id();
        let reader = BufReader::new(
            child
                .stdout
                .take()
                .expect("failed to take Elasticsearch stdout"),
        );
        for line in reader.lines() {
            let line = line.expect("failed to read Elasticsearch stdout line");

            if line.contains("Cluster health status changed") && line.contains("to [GREEN]") {
                // Elasticsearch has started
                sender.send(pid).expect("failed to send start notification");
            }

            eprintln!("{}", line);
        }

        child
            .try_wait()
            .expect("failed waiting for Elasticsearch to shutdown");
    });

    let pid = receiver
        .recv_timeout(Duration::from_secs(90))
        .expect("failed to receive Elasticsearch startup notification after 90s");
    add_shutdown_hook(move || unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    });
}
