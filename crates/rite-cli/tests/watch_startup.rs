use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

use hmac::{Hmac, Mac};
use sha2::Sha256;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn temp_dir() -> PathBuf {
    let root = std::env::temp_dir();
    loop {
        let path = root.join(format!(
            "rite-cod482-cli-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        if fs::create_dir(&path).is_ok() {
            return path;
        }
    }
}

#[test]
fn serving_a_new_watch_commits_its_initial_arm_before_accepting_traffic() {
    let directory = temp_dir();
    let state_path = directory.join("watch-state.json");
    let config_path = directory.join("rite.toml");
    let config = format!(
        "[watch_state]\npath = \"{}\"\n\n[[watches]]\nname = \"heartbeat-monitor\"\nwatch_source = [\"github\"]\nmatch = {{ event_type = \"push\" }}\nwindow_secs = 60\naction = {{ type = \"http_post\", url = \"http://127.0.0.1:9/watch\" }}\n",
        state_path.display()
    );
    fs::write(&config_path, config).expect("write synthetic config");
    let probe = TcpListener::bind("127.0.0.1:0").expect("reserve local test port");
    let address = probe.local_addr().expect("test listener address");
    drop(probe);

    let child = Command::new(env!("CARGO_BIN_EXE_rite"))
        .args([
            "--config",
            config_path.to_str().expect("config path is UTF-8"),
            "--listen",
            &address.to_string(),
        ])
        .env("RITE_GITHUB_WEBHOOK_SECRET", "synthetic-test-secret")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start isolated Rite process");
    let mut child = ChildGuard(child);

    let mut state_bytes = None;
    for _ in 0..100_000 {
        if let Some(status) = child.0.try_wait().expect("inspect Rite process") {
            panic!("Rite exited before creating watch state: {status}");
        }
        if let Ok(bytes) = fs::read(&state_path) {
            state_bytes = Some(bytes);
            break;
        }
        std::thread::yield_now();
    }

    let state_bytes = state_bytes.expect("serving must commit the new watch's initial arm");
    let state: serde_json::Value =
        serde_json::from_slice(&state_bytes).expect("watch state is valid JSON");
    assert_eq!(state["version"], 1);
    assert!(
        state["watches"]["heartbeat-monitor"].is_object(),
        "the initial never-seen arm is durably present"
    );
    drop(child);
    fs::remove_dir_all(directory).expect("remove synthetic fixture");
}

#[test]
fn invalid_durable_record_exits_before_the_cli_binds_and_preserves_saved_bytes() {
    let directory = temp_dir();
    let state_path = directory.join("watch-state.json");
    let config_path = directory.join("rite.toml");
    let config = format!(
        "[watch_state]\npath = \"{}\"\n\n[[watches]]\nname = \"prebind-watch\"\nwatch_source = [\"github\"]\nmatch = {{ event_type = \"push\" }}\nwindow_secs = 60\naction = {{ type = \"http_post\", url = \"http://127.0.0.1:9/watch\" }}\n",
        state_path.display()
    );
    fs::write(&config_path, config).expect("write synthetic config");
    let corrupt = br#"{"version":99,"high_water_utc_ms":1,"written_at_utc_ms":1,"watches":{}}"#;
    fs::write(&state_path, corrupt).expect("write unsupported version fixture");
    let probe = TcpListener::bind("127.0.0.1:0").expect("reserve local test port");
    let address = probe.local_addr().expect("test listener address");
    drop(probe);
    let child = Command::new(env!("CARGO_BIN_EXE_rite"))
        .args([
            "--config",
            config_path.to_str().expect("config path is UTF-8"),
            "--listen",
            &address.to_string(),
        ])
        .env("RITE_GITHUB_WEBHOOK_SECRET", "synthetic-test-secret")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start isolated Rite process");
    let mut child = ChildGuard(child);
    let mut exit_status = None;
    for _ in 0..100_000 {
        if let Some(status) = child.0.try_wait().expect("inspect pre-bind failure") {
            exit_status = Some(status);
            break;
        }
        std::thread::yield_now();
    }
    let status = exit_status.expect("invalid state fails before any serving loop");
    assert!(!status.success(), "invalid state exits nonzero");
    assert_eq!(fs::read(&state_path).unwrap(), corrupt);
    assert!(TcpStream::connect_timeout(&address, std::time::Duration::from_millis(100)).is_err());
    drop(child);
    fs::remove_dir_all(directory).expect("remove synthetic fixture");
}

fn exchange(address: SocketAddr, request: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&address, std::time::Duration::from_millis(50))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(1)))?;
    stream.write_all(request.as_bytes())?;
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    Ok(response)
}

fn github_signature(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("synthetic key");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

#[test]
fn required_runtime_persistence_failure_exits_cli_and_closes_serving_listener() {
    let directory = temp_dir();
    let moved_directory = directory.with_extension("offline");
    let state_path = directory.join("watch-state.json");
    let config_path = directory.join("rite.toml");
    let config = format!(
        "[watch_state]\npath = \"{}\"\n\n[[watches]]\nname = \"heartbeat-monitor\"\nwatch_source = [\"github\"]\nmatch = {{ event_type = \"push\" }}\nwindow_secs = 60\naction = {{ type = \"http_post\", url = \"http://127.0.0.1:9/watch\" }}\n",
        state_path.display()
    );
    fs::write(&config_path, config).expect("write synthetic config");
    let probe = TcpListener::bind("127.0.0.1:0").expect("reserve local test port");
    let address = probe.local_addr().expect("test listener address");
    drop(probe);
    let secret = "synthetic-test-secret";
    let child = Command::new(env!("CARGO_BIN_EXE_rite"))
        .args([
            "--config",
            config_path.to_str().expect("config path is UTF-8"),
            "--listen",
            &address.to_string(),
        ])
        .env("RITE_GITHUB_WEBHOOK_SECRET", secret)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start isolated Rite process");
    let mut child = ChildGuard(child);
    let mut ready = false;
    for _ in 0..100_000 {
        if let Some(status) = child.0.try_wait().expect("inspect Rite process") {
            panic!("Rite exited before readiness: {status}");
        }
        if exchange(
            address,
            "GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .is_ok_and(|response| response.contains("200 OK"))
        {
            ready = true;
            break;
        }
        std::thread::yield_now();
    }
    assert!(ready, "Rite listener becomes ready before fault injection");

    // The process still holds its advisory lock descriptor, but its configured
    // state parent disappears so the next required admission commit fails.
    fs::rename(&directory, &moved_directory).expect("remove the configured state directory");
    let body = br#"{"ref":"refs/heads/main","repository":{"name":"synthetic"}}"#;
    let signature = github_signature(secret, body);
    let request = format!(
        "POST /event/github HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nX-GitHub-Event: push\r\nX-Hub-Signature-256: {signature}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        std::str::from_utf8(body).expect("synthetic JSON is UTF-8")
    );
    let _ = exchange(address, &request);

    let mut exit_status = None;
    for _ in 0..100_000 {
        if let Some(status) = child.0.try_wait().expect("inspect supervised exit") {
            exit_status = Some(status);
            break;
        }
        std::thread::yield_now();
    }
    let exit_status =
        exit_status.expect("required write failure terminates the actual CLI process");
    assert!(
        !exit_status.success(),
        "required write failure exits nonzero"
    );
    assert!(
        TcpStream::connect_timeout(&address, std::time::Duration::from_millis(100)).is_err(),
        "fatal watch failure stops accepting requests"
    );
    drop(child);
    fs::remove_dir_all(moved_directory).expect("remove synthetic state fixture");
}

#[test]
fn generated_read_command_never_initializes_or_locks_watch_state() {
    let directory = temp_dir();
    let state_path = directory.join("watch-state.json");
    let config_path = directory.join("rite.toml");
    let config = format!(
        "[watch_state]\npath = \"{}\"\n\n[[watches]]\nname = \"read-only-watch\"\nwatch_source = [\"github\"]\nmatch = {{ event_type = \"push\" }}\nwindow_secs = 60\naction = {{ type = \"http_post\", url = \"http://127.0.0.1:9/watch\" }}\n",
        state_path.display()
    );
    fs::write(&config_path, config).expect("write synthetic config");
    let result = Command::new(env!("CARGO_BIN_EXE_rite"))
        .args([
            "--config",
            config_path.to_str().expect("config path is UTF-8"),
            "list-sources",
        ])
        .output()
        .expect("run generated read operation");
    assert!(result.status.success());
    assert!(!state_path.exists());
    assert!(!state_path.with_extension("json.lock").exists());
    fs::remove_dir_all(directory).expect("remove synthetic fixture");
}
