//! Integration test for `docker-entrypoint.sh` TOML materialization.
//!
//! The entrypoint generates `rite.toml` from `RITE_IRIS_BASE_URL` and
//! `RITE_IRIS_API_TOKEN` at container start. Generated files must be valid
//! TOML parseable by Rite's own production parser (`rite_server::load_config`)
//! and must round-trip the exact environment values, including values
//! containing quotes, backslashes, and control characters.
//!
//! Regression coverage for COD-470: the entrypoint previously escaped the
//! token as a TOML literal string (`'...'`) using SQL-style quote doubling,
//! which cannot represent a single quote at all — a token like `o'brien`
//! produced an unparseable config and a crashing container. A first fix
//! attempt also escaped only SOH and used STX as a newline sentinel, which
//! collided with real STX bytes and let other control bytes through raw.

use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static CALL: AtomicUsize = AtomicUsize::new(0);

fn repo_root() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR = crates/rite-server
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_path_buf()
}

/// Tests run in parallel threads within one process, so the PID alone does
/// not isolate their temp directories.
fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let n = CALL.fetch_add(1, Ordering::SeqCst);
    let dir =
        std::env::temp_dir().join(format!("rite-entrypoint-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn materialize(base_url: &str, api_token: Option<&str>) -> String {
    let dir = scratch_dir("test");
    let conf = dir.join("rite.toml");

    let mut cmd = Command::new("/bin/sh");
    cmd.arg(repo_root().join("docker-entrypoint.sh"))
        .env("RITE_CONFIG", &conf)
        .env("RITE_IRIS_BASE_URL", base_url)
        .arg("true");
    if let Some(token) = api_token {
        cmd.env("RITE_IRIS_API_TOKEN", token);
    } else {
        cmd.env_remove("RITE_IRIS_API_TOKEN");
    }
    let output = cmd.output().unwrap();
    assert!(
        output.status.success(),
        "entrypoint failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let generated = std::fs::read_to_string(&conf).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    generated
}

/// Assert a generated config parses through the production parser and
/// round-trips the exact env values. Failure messages deliberately omit the
/// generated TOML so the token value is never echoed into test output
/// (COD-433 secret-safe precedent).
fn assert_round_trip(base_url: &str, api_token: Option<&str>) {
    let generated = materialize(base_url, api_token);
    let config = rite_server::load_config(&generated)
        .unwrap_or_else(|e| panic!("generated TOML failed to parse: {e}"));
    let iris = config
        .sources
        .iris
        .expect("iris source present when base_url is set");
    assert!(iris.enabled, "iris must be enabled when base_url is set");
    assert_eq!(
        iris.base_url,
        base_url,
        "base_url round-trip failed (length {} vs {})",
        iris.base_url.len(),
        base_url.len()
    );
    match api_token {
        Some(expected) => assert_eq!(
            iris.api_token.as_deref(),
            Some(expected),
            "api_token round-trip failed (length {} vs {})",
            iris.api_token.as_deref().map_or(0, str::len),
            expected.len()
        ),
        None => assert!(
            iris.api_token.is_none(),
            "api_token must be absent when unset"
        ),
    }
}

#[test]
fn plain_values_round_trip() {
    assert_round_trip("https://iris.invalid:8080", Some("secret"));
}

#[test]
fn token_absent_when_unset() {
    assert_round_trip("https://iris.invalid:8080", None);
}

#[test]
fn single_quote_token_round_trips() {
    // COD-470 regression: `'o''brien'` is not valid TOML escaping; the
    // original literal-string scheme could not represent this token at all.
    assert_round_trip("https://iris.example", Some("o'brien"));
}

#[test]
fn double_quote_token_round_trips() {
    assert_round_trip("https://iris.example", Some("to\"ken"));
}

#[test]
fn backslash_token_round_trips() {
    assert_round_trip("https://iris.example", Some("back\\slash"));
}

#[test]
fn mixed_delimiters_token_round_trips() {
    assert_round_trip("https://x\"a\\b", Some("q'q\"b\\s"));
}

#[test]
fn multiline_token_round_trips() {
    // Raw newlines terminate a TOML basic string; the entrypoint must emit
    // the \n escape instead. Environment variables may contain newlines.
    assert_round_trip("https://iris.example", Some("two\nlines"));
}

#[test]
fn crlf_token_round_trips() {
    assert_round_trip("https://iris.example", Some("cr\r\nlf"));
}

#[test]
fn tab_token_round_trips() {
    // Tab is the one control byte that IS legal raw in a TOML basic string.
    assert_round_trip("https://iris.example", Some("ta\tb"));
}

#[test]
fn stx_token_round_trips() {
    // Second-generation regression: an earlier fix used STX (0x02) as a
    // newline sentinel, so a real STX byte was emitted as `\n`.
    assert_round_trip("https://iris.example", Some("s\u{02}x"));
}

#[test]
fn all_other_control_bytes_and_del_round_trip() {
    // Every control byte except tab, plus DEL, must be \uXXXX-escaped and
    // round-trip exactly. Newline is covered separately above.
    let bytes: Vec<char> = (0x01u8..=0x1f)
        .chain(0x7f..=0x7f)
        .filter(|b| *b != b'\t' && *b != b'\n')
        .map(|b| b as char)
        .collect();
    let token: String = std::iter::once('x')
        .chain(bytes.iter().copied())
        .chain(std::iter::once('y'))
        .collect();
    assert_round_trip("https://iris.example", Some(&token));
}

#[test]
fn control_chars_in_base_url_round_trip() {
    // base_url goes through the same escaper; it must survive control
    // bytes, quotes, and backslashes too.
    assert_round_trip("https://x\ny\"z\\w\u{01}", None);
}

#[test]
fn blank_base_url_emits_no_iris_section() {
    let generated = materialize("", None);
    let config = rite_server::load_config(&generated).unwrap();
    assert!(
        config.sources.iris.is_none(),
        "empty base_url must not emit [sources.iris]"
    );
}

#[test]
fn generated_token_is_never_logged() {
    // The entrypoint writes the token to the config file only; stdout and
    // stderr must not echo it (COD-433 secret-safe precedent).
    let dir = scratch_dir("log");
    let conf = dir.join("rite.toml");
    let output = Command::new("/bin/sh")
        .arg(repo_root().join("docker-entrypoint.sh"))
        .env("RITE_CONFIG", &conf)
        .env("RITE_IRIS_BASE_URL", "https://iris.example")
        .env("RITE_IRIS_API_TOKEN", "super-secret-token-value")
        .arg("true")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "entrypoint failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stdout.contains("super-secret-token-value")
            && !stderr.contains("super-secret-token-value"),
        "token leaked to stdout/stderr"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
