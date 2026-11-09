//! Synthetic parser refusals; no caller-provided compiler identity grants runtime support.
// req: STO-020, PRJ-004, REC-001
use super::supported_identity;
const VERBOSE: &str = "rustc 1.99.1 (58cf8f9c3 2026-10-13)\nbinary: rustc\ncommit-hash: 58cf8f9c35b355199727627d07ae34a4e2846d67\ncommit-date: 2026-10-13\nhost: x86_64-unknown-linux-gnu\nrelease: 1.99.1\nLLVM version: 23.1.1\n";
fn supported(text: &str) -> bool {
    supported_identity(
        text,
        "x86_64-unknown-linux-gnu",
        "x86_64",
        "64",
        "fxsr,sse,sse2",
    )
}
#[test]
fn native_resource_identity_refuses_unknown_compiler_and_malformed_metadata() {
    assert!(supported(VERBOSE));
    assert!(!supported(
        &VERBOSE.replace("release: 1.99.1", "release: 1.99.2")
    ));
    assert!(!supported(
        &VERBOSE.replace("release: 1.99.1", "release: 1.99.1-nightly")
    ));
    assert!(!supported(&VERBOSE.replace(
        "58cf8f9c35b355199727627d07ae34a4e2846d67",
        "0000000000000000000000000000000000000000"
    )));
    assert!(!supported(&VERBOSE.replace("release: 1.99.1\n", "")));
    assert!(!supported(&format!("{VERBOSE}release: 1.99.1\n")));
    assert!(!supported(&format!(
        "{VERBOSE}commit-hash: 58cf8f9c35b355199727627d07ae34a4e2846d67\n"
    )));
    assert!(!supported(&"x".repeat(4097)));
}
#[test]
fn native_resource_identity_refuses_unsupported_target_group_profile() {
    assert!(!supported_identity(
        VERBOSE,
        "x86_64-unknown-linux-musl",
        "x86_64",
        "64",
        "sse2"
    ));
    assert!(!supported_identity(
        VERBOSE,
        "x86_64-unknown-linux-gnu",
        "aarch64",
        "64",
        "neon"
    ));
    assert!(!supported_identity(
        VERBOSE,
        "x86_64-unknown-linux-gnu",
        "x86_64",
        "32",
        "sse2"
    ));
    assert!(!supported_identity(
        VERBOSE,
        "x86_64-unknown-linux-gnu",
        "x86_64",
        "64",
        "sse"
    ));
    assert!(!supported_identity(
        VERBOSE,
        "x86_64-unknown-linux-gnu",
        "x86_64",
        "64",
        "nosse2"
    ));
}
