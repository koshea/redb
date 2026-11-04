//! Pinned trusted-compiler identity parser shared by build.rs and native tests.
const RELEASE: &str = "1.98.1";
const COMMIT: &str = "48a229ceaefd4985c50990b14116b6d856af0985";
const TARGET: &str = "x86_64-unknown-linux-gnu";
const MAX_VERBOSE_BYTES: usize = 4096;

pub(super) fn supported_identity(
    verbose: &str,
    target: &str,
    arch: &str,
    pointer_width: &str,
    features: &str,
) -> bool {
    if verbose.len() > MAX_VERBOSE_BYTES
        || features.len() > MAX_VERBOSE_BYTES
        || !verbose.is_ascii()
        || target != TARGET
        || arch != "x86_64"
        || pointer_width != "64"
        || !features.split(',').any(|feature| feature == "sse2")
    {
        return false;
    }
    let mut lines = verbose.lines();
    if lines
        .next()
        .is_none_or(|line| !line.starts_with("rustc 1.98.1 ("))
    {
        return false;
    }
    let mut release_seen = false;
    let mut commit_seen = false;
    for line in lines {
        if let Some(release) = line.strip_prefix("release: ") {
            if release_seen || release != RELEASE {
                return false;
            }
            release_seen = true;
        } else if let Some(commit) = line.strip_prefix("commit-hash: ") {
            if commit_seen || commit != COMMIT {
                return false;
            }
            commit_seen = true;
        }
    }
    release_seen && commit_seen
}

#[cfg(test)]
#[path = "resource_build_identity_tests.rs"]
mod tests;
