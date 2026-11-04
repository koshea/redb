#[path = "src/tree_store/page_store/resource_build_identity.rs"]
mod resource_build_identity;

fn resource_layout_supported() -> bool {
    if std::env::var_os("CARGO_CFG_MIRI").is_some() {
        return false;
    }
    let Some(rustc) = std::env::var_os("RUSTC") else {
        return false;
    };
    let Ok(output) = std::process::Command::new(rustc).arg("-vV").output() else {
        return false;
    };
    if !output.status.success() || output.stdout.len() > 4096 {
        return false;
    }
    let Ok(verbose) = core::str::from_utf8(&output.stdout) else {
        return false;
    };
    let target = std::env::var("TARGET").unwrap_or_default();
    let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let width = std::env::var("CARGO_CFG_TARGET_POINTER_WIDTH").unwrap_or_default();
    let features = std::env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    resource_build_identity::supported_identity(verbose, &target, &arch, &width, &features)
}

fn main() {
    println!("cargo:rustc-check-cfg=cfg(redb_resource_layout_supported)");
    // No opt-in environment switch: only Cargo's actual compiler identity and
    // fixed target profile can emit the support cfg. Unknown builds still work.
    if resource_layout_supported() {
        println!("cargo:rustc-cfg=redb_resource_layout_supported");
    }
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    println!("cargo:rustc-check-cfg=cfg(redb_no_std)");

    // Building without the standard library is only offered under the redb 5 API preview. Cargo
    // features cannot express "experimental-api-5 and not std", so it is computed here and used as
    // `cfg(redb_no_std)` throughout the crate. Turning "std" off without "experimental-api-5" keeps
    // the std build, so that 4.x dependents who set `default-features = false` are unaffected.
    if std::env::var_os("CARGO_FEATURE_EXPERIMENTAL_API_5").is_some()
        && std::env::var_os("CARGO_FEATURE_STD").is_none()
    {
        println!("cargo:rustc-cfg=redb_no_std");
    }

    if std::env::var("CARGO_CFG_FUZZING").is_ok()
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
    {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
