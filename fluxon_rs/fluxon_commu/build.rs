use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");

    let target_dir = get_target_dir()?;
    emit_fluxon_native_link_args_for_tests(&target_dir)?;
    Ok(())
}

fn get_target_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(target_dir) = std::env::var("CARGO_TARGET_DIR") {
        let path = PathBuf::from(target_dir);
        return Ok(if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        });
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    for dir in out_dir.ancestors() {
        if dir
            .file_name()
            .map(|name| name == "target")
            .unwrap_or(false)
        {
            return Ok(dir.to_path_buf());
        }
    }

    Err("failed to locate target directory from OUT_DIR".into())
}

fn emit_fluxon_native_link_args_for_tests(
    target_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(manifest_path) = native_link_args_manifest_path(target_dir)? else {
        return Ok(());
    };
    println!("cargo:rerun-if-changed={}", manifest_path.display());

    let manifest = std::fs::read_to_string(&manifest_path)
        .map_err(|e| format!("read {} failed: {}", manifest_path.display(), e))?;

    for line in manifest
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if !line.starts_with("-Wl,-rpath,") {
            continue;
        }
        println!("cargo:rustc-link-arg={line}");
    }

    Ok(())
}

fn native_link_args_manifest_path(
    target_dir: &Path,
) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    let manifest_path = target_dir
        .join("native_runtime")
        .join("lib")
        .join("cmake")
        .join("FluxonNative")
        .join("FluxonNativeLinkArgs.txt");

    if !manifest_path.is_file() {
        println!(
            "cargo:warning=FluxonNative link args manifest is missing: {}. Continuing without extra rpath link args.",
            manifest_path.display()
        );
        return Ok(None);
    }
    Ok(Some(manifest_path))
}
