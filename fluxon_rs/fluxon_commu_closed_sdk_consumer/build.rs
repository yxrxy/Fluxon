use std::{env, fs, path::Path, path::PathBuf};

const CLOSED_SDK_ROOT_ENV: &str = "FLUXON_COMMU_CLOSED_SDK_ROOT";
const DEFAULT_CLOSED_SDK_ROOT_REPO_RELATIVE: &str = "fluxon_release/closed_sdk";
const EXPECTED_BOUNDARY_MODE: &str = "closed-sdk-consumer";

fn load_manifest_boundary_mode(manifest_path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let manifest_text = fs::read_to_string(manifest_path)?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_text)?;
    let boundary_mode = manifest
        .get("feature_contract")
        .and_then(serde_json::Value::as_object)
        .and_then(|feature_contract| feature_contract.get("boundary_mode"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            format!(
                "closed SDK manifest is missing feature_contract.boundary_mode: {}",
                manifest_path.display()
            )
        })?;
    Ok(boundary_mode.to_string())
}

fn default_closed_sdk_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| {
            format!(
                "failed to derive repo root from CARGO_MANIFEST_DIR={}",
                manifest_dir.display()
            )
        })?;
    Ok(repo_root.join(DEFAULT_CLOSED_SDK_ROOT_REPO_RELATIVE))
}

fn resolve_closed_sdk_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    match env::var(CLOSED_SDK_ROOT_ENV) {
        Ok(raw_sdk_root) if !raw_sdk_root.trim().is_empty() => {
            let sdk_root = PathBuf::from(raw_sdk_root.trim());
            if !sdk_root.is_absolute() {
                return Err(format!(
                    "{CLOSED_SDK_ROOT_ENV} must be an absolute path when set: {sdk_root:?}"
                )
                .into());
            }
            Ok(sdk_root)
        }
        _ => default_closed_sdk_root(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed={CLOSED_SDK_ROOT_ENV}");

    let sdk_root = resolve_closed_sdk_root()?;
    if !sdk_root.is_dir() {
        return Err(format!(
            "closed SDK root is missing or not a directory: {}. \
             Set {CLOSED_SDK_ROOT_ENV} or generate the default repo SDK at {}",
            sdk_root.display(),
            default_closed_sdk_root()?.display()
        )
        .into());
    }

    let manifest_path = sdk_root.join("manifest.json");
    if !manifest_path.is_file() {
        return Err(format!(
            "closed SDK manifest is missing: {}",
            manifest_path.display()
        )
        .into());
    }
    println!("cargo:rerun-if-changed={}", manifest_path.display());

    let manifest_boundary_mode = load_manifest_boundary_mode(&manifest_path)?;
    if manifest_boundary_mode != EXPECTED_BOUNDARY_MODE {
        return Err(format!(
            "closed SDK boundary mode mismatch: expected {}, actual {} ({})",
            EXPECTED_BOUNDARY_MODE,
            manifest_boundary_mode,
            manifest_path.display()
        )
        .into());
    }

    let lib_dir = sdk_root.join("lib");
    if !lib_dir.is_dir() {
        return Err(format!("closed SDK lib directory is missing: {}", lib_dir.display()).into());
    }
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=fluxon_commu_core");
    Ok(())
}
