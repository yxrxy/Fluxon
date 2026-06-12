use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    // Build.rs output (generated Rust + DAG HTML) depends on the init-dag compiler and
    // the HTML renderer from fluxon_util. Declare them explicitly to keep artifacts in sync.
    println!("cargo:rerun-if-changed=../fluxon_util/src/init_dag_compiler.rs");
    println!("cargo:rerun-if-changed=../fluxon_util/src/dag_viz_html.rs");

    // We intentionally re-run this build script on any change under these directories,
    // because init-dag declarations are embedded in Rust source files via explicit markers.
    //
    // This avoids implicit guessing: only files that contain a `fluxon-init-dag` marker
    // are compiled, but adding/removing a marker must still trigger a rerun.
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=examples");
    println!("cargo:rerun-if-changed=tests");

    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set by cargo");
    let manifest_dir = PathBuf::from(manifest_dir);

    let workspace_root = manifest_dir
        .parent()
        .ok_or("fluxon_kv build.rs expects to run inside the fluxon_rs workspace")?
        .to_path_buf();
    emit_fluxon_native_link_args_for_kv_test(&workspace_root)?;
    let target_dir = get_target_dir()?;
    let dagviz_dir = target_dir.join("dagviz");
    std::fs::create_dir_all(&dagviz_dir)
        .map_err(|e| format!("create dir {} failed: {}", dagviz_dir.display(), e))?;

    let decls = collect_init_dag_decls(&manifest_dir)?;
    if decls.is_empty() {
        return Err("no fluxon-init-dag marker found under src/examples/tests".into());
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let out_dir = out_dir.join("fluxon_init_dag");
    std::fs::create_dir_all(&out_dir)
        .map_err(|e| format!("create dir {} failed: {}", out_dir.display(), e))?;

    for decl in decls {
        println!("cargo:rerun-if-changed={}", decl.rs_path.display());
        println!("cargo:rerun-if-changed={}", decl.yaml_path.display());

        let yaml = std::fs::read_to_string(&decl.yaml_path)
            .map_err(|e| format!("read {} failed: {}", decl.yaml_path.display(), e))?;

        let rust_cfg = fluxon_util::init_dag_compiler::RustGenConfig {
            init_fn_name: "init_framework".to_string(),
            // The generated file nests most code inside `mod framework_init_generated { ... }`.
            // Using `super::` makes it work for both crate-root frameworks and module-local
            // frameworks (examples/tests), as long as the include! is placed next to the
            // `define_framework!` expansion.
            framework_type_path: "super::Framework".to_string(),
            framework_args_type_path: "super::FrameworkArgs".to_string(),
            result_type_path: "anyhow::Result<()>".to_string(),
        };

        let compiled = fluxon_util::init_dag_compiler::compile_from_yaml_str(&yaml, &rust_cfg)
            .map_err(|e| {
                format!(
                    "compile init dag yaml failed: {} (yaml={})",
                    e,
                    decl.yaml_path.display()
                )
            })?;
        let html = compiled.html;
        let rust = compiled.rust;

        // Write per-Rust-source HTML under target-owned build output so read-only
        // workspace mounts still support generated DAG artifacts.
        let html_out = dagviz_dir.join(
            decl.rs_path
                .strip_prefix(&manifest_dir)
                .unwrap_or(&decl.rs_path)
                .with_extension("html"),
        );
        if let Some(parent) = html_out.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create dir {} failed: {}", parent.display(), e))?;
        }
        std::fs::write(&html_out, &html)
            .map_err(|e| format!("write {} failed: {}", html_out.display(), e))?;

        // Also publish the main fluxon_kv init DAG for packaging/tools.
        if decl.rs_path == manifest_dir.join("src").join("lib.rs") {
            let init_html = dagviz_dir.join("fluxon_kv_init.html");
            std::fs::write(&init_html, &html)
                .map_err(|e| format!("write {} failed: {}", init_html.display(), e))?;
        }

        // Write generated Rust into OUT_DIR. The declaring Rust file must include this by
        // referencing its own stem name (e.g. `cluster_example.rs` -> `<OUT_DIR>/fluxon_init_dag/cluster_example.rs`).
        let stem = decl
            .rs_path
            .file_stem()
            .ok_or_else(|| format!("rs file has no stem: {}", decl.rs_path.display()))?
            .to_string_lossy();
        let rs_out = out_dir.join(format!("{}.rs", stem));
        std::fs::write(&rs_out, rust)
            .map_err(|e| format!("write {} failed: {}", rs_out.display(), e))?;
    }

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

fn emit_fluxon_native_link_args_for_kv_test(
    workspace_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let closed_sdk_manifest_path = workspace_root
        .join("target")
        .join("native_runtime")
        .join("lib")
        .join("cmake")
        .join("FluxonNative")
        .join("FluxonNativeLinkArgs.txt");
    println!(
        "cargo:rerun-if-changed={}",
        closed_sdk_manifest_path.display()
    );

    if !closed_sdk_manifest_path.exists() {
        return Ok(());
    }

    let manifest = std::fs::read_to_string(&closed_sdk_manifest_path)
        .map_err(|e| format!("read {} failed: {}", closed_sdk_manifest_path.display(), e))?;

    // The native manifest is authoritative for absolute library paths and group ordering.
    // Forward it directly to the final kv_test binary link so packed RDMA closures do not
    // depend on Cargo's dependency-level native link propagation details.
    for line in manifest
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if line.starts_with("-Wl,-rpath,") {
            continue;
        }
        println!("cargo:rustc-link-arg-bin=kv_test={line}");
    }

    // The forwarded native manifest is appended very late in the final `kv_test` link command.
    // When it contains late static C++ runtime archives (for example `libstdc++.a`), linkers
    // driven via `clang` can no longer satisfy those archives from earlier `-lc/-lpthread/...`
    // arguments under left-to-right resolution. Re-emit the minimal libc/system tail here so a
    // native runtime rebuild can be relinked without spurious `atexit` / pthread resolution
    // failures.
    for tail in ["-lc", "-lpthread", "-ldl", "-lm", "-lrt"] {
        println!("cargo:rustc-link-arg-bin=kv_test={tail}");
    }

    Ok(())
}

#[derive(Debug)]
struct InitDagDecl {
    rs_path: PathBuf,
    yaml_path: PathBuf,
}

fn collect_init_dag_decls(
    manifest_dir: &Path,
) -> Result<Vec<InitDagDecl>, Box<dyn std::error::Error>> {
    let mut decls = Vec::new();
    for d in ["src", "examples", "tests"] {
        let dir = manifest_dir.join(d);
        if !dir.exists() {
            continue;
        }
        collect_init_dag_decls_under_dir(&dir, &mut decls)?;
    }
    Ok(decls)
}

fn collect_init_dag_decls_under_dir(
    dir: &Path,
    out: &mut Vec<InitDagDecl>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in
        std::fs::read_dir(dir).map_err(|e| format!("read_dir {} failed: {}", dir.display(), e))?
    {
        let entry = entry.map_err(|e| format!("read_dir entry failed: {}", e))?;
        let p = entry.path();
        if p.is_dir() {
            collect_init_dag_decls_under_dir(&p, out)?;
            continue;
        }
        if p.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        if let Some(yaml) = parse_init_dag_marker(&p)? {
            out.push(InitDagDecl {
                rs_path: p,
                yaml_path: yaml,
            });
        }
    }
    Ok(())
}

fn parse_init_dag_marker(rs_path: &Path) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    let src = std::fs::read_to_string(rs_path)
        .map_err(|e| format!("read {} failed: {}", rs_path.display(), e))?;
    let mut found: Option<PathBuf> = None;
    for (idx, line) in src.lines().enumerate() {
        let line = line.trim();
        if !line.contains("fluxon-init-dag:") {
            continue;
        }

        if found.is_some() {
            return Err(format!(
                "multiple fluxon-init-dag markers found in {} (second at line {})",
                rs_path.display(),
                idx + 1
            )
            .into());
        }

        let (_, rest) = line
            .split_once("fluxon-init-dag:")
            .ok_or_else(|| format!("invalid marker line: {}", line))?;
        let rest = rest.trim();
        let mut yaml_rel: Option<&str> = None;
        for part in rest.split_whitespace() {
            if let Some(v) = part.strip_prefix("yaml=") {
                yaml_rel = Some(v);
            } else {
                return Err(format!(
                    "unknown fluxon-init-dag marker field '{}' in {}",
                    part,
                    rs_path.display()
                )
                .into());
            }
        }
        let yaml_rel = yaml_rel.ok_or_else(|| {
            format!(
                "fluxon-init-dag marker missing yaml=... in {}",
                rs_path.display()
            )
        })?;
        let yaml_rel_path = Path::new(yaml_rel);
        if yaml_rel_path.is_absolute() {
            return Err(format!(
                "fluxon-init-dag yaml path must be relative: {} (rs={})",
                yaml_rel,
                rs_path.display()
            )
            .into());
        }
        let yaml_path = rs_path
            .parent()
            .ok_or_else(|| format!("rs file has no parent dir: {}", rs_path.display()))?
            .join(yaml_rel_path);
        if !yaml_path.exists() {
            return Err(format!(
                "fluxon-init-dag yaml not found: {} (declared in {})",
                yaml_path.display(),
                rs_path.display()
            )
            .into());
        }
        found = Some(yaml_path);
    }
    Ok(found)
}
