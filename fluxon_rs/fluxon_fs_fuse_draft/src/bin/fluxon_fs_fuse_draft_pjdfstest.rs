#[cfg(not(feature = "runtime_fuser"))]
fn main() {
    panic!(
        "enable the runtime_fuser feature to use fluxon_fs_fuse_draft_pjdfstest"
    );
}

#[cfg(feature = "runtime_fuser")]
mod runtime_main {
    use std::fs;
    use std::io;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use fluxon_fs_fuse_draft::{
        FluxonFuseAtimePolicy, FluxonFuseFileSystem, FluxonFuseMountConfig, FluxonFuseSemantics,
        FluxonInProcessFsExportMock, FluxonInProcessRpcKvApi, FluxonPjdfstestConfig,
        FluxonRpcKvExportBackend, FuserConfig, FuserMountOption, FuserSessionAcl, UserRpcClient,
        run_pjdfstest, spawn_fuser_mount,
    };
    use serde::Deserialize;

    struct MockBackendFixture {
        rpc_client: Arc<dyn UserRpcClient>,
        _export_mock: FluxonInProcessFsExportMock,
    }

    #[derive(Debug, Deserialize)]
    struct RunnerConfig {
        export_root_dir_abs: String,
        mountpoint_dir_abs: String,
        export_name: String,
        suite_root_dir_abs: String,
        test_targets: Vec<String>,
        mount_timeout_ms: u64,
        umount_timeout_ms: u64,
    }

    pub fn run() -> io::Result<()> {
        ensure_runner_is_root()?;
        let config_path = parse_config_path()?;
        let config: RunnerConfig = serde_json::from_str(fs::read_to_string(config_path)?.as_str())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        validate_config(&config)?;

        let fixture = build_mock_backend_fixture(
            config.export_name.as_str(),
            config.export_root_dir_abs.as_str(),
            config.mountpoint_dir_abs.as_str(),
        )?;
        let backend = Arc::new(
            FluxonRpcKvExportBackend::new(
                fixture.rpc_client.clone(),
                config.mountpoint_dir_abs.clone(),
                config.export_name.clone(),
                None,
            )
            .map_err(io_error_from_boxed)?,
        );
        let filesystem = Arc::new(
            FluxonFuseFileSystem::new(
                FluxonFuseMountConfig {
                    mountpoint_dir_abs: config.mountpoint_dir_abs.clone(),
                    export_name: config.export_name.clone(),
                    semantics: FluxonFuseSemantics {
                        read_only: false,
                        suid_enabled: true,
                        dev_enabled: true,
                        exec_enabled: true,
                        atime_policy: FluxonFuseAtimePolicy::RelAtime,
                        dir_atime_enabled: true,
                    },
                },
                backend,
            )
            .map_err(io_error_from_boxed)?,
        );
        validate_root_filesystem_state(filesystem.as_ref())?;

        let mut fuser_config = FuserConfig::default();
        fuser_config.mount_options = vec![
            FuserMountOption::FSName(config.export_name.clone()),
            FuserMountOption::Subtype("fluxonfs".to_string()),
            FuserMountOption::RW,
            FuserMountOption::DefaultPermissions,
        ];
        // Allow pjdfstest child identities to reach the filesystem while kernel
        // default_permissions continues to enforce inode mode checks.
        fuser_config.acl = FuserSessionAcl::All;
        fuser_config.n_threads = Some(1);
        fuser_config.clone_fd = false;

        let mut mount = spawn_fuser_mount(filesystem, fuser_config)?;
        mount.wait_until_mounted(Duration::from_millis(config.mount_timeout_ms))?;

        let run_result = run_pjdfstest(
            config.mountpoint_dir_abs.as_str(),
            &FluxonPjdfstestConfig {
                suite_root_dir_abs: config.suite_root_dir_abs.clone(),
                test_targets: config.test_targets.clone(),
            },
        );
        let umount_result =
            mount.umount_and_join(false, Duration::from_millis(config.umount_timeout_ms));
        run_result?;
        umount_result?;
        Ok(())
    }

    fn build_mock_backend_fixture(
        export_name: &str,
        export_root_dir_abs: &str,
        _mountpoint_dir_abs: &str,
    ) -> io::Result<MockBackendFixture> {
        let api = FluxonInProcessRpcKvApi::new();
        let export_mock = FluxonInProcessFsExportMock::new(
            api.clone(),
            export_name.to_string(),
            export_root_dir_abs.to_string(),
        )
        .map_err(io::Error::other)?;
        Ok(MockBackendFixture {
            rpc_client: api.rpc_client(),
            _export_mock: export_mock,
        })
    }

    fn validate_root_filesystem_state(filesystem: &FluxonFuseFileSystem) -> io::Result<()> {
        let root_stat = filesystem.getattr("/").map_err(io_error_from_boxed)?;
        if !root_stat.exists || !root_stat.is_dir {
            return Err(io::Error::other(format!(
                "root getattr must return an existing directory: exists={} is_dir={}",
                root_stat.exists, root_stat.is_dir
            )));
        }
        filesystem.readdir("/").map_err(io_error_from_boxed)?;
        Ok(())
    }

    fn ensure_runner_is_root() -> io::Result<()> {
        if unsafe { libc::geteuid() } == 0 {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "fluxon_fs_fuse_draft_pjdfstest must run as root",
        ))
    }

    fn parse_config_path() -> io::Result<String> {
        let mut args = std::env::args().skip(1);
        let flag = args.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: fluxon_fs_fuse_draft_pjdfstest --config <path>",
            )
        })?;
        if flag != "--config" && flag != "-c" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported flag: {flag}"),
            ));
        }
        let config_path = args.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing config path after --config")
        })?;
        if args.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected extra arguments",
            ));
        }
        Ok(config_path)
    }

    fn validate_config(config: &RunnerConfig) -> io::Result<()> {
        require_abs_dir(config.export_root_dir_abs.as_str(), "export_root_dir_abs")?;
        require_abs_dir(config.mountpoint_dir_abs.as_str(), "mountpoint_dir_abs")?;
        require_abs_dir(config.suite_root_dir_abs.as_str(), "suite_root_dir_abs")?;
        if config.export_name.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "export_name must be non-empty",
            ));
        }
        if config.test_targets.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "test_targets must be non-empty",
            ));
        }
        if config.mount_timeout_ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "mount_timeout_ms must be positive",
            ));
        }
        if config.umount_timeout_ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "umount_timeout_ms must be positive",
            ));
        }
        Ok(())
    }

    fn require_abs_dir(path: &str, field_name: &str) -> io::Result<()> {
        let path = Path::new(path);
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{field_name} must be absolute"),
            ));
        }
        if !path.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{field_name} must point to an existing directory"),
            ));
        }
        Ok(())
    }

    fn io_error_from_boxed(err: impl std::error::Error) -> io::Error {
        io::Error::other(err.to_string())
    }
}

#[cfg(feature = "runtime_fuser")]
fn main() {
    if let Err(err) = runtime_main::run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
