use anyhow::Result;
use clap::{Parser, Subcommand};
use fluxon_kv::config::ClientConfig;
use fluxon_kv::{
    ConfigArg, Framework, load_client_config, run_client as init_client, run_master as init_master,
};
use limit_thirdparty::tokio;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "kvcli_example")]
#[command(
    about = "Interactive KV Cache CLI - supports client and master modes with get, put, delete operations"
)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run as client node (interactive CLI)
    Client {
        /// Configuration file path
        #[arg(short = 'f', long = "config")]
        config: Option<PathBuf>,
    },
    /// Run as master node
    Master {
        /// Configuration file path
        #[arg(short = 'f', long = "config")]
        config: Option<PathBuf>,
    },
}

struct InteractiveClient {
    framework: Arc<Framework>,
    shutdown: Arc<AtomicBool>,
}

impl InteractiveClient {
    fn new(framework: Arc<Framework>) -> Self {
        Self {
            framework,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn run_interactive_loop(&self) -> Result<()> {
        println!("🚀 KV Cache Interactive CLI (Client Mode)");
        println!("Available commands:");
        println!("  get <key>           - Get value by key");
        println!("  put <key> <value>   - Put key-value pair");
        println!("  delete <key>        - Delete key");
        println!("  help                - Show this help message");
        println!("  quit/exit           - Exit the CLI");
        println!();

        // 等待集群连接建立
        println!("⏳ Waiting for cluster connection...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        println!("✅ Connected to cluster");
        println!();

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            print!("kvcli> ");
            io::stdout().flush().unwrap();

            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(_) => {
                    let input = input.trim();
                    if input.is_empty() {
                        continue;
                    }

                    let parts: Vec<&str> = input.split_whitespace().collect();
                    if parts.is_empty() {
                        continue;
                    }

                    match parts[0].to_lowercase().as_str() {
                        "get" => {
                            if parts.len() != 2 {
                                println!("❌ Usage: get <key>");
                                continue;
                            }
                            self.handle_get(parts[1]).await;
                        }
                        "put" => {
                            if parts.len() < 3 {
                                println!("❌ Usage: put <key> <value>");
                                continue;
                            }
                            let value = parts[2..].join(" ");
                            self.handle_put(parts[1], &value).await;
                        }
                        "delete" | "del" => {
                            if parts.len() != 2 {
                                println!("❌ Usage: delete <key>");
                                continue;
                            }
                            self.handle_delete(parts[1]).await;
                        }
                        "help" | "h" => {
                            self.show_help();
                        }
                        "quit" | "exit" | "q" => {
                            println!("👋 Goodbye!");
                            self.shutdown.store(true, Ordering::Relaxed);
                            break;
                        }
                        _ => {
                            println!(
                                "❌ Unknown command: {}. Type 'help' for available commands.",
                                parts[0]
                            );
                        }
                    }
                }
                Err(error) => {
                    error!("Failed to read input: {}", error);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_get(&self, key: &str) {
        print!("🔍 Getting key '{}'... ", key);
        io::stdout().flush().unwrap();

        match self
            .framework
            .client_kv_api_view()
            .client_kv_api()
            .get(key)
            .await
        {
            Ok(Some((mem_holder, _))) => {
                let data = mem_holder.bytes();
                match std::str::from_utf8(data) {
                    Ok(value_str) => {
                        println!("✅ Found: '{}'", value_str);
                    }
                    Err(_) => {
                        println!("✅ Found binary data ({} bytes): {:?}", data.len(), data);
                    }
                }
            }
            Ok(None) => {
                println!("❌ Key '{}' not found", key);
            }
            Err(e) => {
                println!("❌ Error getting key '{}': {}", key, e);
            }
        }
    }

    async fn handle_put(&self, key: &str, value: &str) {
        print!("💾 Putting key '{} = {}'... ", key, value);
        io::stdout().flush().unwrap();

        match self
            .framework
            .client_kv_api_view()
            .client_kv_api()
            .inner()
            .put(
                key,
                value.as_bytes(),
                fluxon_kv::client_kv_api::PutOptionalArgs::default(),
            )
            .await
        {
            Ok(()) => {
                println!("✅ Successfully stored");
            }
            Err(e) => {
                println!("❌ Error putting key '{}': {}", key, e);
            }
        }
    }

    async fn handle_delete(&self, key: &str) {
        print!("🗑️ Deleting key '{}'... ", key);
        io::stdout().flush().unwrap();

        match self
            .framework
            .client_kv_api_view()
            .client_kv_api()
            .inner()
            .delete(key)
            .await
        {
            Ok(()) => {
                println!("✅ Successfully deleted");
            }
            Err(e) => {
                println!("❌ Error deleting key '{}': {}", key, e);
            }
        }
    }

    fn show_help(&self) {
        println!("📖 KV Cache CLI Help:");
        println!("  get <key>           - Retrieve value for the specified key");
        println!("  put <key> <value>   - Store key-value pair (value can contain spaces)");
        println!("  delete <key>        - Remove the specified key and its value");
        println!("  help                - Show this help message");
        println!("  quit/exit           - Exit the CLI application");
        println!();
        println!("Examples:");
        println!("  get user:123");
        println!("  put user:123 John Doe");
        println!("  delete user:123");
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down interactive client...");
        self.framework
            .shutdown()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to shutdown framework: {:?}", e))?;
        info!("Interactive client shutdown completed");
        Ok(())
    }
}

async fn run_client(config_path: Option<PathBuf>) -> Result<()> {
    info!("Starting KV Cache CLI in CLIENT mode");

    let config_arg = match config_path {
        Some(path) => ConfigArg::File(path),
        None => ConfigArg::None,
    };

    // Validate config early for clearer errors.
    let _cfg: ClientConfig = load_client_config(config_arg.clone()).await?;

    let (framework, cfg) = init_client(config_arg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize client framework: {:#}", e))?;
    info!("Client config: {:?}", cfg);

    let client = InteractiveClient::new(framework.clone());

    // 设置信号处理，优雅关闭
    let shutdown_clone = client.shutdown.clone();
    let framework_clone = client.framework.clone();
    let spawn_view = framework_clone.cluster_manager_view().clone();
    spawn_view.spawn("kvcli_shutdown_wait", async move {
        framework_clone.wait_shutdown_signal().await;
        info!("Received shutdown signal");
        shutdown_clone.store(true, Ordering::Relaxed);
    });

    // 运行交互式循环
    let result = client.run_interactive_loop().await;

    // 关闭客户端
    client.shutdown().await?;

    result
}

async fn run_master(config_path: Option<PathBuf>) -> Result<()> {
    info!("Starting KV Cache CLI in MASTER mode");

    let config_arg = match config_path {
        Some(path) => ConfigArg::File(path),
        None => ConfigArg::None,
    };

    let (framework, _cfg) = init_master(config_arg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize master framework: {:#}", e))?;

    info!("Master framework initialized successfully");
    println!("KV Cache Master Node is running...");
    println!("Press Ctrl+C to shutdown");

    framework.wait_shutdown_signal().await;

    info!("Shutting down master...");
    framework
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to shutdown framework: {:?}", e))?;
    info!("Master shutdown completed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    match args.command {
        Commands::Client { config } => run_client(config).await,
        Commands::Master { config } => run_master(config).await,
    }
}
