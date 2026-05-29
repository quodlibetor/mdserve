use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

mod app;

use app::serve_markdown;

#[derive(Parser)]
#[command(name = "mdserve")]
#[command(about = "A simple HTTP server for markdown preview")]
#[command(version)]
struct Args {
    /// Path to markdown file or directory to serve
    path: PathBuf,

    /// Hostname (domain or IP address) to listen on
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    hostname: String,

    /// Port to serve on
    #[arg(short, long, default_value = "3000")]
    port: u16,

    /// Open the preview in the default browser
    #[arg(short, long)]
    open: bool,

    /// Inline mermaid and panzoom JS into the HTML so saved pages render
    /// diagrams without the server
    #[arg(long)]
    standalone: bool,

    /// Recursively scan subdirectories for markdown files (directory mode).
    /// Enabled by default; honors .gitignore and skips hidden directories.
    #[arg(long, overrides_with = "no_recursive")]
    recursive: bool,

    /// Scan only the immediate directory, not subdirectories (directory mode)
    #[arg(long = "no-recursive", overrides_with = "recursive")]
    no_recursive: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let recursive = !args.no_recursive;
    let absolute_path = args.path.canonicalize().unwrap_or(args.path);

    let (base_dir, tracked_files, is_directory_mode) = if absolute_path.is_file() {
        // Single-file mode: derive parent directory
        let base_dir = absolute_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        let tracked_files = vec![absolute_path];
        (base_dir, tracked_files, false)
    } else if absolute_path.is_dir() {
        // Directory mode: the scan runs in the background once the server is up,
        // so a large tree doesn't hold up the first page.
        (absolute_path, Vec::new(), true)
    } else {
        anyhow::bail!("Path must be a file or directory");
    };

    // Single unified serve function
    serve_markdown(
        base_dir,
        tracked_files,
        is_directory_mode,
        args.hostname,
        args.port,
        args.open,
        args.standalone,
        // Recursion only applies to directory mode; single-file mode never
        // watches the parent directory's subtree.
        is_directory_mode && recursive,
    )
    .await?;

    Ok(())
}
