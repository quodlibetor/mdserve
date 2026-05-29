use std::process::Command;

fn main() {
    // Embed HTML templates at compile-time so runtime rendering stays self-contained.
    minijinja_embed::embed_templates!("templates", &[".html"]);

    emit_git_commit();
}

/// Embed the short git commit (with a `-dirty` suffix when the tree has
/// uncommitted changes) so `--version` reports exactly which build is running.
/// Falls back to "unknown" when git isn't available (e.g. building from a
/// source tarball or crates.io).
fn emit_git_commit() {
    let value = match git(&["rev-parse", "--short=12", "HEAD"]) {
        Some(hash) => {
            let dirty = git(&["status", "--porcelain"])
                .map(|s| !s.is_empty())
                .unwrap_or(false);
            if dirty {
                format!("{hash}-dirty")
            } else {
                hash
            }
        }
        None => "unknown".to_string(),
    };
    println!("cargo:rustc-env=MDSERVE_GIT_COMMIT={value}");

    // Rebuild when the checked-out commit changes.
    for path in [".git/HEAD", ".git/logs/HEAD"] {
        if std::path::Path::new(path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}

/// Run a git command, returning its trimmed stdout, or None if git is missing,
/// fails, or produces no output.
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let out = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!out.is_empty()).then_some(out)
}
