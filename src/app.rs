use anyhow::{Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path as AxumPath, State, WebSocketUpgrade,
    },
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use ignore::{gitignore::GitignoreBuilder, Match, WalkBuilder};
use minijinja::{context, value::Value, Environment};
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    net::{Ipv4Addr, Ipv6Addr},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, Mutex},
};
use tower_http::cors::CorsLayer;

const TEMPLATE_NAME: &str = "main.html";
static TEMPLATE_ENV: OnceLock<Environment<'static>> = OnceLock::new();
pub(crate) const MERMAID_JS: &str = include_str!("../static/js/mermaid.min.js");
pub(crate) const PANZOOM_JS: &str = include_str!("../static/js/panzoom.min.js");
const MERMAID_ETAG: &str = concat!("\"", env!("CARGO_PKG_VERSION"), "-mermaid\"");
const PANZOOM_ETAG: &str = concat!("\"", env!("CARGO_PKG_VERSION"), "-panzoom\"");
const MAX_PORT_ATTEMPTS: u16 = 10;
/// Smallest gap between file-list updates pushed to the browser during the
/// background scan. Fast enough to feel live, slow enough that indexing a tree
/// with thousands of files doesn't flood the socket.
const SCAN_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

type SharedMarkdownState = Arc<Mutex<MarkdownState>>;

fn template_env() -> &'static Environment<'static> {
    TEMPLATE_ENV.get_or_init(|| {
        let mut env = Environment::new();
        minijinja_embed::load_templates!(&mut env);
        env
    })
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
enum ServerMessage {
    Reload,
    /// The set of tracked files changed. Carries the whole sorted list so the
    /// client can rebuild its sidebar in place; used by the background scan and
    /// by files appearing later, neither of which changes the page being read.
    Files {
        files: Vec<String>,
        scanning: bool,
    },
}

/// Walks `dir` yielding markdown files. When `recursive` is true, subdirectories
/// are walked too; otherwise only the immediate directory is read. The walk
/// respects `.gitignore`/`.ignore` rules and skips hidden files and directories
/// (e.g. `.git`) via the `ignore` crate's defaults, except that ignore rules
/// *above* `dir` are dropped when they would exclude `dir` itself — see
/// [`ignored_by_ancestor`].
fn markdown_walk(dir: &Path, recursive: bool) -> impl Iterator<Item = PathBuf> {
    let mut builder = WalkBuilder::new(dir);
    // Depth 0 is `dir` itself and depth 1 is its immediate children, so capping
    // at 1 reproduces the non-recursive single-directory behavior.
    builder.max_depth(if recursive { None } else { Some(1) });
    // Naming a directory on the command line is an explicit request to serve it,
    // which outranks a parent `.gitignore` that excludes it. Disabling parent
    // ignore files leaves ignore files *inside* the tree in force, so
    // `mdserve tasks/emails` still honors `tasks/emails/.gitignore`.
    builder.parents(!ignored_by_ancestor(dir));

    builder.build().filter_map(|entry| {
        let entry = entry.ok()?;
        let path = entry.path();
        (entry.file_type().is_some_and(|ft| ft.is_file()) && is_markdown_file(path))
            .then(|| path.to_path_buf())
    })
}

/// Whether an ignore file in a directory above `dir` excludes `dir`. Ancestors
/// are consulted nearest-first (so a closer whitelist wins, as in git) and the
/// search stops at the directory holding `.git`, since gitignore rules don't
/// apply across a repository boundary.
fn ignored_by_ancestor(dir: &Path) -> bool {
    for ancestor in dir.ancestors().skip(1) {
        let mut builder = GitignoreBuilder::new(ancestor);
        for name in [".gitignore", ".ignore"] {
            builder.add(ancestor.join(name));
        }
        if let Ok(gitignore) = builder.build() {
            match gitignore.matched_path_or_any_parents(dir, true) {
                Match::Ignore(_) => return true,
                Match::Whitelist(_) => return false,
                Match::None => {}
            }
        }
        if ancestor.join(".git").exists() {
            break;
        }
    }
    false
}

/// Collects the same walk as a sorted list. The server streams the walk instead
/// (see [`spawn_background_scan`]), so this exists for tests that need the whole
/// result up front.
#[cfg(test)]
fn scan_markdown_files(dir: &Path, recursive: bool) -> Result<Vec<PathBuf>> {
    let mut md_files: Vec<PathBuf> = markdown_walk(dir, recursive).collect();
    md_files.sort();
    Ok(md_files)
}

/// Walks `dir` on a blocking thread, adding each markdown file to `state` as it
/// is found and pushing the growing file list to connected clients at most once
/// every [`SCAN_UPDATE_INTERVAL`]. Serving starts before the walk finishes, so a
/// large tree is browsable while it is still being indexed.
fn spawn_background_scan(state: SharedMarkdownState, dir: PathBuf, recursive: bool) {
    tokio::task::spawn_blocking(move || {
        let mut last_sent = Instant::now();
        let mut unsent_files = false;

        for path in markdown_walk(&dir, recursive) {
            let mut state = state.blocking_lock();
            // A file the watcher already picked up is skipped by key, so a
            // change racing the scan isn't clobbered by the scan's older read.
            unsent_files |= state.add_tracked_file(path).is_ok();
            if unsent_files && last_sent.elapsed() >= SCAN_UPDATE_INTERVAL {
                state.broadcast_file_list();
                last_sent = Instant::now();
                unsent_files = false;
            }
        }

        let mut state = state.blocking_lock();
        state.scanning = false;
        if state.tracked_files.is_empty() {
            eprintln!("⚠ No markdown files found in {}", dir.display());
        }
        // Always sent, even with nothing new, so the client learns the scan is
        // over and stops showing it as in progress.
        state.broadcast_file_list();
    });
}

/// Computes the key used to track and address a file: its path relative to
/// `base_dir`, with forward slashes regardless of platform. For files directly
/// in `base_dir` this is just the filename, so single-file and non-recursive
/// directory modes are unaffected.
pub(crate) fn relative_key(base_dir: &Path, path: &Path) -> String {
    let rel: PathBuf = match path.strip_prefix(base_dir) {
        Ok(p) => p.to_path_buf(),
        Err(_) => match path.canonicalize() {
            Ok(canonical) => canonical
                .strip_prefix(base_dir)
                .map(|p| p.to_path_buf())
                .unwrap_or(canonical),
            Err(_) => path
                .file_name()
                .map(PathBuf::from)
                .unwrap_or_else(|| path.to_path_buf()),
        },
    };

    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// HTML-escapes a string for use in an attribute value, but leaves `/`
/// untouched so multi-segment paths read as `/a/b.md` rather than
/// `/a&#x2f;b.md` while remaining safe against injection.
fn html_escape_keep_slash(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            other => out.push(other),
        }
    }
    out
}

pub(crate) fn is_markdown_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("md") || ext.eq_ignore_ascii_case("markdown"))
        .unwrap_or(false)
}

struct TrackedFile {
    path: PathBuf,
    last_modified: SystemTime,
    html: String,
}

struct MarkdownState {
    base_dir: PathBuf,
    tracked_files: HashMap<String, TrackedFile>,
    is_directory_mode: bool,
    standalone: bool,
    /// Whether the initial directory scan is still running, so the UI can say
    /// "scanning" instead of looking like an empty directory.
    scanning: bool,
    /// Whether the offline-bundle download may collect dependencies that live
    /// outside `base_dir` (set only for loopback binds, so a networked server
    /// can't be used to read arbitrary local files).
    bundle_external: bool,
    change_tx: broadcast::Sender<ServerMessage>,
}

impl MarkdownState {
    fn new(
        base_dir: PathBuf,
        file_paths: Vec<PathBuf>,
        is_directory_mode: bool,
        standalone: bool,
        scanning: bool,
        bundle_external: bool,
    ) -> Result<Self> {
        let (change_tx, _) = broadcast::channel::<ServerMessage>(16);

        let mut tracked_files = HashMap::new();
        for file_path in file_paths {
            let metadata = fs::metadata(&file_path)?;
            let last_modified = metadata.modified()?;
            let content = fs::read_to_string(&file_path)?;
            let html = markdown_to_html(&content)?;

            let key = relative_key(&base_dir, &file_path);

            tracked_files.insert(
                key,
                TrackedFile {
                    path: file_path,
                    last_modified,
                    html,
                },
            );
        }

        Ok(MarkdownState {
            base_dir,
            tracked_files,
            is_directory_mode,
            standalone,
            scanning,
            bundle_external,
            change_tx,
        })
    }

    fn show_navigation(&self) -> bool {
        self.is_directory_mode
    }

    /// Pushes the current file list to connected clients so they can refresh
    /// their sidebar without reloading the page being read.
    fn broadcast_file_list(&self) {
        let _ = self.change_tx.send(ServerMessage::Files {
            files: self.get_sorted_filenames(),
            scanning: self.scanning,
        });
    }

    fn get_sorted_filenames(&self) -> Vec<String> {
        let mut filenames: Vec<_> = self.tracked_files.keys().cloned().collect();
        filenames.sort();
        filenames
    }

    fn refresh_file(&mut self, filename: &str) -> Result<()> {
        if let Some(tracked) = self.tracked_files.get_mut(filename) {
            let content = fs::read_to_string(&tracked.path)?;
            tracked.html = markdown_to_html(&content)?;
            tracked.last_modified = fs::metadata(&tracked.path)?.modified()?;
        }
        Ok(())
    }

    /// Renders and starts tracking `file_path`, returning whether it was new.
    fn add_tracked_file(&mut self, file_path: PathBuf) -> Result<bool> {
        let key = relative_key(&self.base_dir, &file_path);

        if self.tracked_files.contains_key(&key) {
            return Ok(false);
        }

        let metadata = fs::metadata(&file_path)?;
        let content = fs::read_to_string(&file_path)?;

        self.tracked_files.insert(
            key,
            TrackedFile {
                path: file_path,
                last_modified: metadata.modified()?,
                html: markdown_to_html(&content)?,
            },
        );

        Ok(true)
    }
}

/// Renders markdown source to an HTML body fragment (GFM, raw HTML allowed,
/// frontmatter parsed out). Used by the live server. Non-http link protocols
/// (e.g. `javascript:`, `file:`) are sanitized out, as in upstream defaults.
pub(crate) fn markdown_to_html(content: &str) -> Result<String> {
    markdown_to_html_inner(content, false)
}

/// Like [`markdown_to_html`] but keeps non-http link protocols (notably
/// `file://`) so the offline-bundle builder can discover and collect them.
/// Scoped to the bundle so the live preview keeps the stricter sanitization.
pub(crate) fn markdown_to_html_bundle(content: &str) -> Result<String> {
    markdown_to_html_inner(content, true)
}

fn markdown_to_html_inner(content: &str, allow_dangerous_protocol: bool) -> Result<String> {
    let mut options = markdown::Options::gfm();
    options.compile.allow_dangerous_html = true;
    options.compile.allow_dangerous_protocol = allow_dangerous_protocol;
    options.parse.constructs.frontmatter = true;

    let html_body = markdown::to_html_with_options(content, &options)
        .unwrap_or_else(|_| "Error parsing markdown".to_string());

    let html_body = render_github_alerts(&html_body);
    Ok(add_heading_ids(&html_body))
}

/// Alert kinds GitHub recognizes: `(marker, title, emoji)`.
const ALERT_KINDS: &[(&str, &str, &str)] = &[
    ("note", "Note", "ℹ️"),
    ("tip", "Tip", "💡"),
    ("important", "Important", "❗"),
    ("warning", "Warning", "⚠️"),
    ("caution", "Caution", "🛑"),
];

/// Converts GitHub alert blockquotes — a blockquote whose first line is
/// `[!NOTE]` / `[!TIP]` / `[!IMPORTANT]` / `[!WARNING]` / `[!CAUTION]` — into
/// styled callout boxes. Other blockquotes are left untouched.
fn render_github_alerts(html: &str) -> String {
    let (open, close) = ("<blockquote>", "</blockquote>");
    let mut out = String::with_capacity(html.len());
    let mut rest = html;

    while let Some(start) = rest.find(open) {
        out.push_str(&rest[..start]);
        let body_start = start + open.len();
        let Some(end_rel) = matching_blockquote_end(&rest[body_start..]) else {
            out.push_str(&rest[start..]);
            return out;
        };
        let inner = &rest[body_start..body_start + end_rel];
        match alert_from_blockquote(inner) {
            Some(rendered) => out.push_str(&rendered),
            None => {
                out.push_str(open);
                out.push_str(inner);
                out.push_str(close);
            }
        }
        rest = &rest[body_start + end_rel + close.len()..];
    }
    out.push_str(rest);
    out
}

/// Finds the byte offset of the `</blockquote>` that closes the blockquote whose
/// inner content starts at the beginning of `s` (handling nested blockquotes).
fn matching_blockquote_end(s: &str) -> Option<usize> {
    let (open, close) = ("<blockquote>", "</blockquote>");
    let mut depth = 0usize;
    let mut i = 0;
    loop {
        let next_open = s[i..].find(open).map(|r| i + r);
        let next_close = s[i..].find(close).map(|r| i + r);
        match (next_open, next_close) {
            (Some(o), Some(c)) if o < c => {
                depth += 1;
                i = o + open.len();
            }
            (_, Some(c)) => {
                if depth == 0 {
                    return Some(c);
                }
                depth -= 1;
                i = c + close.len();
            }
            _ => return None,
        }
    }
}

/// If `inner` (a blockquote's contents) begins with a recognized `[!TYPE]`
/// marker alone on the first line, returns the alert `<div>` markup.
fn alert_from_blockquote(inner: &str) -> Option<String> {
    let after_p = inner.trim_start().strip_prefix("<p>")?;
    let after_bracket = after_p.strip_prefix("[!")?;
    let close = after_bracket.find(']')?;
    let kind = after_bracket[..close].to_ascii_lowercase();
    let (_, title, icon) = *ALERT_KINDS.iter().find(|(k, _, _)| *k == kind)?;

    // The marker must be alone on the first line: what follows `]` is either a
    // soft break before the body, or the paragraph close (title-only alert).
    let after_marker = &after_bracket[close + 1..];
    let body = if let Some(rest) = after_marker.strip_prefix('\n') {
        format!("<p>{rest}")
    } else if let Some(rest) = after_marker.strip_prefix("</p>") {
        rest.to_string()
    } else {
        return None;
    };

    Some(format!(
        "<div class=\"markdown-alert markdown-alert-{kind}\">\n\
         <p class=\"markdown-alert-title\">{icon} {title}</p>\n{body}\n</div>"
    ))
}

// Link (chain) octicon shown on heading hover, linking to the heading's anchor.
const HEADING_ANCHOR_ICON: &str = r##"<svg viewBox="0 0 16 16" width="16" height="16" fill="currentColor" aria-hidden="true"><path d="m7.775 3.275 1.25-1.25a3.5 3.5 0 1 1 4.95 4.95l-2.5 2.5a3.5 3.5 0 0 1-4.95 0 .751.751 0 0 1 .018-1.042.751.751 0 0 1 1.042-.018 2 2 0 0 0 2.83 0l2.5-2.5a2 2 0 0 0-2.83-2.83l-1.25 1.25a.751.751 0 0 1-1.042-.018.751.751 0 0 1-.018-1.042Zm-4.69 9.64a2 2 0 0 0 2.83 0l1.25-1.25a.751.751 0 0 1 1.042.018.751.751 0 0 1 .018 1.042l-1.25 1.25a3.5 3.5 0 1 1-4.95-4.95l2.5-2.5a3.5 3.5 0 0 1 4.95 0 .751.751 0 0 1-.018 1.042.751.751 0 0 1-1.042.018 2 2 0 0 0-2.83 0l-2.5 2.5a2 2 0 0 0 0 2.83Z"></path></svg>"##;

/// Adds GitHub-style slug `id` attributes to `<h1>`..`<h6>` tags so in-page
/// `#anchor` links resolve, and injects a clickable anchor link (shown on hover)
/// into each. Only plain (attribute-less) heading tags emitted by the markdown
/// renderer are touched; author-written headings that already have attributes
/// are left alone. Duplicate slugs get a `-1`, `-2`, ... suffix.
fn add_heading_ids(html: &str) -> String {
    let mut out = String::with_capacity(html.len() + 64);
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut rest = html;

    while let Some((pos, level)) = next_heading_open(rest) {
        out.push_str(&rest[..pos]);
        let after_open = pos + 4; // past "<hN>"
        let close = format!("</h{level}>");
        let Some(crel) = rest[after_open..].find(&close) else {
            // Unterminated heading: emit as-is and stop transforming.
            out.push_str(&rest[pos..]);
            return out;
        };
        let inner = &rest[after_open..after_open + crel];
        let slug = unique_slug(&slugify(&strip_html_tags(inner)), &mut seen);
        if slug.is_empty() {
            out.push_str(&format!("<h{level}>"));
        } else {
            out.push_str(&format!(
                "<h{level} id=\"{slug}\">\
                 <a class=\"heading-anchor\" href=\"#{slug}\" aria-label=\"Permalink to this heading\">{HEADING_ANCHOR_ICON}</a>"
            ));
        }
        out.push_str(inner);
        out.push_str(&close);
        rest = &rest[after_open + crel + close.len()..];
    }
    out.push_str(rest);
    out
}

/// Finds the next plain heading open tag (`<h1>`..`<h6>`), returning its byte
/// offset and level digit.
fn next_heading_open(s: &str) -> Option<(usize, char)> {
    let bytes = s.as_bytes();
    let mut from = 0;
    while let Some(rel) = s[from..].find("<h") {
        let idx = from + rel;
        if idx + 3 < s.len() && bytes[idx + 3] == b'>' && bytes[idx + 2].is_ascii_digit() {
            let level = bytes[idx + 2] as char;
            if ('1'..='6').contains(&level) {
                return Some((idx, level));
            }
        }
        from = idx + 2;
    }
    None
}

/// Strips HTML tags and decodes a few basic entities to recover heading text.
fn strip_html_tags(s: &str) -> String {
    let mut text = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => text.push(c),
            _ => {}
        }
    }
    // `&amp;` last so entities aren't double-decoded.
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

/// GitHub-ish slug: lowercase; keep alphanumerics and `_`; turn whitespace and
/// `-` runs into a single `-`; drop other punctuation; trim leading/trailing `-`.
fn slugify(text: &str) -> String {
    let mut slug = String::with_capacity(text.len());
    let mut pending_hyphen = false;
    for c in text.chars() {
        if c.is_alphanumeric() {
            if pending_hyphen && !slug.is_empty() {
                slug.push('-');
            }
            pending_hyphen = false;
            slug.extend(c.to_lowercase());
        } else if c == '_' {
            pending_hyphen = false;
            slug.push('_');
        } else if c.is_whitespace() || c == '-' {
            pending_hyphen = true;
        }
        // any other punctuation is dropped
    }
    slug
}

/// Disambiguates a slug against previously-seen ones (`slug`, `slug-1`, ...).
fn unique_slug(slug: &str, seen: &mut HashMap<String, usize>) -> String {
    if slug.is_empty() {
        return String::new();
    }
    let count = seen.entry(slug.to_string()).or_insert(0);
    let result = if *count == 0 {
        slug.to_string()
    } else {
        format!("{slug}-{count}")
    };
    *count += 1;
    result
}

/// Handles a markdown file that may have been created or modified.
/// Refreshes tracked files or adds new files in directory mode, sending reload notifications.
async fn handle_markdown_file_change(path: &Path, state: &SharedMarkdownState) {
    if !is_markdown_file(path) {
        return;
    }

    let mut state_guard = state.lock().await;

    let key = relative_key(&state_guard.base_dir, path);

    // If file is already tracked, refresh its content
    if state_guard.tracked_files.contains_key(&key) {
        if state_guard.refresh_file(&key).is_ok() {
            let _ = state_guard.change_tx.send(ServerMessage::Reload);
        }
    } else if state_guard.is_directory_mode {
        // New file in directory mode: nothing on the open page changed, so
        // clients only need the updated file list for their sidebar.
        if matches!(state_guard.add_tracked_file(path.to_path_buf()), Ok(true)) {
            state_guard.broadcast_file_list();
        }
    }
}

async fn handle_file_event(event: Event, state: &SharedMarkdownState) {
    match event.kind {
        notify::EventKind::Modify(notify::event::ModifyKind::Name(rename_mode)) => {
            use notify::event::RenameMode;
            match rename_mode {
                RenameMode::Both => {
                    // Linux/Windows: Both old and new paths provided in single event
                    if event.paths.len() == 2 {
                        let new_path = &event.paths[1];
                        handle_markdown_file_change(new_path, state).await;
                    }
                }
                RenameMode::From => {
                    // File being renamed away - ignore
                }
                RenameMode::To => {
                    // File renamed to this location
                    if let Some(path) = event.paths.first() {
                        handle_markdown_file_change(path, state).await;
                    }
                }
                RenameMode::Any => {
                    // macOS: Sends separate events for old and new paths
                    // Use file existence to distinguish old (doesn't exist) from new (exists)
                    if let Some(path) = event.paths.first() {
                        if path.exists() {
                            handle_markdown_file_change(path, state).await;
                        }
                    }
                }
                _ => {}
            }
        }
        _ => {
            for path in &event.paths {
                if is_markdown_file(path) {
                    match event.kind {
                        notify::EventKind::Create(_)
                        | notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) => {
                            handle_markdown_file_change(path, state).await;
                        }
                        notify::EventKind::Remove(_) => {
                            // Don't remove files from tracking. Editors like neovim save by
                            // renaming the file to a backup, then creating a new one. If we
                            // removed the file here, HTTP requests during that window would
                            // see empty tracked_files and return 404.
                        }
                        _ => {}
                    }
                } else if path.is_file() && is_image_file(path.to_str().unwrap_or("")) {
                    match event.kind {
                        notify::EventKind::Modify(_)
                        | notify::EventKind::Create(_)
                        | notify::EventKind::Remove(_) => {
                            let state_guard = state.lock().await;
                            let _ = state_guard.change_tx.send(ServerMessage::Reload);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn new_router(
    base_dir: PathBuf,
    tracked_files: Vec<PathBuf>,
    is_directory_mode: bool,
    standalone: bool,
    recursive: bool,
    background_scan: bool,
    bundle_external: bool,
) -> Result<Router> {
    let base_dir = base_dir.canonicalize()?;

    let state = Arc::new(Mutex::new(MarkdownState::new(
        base_dir.clone(),
        tracked_files,
        is_directory_mode,
        standalone,
        background_scan,
        bundle_external,
    )?));

    if background_scan {
        spawn_background_scan(state.clone(), base_dir.clone(), recursive);
    }

    let watcher_state = state.clone();
    let (tx, mut rx) = mpsc::channel(100);

    let mut watcher = RecommendedWatcher::new(
        move |res: std::result::Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        },
        Config::default(),
    )?;

    let watch_mode = if recursive {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    watcher.watch(&base_dir, watch_mode)?;

    tokio::spawn(async move {
        let _watcher = watcher;
        while let Some(event) = rx.recv().await {
            handle_file_event(event, &watcher_state).await;
        }
    });

    let router = Router::new()
        .route("/", get(serve_html_root))
        .route("/ws", get(websocket_handler))
        .route("/api/mermaid-error", post(log_mermaid_error))
        .route("/api/download", get(download_bundle))
        .route("/mermaid.min.js", get(serve_mermaid_js))
        .route("/panzoom.min.js", get(serve_panzoom_js))
        .route("/*filename", get(serve_file))
        .layer(CorsLayer::permissive())
        .with_state(state);

    Ok(router)
}

async fn bind_with_retry(hostname: &str, port: u16) -> Result<(TcpListener, u16)> {
    let mut last_err = None;
    for offset in 0..MAX_PORT_ATTEMPTS {
        let try_port = match port.checked_add(offset) {
            Some(p) => p,
            None => break,
        };
        match TcpListener::bind((hostname, try_port)).await {
            Ok(listener) => return Ok((listener, try_port)),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => last_err = Some(e),
            Err(e) => return Err(e.into()),
        }
    }
    Err(last_err
        .map(|e| anyhow::anyhow!(e))
        .unwrap_or_else(|| anyhow::anyhow!("no valid port in range"))
        .context(format!(
            "could not bind to ports {}--{}",
            port,
            port.saturating_add(MAX_PORT_ATTEMPTS - 1)
        )))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_markdown(
    base_dir: PathBuf,
    tracked_files: Vec<PathBuf>,
    is_directory_mode: bool,
    hostname: impl AsRef<str>,
    port: u16,
    open: bool,
    standalone: bool,
    recursive: bool,
) -> Result<()> {
    let hostname = hostname.as_ref();

    let first_file = tracked_files.first().cloned();
    // Only allow the offline bundle to collect files outside the served
    // directory on loopback binds; a networked server must not be usable to
    // read arbitrary local files.
    let bundle_external = is_loopback_host(hostname);
    let router = new_router(
        base_dir.clone(),
        tracked_files,
        is_directory_mode,
        standalone,
        recursive,
        // Directory mode always discovers its files in the background so a large
        // tree doesn't delay the first page; single-file mode has nothing to scan.
        is_directory_mode,
        bundle_external,
    )?;

    let (listener, actual_port) = bind_with_retry(hostname, port).await?;

    if actual_port != port {
        println!("⚠ Port {port} in use, using {actual_port} instead");
    }

    let listen_addr = format_host(hostname, actual_port);

    if is_directory_mode {
        println!("📁 Serving markdown files from: {}", base_dir.display());
    } else if let Some(file_path) = first_file {
        println!("📄 Serving markdown file: {}", file_path.display());
    }

    println!("🌐 Server running at: http://{listen_addr}");
    println!("⚡ Live reload enabled");
    println!("\nPress Ctrl+C to stop the server");

    if open {
        let browse_addr = format_host(&browsable_host(hostname), actual_port);
        open_browser(&format!("http://{browse_addr}"))?;
    }

    axum::serve(listener, router).await?;

    Ok(())
}

/// Format the host address (hostname + port) for printing.
fn format_host(hostname: &str, port: u16) -> String {
    if hostname.parse::<Ipv6Addr>().is_ok() {
        format!("[{hostname}]:{port}")
    } else {
        format!("{hostname}:{port}")
    }
}

/// Whether a bind hostname refers to the local machine only. Unknown hostnames
/// are treated as non-loopback (conservative).
fn is_loopback_host(hostname: &str) -> bool {
    hostname.eq_ignore_ascii_case("localhost")
        || hostname
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

/// Map wildcard bind addresses to loopback so the browser gets a
/// reachable URL.
fn browsable_host(hostname: &str) -> String {
    if hostname
        .parse::<Ipv4Addr>()
        .ok()
        .is_some_and(|ip| ip.is_unspecified())
    {
        "127.0.0.1".into()
    } else if hostname
        .parse::<Ipv6Addr>()
        .ok()
        .is_some_and(|ip| ip.is_unspecified())
    {
        "::1".into()
    } else {
        hostname.into()
    }
}

/// Open a URL in the default browser using platform commands.
///
/// Fails immediately if the command cannot be spawned (e.g. not
/// installed). Exit status is monitored in a background thread
/// since opener commands may block until their handler process
/// returns.
fn open_browser(url: &str) -> Result<()> {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(target_os = "linux") {
        "xdg-open"
    } else {
        anyhow::bail!("--open is not supported on this platform");
    };

    let mut child = std::process::Command::new(program)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .with_context(|| format!("failed to run {program}"))?;

    std::thread::spawn(move || match child.wait() {
        Ok(status) if !status.success() => {
            eprintln!("{program} exited with {status}");
        }
        Err(e) => eprintln!("Failed waiting on {program}: {e}"),
        _ => {}
    });

    Ok(())
}

async fn serve_html_root(State(state): State<SharedMarkdownState>) -> impl IntoResponse {
    let state = state.lock().await;

    let filename = match state.get_sorted_filenames().into_iter().next() {
        Some(name) => name,
        // Nothing indexed yet: either the background scan is still running, or
        // the directory has no markdown in it. Both are served as a live page
        // that replaces itself once a file shows up.
        None => return render_empty_page(&state),
    };

    render_markdown(&state, &filename).await
}

/// The page shown at `/` before any markdown file is known. It carries the
/// live-reload script and is marked as a placeholder, so the client reloads as
/// soon as the file list becomes non-empty.
fn render_empty_page(state: &MarkdownState) -> (StatusCode, Html<String>) {
    let message = if state.scanning {
        "Scanning for markdown files…"
    } else {
        "No markdown files found."
    };

    let env = template_env();
    let Ok(template) = env.get_template(TEMPLATE_NAME) else {
        return (StatusCode::OK, Html(message.to_string()));
    };

    let rendered = template
        .render(context! {
            content => Value::from_safe_string(format!("<p>{message}</p>")),
            mermaid_enabled => false,
            show_navigation => state.show_navigation(),
            files => Vec::<Value>::new(),
            current_file => "",
            page_title => "mdserve",
            standalone => state.standalone,
            mermaid_js => "",
            panzoom_js => "",
            awaiting_files => true,
        })
        .unwrap_or_else(|e| format!("Rendering error: {e}"));

    (StatusCode::OK, Html(rendered))
}

async fn serve_file(
    AxumPath(filename): AxumPath<String>,
    State(state): State<SharedMarkdownState>,
) -> axum::response::Response {
    if filename.ends_with(".md") || filename.ends_with(".markdown") {
        let state = state.lock().await;

        if !state.tracked_files.contains_key(&filename) {
            return (StatusCode::NOT_FOUND, Html("File not found".to_string())).into_response();
        }

        let (status, html) = render_markdown(&state, &filename).await;
        (status, html).into_response()
    } else if is_image_file(&filename) {
        serve_static_file_inner(filename, state).await
    } else {
        (StatusCode::NOT_FOUND, Html("File not found".to_string())).into_response()
    }
}

async fn render_markdown(state: &MarkdownState, current_file: &str) -> (StatusCode, Html<String>) {
    let env = template_env();
    let template = match env.get_template(TEMPLATE_NAME) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(format!("Template error: {e}")),
            );
        }
    };

    let (content, has_mermaid) = if let Some(tracked) = state.tracked_files.get(current_file) {
        let html = &tracked.html;
        let mermaid = html.contains(r#"class="language-mermaid""#);
        (Value::from_safe_string(html.clone()), mermaid)
    } else {
        return (StatusCode::NOT_FOUND, Html("File not found".to_string()));
    };

    // Derive page title from filename (stem without extension)
    let page_title = std::path::Path::new(current_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(current_file);

    let (mermaid_js, panzoom_js) = if state.standalone && has_mermaid {
        (
            Value::from_safe_string(MERMAID_JS.to_string()),
            Value::from_safe_string(PANZOOM_JS.to_string()),
        )
    } else {
        (Value::from(""), Value::from(""))
    };

    let rendered = if state.show_navigation() {
        let filenames = state.get_sorted_filenames();
        let files: Vec<Value> = filenames
            .iter()
            .map(|name| {
                Value::from_object({
                    let mut map = std::collections::HashMap::new();
                    map.insert("name".to_string(), Value::from(name.clone()));
                    // Build the href ourselves so path separators stay as "/"
                    // instead of being autoescaped to "&#x2f;". Each path
                    // component is still HTML-escaped to stay XSS-safe.
                    map.insert(
                        "href".to_string(),
                        Value::from_safe_string(format!("/{}", html_escape_keep_slash(name))),
                    );
                    map
                })
            })
            .collect();

        match template.render(context! {
            content => content,
            mermaid_enabled => has_mermaid,
            show_navigation => true,
            files => files,
            current_file => current_file,
            page_title => page_title,
            standalone => state.standalone,
            mermaid_js => mermaid_js,
            panzoom_js => panzoom_js,
            mermaid_js_src => Value::from_safe_string("/mermaid.min.js".to_string()),
            panzoom_js_src => Value::from_safe_string("/panzoom.min.js".to_string()),
            // The download button is a live-server affordance: always shown here
            // (including under --standalone), never in the generated bundle pages.
            show_download => true,
        }) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html(format!("Rendering error: {e}")),
                );
            }
        }
    } else {
        match template.render(context! {
            content => content,
            mermaid_enabled => has_mermaid,
            show_navigation => false,
            page_title => page_title,
            standalone => state.standalone,
            mermaid_js => mermaid_js,
            panzoom_js => panzoom_js,
            mermaid_js_src => Value::from_safe_string("/mermaid.min.js".to_string()),
            panzoom_js_src => Value::from_safe_string("/panzoom.min.js".to_string()),
            // The download button is a live-server affordance: always shown here
            // (including under --standalone), never in the generated bundle pages.
            show_download => true,
        }) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html(format!("Rendering error: {e}")),
                );
            }
        }
    };

    (StatusCode::OK, Html(rendered))
}

/// Renders a full HTML page for the offline bundle from an already-rendered
/// HTML body. The page references the shared mermaid/panzoom libraries bundled
/// once at `_assets/` (via `asset_prefix`, the relative path back to the bundle
/// root, e.g. "" or "../") rather than inlining them, so a bundle with many
/// diagram pages stays small. There is no navigation sidebar and no download
/// button, and it works opened directly from `file://`. Returns a best-effort
/// page even on template error so bundling never aborts.
pub(crate) fn render_bundle_page(html_body: &str, page_title: &str, asset_prefix: &str) -> String {
    let env = template_env();
    let template = match env.get_template(TEMPLATE_NAME) {
        Ok(t) => t,
        Err(e) => return format!("Template error: {e}"),
    };

    let has_mermaid = html_body.contains(r#"class="language-mermaid""#);

    template
        .render(context! {
            content => Value::from_safe_string(html_body.to_string()),
            mermaid_enabled => has_mermaid,
            show_navigation => false,
            page_title => page_title,
            standalone => true,
            // Empty inline JS forces the template to use the *_src references.
            mermaid_js => "",
            panzoom_js => "",
            mermaid_js_src => Value::from_safe_string(
                format!("{asset_prefix}{}", crate::bundle::BUNDLE_MERMAID_JS_PATH)
            ),
            panzoom_js_src => Value::from_safe_string(
                format!("{asset_prefix}{}", crate::bundle::BUNDLE_PANZOOM_JS_PATH)
            ),
            // No download button inside the bundle's own pages.
            show_download => false,
        })
        .unwrap_or_else(|e| format!("Rendering error: {e}"))
}

/// Computes the download filename stem: the served directory's name in
/// directory mode, otherwise the single file's stem. Falls back to "bundle".
/// Strips characters unsafe for a Content-Disposition header.
fn bundle_filename(state: &MarkdownState) -> String {
    let raw = if state.is_directory_mode {
        state
            .base_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("bundle")
            .to_string()
    } else {
        state
            .tracked_files
            .values()
            .next()
            .and_then(|t| t.path.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("bundle")
            .to_string()
    };
    let cleaned: String = raw
        .chars()
        .filter(|c| !matches!(c, '"' | '\\' | '\r' | '\n' | '/'))
        .collect();
    if cleaned.is_empty() {
        "bundle".to_string()
    } else {
        cleaned
    }
}

/// Rejects cross-origin requests. The bundle can contain local file contents,
/// and permissive CORS would otherwise let a page the user visits read it via
/// `fetch`. Requests with no `Origin` (same-origin navigation, curl) are
/// allowed; an `Origin` whose authority differs from `Host` is rejected.
fn is_same_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return true;
    };
    let origin_authority = origin.split_once("://").map(|(_, a)| a).unwrap_or(origin);
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    origin_authority == host
}

/// Builds and returns a zip bundle of the served markdown rendered to
/// self-contained offline HTML plus all recursively-collected local
/// dependencies. See `crate::bundle`.
async fn download_bundle(
    headers: HeaderMap,
    State(state): State<SharedMarkdownState>,
) -> axum::response::Response {
    if !is_same_origin(&headers) {
        return (
            StatusCode::FORBIDDEN,
            "cross-origin requests are not allowed",
        )
            .into_response();
    }

    let (base_dir, is_directory_mode, bundle_external, roots, zip_name) = {
        let state = state.lock().await;
        let roots: Vec<crate::bundle::RootDoc> = state
            .tracked_files
            .values()
            .map(|tracked| crate::bundle::RootDoc {
                abs_path: tracked.path.clone(),
                source: fs::read_to_string(&tracked.path).unwrap_or_default(),
            })
            .collect();
        (
            state.base_dir.clone(),
            state.is_directory_mode,
            state.bundle_external,
            roots,
            bundle_filename(&state),
        )
    };

    let build = tokio::task::spawn_blocking(move || {
        crate::bundle::build_zip(
            &base_dir,
            &roots,
            is_directory_mode,
            bundle_external,
            render_bundle_page,
        )
    })
    .await;

    match build {
        Ok(Ok(bytes)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/zip".to_string()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{zip_name}.zip\""),
                ),
            ],
            bytes,
        )
            .into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bundle error: {e}"),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bundle task failed: {e}"),
        )
            .into_response(),
    }
}

async fn serve_mermaid_js(headers: HeaderMap) -> impl IntoResponse {
    serve_bundled_js(&headers, MERMAID_JS, MERMAID_ETAG)
}

async fn serve_panzoom_js(headers: HeaderMap) -> impl IntoResponse {
    serve_bundled_js(&headers, PANZOOM_JS, PANZOOM_ETAG)
}

fn serve_bundled_js(
    headers: &HeaderMap,
    content: &'static str,
    etag: &'static str,
) -> axum::response::Response {
    if is_etag_match(headers, etag) {
        return js_response(StatusCode::NOT_MODIFIED, None, etag);
    }
    js_response(StatusCode::OK, Some(content), etag)
}

fn is_etag_match(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|etags| etags.split(',').any(|tag| tag.trim() == etag))
}

fn js_response(
    status: StatusCode,
    body: Option<&'static str>,
    etag: &'static str,
) -> axum::response::Response {
    // Use no-cache to force revalidation on each request. This ensures clients
    // get updated content when mdserve is rebuilt with a new bundled version,
    // while still benefiting from 304 responses via ETag matching.
    let headers = [
        (header::CONTENT_TYPE, "application/javascript"),
        (header::ETAG, etag),
        (header::CACHE_CONTROL, "public, no-cache"),
    ];

    match body {
        Some(content) => (status, headers, content).into_response(),
        None => (status, headers).into_response(),
    }
}

async fn serve_static_file_inner(
    filename: String,
    state: SharedMarkdownState,
) -> axum::response::Response {
    let state = state.lock().await;

    let full_path = state.base_dir.join(&filename);

    match full_path.canonicalize() {
        Ok(canonical_path) => {
            if !canonical_path.starts_with(&state.base_dir) {
                return (
                    StatusCode::FORBIDDEN,
                    [(header::CONTENT_TYPE, "text/plain")],
                    "Access denied".to_string(),
                )
                    .into_response();
            }

            match fs::read(&canonical_path) {
                Ok(contents) => {
                    let content_type = guess_image_content_type(&filename);
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, content_type.as_str())],
                        contents,
                    )
                        .into_response()
                }
                Err(_) => (
                    StatusCode::NOT_FOUND,
                    [(header::CONTENT_TYPE, "text/plain")],
                    "File not found".to_string(),
                )
                    .into_response(),
            }
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain")],
            "File not found".to_string(),
        )
            .into_response(),
    }
}

fn is_image_file(file_path: &str) -> bool {
    guess_image_content_type(file_path).starts_with("image/")
}

fn guess_image_content_type(file_path: &str) -> String {
    let extension = std::path::Path::new(file_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("");

    match extension.to_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "ico" => "image/x-icon",
        _ => "application/octet-stream",
    }
    .to_string()
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<SharedMarkdownState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_websocket(socket, state))
}

/// A client-reported mermaid render failure. The browser renders mermaid
/// diagrams, so parse errors are only visible there; the client posts them
/// back here so they surface in the terminal running the server.
#[derive(Debug, Deserialize)]
struct MermaidErrorReport {
    #[serde(default)]
    id: String,
    #[serde(default)]
    source: String,
    message: String,
}

async fn log_mermaid_error(Json(report): Json<MermaidErrorReport>) -> StatusCode {
    let id = if report.id.is_empty() {
        "(unknown)".to_string()
    } else {
        report.id
    };
    eprintln!("⚠ mermaid render error in diagram {id}: {}", report.message);
    if !report.source.is_empty() {
        eprintln!("{}", report.source);
    }
    StatusCode::NO_CONTENT
}

async fn handle_websocket(socket: WebSocket, state: SharedMarkdownState) {
    let (mut sender, mut receiver) = socket.split();

    let mut change_rx = {
        let state = state.lock().await;
        state.change_tx.subscribe()
    };

    let recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(_)) => {}
                Ok(Message::Close(_)) => break,
                _ => {}
            }
        }
    });

    let send_task = tokio::spawn(async move {
        while let Ok(reload_msg) = change_rx.recv().await {
            if let Ok(json) = serde_json::to_string(&reload_msg) {
                if sender.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    });

    tokio::select! {
        _ = recv_task => {},
        _ = send_task => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_is_markdown_file() {
        assert!(is_markdown_file(Path::new("test.md")));
        assert!(is_markdown_file(Path::new("/path/to/file.md")));

        assert!(is_markdown_file(Path::new("test.markdown")));
        assert!(is_markdown_file(Path::new("/path/to/file.markdown")));

        assert!(is_markdown_file(Path::new("test.MD")));
        assert!(is_markdown_file(Path::new("test.Md")));
        assert!(is_markdown_file(Path::new("test.MARKDOWN")));
        assert!(is_markdown_file(Path::new("test.MarkDown")));

        assert!(!is_markdown_file(Path::new("test.txt")));
        assert!(!is_markdown_file(Path::new("test.rs")));
        assert!(!is_markdown_file(Path::new("test.html")));
        assert!(!is_markdown_file(Path::new("test")));
        assert!(!is_markdown_file(Path::new("README")));
    }

    #[test]
    fn test_is_image_file() {
        assert!(is_image_file("test.png"));
        assert!(is_image_file("test.jpg"));
        assert!(is_image_file("test.jpeg"));
        assert!(is_image_file("test.gif"));
        assert!(is_image_file("test.svg"));
        assert!(is_image_file("test.webp"));
        assert!(is_image_file("test.bmp"));
        assert!(is_image_file("test.ico"));

        assert!(is_image_file("test.PNG"));
        assert!(is_image_file("test.JPG"));
        assert!(is_image_file("test.JPEG"));

        assert!(is_image_file("/path/to/image.png"));
        assert!(is_image_file("./images/photo.jpg"));

        assert!(!is_image_file("test.txt"));
        assert!(!is_image_file("test.md"));
        assert!(!is_image_file("test.rs"));
        assert!(!is_image_file("test"));
    }

    #[test]
    fn test_guess_image_content_type() {
        assert_eq!(guess_image_content_type("test.png"), "image/png");
        assert_eq!(guess_image_content_type("test.jpg"), "image/jpeg");
        assert_eq!(guess_image_content_type("test.jpeg"), "image/jpeg");
        assert_eq!(guess_image_content_type("test.gif"), "image/gif");
        assert_eq!(guess_image_content_type("test.svg"), "image/svg+xml");
        assert_eq!(guess_image_content_type("test.webp"), "image/webp");
        assert_eq!(guess_image_content_type("test.bmp"), "image/bmp");
        assert_eq!(guess_image_content_type("test.ico"), "image/x-icon");

        assert_eq!(guess_image_content_type("test.PNG"), "image/png");
        assert_eq!(guess_image_content_type("test.JPG"), "image/jpeg");

        assert_eq!(
            guess_image_content_type("test.xyz"),
            "application/octet-stream"
        );
        assert_eq!(guess_image_content_type("test"), "application/octet-stream");
    }

    #[test]
    fn test_headings_get_slug_ids() {
        let md = "# Intro\n\n## Details Section\n\n## Details Section\n\n### Café & Bar!\n";
        let html = markdown_to_html(md).unwrap();
        assert!(html.contains(r#"<h1 id="intro">"#), "{html}");
        assert!(html.contains(r#"<h2 id="details-section">"#), "{html}");
        // Duplicate heading text gets a numeric suffix.
        assert!(html.contains(r#"<h2 id="details-section-1">"#), "{html}");
        // Punctuation dropped, spaces collapsed, unicode letters kept.
        assert!(html.contains(r#"<h3 id="café-bar">"#), "{html}");
        // Each heading gets a clickable anchor link to its own id.
        assert!(
            html.contains(r##"<h1 id="intro"><a class="heading-anchor" href="#intro""##),
            "{html}"
        );
    }

    #[test]
    fn test_github_alerts_render() {
        let md = "> [!NOTE]\n> Heads up.\n\n> [!WARNING]\n> One.\n>\n> Two.\n";
        let html = markdown_to_html(md).unwrap();
        assert!(
            html.contains(r#"<div class="markdown-alert markdown-alert-note">"#),
            "{html}"
        );
        assert!(
            html.contains(r#"markdown-alert-title">ℹ️ Note</p>"#),
            "{html}"
        );
        assert!(html.contains("Heads up."), "{html}");
        // Multi-paragraph warning keeps both paragraphs.
        assert!(
            html.contains(r#"markdown-alert-warning"#)
                && html.contains("One.")
                && html.contains("Two."),
            "{html}"
        );
    }

    #[test]
    fn test_non_alert_blockquotes_unchanged() {
        // A plain quote and an unknown marker stay as blockquotes.
        let html = markdown_to_html("> just a quote\n\n> [!BOGUS]\n> nope\n").unwrap();
        assert_eq!(html.matches("<blockquote>").count(), 2, "{html}");
        assert!(!html.contains("markdown-alert"), "{html}");
        // An inline marker (not alone on the line) is not an alert.
        let inline = markdown_to_html("> [!NOTE] with trailing text\n").unwrap();
        assert!(!inline.contains("markdown-alert"), "{inline}");
    }

    #[test]
    fn test_slugify_rules() {
        assert_eq!(slugify("Hello, World!"), "hello-world");
        assert_eq!(slugify("  leading and  trailing  "), "leading-and-trailing");
        assert_eq!(slugify("keep_underscores"), "keep_underscores");
        assert_eq!(slugify("a -- b"), "a-b");
        assert_eq!(slugify("!!!"), "");
    }

    #[test]
    fn test_scan_markdown_files_empty_directory() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        let result = scan_markdown_files(temp_dir.path(), false).expect("Failed to scan");
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_scan_markdown_files_with_markdown_files() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("test1.md"), "# Test 1").expect("Failed to write");
        fs::write(temp_dir.path().join("test2.markdown"), "# Test 2").expect("Failed to write");
        fs::write(temp_dir.path().join("test3.md"), "# Test 3").expect("Failed to write");

        fs::write(temp_dir.path().join("test.txt"), "text").expect("Failed to write");
        fs::write(temp_dir.path().join("README"), "readme").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path(), false).expect("Failed to scan");

        assert_eq!(result.len(), 3);

        let filenames: Vec<_> = result
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(filenames, vec!["test1.md", "test2.markdown", "test3.md"]);
    }

    #[test]
    fn test_scan_markdown_files_non_recursive_ignores_subdirectories() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("root.md"), "# Root").expect("Failed to write");

        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir).expect("Failed to create subdir");
        fs::write(sub_dir.join("nested.md"), "# Nested").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path(), false).expect("Failed to scan");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].file_name().unwrap().to_str().unwrap(), "root.md");
    }

    #[test]
    fn test_scan_markdown_files_recursive_includes_subdirectories() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("root.md"), "# Root").expect("Failed to write");

        let sub_dir = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir).expect("Failed to create subdir");
        fs::write(sub_dir.join("nested.md"), "# Nested").expect("Failed to write");

        let deep_dir = sub_dir.join("deeper");
        fs::create_dir(&deep_dir).expect("Failed to create nested subdir");
        fs::write(deep_dir.join("deep.md"), "# Deep").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path(), true).expect("Failed to scan");

        assert_eq!(result.len(), 3);
        let filenames: Vec<_> = result
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert!(filenames.contains(&"root.md"));
        assert!(filenames.contains(&"nested.md"));
        assert!(filenames.contains(&"deep.md"));
    }

    #[test]
    fn test_scan_markdown_files_recursive_skips_hidden_directories() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("root.md"), "# Root").expect("Failed to write");

        // The `ignore` crate skips hidden directories by default, so markdown
        // inside e.g. `.git` should never be picked up.
        let hidden_dir = temp_dir.path().join(".git");
        fs::create_dir(&hidden_dir).expect("Failed to create hidden dir");
        fs::write(hidden_dir.join("COMMIT_EDITMSG.md"), "# Hidden").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path(), true).expect("Failed to scan");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].file_name().unwrap().to_str().unwrap(), "root.md");
    }

    /// Builds a repository whose `.gitignore` blanket-excludes `tasks/`, with a
    /// second `.gitignore` inside `tasks/notes/` excluding `drafts/`.
    fn ignored_subdir_repo() -> TempDir {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let root = temp_dir.path();

        // The ancestor search stops at the repository root, so mark one.
        fs::create_dir(root.join(".git")).expect("Failed to create .git");
        fs::write(root.join(".gitignore"), "tasks/**\n").expect("Failed to write");
        fs::write(root.join("readme.md"), "# Readme").expect("Failed to write");

        let notes = root.join("tasks").join("notes");
        fs::create_dir_all(&notes).expect("Failed to create dirs");
        fs::write(notes.join("kept.md"), "# Kept").expect("Failed to write");
        fs::write(notes.join(".gitignore"), "drafts/\n").expect("Failed to write");

        let drafts = notes.join("drafts");
        fs::create_dir(&drafts).expect("Failed to create drafts dir");
        fs::write(drafts.join("draft.md"), "# Draft").expect("Failed to write");

        temp_dir
    }

    #[test]
    fn test_scan_markdown_files_explicit_path_overrides_parent_gitignore() {
        let temp_dir = ignored_subdir_repo();
        let notes = temp_dir.path().join("tasks").join("notes");

        let result = scan_markdown_files(&notes, true).expect("Failed to scan");

        let names: Vec<_> = result
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        // Naming the directory overrides the repo's `tasks/**` rule, ...
        assert!(
            names.contains(&"kept.md"),
            "explicitly served directory should be scanned despite parent .gitignore, got {names:?}"
        );
        // ... but ignore files inside the served tree still apply.
        assert!(
            !names.contains(&"draft.md"),
            "a .gitignore inside the served directory should still be honored, got {names:?}"
        );
    }

    #[test]
    fn test_scan_markdown_files_honors_gitignore_below_the_served_directory() {
        let temp_dir = ignored_subdir_repo();

        let result = scan_markdown_files(temp_dir.path(), true).expect("Failed to scan");

        let names: Vec<_> = result
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec!["readme.md"],
            "serving the repo root keeps its own .gitignore in force"
        );
    }

    #[test]
    fn test_scan_markdown_files_case_insensitive() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("test1.md"), "# Test 1").expect("Failed to write");
        fs::write(temp_dir.path().join("test2.MD"), "# Test 2").expect("Failed to write");
        fs::write(temp_dir.path().join("test3.Md"), "# Test 3").expect("Failed to write");
        fs::write(temp_dir.path().join("test4.MARKDOWN"), "# Test 4").expect("Failed to write");

        let result = scan_markdown_files(temp_dir.path(), false).expect("Failed to scan");

        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_format_host() {
        assert_eq!(format_host("127.0.0.1", 3000), "127.0.0.1:3000");
        assert_eq!(format_host("192.168.1.1", 8080), "192.168.1.1:8080");

        assert_eq!(format_host("localhost", 3000), "localhost:3000");
        assert_eq!(format_host("example.com", 80), "example.com:80");

        assert_eq!(format_host("::1", 3000), "[::1]:3000");
        assert_eq!(format_host("2001:db8::1", 8080), "[2001:db8::1]:8080");
    }

    #[test]
    fn test_browsable_host() {
        assert_eq!(browsable_host("0.0.0.0"), "127.0.0.1");
        assert_eq!(browsable_host("::"), "::1");
        assert_eq!(browsable_host("127.0.0.1"), "127.0.0.1");
        assert_eq!(browsable_host("::1"), "::1");
        assert_eq!(browsable_host("192.168.1.1"), "192.168.1.1");
        assert_eq!(browsable_host("localhost"), "localhost");
        assert_eq!(browsable_host("example.com"), "example.com");
    }

    #[tokio::test]
    async fn test_bind_retries_on_addr_in_use() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blocked_port = listener.local_addr().unwrap().port();

        let (retry_listener, actual_port) =
            bind_with_retry("127.0.0.1", blocked_port).await.unwrap();

        assert!(
            actual_port > blocked_port,
            "Should bind to a higher port when requested port is in use"
        );

        drop(retry_listener);
        drop(listener);
    }

    use axum_test::TestServer;
    use std::time::Duration;
    use tempfile::{Builder, NamedTempFile, TempDir};

    const FILE_WATCH_DELAY_MS: u64 = 100;
    const WEBSOCKET_TIMEOUT_SECS: u64 = 5;

    const TEST_FILE_1_CONTENT: &str = "# Test 1\n\nContent of test1";
    const TEST_FILE_2_CONTENT: &str = "# Test 2\n\nContent of test2";
    const TEST_FILE_3_CONTENT: &str = "# Test 3\n\nContent of test3";
    const YAML_FRONTMATTER_CONTENT: &str =
        "---\ntitle: Test Post\nauthor: Name\n---\n\n# Test Post\n";
    const TOML_FRONTMATTER_CONTENT: &str = "+++\ntitle = \"Test Post\"\n+++\n\n# Test Post\n";

    fn create_test_server_impl(content: &str, use_http: bool) -> (TestServer, NamedTempFile) {
        let temp_file = Builder::new()
            .suffix(".md")
            .tempfile()
            .expect("Failed to create temp file");
        fs::write(&temp_file, content).expect("Failed to write temp file");

        let canonical_path = temp_file
            .path()
            .canonicalize()
            .unwrap_or_else(|_| temp_file.path().to_path_buf());

        let base_dir = canonical_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        let tracked_files = vec![canonical_path];
        let is_directory_mode = false;

        let router = new_router(
            base_dir,
            tracked_files,
            is_directory_mode,
            false,
            false,
            false,
            true,
        )
        .expect("Failed to create router");

        let server = if use_http {
            TestServer::builder()
                .http_transport()
                .build(router)
                .expect("Failed to create test server")
        } else {
            TestServer::new(router).expect("Failed to create test server")
        };

        (server, temp_file)
    }

    async fn create_test_server(content: &str) -> (TestServer, NamedTempFile) {
        create_test_server_impl(content, false)
    }

    async fn create_test_server_with_http(content: &str) -> (TestServer, NamedTempFile) {
        create_test_server_impl(content, true)
    }

    fn create_directory_server_impl(use_http: bool) -> (TestServer, TempDir) {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("test1.md"), TEST_FILE_1_CONTENT)
            .expect("Failed to write test1.md");
        fs::write(temp_dir.path().join("test2.markdown"), TEST_FILE_2_CONTENT)
            .expect("Failed to write test2.markdown");
        fs::write(temp_dir.path().join("test3.md"), TEST_FILE_3_CONTENT)
            .expect("Failed to write test3.md");

        let base_dir = temp_dir.path().to_path_buf();
        let tracked_files =
            scan_markdown_files(&base_dir, true).expect("Failed to scan markdown files");
        let is_directory_mode = true;

        let router = new_router(
            base_dir,
            tracked_files,
            is_directory_mode,
            true,
            true,
            false,
            true,
        )
        .expect("Failed to create router");

        let server = if use_http {
            TestServer::builder()
                .http_transport()
                .build(router)
                .expect("Failed to create test server")
        } else {
            TestServer::new(router).expect("Failed to create test server")
        };

        (server, temp_dir)
    }

    async fn create_directory_server() -> (TestServer, TempDir) {
        create_directory_server_impl(false)
    }

    #[tokio::test]
    async fn test_recursive_directory_serves_nested_files() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        fs::write(temp_dir.path().join("root.md"), "# Root").expect("Failed to write");

        let sub_dir = temp_dir.path().join("docs");
        fs::create_dir(&sub_dir).expect("Failed to create subdir");
        fs::write(sub_dir.join("nested.md"), "# Nested page").expect("Failed to write");

        let base_dir = temp_dir
            .path()
            .canonicalize()
            .expect("Failed to canonicalize base dir");
        let tracked_files =
            scan_markdown_files(&base_dir, true).expect("Failed to scan markdown files");
        let router = new_router(base_dir, tracked_files, true, false, true, false, true)
            .expect("Failed to create router");
        let server = TestServer::new(router).expect("Failed to create test server");

        // Nested file is addressable at its relative-path URL.
        let response = server.get("/docs/nested.md").await;
        assert_eq!(response.status_code(), 200);
        assert!(response.text().contains("Nested page"));

        // The sidebar links to it using the relative path.
        let root = server.get("/").await;
        assert_eq!(root.status_code(), 200);
        assert!(
            root.text().contains(r#"href="/docs/nested.md""#),
            "sidebar should link to the nested file by relative path"
        );
    }

    #[tokio::test]
    async fn test_background_scan_indexes_the_directory_after_startup() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        fs::write(temp_dir.path().join("root.md"), "# Root page").expect("Failed to write");
        let sub_dir = temp_dir.path().join("docs");
        fs::create_dir(&sub_dir).expect("Failed to create subdir");
        fs::write(sub_dir.join("nested.md"), "# Nested page").expect("Failed to write");

        let base_dir = temp_dir
            .path()
            .canonicalize()
            .expect("Failed to canonicalize base dir");
        // No files are handed over: the server starts empty and finds them itself.
        let router = new_router(base_dir, Vec::new(), true, false, true, true, false)
            .expect("Failed to create router");
        let server = TestServer::new(router).expect("Failed to create test server");

        // The walk emits directories in whatever order the OS lists them, so
        // wait for both files rather than treating either one as "done".
        let mut body = String::new();
        for _ in 0..100 {
            body = server.get("/").await.text();
            if body.contains(r#"href="/root.md""#) && body.contains(r#"href="/docs/nested.md""#) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(
            body.contains(r#"href="/root.md""#),
            "background scan should have indexed the directory"
        );
        assert!(
            body.contains(r#"href="/docs/nested.md""#),
            "background scan should recurse into subdirectories"
        );
        assert!(
            server
                .get("/docs/nested.md")
                .await
                .text()
                .contains("Nested page"),
            "a file found by the background scan should be servable"
        );
    }

    #[tokio::test]
    async fn test_empty_directory_serves_a_page_instead_of_failing() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_dir = temp_dir
            .path()
            .canonicalize()
            .expect("Failed to canonicalize base dir");
        let router = new_router(base_dir, Vec::new(), true, false, true, true, false)
            .expect("Failed to create router");
        let server = TestServer::new(router).expect("Failed to create test server");

        // A directory with no markdown is a live page that fills in when one is
        // written, not a startup error.
        assert_eq!(server.get("/").await.status_code(), 200);
    }

    #[test]
    fn test_empty_page_distinguishes_scanning_from_finished() {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let base_dir = temp_dir.path().to_path_buf();

        let scanning = MarkdownState::new(base_dir.clone(), Vec::new(), true, false, true, false)
            .expect("Failed to build state");
        let (status, body) = render_empty_page(&scanning);
        assert_eq!(status, StatusCode::OK);
        assert!(body.0.contains("Scanning for markdown files"));
        assert!(
            body.0.contains("data-awaiting-files"),
            "the client needs the marker to reload once the first file lands"
        );

        let finished = MarkdownState::new(base_dir, Vec::new(), true, false, false, false)
            .expect("Failed to build state");
        assert!(render_empty_page(&finished)
            .1
             .0
            .contains("No markdown files found"));
    }

    async fn create_directory_server_with_http() -> (TestServer, TempDir) {
        create_directory_server_impl(true)
    }

    #[tokio::test]
    async fn test_server_starts_and_serves_basic_markdown() {
        let (server, _temp_file) =
            create_test_server("# Hello World\n\nThis is **bold** text.").await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(body.contains("<h1 id=\"hello-world\">"));
        assert!(body.contains("<strong>bold</strong>"));
        assert!(body.contains("theme-toggle"));
        assert!(body.contains("openThemeModal"));
        assert!(body.contains("--bg-color"));
        assert!(body.contains("data-theme=\"dark\""));
    }

    #[tokio::test]
    async fn test_websocket_connection() {
        let (server, _temp_file) = create_test_server_with_http("# WebSocket Test").await;

        let response = server.get_websocket("/ws").await;
        response.assert_status_switching_protocols();
    }

    #[tokio::test]
    async fn test_file_modification_updates_via_websocket() {
        let (server, temp_file) = create_test_server_with_http("# Original Content").await;

        let mut websocket = server.get_websocket("/ws").await.into_websocket().await;

        fs::write(&temp_file, "# Modified Content").expect("Failed to modify file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let update_result = tokio::time::timeout(
            Duration::from_secs(WEBSOCKET_TIMEOUT_SECS),
            websocket.receive_json::<ServerMessage>(),
        )
        .await;

        update_result.expect("Timeout waiting for WebSocket update after file modification");
    }

    #[tokio::test]
    async fn test_server_handles_gfm_features() {
        let markdown_content = r#"# GFM Test

## Table
| Name | Age |
|------|-----|
| John | 30  |
| Jane | 25  |

## Strikethrough
~~deleted text~~

## Code block
```rust
fn main() {
    println!("Hello!");
}
```
"#;

        let (server, _temp_file) = create_test_server(markdown_content).await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(body.contains("<table>"));
        assert!(body.contains("<th>Name</th>"));
        assert!(body.contains("<td>John</td>"));
        assert!(body.contains("<del>deleted text</del>"));
        assert!(body.contains("<pre>"));
        assert!(body.contains("fn main()"));
    }

    #[tokio::test]
    async fn test_404_for_unknown_routes() {
        let (server, _temp_file) = create_test_server("# 404 Test").await;

        let response = server.get("/unknown-route").await;

        assert_eq!(response.status_code(), 404);
    }

    #[tokio::test]
    async fn test_image_serving() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        let md_content =
            "# Test with Image\n\n![Test Image](test.png)\n\nThis markdown references an image.";
        let md_path = temp_dir.path().join("test.md");
        fs::write(&md_path, md_content).expect("Failed to write markdown file");

        let png_data = vec![
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08,
            0xD7, 0x63, 0xF8, 0x0F, 0x00, 0x00, 0x01, 0x00, 0x01, 0x5C, 0xDD, 0x8D, 0xB4, 0x00,
            0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ];
        let img_path = temp_dir.path().join("test.png");
        fs::write(&img_path, png_data).expect("Failed to write image file");

        let base_dir = temp_dir.path().to_path_buf();
        let tracked_files = vec![md_path];
        let is_directory_mode = false;
        let router = new_router(
            base_dir,
            tracked_files,
            is_directory_mode,
            false,
            false,
            false,
            true,
        )
        .expect("Failed to create router");
        let server = TestServer::new(router).expect("Failed to create test server");

        let response = server.get("/").await;
        assert_eq!(response.status_code(), 200);
        let body = response.text();
        assert!(body.contains("<img src=\"test.png\" alt=\"Test Image\""));

        let img_response = server.get("/test.png").await;
        assert_eq!(img_response.status_code(), 200);
        assert_eq!(img_response.header("content-type"), "image/png");
        assert!(!img_response.as_bytes().is_empty());
    }

    // A minimal valid 1x1 PNG, shared by the image/bundle tests.
    fn tiny_png() -> Vec<u8> {
        vec![
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08,
            0xD7, 0x63, 0xF8, 0x0F, 0x00, 0x00, 0x01, 0x00, 0x01, 0x5C, 0xDD, 0x8D, 0xB4, 0x00,
            0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ]
    }

    #[tokio::test]
    async fn test_download_bundle_directory_mode() {
        // base_dir with a page that references an image, a linked .md in a
        // subdir, an external link, and a file:// link to a file OUTSIDE
        // base_dir; plus a mermaid page.
        let temp_dir = tempdir().expect("temp dir");
        let outside_dir = tempdir().expect("outside temp dir");

        let shared = outside_dir.path().join("shared.md");
        fs::write(&shared, "# Shared external doc").expect("write shared.md");
        let shared_url = format!(
            "file://{}",
            shared.canonicalize().unwrap().to_string_lossy()
        );

        let home = format!(
            "# Home\n\n![pic](img/a.png)\n\n[guide](sub/other.md)\n\n\
             [site](https://example.com)\n\n[outside]({shared_url})\n"
        );
        fs::write(temp_dir.path().join("home.md"), home).expect("write home.md");

        fs::create_dir_all(temp_dir.path().join("img")).unwrap();
        fs::write(temp_dir.path().join("img/a.png"), tiny_png()).expect("write png");

        fs::create_dir_all(temp_dir.path().join("sub")).unwrap();
        fs::write(
            temp_dir.path().join("sub/other.md"),
            "# Other\n\nback [home](../home.md)\n",
        )
        .expect("write other.md");

        fs::write(
            temp_dir.path().join("diagram.md"),
            "# Diagram\n\n```mermaid\ngraph TD\n  A --> B\n```\n",
        )
        .expect("write diagram.md");

        let base_dir = temp_dir.path().to_path_buf();
        let tracked = scan_markdown_files(&base_dir, true).expect("scan");
        let router = new_router(base_dir, tracked, true, false, true, false, true).expect("router");
        let server = TestServer::new(router).expect("test server");

        let response = server.get("/api/download").await;
        assert_eq!(response.status_code(), 200);
        assert_eq!(response.header("content-type"), "application/zip");
        assert!(response
            .header("content-disposition")
            .to_str()
            .unwrap()
            .contains("attachment; filename="));

        let bytes = response.as_bytes().to_vec();
        let mut archive =
            zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("valid zip archive");

        let mut names = Vec::new();
        let mut contents: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap();
            let name = file.name().to_string();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut file, &mut buf).unwrap();
            contents.insert(name.clone(), buf);
            names.push(name);
        }

        // Markdown rendered to HTML, nested path preserved.
        assert!(names.contains(&"home.html".to_string()), "names: {names:?}");
        assert!(
            names.contains(&"sub/other.html".to_string()),
            "names: {names:?}"
        );
        assert!(
            names.contains(&"diagram.html".to_string()),
            "names: {names:?}"
        );
        // Asset bundled byte-exact.
        assert_eq!(
            contents.get("img/a.png").map(|b| b.as_slice()),
            Some(tiny_png().as_slice())
        );
        // Outside-dir dep pulled into _external/, rendered to .html.
        let external = names
            .iter()
            .find(|n| n.starts_with("_external/") && n.ends_with("shared.html"));
        assert!(
            external.is_some(),
            "expected _external shared.html, names: {names:?}"
        );
        // Directory index generated (no tracked index.md here).
        assert!(
            names.contains(&"index.html".to_string()),
            "names: {names:?}"
        );

        // Links rewritten in home.html.
        let home_html = String::from_utf8(contents["home.html"].clone()).unwrap();
        assert!(home_html.contains("src=\"img/a.png\""), "{home_html}");
        assert!(home_html.contains("href=\"sub/other.html\""), "{home_html}");
        assert!(
            home_html.contains(&format!("href=\"{}\"", external.unwrap())),
            "outside link should point at bundled _external path"
        );
        // External link untouched.
        assert!(
            home_html.contains("href=\"https://example.com\""),
            "{home_html}"
        );
        // Bundle pages don't carry a (dead) download button.
        assert!(
            !home_html.contains("/api/download"),
            "bundle pages should not include the download button"
        );

        // Nested page link back to ../home.md rewritten to ../home.html.
        let other_html = String::from_utf8(contents["sub/other.html"].clone()).unwrap();
        assert!(other_html.contains("href=\"../home.html\""), "{other_html}");

        // Mermaid page references the shared library (not the server path, not
        // inlined), and the library is bundled once under _assets/.
        let diagram_html = String::from_utf8(contents["diagram.html"].clone()).unwrap();
        assert!(!diagram_html.contains("src=\"/mermaid.min.js\""));
        assert!(
            diagram_html.contains("src=\"_assets/mermaid.min.js\""),
            "diagram page should reference the shared mermaid lib"
        );
        // The full library is NOT inlined into the page.
        assert!(
            !diagram_html.contains(&MERMAID_JS[..120]),
            "mermaid JS must not be inlined into the page"
        );
        assert!(
            diagram_html.len() < 100_000,
            "page should be small without inlined JS"
        );
        // Shared libraries bundled once, byte-exact.
        assert_eq!(
            contents.get("_assets/mermaid.min.js").map(|b| b.as_slice()),
            Some(MERMAID_JS.as_bytes())
        );
        assert!(
            names.contains(&"_assets/panzoom.min.js".to_string()),
            "names: {names:?}"
        );
    }

    #[tokio::test]
    async fn test_download_rejects_cross_origin() {
        let temp_dir = tempdir().expect("temp dir");
        let md_path = temp_dir.path().join("notes.md");
        fs::write(&md_path, "# Notes").expect("write");
        let base_dir = temp_dir.path().to_path_buf();
        let router =
            new_router(base_dir, vec![md_path], false, false, false, false, true).expect("router");
        let server = TestServer::new(router).expect("test server");

        // Same-origin (no Origin header) is allowed.
        assert_eq!(server.get("/api/download").await.status_code(), 200);

        // A cross-origin Origin is rejected (CSRF/CORS read protection).
        let resp = server
            .get("/api/download")
            .add_header(
                axum::http::header::ORIGIN,
                axum::http::HeaderValue::from_static("http://evil.example"),
            )
            .await;
        assert_eq!(resp.status_code(), 403);
    }

    #[tokio::test]
    async fn test_download_bundle_excludes_external_when_not_loopback() {
        let temp_dir = tempdir().expect("temp dir");
        let outside_dir = tempdir().expect("outside temp dir");
        let shared = outside_dir.path().join("shared.md");
        fs::write(&shared, "# Shared").expect("write shared.md");
        let shared_url = format!(
            "file://{}",
            shared.canonicalize().unwrap().to_string_lossy()
        );

        fs::write(temp_dir.path().join("img.png"), tiny_png()).unwrap();
        let home = format!("# Home\n\n![pic](img.png)\n\n[outside]({shared_url})\n");
        fs::write(temp_dir.path().join("home.md"), &home).expect("write home.md");

        let base_dir = temp_dir.path().to_path_buf();
        let tracked = scan_markdown_files(&base_dir, true).expect("scan");
        // bundle_external = false (simulating a non-loopback bind).
        let router =
            new_router(base_dir, tracked, true, false, true, false, false).expect("router");
        let server = TestServer::new(router).expect("test server");

        let bytes = server.get("/api/download").await.as_bytes().to_vec();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("zip");
        let mut names = Vec::new();
        let mut home_html = String::new();
        for i in 0..archive.len() {
            let mut f = archive.by_index(i).unwrap();
            let name = f.name().to_string();
            if name == "home.html" {
                std::io::Read::read_to_string(&mut f, &mut home_html).unwrap();
            }
            names.push(name);
        }

        // In-base dependency still bundled...
        assert!(names.contains(&"img.png".to_string()), "names: {names:?}");
        // ...but nothing outside base_dir was collected.
        assert!(
            !names.iter().any(|n| n.starts_with("_external/")),
            "no external files expected, names: {names:?}"
        );
        // The outside link is left as-authored (not rewritten).
        assert!(
            home_html.contains("file://"),
            "outside link should be untouched"
        );
    }

    #[tokio::test]
    async fn test_download_bundle_single_file_mode() {
        let temp_dir = tempdir().expect("temp dir");
        let md_path = temp_dir.path().join("notes.md");
        fs::write(&md_path, "# Notes\n\n![pic](a.png)\n").expect("write notes.md");
        fs::write(temp_dir.path().join("a.png"), tiny_png()).expect("write png");

        let base_dir = temp_dir.path().to_path_buf();
        let router =
            new_router(base_dir, vec![md_path], false, false, false, false, true).expect("router");
        let server = TestServer::new(router).expect("test server");

        let response = server.get("/api/download").await;
        assert_eq!(response.status_code(), 200);
        // Filename derives from the file stem.
        assert!(response
            .header("content-disposition")
            .to_str()
            .unwrap()
            .contains("filename=\"notes.zip\""));

        let bytes = response.as_bytes().to_vec();
        let mut archive =
            zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("valid zip archive");
        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_string())
            .collect();

        assert!(
            names.contains(&"notes.html".to_string()),
            "names: {names:?}"
        );
        assert!(names.contains(&"a.png".to_string()), "names: {names:?}");
        // No generated index in single-file mode.
        assert!(
            !names.contains(&"index.html".to_string()),
            "names: {names:?}"
        );
    }

    #[tokio::test]
    async fn test_non_image_files_not_served() {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        let md_content = "# Test";
        let md_path = temp_dir.path().join("test.md");
        fs::write(&md_path, md_content).expect("Failed to write markdown file");

        let txt_path = temp_dir.path().join("secret.txt");
        fs::write(&txt_path, "secret content").expect("Failed to write txt file");

        let base_dir = temp_dir.path().to_path_buf();
        let tracked_files = vec![md_path];
        let is_directory_mode = false;
        let router = new_router(
            base_dir,
            tracked_files,
            is_directory_mode,
            false,
            false,
            false,
            true,
        )
        .expect("Failed to create router");
        let server = TestServer::new(router).expect("Failed to create test server");

        let response = server.get("/secret.txt").await;
        assert_eq!(response.status_code(), 404);
    }

    #[tokio::test]
    async fn test_html_tags_in_markdown_are_rendered() {
        let markdown_content = r#"# HTML Test

This markdown contains HTML tags:

<div class="highlight">
    <p>This should be rendered as HTML, not escaped</p>
    <span style="color: red;">Red text</span>
</div>

Regular **markdown** still works.
"#;

        let (server, _temp_file) = create_test_server(markdown_content).await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(body.contains(r#"<div class="highlight">"#));
        assert!(body.contains(r#"<span style="color: red;">"#));
        assert!(body.contains("<p>This should be rendered as HTML, not escaped</p>"));
        assert!(!body.contains("&lt;div"));
        assert!(!body.contains("&gt;"));
        assert!(body.contains("<strong>markdown</strong>"));
    }

    #[tokio::test]
    async fn test_mermaid_diagram_detection_and_script_injection() {
        let markdown_content = r#"# Mermaid Test

Regular content here.

```mermaid
graph TD
    A[Start] --> B{Decision}
    B -->|Yes| C[End]
    B -->|No| D[Continue]
```

More regular content.

```javascript
// This is a regular code block, not mermaid
console.log("Hello World");
```
"#;

        let (server, _temp_file) = create_test_server(markdown_content).await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(body.contains(r#"class="language-mermaid""#));
        assert!(body.contains("graph TD"));

        let has_raw_content = body.contains("A[Start] --> B{Decision}");
        let has_encoded_content = body.contains("A[Start] --&gt; B{Decision}");
        assert!(
            has_raw_content || has_encoded_content,
            "Expected mermaid content not found in body"
        );

        assert!(body.contains(r#"<script src="/mermaid.min.js"></script>"#));
        assert!(body.contains(r#"<script src="/panzoom.min.js"></script>"#));
        assert!(body.contains("function initMermaid()"));
        assert!(body.contains("function transformMermaidCodeBlocks()"));
        assert!(body.contains("function getMermaidTheme()"));
        assert!(body.contains(r#"class="language-javascript""#));
        assert!(body.contains("console.log"));
    }

    #[tokio::test]
    async fn test_mermaid_error_endpoint_accepts_report() {
        let (server, _temp_file) = create_test_server("# Mermaid Error Test").await;

        let response = server
            .post("/api/mermaid-error")
            .json(&serde_json::json!({
                "id": "mermaid-0",
                "source": "graph TD\n  A --> ",
                "message": "Parse error on line 2"
            }))
            .await;

        assert_eq!(response.status_code(), 204);
    }

    #[tokio::test]
    async fn test_mermaid_error_endpoint_requires_message() {
        let (server, _temp_file) = create_test_server("# Mermaid Error Test").await;

        // Missing the required `message` field should be rejected.
        let response = server
            .post("/api/mermaid-error")
            .json(&serde_json::json!({ "id": "mermaid-0" }))
            .await;

        assert!(
            response.status_code().is_client_error(),
            "expected 4xx for missing message, got {}",
            response.status_code()
        );
    }

    #[tokio::test]
    async fn test_no_mermaid_script_injection_without_mermaid_blocks() {
        let markdown_content = r#"# No Mermaid Test

This content has no mermaid diagrams.

```javascript
console.log("Hello World");
```

```bash
echo "Regular code block"
```

Just regular markdown content.
"#;

        let (server, _temp_file) = create_test_server(markdown_content).await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(!body.contains(r#"<script src="https://cdn.jsdelivr.net/npm/mermaid@11.12.0/dist/mermaid.min.js"></script>"#));
        assert!(body.contains("function initMermaid()"));
        assert!(body.contains(r#"class="language-javascript""#));
        assert!(body.contains(r#"class="language-bash""#));
    }

    #[tokio::test]
    async fn test_multiple_mermaid_diagrams() {
        let markdown_content = r#"# Multiple Mermaid Diagrams

## Flowchart
```mermaid
graph LR
    A --> B
```

## Sequence Diagram
```mermaid
sequenceDiagram
    Alice->>Bob: Hello
    Bob-->>Alice: Hi
```

## Class Diagram
```mermaid
classDiagram
    Animal <|-- Duck
```
"#;

        let (server, _temp_file) = create_test_server(markdown_content).await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        let mermaid_occurrences = body.matches(r#"class="language-mermaid""#).count();
        assert_eq!(mermaid_occurrences, 3);

        assert!(body.contains("graph LR"));
        assert!(body.contains("sequenceDiagram"));
        assert!(body.contains("classDiagram"));

        assert!(body.contains("A --&gt; B") || body.contains("A --> B"));
        assert!(body.contains("Alice-&gt;&gt;Bob") || body.contains("Alice->>Bob"));
        assert!(body.contains("Animal &lt;|-- Duck") || body.contains("Animal <|-- Duck"));

        let script_occurrences = body
            .matches(r#"<script src="/mermaid.min.js"></script>"#)
            .count();
        assert_eq!(script_occurrences, 1);
    }

    #[tokio::test]
    async fn test_panzoom_js_etag_caching() {
        let (server, _temp_file) = create_test_server("# Test").await;

        let response = server.get("/panzoom.min.js").await;
        assert_eq!(response.status_code(), 200);

        let etag = response.header("etag");
        assert!(!etag.is_empty(), "ETag header should be present");

        let content_type = response.header("content-type");
        assert_eq!(content_type, "application/javascript");
        assert!(!response.as_bytes().is_empty());

        let response_304 = server
            .get("/panzoom.min.js")
            .add_header(
                axum::http::header::IF_NONE_MATCH,
                axum::http::HeaderValue::from_str(etag.to_str().unwrap()).unwrap(),
            )
            .await;
        assert_eq!(response_304.status_code(), 304);
        assert!(response_304.as_bytes().is_empty());
    }

    #[tokio::test]
    async fn test_mermaid_js_etag_caching() {
        let (server, _temp_file) = create_test_server("# Test").await;

        let response = server.get("/mermaid.min.js").await;
        assert_eq!(response.status_code(), 200);

        let etag = response.header("etag");
        assert!(!etag.is_empty(), "ETag header should be present");

        let cache_control = response.header("cache-control");
        let cache_control_str = cache_control.to_str().unwrap();
        assert!(cache_control_str.contains("public"));
        assert!(cache_control_str.contains("no-cache"));

        let content_type = response.header("content-type");
        assert_eq!(content_type, "application/javascript");

        assert!(!response.as_bytes().is_empty());

        let response_304 = server
            .get("/mermaid.min.js")
            .add_header(
                axum::http::header::IF_NONE_MATCH,
                axum::http::HeaderValue::from_str(etag.to_str().unwrap()).unwrap(),
            )
            .await;

        assert_eq!(response_304.status_code(), 304);
        assert_eq!(response_304.header("etag"), etag);
        assert!(response_304.as_bytes().is_empty());

        let response_200 = server
            .get("/mermaid.min.js")
            .add_header(
                axum::http::header::IF_NONE_MATCH,
                axum::http::HeaderValue::from_static("\"different-etag\""),
            )
            .await;

        assert_eq!(response_200.status_code(), 200);
        assert!(!response_200.as_bytes().is_empty());
    }

    #[tokio::test]
    async fn test_standalone_inlines_mermaid_and_panzoom_js() {
        let content = "# Standalone\n\n```mermaid\ngraph LR\n  A --> B\n```\n";

        let temp_file = Builder::new().suffix(".md").tempfile().unwrap();
        fs::write(&temp_file, content).unwrap();
        let canonical_path = temp_file
            .path()
            .canonicalize()
            .unwrap_or_else(|_| temp_file.path().to_path_buf());
        let base_dir = canonical_path.parent().unwrap().to_path_buf();
        let router = new_router(
            base_dir,
            vec![canonical_path],
            false,
            true,
            false,
            false,
            true,
        )
        .unwrap();
        let server = TestServer::new(router).unwrap();

        let body = server.get("/").await.text();

        assert!(
            !body.contains(r#"<script src="/mermaid.min.js"></script>"#),
            "standalone mode should not emit external mermaid script tag"
        );
        assert!(
            !body.contains(r#"<script src="/panzoom.min.js"></script>"#),
            "standalone mode should not emit external panzoom script tag"
        );
        assert!(
            body.contains(MERMAID_JS),
            "mermaid.min.js content should be inlined in the HTML body"
        );
        assert!(
            body.contains(PANZOOM_JS),
            "panzoom.min.js content should be inlined in the HTML body"
        );
        // The live server still offers the download button under --standalone;
        // it's a server affordance, not gated by the standalone flag.
        assert!(
            body.contains("/api/download"),
            "download button should be present even in standalone mode"
        );
    }

    #[tokio::test]
    async fn test_live_reload_skips_non_http_protocols() {
        let (server, _temp_file) = create_test_server("# Test").await;
        let body = server.get("/").await.text();
        // The page must guard against opening a WebSocket when loaded from
        // a saved file (file:// or other non-http schemes).
        assert!(
            body.contains("window.location.protocol !== 'http:'"),
            "live reload must check the page protocol before connecting"
        );
    }

    #[tokio::test]
    async fn test_standalone_no_inlining_without_mermaid() {
        let content = "# No diagrams here\n\nJust plain text.\n";

        let temp_file = Builder::new().suffix(".md").tempfile().unwrap();
        fs::write(&temp_file, content).unwrap();
        let canonical_path = temp_file
            .path()
            .canonicalize()
            .unwrap_or_else(|_| temp_file.path().to_path_buf());
        let base_dir = canonical_path.parent().unwrap().to_path_buf();
        let router = new_router(
            base_dir,
            vec![canonical_path],
            false,
            true,
            false,
            false,
            true,
        )
        .unwrap();
        let server = TestServer::new(router).unwrap();

        let body = server.get("/").await.text();

        assert!(
            !body.contains(MERMAID_JS),
            "mermaid.min.js should not be inlined when page has no mermaid blocks"
        );
        assert!(
            !body.contains(PANZOOM_JS),
            "panzoom.min.js should not be inlined when page has no mermaid blocks"
        );
    }

    #[tokio::test]
    async fn test_directory_mode_serves_multiple_files() {
        let (server, _temp_dir) = create_directory_server().await;

        let response1 = server.get("/test1.md").await;
        assert_eq!(response1.status_code(), 200);
        let body1 = response1.text();
        assert!(body1.contains("<h1 id=\"test-1\">"));
        assert!(body1.contains("Content of test1"));

        let response2 = server.get("/test2.markdown").await;
        assert_eq!(response2.status_code(), 200);
        let body2 = response2.text();
        assert!(body2.contains("<h1 id=\"test-2\">"));
        assert!(body2.contains("Content of test2"));

        let response3 = server.get("/test3.md").await;
        assert_eq!(response3.status_code(), 200);
        let body3 = response3.text();
        assert!(body3.contains("<h1 id=\"test-3\">"));
        assert!(body3.contains("Content of test3"));
    }

    #[tokio::test]
    async fn test_directory_mode_file_not_found() {
        let (server, _temp_dir) = create_directory_server().await;

        let response = server.get("/nonexistent.md").await;
        assert_eq!(response.status_code(), 404);
    }

    #[tokio::test]
    async fn test_directory_mode_has_navigation_sidebar() {
        let (server, _temp_dir) = create_directory_server().await;

        let response = server.get("/test1.md").await;
        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(body.contains(r#"<nav class="sidebar">"#));
        assert!(body.contains(r#"<ul class="file-list">"#));
        assert!(body.contains("test1.md"));
        assert!(body.contains("test2.markdown"));
        assert!(body.contains("test3.md"));
    }

    #[tokio::test]
    async fn test_single_file_mode_no_navigation_sidebar() {
        let (server, _temp_file) = create_test_server("# Single File Test").await;

        let response = server.get("/").await;
        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(!body.contains(r#"<nav class="sidebar">"#));
        assert!(!body.contains("<h3>Files</h3>"));
        assert!(!body.contains(r#"<ul class="file-list">"#));
    }

    #[tokio::test]
    async fn test_directory_mode_active_file_highlighting() {
        let (server, _temp_dir) = create_directory_server().await;

        let response1 = server.get("/test1.md").await;
        assert_eq!(response1.status_code(), 200);
        let body1 = response1.text();

        assert!(
            body1.contains(r#"href="/test1.md" class="active""#),
            "test1.md link should have href and class on same line"
        );

        let active_link_count = body1.matches(r#"class="active""#).count();
        assert_eq!(active_link_count, 1, "Should have exactly one active link");

        let response2 = server.get("/test2.markdown").await;
        assert_eq!(response2.status_code(), 200);
        let body2 = response2.text();

        assert!(
            body2.contains(r#"href="/test2.markdown" class="active""#),
            "test2.markdown link should have href and class on same line"
        );
    }

    #[tokio::test]
    async fn test_directory_mode_file_order() {
        let (server, _temp_dir) = create_directory_server().await;

        let response = server.get("/test1.md").await;
        assert_eq!(response.status_code(), 200);
        let body = response.text();

        let test1_pos = body.find("test1.md").expect("test1.md not found");
        let test2_pos = body
            .find("test2.markdown")
            .expect("test2.markdown not found");
        let test3_pos = body.find("test3.md").expect("test3.md not found");

        assert!(
            test1_pos < test2_pos,
            "test1.md should appear before test2.markdown"
        );
        assert!(
            test2_pos < test3_pos,
            "test2.markdown should appear before test3.md"
        );
    }

    #[tokio::test]
    async fn test_directory_mode_websocket_file_modification() {
        let (server, temp_dir) = create_directory_server_with_http().await;

        let mut websocket = server.get_websocket("/ws").await.into_websocket().await;

        let test_file = temp_dir.path().join("test1.md");
        fs::write(&test_file, "# Modified Test 1\n\nContent has changed")
            .expect("Failed to modify file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let update_result = tokio::time::timeout(
            Duration::from_secs(WEBSOCKET_TIMEOUT_SECS),
            websocket.receive_json::<ServerMessage>(),
        )
        .await;

        update_result.expect("Timeout waiting for WebSocket update after file modification");
    }

    #[tokio::test]
    async fn test_directory_mode_new_file_triggers_reload() {
        let (server, temp_dir) = create_directory_server_with_http().await;

        let mut websocket = server.get_websocket("/ws").await.into_websocket().await;

        let new_file = temp_dir.path().join("test4.md");
        fs::write(&new_file, "# Test 4\n\nThis is a new file").expect("Failed to create new file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let update_result = tokio::time::timeout(
            Duration::from_secs(WEBSOCKET_TIMEOUT_SECS),
            websocket.receive_json::<ServerMessage>(),
        )
        .await;

        let message =
            update_result.expect("Timeout waiting for WebSocket update after new file creation");

        // A new file leaves the open page untouched, so clients get the file
        // list to refresh their sidebar rather than a full reload.
        match message {
            ServerMessage::Files { files, scanning } => {
                assert!(files.contains(&"test4.md".to_string()), "got {files:?}");
                assert!(!scanning, "the initial scan is not running in this server");
            }
            other => panic!("expected a file-list update, got {other:?}"),
        }

        let response = server.get("/test1.md").await;
        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(
            body.contains("test4.md"),
            "New file should appear in navigation"
        );

        let new_file_response = server.get("/test4.md").await;
        assert_eq!(new_file_response.status_code(), 200);
        let new_file_body = new_file_response.text();
        assert!(new_file_body.contains("<h1 id=\"test-4\">"));
        assert!(new_file_body.contains("This is a new file"));
    }

    #[tokio::test]
    async fn test_editor_save_simulation_single_file_mode() {
        let (server, temp_file) =
            create_test_server_with_http("# Original\n\nOriginal content").await;

        let file_path = temp_file.path().to_path_buf();
        let backup_path = file_path.with_extension("md~");

        let initial_response = server.get("/").await;
        assert_eq!(initial_response.status_code(), 200);
        assert!(initial_response.text().contains("Original content"));

        fs::rename(&file_path, &backup_path).expect("Failed to rename to backup");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let during_save_response = server.get("/").await;
        assert_eq!(
            during_save_response.status_code(),
            200,
            "File should not return 404 during editor save"
        );

        fs::write(&file_path, "# Updated\n\nUpdated content").expect("Failed to write new file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let final_response = server.get("/").await;
        assert_eq!(final_response.status_code(), 200);
        let final_body = final_response.text();
        assert!(
            final_body.contains("Updated content"),
            "Should serve updated content after save"
        );
        assert!(
            !final_body.contains("Original content"),
            "Should not serve old content"
        );

        let _ = fs::remove_file(&backup_path);
    }

    #[tokio::test]
    async fn test_editor_save_simulation_directory_mode() {
        let (server, temp_dir) = create_directory_server_with_http().await;

        let file_path = temp_dir.path().join("test1.md");
        let backup_path = temp_dir.path().join("test1.md~");

        let initial_response = server.get("/test1.md").await;
        assert_eq!(initial_response.status_code(), 200);
        assert!(initial_response.text().contains("Content of test1"));

        fs::rename(&file_path, &backup_path).expect("Failed to rename to backup");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let during_save_response = server.get("/test1.md").await;
        assert_eq!(
            during_save_response.status_code(),
            200,
            "File should not return 404 during editor save in directory mode"
        );

        fs::write(&file_path, "# Test 1 Updated\n\nUpdated content")
            .expect("Failed to write new file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let final_response = server.get("/test1.md").await;
        assert_eq!(final_response.status_code(), 200);
        let final_body = final_response.text();
        assert!(
            final_body.contains("Updated content"),
            "Should serve updated content after save"
        );

        let _ = fs::remove_file(&backup_path);
    }

    #[tokio::test]
    async fn test_no_404_during_editor_save_sequence() {
        let (server, temp_dir) = create_directory_server_with_http().await;
        let mut websocket = server.get_websocket("/ws").await.into_websocket().await;

        let file_path = temp_dir.path().join("test1.md");
        let backup_path = temp_dir.path().join("test1.md~");

        fs::rename(&file_path, &backup_path).expect("Failed to rename to backup");
        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let response_after_rename = server.get("/test1.md").await;
        assert_eq!(
            response_after_rename.status_code(),
            200,
            "Should not get 404 after rename to backup"
        );

        fs::write(&file_path, "# Test 1 Updated\n\nNew content").expect("Failed to write new file");
        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let response_after_create = server.get("/test1.md").await;
        assert_eq!(
            response_after_create.status_code(),
            200,
            "Should successfully serve after new file created"
        );
        assert!(response_after_create.text().contains("New content"));

        let update_result = tokio::time::timeout(
            Duration::from_secs(WEBSOCKET_TIMEOUT_SECS),
            websocket.receive_json::<ServerMessage>(),
        )
        .await;

        assert!(update_result.is_ok(), "Should receive reload after save");

        let _ = fs::remove_file(&backup_path);
    }

    #[tokio::test]
    async fn test_yaml_frontmatter_is_stripped() {
        let (server, _temp_file) = create_test_server(YAML_FRONTMATTER_CONTENT).await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(!body.contains("title: Test Post"));
        assert!(!body.contains("author: Name"));
        assert!(body.contains("<h1 id=\"test-post\">"));
    }

    #[tokio::test]
    async fn test_toml_frontmatter_is_stripped() {
        let (server, _temp_file) = create_test_server(TOML_FRONTMATTER_CONTENT).await;

        let response = server.get("/").await;

        assert_eq!(response.status_code(), 200);
        let body = response.text();

        assert!(!body.contains("title = \"Test Post\""));
        assert!(body.contains("<h1 id=\"test-post\">"));
    }

    #[tokio::test]
    async fn test_temp_file_rename_triggers_reload_single_file_mode() {
        let (server, temp_file) =
            create_test_server_with_http("# Original\n\nOriginal content").await;

        let mut websocket = server.get_websocket("/ws").await.into_websocket().await;

        let file_path = temp_file.path().to_path_buf();
        let temp_write_path = file_path.with_extension("md.tmp.12345");

        let initial_response = server.get("/").await;
        assert_eq!(initial_response.status_code(), 200);
        assert!(
            initial_response.text().contains("Original content"),
            "File should be tracked and serving content before edit"
        );

        fs::write(
            &temp_write_path,
            "# Updated\n\nUpdated content via temp file",
        )
        .expect("Failed to write temp file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        fs::rename(&temp_write_path, &file_path).expect("Failed to rename temp file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let update_result = tokio::time::timeout(
            Duration::from_secs(WEBSOCKET_TIMEOUT_SECS),
            websocket.receive_json::<ServerMessage>(),
        )
        .await;

        update_result.expect("Timeout waiting for WebSocket update after temp file rename");

        let final_response = server.get("/").await;
        assert_eq!(final_response.status_code(), 200);
        let final_body = final_response.text();
        assert!(
            final_body.contains("Updated content via temp file"),
            "Should serve updated content after temp file rename"
        );
        assert!(
            !final_body.contains("Original content"),
            "Should not serve old content"
        );
    }

    #[tokio::test]
    async fn test_temp_file_rename_triggers_reload_directory_mode() {
        let (server, temp_dir) = create_directory_server_with_http().await;

        let mut websocket = server.get_websocket("/ws").await.into_websocket().await;

        let file_path = temp_dir.path().join("test1.md");
        let temp_write_path = temp_dir.path().join("test1.md.tmp.67890");

        let initial_response = server.get("/test1.md").await;
        assert_eq!(initial_response.status_code(), 200);
        assert!(
            initial_response.text().contains("Content of test1"),
            "File should be tracked and serving content before edit"
        );

        fs::write(
            &temp_write_path,
            "# Test 1 Updated\n\nUpdated via temp file rename",
        )
        .expect("Failed to write temp file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        fs::rename(&temp_write_path, &file_path).expect("Failed to rename temp file");

        tokio::time::sleep(Duration::from_millis(FILE_WATCH_DELAY_MS)).await;

        let update_result = tokio::time::timeout(
            Duration::from_secs(WEBSOCKET_TIMEOUT_SECS),
            websocket.receive_json::<ServerMessage>(),
        )
        .await;

        update_result.expect(
            "Timeout waiting for WebSocket update after temp file rename in directory mode",
        );

        let final_response = server.get("/test1.md").await;
        assert_eq!(final_response.status_code(), 200);
        let final_body = final_response.text();
        assert!(
            final_body.contains("Updated via temp file rename"),
            "Should serve updated content after temp file rename"
        );
        assert!(
            !final_body.contains("Content of test1"),
            "Should not serve old content"
        );
    }
}
