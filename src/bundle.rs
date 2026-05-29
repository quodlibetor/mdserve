//! Offline bundle builder.
//!
//! Produces a self-contained zip of the served markdown: every tracked file is
//! rendered to a standalone HTML page (mermaid/panzoom inlined like
//! `--standalone`), local dependencies (images, PDFs, linked `.md`, ...) are
//! collected recursively, and links are rewritten in the rendered HTML so the
//! unzipped bundle works offline by double-clicking.
//!
//! Discovery and rewriting both operate on the rendered HTML's `href`/`src`
//! attribute values: every markdown link/image/autolink/reference becomes an
//! attribute in the rendered output, so the HTML is the single source of truth.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, CONTROLS};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

use crate::app::{is_markdown_file, markdown_to_html_bundle, relative_key};

/// Hard caps so a pathological link graph can't exhaust memory/disk.
const MAX_FILES: usize = 5_000;
const MAX_TOTAL_BYTES: u64 = 512 * 1024 * 1024;

/// Where the shared mermaid/panzoom libraries live in the bundle. They are
/// written once (when any page uses mermaid) and referenced relatively by each
/// page, so a bundle with many diagram pages stays small.
pub(crate) const BUNDLE_MERMAID_JS_PATH: &str = "_assets/mermaid.min.js";
pub(crate) const BUNDLE_PANZOOM_JS_PATH: &str = "_assets/panzoom.min.js";

/// Characters that must be percent-encoded in a rewritten href/src path. `/` is
/// intentionally left as-is so path structure is preserved.
const PATH_ENC: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'\'')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'\\')
    .add(b'%')
    .add(b'#')
    .add(b'?');

/// A root markdown document to bundle: its absolute path plus the raw markdown
/// source (already in memory on the server).
pub(crate) struct RootDoc {
    pub abs_path: PathBuf,
    pub source: String,
}

/// One file destined for the zip.
pub(crate) struct BundleEntry {
    pub zip_path: String,
    pub bytes: Vec<u8>,
}

/// Build the zip for `roots` and their recursively-collected local deps.
/// `render` turns an (html_body, title) into a full standalone page.
pub(crate) fn build_zip(
    base_dir: &Path,
    roots: &[RootDoc],
    is_directory_mode: bool,
    include_external: bool,
    render: fn(&str, &str, &str) -> String,
) -> Result<Vec<u8>> {
    let entries = build_entries(base_dir, roots, is_directory_mode, include_external, render);
    zip_entries(&entries)
}

/// Relative path back to the bundle root from a page's location, e.g. "" for a
/// root-level page or "../" for `sub/page.html`.
fn asset_prefix(zip_path: &str) -> String {
    "../".repeat(zip_path.matches('/').count())
}

// ---------------------------------------------------------------------------
// URL classification / resolution
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum UrlClass {
    /// A web/non-file URL or same-page anchor — left untouched, never bundled.
    External,
    /// A relative path, absolute filesystem path, or `file://` URL — bundled.
    Local,
}

/// Classify a (HTML-unescaped) attribute value.
fn classify_url(raw: &str) -> UrlClass {
    let t = raw.trim();
    if t.is_empty() || t.starts_with('#') || t.starts_with("//") {
        return UrlClass::External;
    }
    // A single-letter "scheme" followed by a path separator is a Windows drive
    // path (e.g. `C:\dir\f.png`), not a URL scheme.
    if is_windows_drive_path(t) {
        return UrlClass::Local;
    }
    match url_scheme(t) {
        Some(scheme) if scheme == "file" => UrlClass::Local,
        Some(_) => UrlClass::External, // http(s), mailto, tel, data, javascript, ...
        None => UrlClass::Local,       // relative or absolute filesystem path
    }
}

fn is_windows_drive_path(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/')
}

/// Returns the lowercased URL scheme if `raw` begins with `scheme:` where scheme
/// matches `[A-Za-z][A-Za-z0-9+.-]*`. Returns None for relative paths.
fn url_scheme(raw: &str) -> Option<String> {
    let colon = raw.find(':')?;
    let scheme = &raw[..colon];
    if scheme.is_empty() {
        return None;
    }
    let mut chars = scheme.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')) {
        Some(scheme.to_ascii_lowercase())
    } else {
        None
    }
}

/// Split off a trailing `?query` and/or `#fragment` (whichever comes first),
/// returning (path, suffix). The suffix is re-appended after rewriting.
fn split_suffix(raw: &str) -> (&str, &str) {
    let cut = match (raw.find('?'), raw.find('#')) {
        (Some(q), Some(f)) => Some(q.min(f)),
        (Some(q), None) => Some(q),
        (None, Some(f)) => Some(f),
        (None, None) => None,
    };
    match cut {
        Some(i) => (&raw[..i], &raw[i..]),
        None => (raw, ""),
    }
}

fn percent_decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

/// Strip a `file:` scheme, returning the (still percent-encoded) path portion.
/// Handles `file:///abs`, `file://host/abs`, and `file:/abs`.
fn strip_file_scheme(raw: &str) -> Option<String> {
    if raw.len() < 5 || !raw[..5].eq_ignore_ascii_case("file:") {
        return None;
    }
    let rest = &raw[5..];
    if let Some(after) = rest.strip_prefix("//") {
        if after.starts_with('/') {
            return Some(after.to_string()); // file:///abs -> /abs
        }
        // file://host/abs -> /abs ; file://host (no path) -> none
        return after.find('/').map(|i| after[i..].to_string());
    }
    Some(rest.to_string()) // file:/abs -> /abs
}

/// Resolve a local (path-only, no suffix) reference to a canonical file path.
/// Relative paths resolve against `referrer_dir`. Returns None if it doesn't
/// exist or can't be canonicalized.
fn resolve_local(referrer_dir: &Path, path: &str) -> Option<PathBuf> {
    let candidate = if let Some(file_path) = strip_file_scheme(path) {
        PathBuf::from(percent_decode(&file_path))
    } else {
        let decoded = percent_decode(path);
        let p = Path::new(&decoded);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            referrer_dir.join(p)
        }
    };
    candidate.canonicalize().ok()
}

// ---------------------------------------------------------------------------
// Zip path assignment
// ---------------------------------------------------------------------------

/// Assign the in-zip path for an absolute file path. Files inside `base_dir`
/// mirror their relative path; files outside go under `_external/<full-path>`
/// (the full path keeps distinct sources from colliding). Markdown becomes
/// `.html`.
fn zip_path_for(base_dir: &Path, abs: &Path) -> String {
    let rel = if abs.starts_with(base_dir) {
        relative_key(base_dir, abs)
    } else {
        let s = abs.to_string_lossy();
        let sanitized: String = s
            .chars()
            .map(|c| match c {
                '\\' => '/',
                ':' => '_',
                other => other,
            })
            .collect();
        format!("_external/{}", sanitized.trim_start_matches('/'))
    };
    if is_markdown_file(abs) {
        swap_ext_to_html(&rel)
    } else {
        rel
    }
}

/// Reserve a unique zip path, disambiguating with a numeric suffix
/// (`name-1.ext`) if `candidate` is already taken. Distinct source files must
/// never share a zip entry name or the `ZipWriter` would error.
fn unique_zip_path(candidate: String, used: &mut HashSet<String>) -> String {
    if used.insert(candidate.clone()) {
        return candidate;
    }
    let last_slash = candidate.rfind('/').map(|i| i + 1).unwrap_or(0);
    let (base, ext) = match candidate[last_slash..].rfind('.') {
        Some(dot) => {
            let abs_dot = last_slash + dot;
            (&candidate[..abs_dot], Some(&candidate[abs_dot + 1..]))
        }
        None => (candidate.as_str(), None),
    };
    for n in 1.. {
        let next = match ext {
            Some(e) => format!("{base}-{n}.{e}"),
            None => format!("{base}-{n}"),
        };
        if used.insert(next.clone()) {
            return next;
        }
    }
    unreachable!("suffix search always terminates")
}

fn swap_ext_to_html(path: &str) -> String {
    Path::new(path)
        .with_extension("html")
        .to_string_lossy()
        .replace('\\', "/")
}

/// Compute a relative path from one zip entry to another, as forward-slash path
/// math (`../` up to the common ancestor, then down).
fn rel_zip_path(from_page: &str, to_target: &str) -> String {
    let from: Vec<&str> = from_page.split('/').collect();
    let from_dirs = &from[..from.len().saturating_sub(1)];
    let to: Vec<&str> = to_target.split('/').collect();

    let mut i = 0;
    while i < from_dirs.len() && i + 1 < to.len() && from_dirs[i] == to[i] {
        i += 1;
    }
    let mut out = String::new();
    for _ in 0..(from_dirs.len() - i) {
        out.push_str("../");
    }
    out.push_str(&to[i..].join("/"));
    out
}

// ---------------------------------------------------------------------------
// HTML attribute scanning / rewriting
// ---------------------------------------------------------------------------

struct AttrRef {
    attr: &'static str,
    quote: char,
    value: String,
}

/// Find all `href`/`src` attribute values in `html` (both quote styles). The
/// attribute name must be at an attribute boundary (preceded by whitespace, `<`,
/// or start) so we don't match inside `data-src=`, `xlink:href=`, etc.
fn find_attr_refs(html: &str) -> Vec<AttrRef> {
    let mut out = Vec::new();
    for attr in ["href", "src"] {
        for quote in ['"', '\''] {
            let needle = format!("{attr}={quote}");
            let mut from = 0;
            while let Some(rel) = html[from..].find(&needle) {
                let idx = from + rel;
                let start = idx + needle.len();
                let boundary_ok = idx == 0
                    || html[..idx]
                        .chars()
                        .next_back()
                        .is_some_and(|c| c.is_whitespace() || c == '<');
                match html[start..].find(quote) {
                    Some(end_rel) => {
                        if boundary_ok {
                            out.push(AttrRef {
                                attr,
                                quote,
                                value: html[start..start + end_rel].to_string(),
                            });
                        }
                        from = start + end_rel + 1;
                    }
                    None => break,
                }
            }
        }
    }
    out
}

fn html_unescape(s: &str) -> String {
    // `&amp;` is replaced last so we don't double-unescape entities.
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&#x27;", "'")
        .replace("&#x2f;", "/")
        .replace("&#x2F;", "/")
        .replace("&amp;", "&")
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn encode_href(path: &str) -> String {
    utf8_percent_encode(path, PATH_ENC).to_string()
}

/// Rewrite local `href`/`src` values in a rendered HTML body so they point at
/// the bundled relative paths. `visited` maps canonical source path -> zip path.
fn rewrite_html_links(
    body: &str,
    page_zip: &str,
    referrer_dir: &Path,
    visited: &HashMap<PathBuf, String>,
) -> String {
    let mut result = body.to_string();
    let mut applied: Vec<(String, String)> = Vec::new();

    for attr_ref in find_attr_refs(body) {
        let unescaped = html_unescape(&attr_ref.value);
        if classify_url(&unescaped) != UrlClass::Local {
            continue;
        }
        let (path, suffix) = split_suffix(&unescaped);
        let Some(target_abs) = resolve_local(referrer_dir, path) else {
            continue;
        };
        let Some(target_zip) = visited.get(&target_abs) else {
            continue;
        };
        let rel = encode_href(&rel_zip_path(page_zip, target_zip));
        let replacement = format!("{rel}{suffix}");

        let old = format!(
            "{}={}{}{}",
            attr_ref.attr, attr_ref.quote, attr_ref.value, attr_ref.quote
        );
        let new = format!(
            "{}={}{}{}",
            attr_ref.attr,
            attr_ref.quote,
            html_escape(&replacement),
            attr_ref.quote
        );
        if old != new && !applied.iter().any(|(o, _)| o == &old) {
            applied.push((old, new));
        }
    }

    for (old, new) in applied {
        result = result.replace(&old, &new);
    }
    result
}

// ---------------------------------------------------------------------------
// Dependency walk
// ---------------------------------------------------------------------------

struct PageWork {
    abs: PathBuf,
    zip_path: String,
    body: String,
    title: String,
}

fn title_for(abs: &Path) -> String {
    abs.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("document")
        .to_string()
}

fn build_entries(
    base_dir: &Path,
    roots: &[RootDoc],
    is_directory_mode: bool,
    include_external: bool,
    render: fn(&str, &str, &str) -> String,
) -> Vec<BundleEntry> {
    // canonical source path -> zip path
    let mut visited: HashMap<PathBuf, String> = HashMap::new();
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    let mut pages: Vec<PageWork> = Vec::new();
    let mut assets: Vec<(String, PathBuf)> = Vec::new();

    // Track every assigned zip path so distinct sources never collide. Reserve
    // the shared-asset names up front so they always belong to the bundled
    // libraries (a user file of the same name is disambiguated instead).
    let mut used: HashSet<String> = HashSet::new();
    used.insert(BUNDLE_MERMAID_JS_PATH.to_string());
    used.insert(BUNDLE_PANZOOM_JS_PATH.to_string());

    // Root sources are already in memory; followed deps are read from disk.
    let mut sources: HashMap<PathBuf, String> = HashMap::new();
    for root in roots {
        if let Ok(canonical) = root.abs_path.canonicalize() {
            sources.insert(canonical.clone(), root.source.clone());
            if !visited.contains_key(&canonical) {
                let zip_path = unique_zip_path(zip_path_for(base_dir, &canonical), &mut used);
                visited.insert(canonical.clone(), zip_path);
                queue.push_back(canonical);
            }
        }
    }
    // Stable order so index/links are deterministic.
    let mut roots_in_order: Vec<PathBuf> = visited.keys().cloned().collect();
    roots_in_order.sort();

    while let Some(abs) = queue.pop_front() {
        let zip_path = visited.get(&abs).cloned().unwrap_or_default();

        if !is_markdown_file(&abs) {
            assets.push((zip_path, abs));
            continue;
        }

        let source = match sources.get(&abs) {
            Some(s) => s.clone(),
            None => match fs::read_to_string(&abs) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("bundle: skipping {} ({e})", abs.display());
                    continue;
                }
            },
        };
        let body = markdown_to_html_bundle(&source).unwrap_or_default();
        let referrer_dir = abs.parent().unwrap_or(base_dir).to_path_buf();

        for attr_ref in find_attr_refs(&body) {
            let unescaped = html_unescape(&attr_ref.value);
            if classify_url(&unescaped) != UrlClass::Local {
                continue;
            }
            let (path, _suffix) = split_suffix(&unescaped);
            if let Some(dep) = resolve_local(&referrer_dir, path) {
                // When external collection is disabled (non-loopback bind), only
                // bundle dependencies that live inside the served directory, so
                // a network client can't pull arbitrary local files.
                if !include_external && !dep.starts_with(base_dir) {
                    continue;
                }
                if !visited.contains_key(&dep) {
                    if visited.len() >= MAX_FILES {
                        eprintln!("bundle: reached MAX_FILES ({MAX_FILES}); some deps omitted");
                        break;
                    }
                    let zip_path = unique_zip_path(zip_path_for(base_dir, &dep), &mut used);
                    visited.insert(dep.clone(), zip_path);
                    queue.push_back(dep);
                }
            }
        }

        pages.push(PageWork {
            title: title_for(&abs),
            abs,
            zip_path,
            body,
        });
    }

    // Phase 2: rewrite links and render pages; copy asset bytes.
    let mut entries: Vec<BundleEntry> = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut any_mermaid = false;

    for page in &pages {
        if page.body.contains(r#"class="language-mermaid""#) {
            any_mermaid = true;
        }
        let referrer_dir = page.abs.parent().unwrap_or(base_dir).to_path_buf();
        let rewritten = rewrite_html_links(&page.body, &page.zip_path, &referrer_dir, &visited);
        let full = render(&rewritten, &page.title, &asset_prefix(&page.zip_path));
        total_bytes += full.len() as u64;
        entries.push(BundleEntry {
            zip_path: page.zip_path.clone(),
            bytes: full.into_bytes(),
        });
    }

    // Bundle the shared mermaid/panzoom libraries once if any page needs them.
    if any_mermaid {
        entries.push(BundleEntry {
            zip_path: BUNDLE_MERMAID_JS_PATH.to_string(),
            bytes: crate::app::MERMAID_JS.as_bytes().to_vec(),
        });
        entries.push(BundleEntry {
            zip_path: BUNDLE_PANZOOM_JS_PATH.to_string(),
            bytes: crate::app::PANZOOM_JS.as_bytes().to_vec(),
        });
    }

    for (zip_path, abs) in &assets {
        // Only bundle regular files, and check the size from metadata *before*
        // reading so a huge file or a special file (FIFO/device) can't exhaust
        // memory.
        let metadata = match fs::metadata(abs) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("bundle: skipping asset {} ({e})", abs.display());
                continue;
            }
        };
        if !metadata.is_file() {
            eprintln!("bundle: skipping non-regular file {}", abs.display());
            continue;
        }
        if total_bytes.saturating_add(metadata.len()) > MAX_TOTAL_BYTES {
            eprintln!("bundle: reached MAX_TOTAL_BYTES; some assets omitted");
            break;
        }
        match fs::read(abs) {
            Ok(bytes) => {
                total_bytes += bytes.len() as u64;
                entries.push(BundleEntry {
                    zip_path: zip_path.clone(),
                    bytes,
                });
            }
            Err(e) => eprintln!("bundle: skipping asset {} ({e})", abs.display()),
        }
    }

    // Generate a root index in directory mode, unless a tracked file already
    // owns `index.html` (e.g. an `index.md`), in which case that page is the
    // natural entry point.
    if is_directory_mode && !entries.iter().any(|e| e.zip_path == "index.html") {
        let body = build_index_body(&roots_in_order, &visited);
        let full = render(&body, "Index", "");
        entries.push(BundleEntry {
            zip_path: "index.html".to_string(),
            bytes: full.into_bytes(),
        });
    }

    entries
}

/// Build the HTML body for the directory-mode `index.html`: a list linking to
/// each tracked page's bundled `.html`.
fn build_index_body(roots_in_order: &[PathBuf], visited: &HashMap<PathBuf, String>) -> String {
    let mut body = String::from("<h1>Contents</h1>\n<ul>\n");
    for abs in roots_in_order {
        if let Some(zip_path) = visited.get(abs) {
            let href = encode_href(zip_path);
            body.push_str(&format!(
                "<li><a href=\"{}\">{}</a></li>\n",
                html_escape(&href),
                html_escape(zip_path)
            ));
        }
    }
    body.push_str("</ul>\n");
    body
}

// ---------------------------------------------------------------------------
// Zip serialization
// ---------------------------------------------------------------------------

/// Already-compressed file types are stored without recompression.
fn is_precompressed(zip_path: &str) -> bool {
    let ext = Path::new(zip_path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        ext.as_str(),
        "png"
            | "jpg"
            | "jpeg"
            | "gif"
            | "webp"
            | "ico"
            | "pdf"
            | "zip"
            | "gz"
            | "mp4"
            | "webm"
            | "woff"
            | "woff2"
    )
}

fn zip_entries(entries: &[BundleEntry]) -> Result<Vec<u8>> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for entry in entries {
        let method = if is_precompressed(&entry.zip_path) {
            CompressionMethod::Stored
        } else {
            CompressionMethod::Deflated
        };
        let options = SimpleFileOptions::default().compression_method(method);
        writer.start_file(entry.zip_path.as_str(), options)?;
        writer.write_all(&entry.bytes)?;
    }
    Ok(writer.finish()?.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn classify_url_external_vs_local() {
        for ext in [
            "https://example.com",
            "http://x",
            "mailto:a@b.com",
            "tel:+1",
            "data:image/png;base64,AAAA",
            "//cdn/x.js",
            "#section",
        ] {
            assert_eq!(classify_url(ext), UrlClass::External, "{ext}");
        }
        for loc in [
            "img.png",
            "sub/a.md",
            "/abs/x.pdf",
            "file:///x",
            "../up.md",
            r"C:\dir\f.png", // windows drive path, not a URL scheme
            "D:/dir/f.png",
        ] {
            assert_eq!(classify_url(loc), UrlClass::Local, "{loc}");
        }
    }

    #[test]
    fn find_attr_refs_respects_attribute_boundaries() {
        let html = r#"<img src="a.png"><div data-src="b.png"></div><use xlink:href="c.svg"/><a href="d.html">x</a>"#;
        let vals: Vec<String> = find_attr_refs(html).into_iter().map(|r| r.value).collect();
        assert!(vals.contains(&"a.png".to_string()), "{vals:?}");
        assert!(vals.contains(&"d.html".to_string()), "{vals:?}");
        // data-src and xlink:href must NOT be picked up as src/href.
        assert!(!vals.contains(&"b.png".to_string()), "{vals:?}");
        assert!(!vals.contains(&"c.svg".to_string()), "{vals:?}");
    }

    #[test]
    fn unique_zip_path_disambiguates_collisions() {
        let mut used = HashSet::new();
        assert_eq!(
            unique_zip_path("a/b.html".to_string(), &mut used),
            "a/b.html"
        );
        assert_eq!(
            unique_zip_path("a/b.html".to_string(), &mut used),
            "a/b-1.html"
        );
        assert_eq!(
            unique_zip_path("a/b.html".to_string(), &mut used),
            "a/b-2.html"
        );
        // No extension.
        assert_eq!(unique_zip_path("noext".to_string(), &mut used), "noext");
        assert_eq!(unique_zip_path("noext".to_string(), &mut used), "noext-1");
        // A dot in a directory name, not the file, is handled.
        assert_eq!(unique_zip_path("a.b/c".to_string(), &mut used), "a.b/c");
        assert_eq!(unique_zip_path("a.b/c".to_string(), &mut used), "a.b/c-1");
    }

    #[test]
    fn split_suffix_keeps_fragment_and_query() {
        assert_eq!(split_suffix("b.md#sec"), ("b.md", "#sec"));
        assert_eq!(split_suffix("a.md?x=1"), ("a.md", "?x=1"));
        assert_eq!(split_suffix("a.md?x=1#f"), ("a.md", "?x=1#f"));
        assert_eq!(split_suffix("plain.png"), ("plain.png", ""));
    }

    #[test]
    fn strip_file_scheme_forms() {
        assert_eq!(strip_file_scheme("file:///a/b").as_deref(), Some("/a/b"));
        assert_eq!(
            strip_file_scheme("file://localhost/a/b").as_deref(),
            Some("/a/b")
        );
        assert_eq!(strip_file_scheme("file:/a/b").as_deref(), Some("/a/b"));
        assert_eq!(strip_file_scheme("relative/a"), None);
    }

    #[test]
    fn percent_decode_spaces() {
        assert_eq!(percent_decode("a%20b.png"), "a b.png");
    }

    #[test]
    fn resolve_local_relative_and_file_scheme() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        let target = sub.join("a b.png");
        fs::write(&target, b"x").unwrap();
        let canonical = target.canonicalize().unwrap();

        // relative to referrer dir, percent-encoded space
        assert_eq!(resolve_local(&sub, "a%20b.png"), Some(canonical.clone()));
        // file:// absolute
        let url = format!("file://{}", canonical.to_string_lossy());
        assert_eq!(resolve_local(dir.path(), &url), Some(canonical));
        // missing
        assert_eq!(resolve_local(&sub, "missing.png"), None);
    }

    #[test]
    fn zip_path_for_inside_and_outside() {
        let dir = tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let inside_md = base.join("docs/guide.md");
        fs::create_dir_all(inside_md.parent().unwrap()).unwrap();
        fs::write(&inside_md, b"# x").unwrap();
        let inside_png = base.join("img/a.png");
        fs::create_dir_all(inside_png.parent().unwrap()).unwrap();
        fs::write(&inside_png, b"x").unwrap();

        assert_eq!(
            zip_path_for(&base, &inside_md.canonicalize().unwrap()),
            "docs/guide.html"
        );
        assert_eq!(
            zip_path_for(&base, &inside_png.canonicalize().unwrap()),
            "img/a.png"
        );

        // outside base -> _external/<full path>, md -> .html
        let outside = dir.path().parent().unwrap().join("outside-xyz.md");
        let zp = zip_path_for(&base, &outside);
        assert!(zp.starts_with("_external/"), "{zp}");
        assert!(zp.ends_with("outside-xyz.html"), "{zp}");
    }

    #[test]
    fn asset_prefix_by_depth() {
        assert_eq!(asset_prefix("index.html"), "");
        assert_eq!(asset_prefix("sub/other.html"), "../");
        assert_eq!(asset_prefix("_external/a/b/c.html"), "../../../");
    }

    #[test]
    fn rel_zip_path_cases() {
        assert_eq!(rel_zip_path("docs/guide.html", "img/a.png"), "../img/a.png");
        assert_eq!(rel_zip_path("index.html", "img/a.png"), "img/a.png");
        assert_eq!(rel_zip_path("docs/guide.html", "docs/api.html"), "api.html");
        assert_eq!(rel_zip_path("a/b/c.html", "x.png"), "../../x.png");
    }

    #[test]
    fn rewrite_rewrites_local_keeps_external() {
        let dir = tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let img = base.join("img/a.png");
        fs::create_dir_all(img.parent().unwrap()).unwrap();
        fs::write(&img, b"x").unwrap();
        let other = base.join("other.md");
        fs::write(&other, b"# other").unwrap();

        let img_c = img.canonicalize().unwrap();
        let other_c = other.canonicalize().unwrap();
        let mut visited = HashMap::new();
        visited.insert(img_c, "img/a.png".to_string());
        visited.insert(other_c, "other.html".to_string());

        let body = concat!(
            "<img src=\"img/a.png\" alt=\"img/a.png\">",
            "<a href=\"other.md#sec\">go</a>",
            "<a href=\"https://example.com\">ext</a>"
        );
        let out = rewrite_html_links(body, "index.html", &base, &visited);

        // image src rewritten, but the same string in alt text is untouched
        assert!(out.contains("src=\"img/a.png\""));
        assert!(out.contains("alt=\"img/a.png\""));
        // link to .md rewritten to .html, fragment preserved
        assert!(out.contains("href=\"other.html#sec\""), "{out}");
        // external left intact
        assert!(out.contains("href=\"https://example.com\""));
    }

    #[test]
    fn zip_entries_roundtrip() {
        let entries = vec![
            BundleEntry {
                zip_path: "index.html".to_string(),
                bytes: b"<html>hi</html>".to_vec(),
            },
            BundleEntry {
                zip_path: "img/a.png".to_string(),
                bytes: vec![1, 2, 3, 4],
            },
        ];
        let bytes = zip_entries(&entries).unwrap();
        let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_string())
            .collect();
        assert!(names.contains(&"index.html".to_string()));
        assert!(names.contains(&"img/a.png".to_string()));
    }
}
