//! Images a page links from above the served directory.
//!
//! A relative image path that climbs out of `base_dir` (`../../shots/a.png`
//! from `mdserve docs/design.md`) has no URL on this server: the browser
//! clamps `..` at `/`, and the static route only reads inside `base_dir`. So,
//! on loopback binds only, the rendered HTML points those images at
//! `/_mdserve/up/<levels>/<rest>`, where `<levels>` is how many directories
//! above `base_dir` the image's path branches off. Each rewritten image is
//! recorded, and the route serves recorded images only, so it can't be used to
//! read anything the served markdown doesn't already display.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::app::is_image_file;
use crate::bundle::{
    encode_href, is_windows_drive_path, percent_decode, rewrite_attr_refs, split_suffix, url_scheme,
};

/// URL prefix of the outside-image route, followed by `/<levels>/<rest>`.
pub(crate) const ROUTE_PREFIX: &str = "/_mdserve/up";

/// A page's HTML with its outside images rewritten, plus the canonical paths
/// of those images: the only files the outside-image route serves for it.
pub(crate) struct Rewritten {
    pub html: String,
    pub images: HashSet<PathBuf>,
}

/// Points every relative image reference in `html` that resolves outside
/// `base_dir` at the outside-image route. `doc_dir` is the directory of the
/// markdown file the HTML was rendered from; `base_dir` must be canonical.
/// References that don't resolve to an existing file are left as authored.
pub(crate) fn rewrite_outside_images(html: &str, doc_dir: &Path, base_dir: &Path) -> Rewritten {
    let mut images = HashSet::new();
    let html = rewrite_attr_refs(html, |value| {
        if !is_relative_ref(value) {
            return None;
        }
        let (path, suffix) = split_suffix(value);
        let decoded = percent_decode(path);
        if !is_image_file(&decoded) {
            return None;
        }
        let target = doc_dir.join(&decoded).canonicalize().ok()?;
        if target.starts_with(base_dir) {
            return None;
        }
        let url = outside_url(base_dir, &target)?;
        images.insert(target);
        Some(format!("{url}{suffix}"))
    });
    Rewritten { html, images }
}

/// Maps a request on the outside-image route back to a canonical path, which
/// the caller must still check against the recorded images: `rest` comes from
/// the client and may hold `..` or be absolute.
pub(crate) fn resolve_request(base_dir: &Path, levels: usize, rest: &str) -> Option<PathBuf> {
    base_dir
        .ancestors()
        .nth(levels)?
        .join(rest)
        .canonicalize()
        .ok()
}

/// Whether an (HTML-unescaped) attribute value is a path relative to the page,
/// as opposed to a URL with a scheme, a root-relative or absolute path, or a
/// same-page anchor.
fn is_relative_ref(value: &str) -> bool {
    let value = value.trim();
    !(value.is_empty()
        || value.starts_with(['/', '\\', '#', '?'])
        || is_windows_drive_path(value)
        || url_scheme(value).is_some())
}

/// The outside-image URL for `target`, which lies outside `base_dir`. None when
/// the two share no ancestor (different Windows drives).
fn outside_url(base_dir: &Path, target: &Path) -> Option<String> {
    let common = base_dir.ancestors().find(|dir| target.starts_with(dir))?;
    let levels = base_dir.components().count() - common.components().count();
    let rest = target
        .strip_prefix(common)
        .ok()?
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    Some(format!("{ROUTE_PREFIX}/{levels}/{}", encode_href(&rest)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// `<root>/repo/tasks/ux` is served; images live in `<root>/repo/shots`.
    fn layout() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let root = tempdir().expect("temp dir");
        let base = root.path().join("repo/tasks/ux");
        let shots = root.path().join("repo/shots");
        fs::create_dir_all(&base).unwrap();
        fs::create_dir_all(&shots).unwrap();
        fs::write(shots.join("a b.png"), b"png").unwrap();
        fs::write(base.join("in.png"), b"png").unwrap();
        let base = base.canonicalize().unwrap();
        let shots = shots.canonicalize().unwrap();
        (root, base, shots)
    }

    #[test]
    fn rewrites_only_images_that_escape_base_dir() {
        let (_root, base, shots) = layout();
        let html = concat!(
            r#"<img src="../../shots/a%20b.png?v=1" alt="x">"#,
            r#"<img src="in.png">"#,
            r#"<img src="../../shots/missing.png">"#,
            r#"<img src="https://example.com/a.png">"#,
            r#"<a href="../../shots/notes.md">n</a>"#,
        );

        let out = rewrite_outside_images(html, &base, &base);

        assert!(
            out.html
                .contains(r#"<img src="/_mdserve/up/2/shots/a%20b.png?v=1" alt="x">"#),
            "{}",
            out.html
        );
        assert!(out.html.contains(r#"<img src="in.png">"#));
        assert!(out.html.contains(r#"<img src="../../shots/missing.png">"#));
        assert!(out
            .html
            .contains(r#"<img src="https://example.com/a.png">"#));
        assert!(out.html.contains(r#"<a href="../../shots/notes.md">"#));
        assert_eq!(out.images, HashSet::from([shots.join("a b.png")]));
    }

    #[test]
    fn resolve_request_inverts_the_rewritten_url() {
        let (_root, base, shots) = layout();
        assert_eq!(
            resolve_request(&base, 2, "shots/a b.png"),
            Some(shots.join("a b.png"))
        );
        assert_eq!(resolve_request(&base, 2, "shots/missing.png"), None);
        assert_eq!(resolve_request(&base, 999, "a.png"), None);
    }

    #[test]
    fn relative_ref_classification() {
        assert!(is_relative_ref("a.png"));
        assert!(is_relative_ref("../a.png"));
        assert!(is_relative_ref("./dir/a.png"));
        assert!(!is_relative_ref("/a.png"));
        assert!(!is_relative_ref("//cdn/a.png"));
        assert!(!is_relative_ref("#top"));
        assert!(!is_relative_ref("data:image/png;base64,xx"));
        assert!(!is_relative_ref("file:///a.png"));
        assert!(!is_relative_ref(r"C:\a.png"));
    }
}
