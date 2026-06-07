use regex::Regex;
use std::sync::OnceLock;

fn normalize_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"'[^']*'"#).unwrap())
}

pub fn normalize_query(query: &str) -> String {
    normalize_re().replace_all(query, "'?'").into_owned()
}
