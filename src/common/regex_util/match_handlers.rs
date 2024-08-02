use crate::common::bytes_util::{FastRegexMatcher, FastStringMatcher};
use crate::common::regex_util::regex_utils::{skip_first_and_last_char, skip_first_char, skip_last_char};

pub type MatchFn = fn(pattern: &str, candidate: &str) -> bool;

#[derive(Clone, Debug, PartialEq, Hash, Eq)]
pub(crate) enum StringMatchHandler {
    Fsm(FastStringMatcher),
    FastRegex(FastRegexMatcher),
    Alternates(Vec<String>),
    MatchFn(MatchFnHandler),
}

impl Default for StringMatchHandler {
    fn default() -> Self {
        Self::dot_plus()
    }
}

impl StringMatchHandler {
    pub fn literal<T: Into<String>>(value: T) -> Self {
        Self::MatchFn(MatchFnHandler::new(value, matches_literal))
    }
    pub fn literal_mismatch<T: Into<String>>(value: T) -> Self {
        Self::MatchFn(MatchFnHandler::new(value, mismatches_literal))
    }

    pub fn alternates(alts: Vec<String>) -> Self {
        Self::Alternates(alts)
    }

    /// handler for .*
    pub fn dot_star() -> Self {
        Self::MatchFn(MatchFnHandler::new("", dot_star))
    }

    /// handler for .+
    pub fn dot_plus() -> Self {
        Self::MatchFn(MatchFnHandler::new("", dot_plus))
    }

    pub fn match_fn(pattern: String, match_fn: MatchFn) -> Self {
        Self::MatchFn(MatchFnHandler::new(pattern, match_fn))
    }

    pub fn starts_with<T: Into<String>>(prefix: T) -> Self {
        Self::MatchFn(MatchFnHandler::new(prefix.into(), starts_with))
    }

    pub fn contains<T: Into<String>>(needle: T) -> Self {
        Self::MatchFn(MatchFnHandler::new(needle.into(), contains))
    }

    pub fn prefix<T: Into<String>>(prefix: T, is_dot_star: bool) -> Self {
        Self::MatchFn(MatchFnHandler::new(prefix, if is_dot_star { prefix_dot_star } else { prefix_dot_plus }))
    }

    pub fn not_prefix<T: Into<String>>(prefix: T, is_dot_star: bool) -> Self {
        Self::MatchFn(MatchFnHandler::new(prefix, if is_dot_star { not_prefix_dot_star } else { not_prefix_dot_plus }))
    }

    pub fn suffix<T: Into<String>>(suffix: T, is_dot_star: bool) -> Self {
        Self::MatchFn(MatchFnHandler::new(suffix, if is_dot_star { suffix_dot_star } else { suffix_dot_plus }))
    }

    pub fn not_suffix<T: Into<String>>(suffix: T, is_dot_star: bool) -> Self {
        Self::MatchFn(MatchFnHandler::new(suffix, if is_dot_star { not_suffix_dot_star } else { not_suffix_dot_plus }))
    }

    pub(super) fn middle(prefix: &'static str, pattern: String, suffix: &'static str) -> Self {
        match (prefix, suffix) {
            (".+", ".+") => Self::match_fn(pattern, dot_plus_dot_plus),
            (".*", ".*") => Self::match_fn(pattern, dot_star_dot_star),
            (".*", ".+") => Self::match_fn(pattern, dot_star_dot_plus),
            (".+", ".*") => Self::match_fn(pattern, dot_plus_dot_star),
            _ => unreachable!("Invalid prefix and suffix combination"),
        }
    }

    pub(super) fn not_middle(prefix: &'static str, pattern: String, suffix: &'static str) -> Self {
        match (prefix, suffix) {
            (".+", ".+") => Self::match_fn(pattern, not_dot_plus_dot_plus),
            (".+", ".*") => Self::match_fn(pattern, not_dot_plus_dot_star),
            (".*", ".+") => Self::match_fn(pattern, not_dot_star_dot_plus),
            (".*", ".*") => Self::match_fn(pattern, not_dot_star_dot_star),
            _ => unreachable!("Invalid prefix and suffix combination"),
        }
    }

    pub fn matches(&self, s: &str) -> bool {
        match self {
            StringMatchHandler::Alternates(alts) => matches_alternates(&alts, s),
            StringMatchHandler::Fsm(fsm) => fsm.matches(s),
            StringMatchHandler::MatchFn(m) => m.matches(s),
            StringMatchHandler::FastRegex(r) => r.matches(s),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MatchFnHandler {
    pattern: String,
    pub(super) match_fn: MatchFn,
}

impl MatchFnHandler {
    pub(super) fn new<T: Into<String>>(pattern: T, match_fn: MatchFn) -> Self {
        Self {
            pattern: pattern.into(),
            match_fn,
        }
    }

    pub(super) fn matches(&self, s: &str) -> bool {
        (self.match_fn)(&self.pattern, s)
    }
}

fn starts_with(prefix: &str, candidate: &str) -> bool {
    candidate.starts_with(prefix)
}

fn contains(prefix: &str, candidate: &str) -> bool {
    candidate.contains(prefix)
}

fn matches_alternates(or_values: &[String], s: &str) -> bool {
    or_values.iter().any(|v| v == s)
}

fn matches_literal(prefix: &str, candidate: &str) -> bool {
    prefix == candidate
}

fn mismatches_literal(prefix: &str, candidate: &str) -> bool {
    prefix != candidate
}

// .*
fn dot_star(_: &str, _: &str) -> bool {
    true
}

// .+
fn dot_plus(_: &str, candidate: &str) -> bool {
    candidate.len() > 0
}

// prefix + '.*'
fn prefix_dot_star(prefix: &str, candidate: &str) -> bool {
    // Fast path - the pr contains "prefix.*"
    return candidate.starts_with(prefix);
}

fn not_prefix_dot_star(prefix: &str, candidate: &str) -> bool {
    !candidate.starts_with(prefix)
}

// prefix.+'
fn prefix_dot_plus(prefix: &str, candidate: &str) -> bool {
    // dot plus
    candidate.len() > prefix.len() && candidate.starts_with(prefix)
}

fn not_prefix_dot_plus(prefix: &str, candidate: &str) -> bool {
    candidate.len() <= prefix.len() || !candidate.starts_with(prefix)
}

// suffix.*'
fn suffix_dot_star(suffix: &str, candidate: &str) -> bool {
    // Fast path - the pr contains "prefix.*"
    candidate.ends_with(suffix)
}

fn not_suffix_dot_star(suffix: &str, candidate: &str) -> bool {
    !candidate.ends_with(suffix)
}

// suffix.+'
fn suffix_dot_plus(suffix: &str, candidate: &str) -> bool {
    // dot plus
    if candidate.len() > suffix.len() {
        let temp = skip_last_char(candidate);
        temp == suffix
    } else {
        false
    }
}


fn not_suffix_dot_plus(suffix: &str, candidate: &str) -> bool {
    if candidate.len() <= suffix.len() {
        true
    } else {
        let temp = skip_last_char(candidate);
        temp != suffix
    }
}

fn dot_star_dot_star(pattern: &str, candidate: &str) -> bool {
    candidate.contains(pattern)
}

fn not_dot_star_dot_star(pattern: &str, candidate: &str) -> bool {
    !candidate.contains(pattern)
}

// '.+middle.*'
fn dot_plus_dot_star(pattern: &str, candidate: &str) -> bool {
    if candidate.len() > pattern.len() {
        let temp = skip_first_char(candidate);
        temp.contains(pattern)
    } else {
        false
    }
}

fn not_dot_plus_dot_star(pattern: &str, candidate: &str) -> bool {
    if candidate.len() <= pattern.len() {
        true
    } else {
        let temp = skip_first_char(candidate);
        !temp.contains(pattern)
    }
}

// '.*middle.+'
fn dot_star_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() > pattern.len() {
        let temp = skip_last_char(candidate);
        temp.contains(pattern)
    } else {
        false
    }
}

fn not_dot_star_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() <= pattern.len() {
        true
    } else {
        let temp = skip_last_char(candidate);
        !temp.contains(pattern)
    }
}

// '.+middle.+'
fn dot_plus_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() > pattern.len() + 1 {
        let sub = skip_first_and_last_char(candidate);
        sub.contains(pattern)
    } else {
        false
    }
}

fn not_dot_plus_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() <= pattern.len() + 1 {
        true
    } else {
        let sub = skip_first_and_last_char(candidate);
        !sub.contains(pattern)
    }
}