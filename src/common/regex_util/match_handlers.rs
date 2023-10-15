use crate::common::bytes_util::{FastRegexMatcher, FastStringMatcher};
use crate::common::regex_util::regex_utils::{skip_first_and_last_char, skip_first_char, skip_last_char};

pub type StringPredicate = fn(b: &str) -> bool;
pub type MatchFn = fn(pattern: &str, candidate: &str) -> bool;

#[derive(Clone, Debug, Default)]
pub(crate) enum StringMatchHandler {
    #[default]
    DotStar,
    DotPlus,
    Contains(String),
    Fsm(FastStringMatcher),
    FastRegex(FastRegexMatcher),
    Predicate(StringPredicate),
    Literal(String),
    Alternates(Vec<String>),
    MatchFn(MatchFnHandler),
}

impl StringMatchHandler {
    pub fn literal(value: String) -> Self {
        Self::Literal(value)
    }

    pub fn literal_mismatch(value: String) -> Self {
        Self::MatchFn(MatchFnHandler::new(value, mismatches_literal))
    }

    pub fn always_false() -> Self {
        Self::Predicate(|_| false)
    }

    pub fn always_true() -> Self {
        Self::Predicate(|_| true)
    }

    pub fn match_fn(pattern: String, match_fn: MatchFn) -> Self {
        Self::MatchFn(MatchFnHandler::new(pattern, match_fn))
    }

    pub fn starts_with(prefix: String) -> Self {
        Self::MatchFn(MatchFnHandler::new(prefix, starts_with))
    }

    pub fn prefix(prefix: String, is_dot_star: bool) -> Self {
        Self::MatchFn(MatchFnHandler::new(prefix, if is_dot_star { prefix_dot_star } else { prefix_dot_plus }))
    }

    pub fn not_prefix(prefix: String, is_dot_star: bool) -> Self {
        Self::MatchFn(MatchFnHandler::new(prefix, if is_dot_star { not_prefix_dot_star } else { not_prefix_dot_plus }))
    }

    pub fn suffix(suffix: String, is_dot_star: bool) -> Self {
        Self::MatchFn(MatchFnHandler::new(suffix, if is_dot_star { suffix_dot_star } else { suffix_dot_plus }))
    }

    pub fn not_suffix(suffix: String, is_dot_star: bool) -> Self {
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
            (".*", ".*") => Self::match_fn(pattern, not_dot_star_dot_star),
            (".*", ".+") => Self::match_fn(pattern, not_dot_star_dot_plus),
            (".+", ".*") => Self::match_fn(pattern, not_dot_plus_dot_star),
            _ => unreachable!("Invalid prefix and suffix combination"),
        }
    }

    pub fn matches(&self, s: &str) -> bool {
        match self {
            StringMatchHandler::Literal(lit) => lit.as_str() == s,
            StringMatchHandler::Alternates(alts) => matches_alternates(&alts, s),
            StringMatchHandler::MatchFn(m) => m.matches(s),
            StringMatchHandler::Predicate(p) => p(s),
            StringMatchHandler::DotStar => true,
            StringMatchHandler::DotPlus => s.len() > 0,
            StringMatchHandler::Fsm(fsm) => fsm.matches(s),
            StringMatchHandler::Contains(v) => s.contains(v),
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
    pub(super) fn new(pattern: String, match_fn: MatchFn) -> Self {
        Self {
            pattern,
            match_fn,
        }
    }

    pub(super) fn matches(&self, s: &str) -> bool {
        (self.match_fn)(&self.pattern, s)
    }
}

#[allow(unused)]
fn starts_with(prefix: &str, candidate: &str) -> bool {
    candidate.starts_with(prefix)
}

#[allow(unused)]
fn matches_alternates(or_values: &[String], s: &str) -> bool {
    or_values.iter().any(|v| v == s)
}

#[allow(unused)]
fn matches_literal(prefix: &str, candidate: &str) -> bool {
    prefix == candidate
}

#[allow(unused)]
fn mismatches_literal(prefix: &str, candidate: &str) -> bool {
    prefix != candidate
}

// prefix + '.*'
#[allow(unused)]
fn prefix_dot_star(prefix: &str, candidate: &str) -> bool {
    // Fast path - the pr contains "prefix.*"
    return candidate.starts_with(prefix);
}

#[allow(unused)]
fn not_prefix_dot_star(prefix: &str, candidate: &str) -> bool {
    !candidate.starts_with(prefix)
}

// prefix.+'
#[allow(unused)]
fn prefix_dot_plus(prefix: &str, candidate: &str) -> bool {
    // dot plus
    candidate.len() > prefix.len() && candidate.starts_with(prefix)
}

#[allow(unused)]
fn not_prefix_dot_plus(prefix: &str, candidate: &str) -> bool {
    candidate.len() <= prefix.len() || !candidate.starts_with(prefix)
}

// suffix.*'
#[allow(unused)]
fn suffix_dot_star(suffix: &str, candidate: &str) -> bool {
    // Fast path - the pr contains "prefix.*"
    candidate.ends_with(suffix)
}

#[allow(unused)]
fn not_suffix_dot_star(suffix: &str, candidate: &str) -> bool {
    !candidate.ends_with(suffix)
}

// suffix.+'
#[allow(unused)]
fn suffix_dot_plus(suffix: &str, candidate: &str) -> bool {
    // dot plus
    if candidate.len() > suffix.len() {
        let temp = skip_last_char(candidate);
        temp == suffix
    } else {
        false
    }
}

#[allow(unused)]
fn not_suffix_dot_plus(suffix: &str, candidate: &str) -> bool {
    if candidate.len() <= suffix.len() {
        true
    } else {
        let temp = skip_last_char(candidate);
        temp != suffix
    }
}

#[allow(unused)]
fn dot_star_dot_star(pattern: &str, candidate: &str) -> bool {
    candidate.contains(pattern)
}

#[allow(unused)]
fn not_dot_star_dot_star(pattern: &str, candidate: &str) -> bool {
    !candidate.contains(pattern)
}

// '.+middle.*'
#[allow(unused)]
fn dot_plus_dot_star(pattern: &str, candidate: &str) -> bool {
    if candidate.len() > pattern.len() {
        let temp = skip_first_char(candidate);
        temp.contains(pattern)
    } else {
        false
    }
}

#[allow(unused)]
fn not_dot_plus_dot_star(pattern: &str, candidate: &str) -> bool {
    if candidate.len() <= pattern.len() {
        true
    } else {
        let temp = skip_first_char(candidate);
        !temp.contains(pattern)
    }
}

// '.*middle.+'
#[allow(unused)]
fn dot_star_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() > pattern.len() {
        let temp = skip_last_char(candidate);
        temp.contains(pattern)
    } else {
        false
    }
}

#[allow(unused)]
fn not_dot_star_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() <= pattern.len() {
        true
    } else {
        let temp = skip_last_char(candidate);
        !temp.contains(pattern)
    }
}

// '.+middle.+'
#[allow(unused)]
fn dot_plus_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() > pattern.len() + 1 {
        let sub = skip_first_and_last_char(candidate);
        sub.contains(pattern)
    } else {
        false
    }
}

#[allow(unused)]
fn not_dot_plus_dot_plus(pattern: &str, candidate: &str) -> bool {
    if candidate.len() <= pattern.len() + 1 {
        true
    } else {
        let sub = skip_first_and_last_char(candidate);
        !sub.contains(pattern)
    }
}