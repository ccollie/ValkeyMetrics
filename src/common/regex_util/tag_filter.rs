use super::regexp_cache::{RegexpCache, RegexpCacheValue};
use crate::common::METRIC_NAME_LABEL;
use get_size::GetSize;
use metricsql_common::regex_util::match_handlers::StringMatchHandler;
use metricsql_common::regex_util::{get_optimized_re_match_func, FULL_MATCH_COST};
use metricsql_parser::label::{LabelFilter, LabelFilterOp};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::mem::size_of;
use std::sync::{Arc, LazyLock, OnceLock};

// todo: read from env
static USE_REGEXP_CACHE: LazyLock<bool> = LazyLock::new(|| false);

/// TagFilters represents filters used for filtering tags.
#[derive(Clone, Default, Debug)]
pub struct TagFilters(pub Vec<TagFilter>);

impl TagFilters {
    pub fn new(filters: Vec<TagFilter>) -> Self {
        let mut filters = filters;
        filters.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Self(filters)
    }

    pub fn add_label_filters(&mut self, filters: &[LabelFilter]) -> Result<(), String> {
        for filter in filters.iter() {
            self.add_label_filter(filter)?;
        }
        self.sort();
        Ok(())
    }

    pub fn add_label_filter(&mut self, filter: &LabelFilter) -> Result<(), String> {
        match filter.op {
            LabelFilterOp::Equal=> {
                self.add(&filter.label, &filter.value, false, false)
            }
            LabelFilterOp::NotEqual => {
                self.add(&filter.label, &filter.value, true, false)
            }
            LabelFilterOp::RegexEqual => {
                self.add(&filter.label, &filter.value, false, true)
            }
            LabelFilterOp::RegexNotEqual => {
                self.add(&filter.label, &filter.value, true, true)
            }
        }
    }

    pub fn is_match(&self, b: &str) -> bool {
        // todo: should sort first
        self.0.iter().all(|tf| tf.matches(b))
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn get(&self, index: usize) -> Option<&TagFilter> {
        self.0.get(index)
    }
    pub fn sort(&mut self) {
        self.0.sort_by(|a, b| a.partial_cmp(b).unwrap());
    }

    /// Adds the given tag filter.
    ///
    /// metric_group must be encoded with nil key.
    pub fn add(
        &mut self,
        key: &str,
        value: &str,
        is_negative: bool,
        is_regexp: bool,
    ) -> Result<(), String> {
        let mut is_negative = is_negative;
        let mut is_regexp = is_regexp;

        let mut value_ = value;
        // Verify whether tag filter is empty.
        if value.is_empty() {
            // Substitute an empty tag value with the negative match of `.+` regexp in order to
            // filter out all the values with the given tag.
            is_negative = !is_negative;
            is_regexp = true;
            value_ = ".+";
        }
        if is_regexp && value == ".*" {
            if !is_negative {
                // Skip tag filter matching anything, since it equals to no filter.
                return Ok(());
            }

            // Substitute negative tag filter matching anything with negative tag filter matching non-empty value
            // in order to filter out all the time series with the given key.
            value_ = ".+";
        }

        let tf = TagFilter::new(key, value_, is_negative, is_regexp)
            .map_err(|err| format!("cannot parse tag filter: {}", err))?;

        if tf.is_negative && tf.is_empty_match {
            // We have {key!~"|foo"} tag filter, which matches non-empty key values.
            // So add {key=~".+"} tag filter in order to enforce this.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/546 for details.
            let tf_new = TagFilter::new(key, ".+", false, true)
                .map_err(|err| format!("cannot parse tag filter: {}", err))?;

            self.0.push(tf_new);
        }

        self.0.push(tf);
        Ok(())
    }

    /// Reset resets the tf
    pub(crate) fn reset(&mut self) {
        self.0.clear();
    }

}

impl Display for TagFilters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let a = self
            .0
            .iter()
            .map(|tf| tf.to_string())
            .collect::<Vec<String>>();
        write!(f, "{:?}", a)
    }
}

/// TagFilter represents a filter used for filtering tags.
#[derive(Clone, Default, Debug)]
pub(crate) struct TagFilter {
    pub key: String,
    pub value: String,
    pub is_negative: bool,
    pub is_regexp: bool,

    /// match_cost is a cost for matching a filter against a single string.
    pub match_cost: usize,
    pub matcher: StringMatchHandler,

    /// Set to true for filters matching empty value.
    pub is_empty_match: bool,
}

impl TagFilter {
    pub fn from_label_filter(filter: &LabelFilter) -> Result<Self, String> {
        match filter.op {
            LabelFilterOp::Equal=> {
                Self::new(&filter.label, &filter.value, false, false)
            }
            LabelFilterOp::NotEqual => {
                Self::new(&filter.label, &filter.value, true, false)
            }
            LabelFilterOp::RegexEqual => {
                Self::new(&filter.label, &filter.value, false, true)
            }
            LabelFilterOp::RegexNotEqual => {
                Self::new(&filter.label, &filter.value, true, true)
            }
        }
    }

    /// creates the tag filter for the given common_prefix, key and value.
    ///
    /// If is_negative is true, then the tag filter matches all the values except the given one.
    ///
    /// If is_regexp is true, then the value is interpreted as anchored regexp, i.e. '^(tag.Value)$'.
    pub fn new(
        key: &str,
        value: &str,
        is_negative: bool,
        is_regexp: bool,
    ) -> Result<TagFilter, String> {
        if is_regexp && value.is_empty() {
            return Err("cannot use empty regexp".to_string());
        }
        let (matcher, match_cost) = if is_regexp {
            compile_regexp_anchored(value)?
        } else {
            (StringMatchHandler::Literal(key.to_string()), FULL_MATCH_COST)
        };
        // tf.is_empty_match = prefix.is_empty() && tf.suffix_match.matches("");

        Ok(TagFilter {
            key: key.to_string(),
            value: value.to_string(),
            is_negative,
            is_regexp,
            match_cost,
            matcher,
            is_empty_match: false,
        })
    }

    pub fn matches(&self, b: &str) -> bool {
        let good = self.matcher.matches(b);
        if self.is_negative {
            !good
        } else {
            good
        }
    }

    pub fn get_op(&self) -> &'static str {
        if self.is_negative {
            if self.is_regexp {
                return "!~";
            }
            return "!=";
        }
        if self.is_regexp {
            return "=~";
        }
        "="
    }
}


impl PartialEq<Self> for TagFilter {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for TagFilter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.match_cost != other.match_cost {
            return Some(self.match_cost.cmp(&other.match_cost));
        }
        if self.is_regexp != other.is_regexp {
            return Some(self.is_regexp.cmp(&other.is_regexp));
        }
        if self.is_negative != other.is_negative {
            return Some(self.is_negative.cmp(&other.is_negative));
        }
        Some(Ordering::Equal)
    }
}

// String returns human-readable tf value.
impl Display for TagFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = self.get_op();
        let value = if self.value.len() > 60 {
            // todo: could panic for non-ascii
            &self.value[0..60]
        } else {
            &self.value
        };

        if self.key.is_empty() {
            return write!(f, "{METRIC_NAME_LABEL}{op}{value}");
        }
        write!(f, "{}{}{}", self.key, op, value)
    }
}


fn matcher_size_bytes(m: &StringMatchHandler) -> usize {
    use StringMatchHandler::*;
    let base = size_of::<StringMatchHandler>();
    let extra = match m {
        Alternates(alts, _) | OrderedAlternates(alts) => {
            alts.get_size()
        },
        And(first, second) => {
            matcher_size_bytes(first) + matcher_size_bytes(second)
        }
        MatchAll | MatchNone | Empty | NotEmpty => 0,
        Literal(s) |
        Contains(s) |
        StartsWith(s) |
        EndsWith(s) => s.get_size(),
        Fsm(fsm) => fsm.get_size(),
        FastRegex(fr) => fr.get_size(),
        MatchFn(_) => {
            size_of::<fn(&str, &str) -> bool>()
        }
        Regex(r) => r.get_size(),
    };
    base + extra
}

pub fn compile_regexp(expr: &str) -> Result<(StringMatchHandler, usize), String> {
    if *USE_REGEXP_CACHE {
        let cached = get_regexp_from_cache(expr)?;
        Ok((cached.re_match.clone(), cached.re_cost))
    } else {
        let compiled = compile_regexp_ex(expr)?;
        Ok((compiled.re_match, compiled.re_cost))
    }
}

pub fn compile_regexp_anchored(expr: &str) -> Result<(StringMatchHandler, usize), String> {
    // all this is to ensure start and end anchors, avoiding allocation if possible
    let mut has_start_anchor = false;
    let mut has_end_anchor = false;

    let mut cursor = expr;
    while let Some(t) = cursor.strip_prefix('^') {
        cursor = t;
        has_start_anchor = true;
    }
    while cursor.ends_with("$") && !cursor.ends_with("\\$") {
        if let Some(t) = cursor.strip_suffix("$") {
            cursor = t;
            has_end_anchor = true;
        } else {
            break;
        }
    }

    if has_start_anchor && has_end_anchor {
        // no need to allocate
        compile_regexp(expr)
    } else {
        let anchored = format!("^{}$", cursor);
        compile_regexp(&anchored)
    }
}

fn compile_regexp_ex(expr: &str) -> Result<RegexpCacheValue, String> {
    let (matcher, cost) =
        get_optimized_re_match_func(&expr)
            .map_err(|_| {
                format!("cannot build regexp from {}", expr)
            })?;

    // heuristic for rcv in-memory size
    let size_bytes = matcher_size_bytes(&matcher);

    // Put the re_match in the cache.
    Ok(RegexpCacheValue {
        re_match: matcher,
        re_cost: cost,
        size_bytes,
    })
}

pub fn get_regexp_from_cache(expr: &str) -> Result<Arc<RegexpCacheValue>, String> {
    let cache = get_regexp_cache();
    if let Some(rcv) = cache.get(expr) {
        // Fast path - the regexp found in the cache.
        return Ok(rcv);
    }

    // Put the re_match in the cache.
    let (re_match, re_cost) = compile_regexp_anchored(expr)?;
    // heuristic for rcv in-memory size
    let size_bytes = matcher_size_bytes(&re_match);

    let rcv = RegexpCacheValue {
        re_match,
        re_cost,
        size_bytes,
    };
    let result = Arc::new(rcv);
    cache.put(expr, result.clone());

    Ok(result)
}

const DEFAULT_MAX_REGEXP_CACHE_SIZE: usize = 2048;
const DEFAULT_MAX_PREFIX_CACHE_SIZE: usize = 2048;

fn get_regexp_cache_max_size() -> &'static usize {
    static REGEXP_CACHE_MAX_SIZE: OnceLock<usize> = OnceLock::new();
    REGEXP_CACHE_MAX_SIZE.get_or_init(|| {
        // todo: read value from env
        DEFAULT_MAX_REGEXP_CACHE_SIZE
    })
}

fn get_prefix_cache_max_size() -> &'static usize {
    static REGEXP_CACHE_MAX_SIZE: OnceLock<usize> = OnceLock::new();
    REGEXP_CACHE_MAX_SIZE.get_or_init(|| {
        // todo: read value from env
        DEFAULT_MAX_PREFIX_CACHE_SIZE
    })
}

static REGEX_CACHE: LazyLock<RegexpCache> = LazyLock::new(|| {
    let size = get_regexp_cache_max_size();
    RegexpCache::new(*size)
});

// todo: get from env

pub fn get_regexp_cache() -> &'static RegexpCache {
    &REGEX_CACHE
}
