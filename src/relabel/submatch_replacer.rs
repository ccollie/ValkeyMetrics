use dynamic_lru_cache::DynamicCache;
use regex::Regex;
use crate::relabel::regex_parse::parse_regex;

#[derive(Clone, Debug)]
pub(crate) struct SubmatchReplacer {
    pub(crate) regex: PromRegex,
    pub(crate) replacement: String,
    cache: DynamicCache<String, String>, // todo: AHash/gxhash
    regex_original: Regex,
    regex_anchored: Regex,
    has_capture_group_in_replacement: bool,
}


impl SubmatchReplacer {
    pub fn new(regex: Regex, replacement: String) -> Result<SubmatchReplacer, String> {
        let (regex_anchored, regex_original, regex_prom) = parse_regex(Some(regex), false)?;
        let cache = DynamicCache::new(250);
        let has_capture_group_in_replacement = replacement.contains("$");
        Ok(SubmatchReplacer {
            regex: regex_prom,
            replacement,
            cache,
            regex_original,
            regex_anchored,
            has_capture_group_in_replacement,
        })
    }

    pub fn replace(&self, val: &str) -> String {
        // how to avoid this alloc ?
        let key = val.to_string();
        let res = self.cache.get_or_insert(&key, || {
            self.replace_slow(val)
        });
        res.into()
    }

    /// replaces all the regex matches with the replacement in s.
    pub(crate) fn replace_fast(&self, s: &str) -> String {
        let (prefix, complete) = self.regex_original.LiteralPrefix();
        if complete && !self.has_capture_group_in_replacement && !s.contains(prefix) {
            // Fast path - zero regex matches in s.
            return s.to_string();
        }
        // Slow path - replace all the regex matches in s with the replacement.
        self.replace(s)
    }

    /// replaces all the regex matches with the replacement in s.
    pub(crate) fn replace_slow(&self, s: &str) -> String {
        let res = self.regex_original.replace_all(s, &self.replacement);
        res.to_string()
    }
}