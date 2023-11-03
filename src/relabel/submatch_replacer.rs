use dynamic_lru_cache::DynamicCache;
use regex::Regex;
use crate::common::regex_util::PromRegex;
use crate::relabel::actions::regex_parse::parse_regex;

pub(crate) struct StringReplacer {
    pub(crate) regex: PromRegex,
    pub(crate) replacement: String,
    cache: DynamicCache<String, String>, // todo: AHash/gxhash
    regex_original: Regex,
    regex_anchored: Regex,
    has_capture_group_in_replacement: bool,
    has_label_reference_in_replacement: bool,
}


impl StringReplacer {
    pub fn new(regex: Regex, replacement: String) -> Result<StringReplacer, String> {
        let (regex_anchored, regex_original, regex_prom) = parse_regex(Some(regex), false)?;
        let cache = DynamicCache::new(1000);
        let has_capture_group_in_replacement = replacement.contains("$");
        let has_label_reference_in_replacement = replacement.contains("{{");
        Ok(StringReplacer {
            regex: regex_prom,
            replacement,
            cache,
            regex_original,
            regex_anchored,
            has_capture_group_in_replacement,
            has_label_reference_in_replacement,
        })
    }

    /// replaces s with the replacement if s matches '^regex$'.
    ///
    /// s is returned as is if it doesn't match '^regex$'.
    pub(crate) fn replace_full_string_fast(&self, s: &str) -> String {
        // todo: use a COW here
        let (prefix, complete) = self.regex_original.LiteralPrefix();
        let replacement = &self.replacement;
        if complete && !self.has_capture_group_in_replacement {
            if s == prefix {
                // Fast path - s matches literal regex
                return replacement.clone();
            }
            // Fast path - s doesn't match literal regex
            return s.to_string();
        }
        if !s.starts_with(prefix) {
            // Fast path - s doesn't match literal prefix from regex
            return s.to_string();
        }
        if replacement == "$1" {
            // Fast path for commonly used rule for deleting label prefixes such as:
            //
            // - action: labelmap
            //   regex: __meta_kubernetes_node_label_(.+)
            //
            let re_str = self.regex_original.to_string();
            if re_str.starts_with(prefix) {
                let suffix = &s[prefix.len()..];
                let re_suffix = &re_str[prefix.len()..];
                if re_suffix == "(.*)" {
                    return suffix.to_string();
                } else if re_suffix == "(.+)" {
                    if !suffix.is_empty() {
                        return suffix.to_string();
                    }
                    return s.to_string();
                }
            }
        }
        if !self.regex.is_match(s) {
            // Fast path - regex mismatch
            return s.to_string();
        }
        // Slow path - handle the rest of cases.
        return self.replace_string(s);
    }

    pub fn replace_string(&self, val: &str) -> String {
        // how to avoid this alloc ?
        let key = val.to_string();
        let res = self.cache.get_or_insert(&key, || {
            self.replace_full_string_slow(val)
        });
        res.into()
    }

    /// replaces s with the replacement if s matches '^regex$'.
    ///
    /// s is returned as is if it doesn't match '^regex$'.
    pub fn replace_full_string_slow(&self, s: &str) -> String {
        // Slow path - regexp processing
        self.expand_capture_groups(&self.replacement, s)
    }

    fn expand_capture_groups(&self, template: &str, source: &str) -> String {
        if let Some(captures) = self.regex_anchored.captures(source) {
            let mut s = String::with_capacity(template.len() + 16);
            captures.expand(template, &mut s);
            s
        }
        source.to_string()
    }
}