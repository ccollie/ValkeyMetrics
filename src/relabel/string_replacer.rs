use dynamic_lru_cache::DynamicCache;
use regex::Regex;
use crate::common::PromRegex;
use crate::relabel::regex_parse::parse_regex;
use crate::relabel::utils::get_regex_literal_prefix;

#[derive(Debug, Clone)]
pub(crate) struct StringReplacer {
    pub regex: PromRegex,
    pub replacement: String,
    pub regex_original: Regex,
    pub regex_anchored: Regex,
    prefix: String,
    /// returns false if the .
    has_suffix: bool,
    has_capture_group_in_replacement: bool,
    has_label_reference_in_replacement: bool,
    cache: DynamicCache<String, String>, // todo: AHash/gxhash
}


impl StringReplacer {
    pub fn new(regex: Regex, replacement: String) -> Result<StringReplacer, String> {
        let (regex_anchored, regex_original, regex_prom) = parse_regex(Some(regex), false)?;
        let cache = DynamicCache::new(1000);
        let has_capture_group_in_replacement = replacement.contains("$");
        let has_label_reference_in_replacement = replacement.contains("{{");
        let (prefix, complete) = get_regex_literal_prefix(&regex_original);

        Ok(StringReplacer {
            regex: regex_prom,
            prefix,
            has_suffix: !complete,
            replacement,
            cache,
            regex_original,
            regex_anchored,
            has_capture_group_in_replacement,
            has_label_reference_in_replacement,
        })
    }

    pub fn is_match(&self, s: &str) -> bool {
        self.regex.is_match(s)
    }

    /// replaces s with the replacement if s matches '^regex$'.
    ///
    /// s is returned as is if it doesn't match '^regex$'.
    pub(crate) fn replace_fast(&self, s: &str) -> String {
        // todo: use a COW here
        let prefix = &self.prefix;
        let replacement = &self.replacement;
        if !self.has_suffix && !self.has_capture_group_in_replacement {
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
        self.replace_string(s)
    }

    pub fn replace_string(&self, val: &str) -> String {
        // how to avoid this alloc ?
        let key = val.to_string();
        let res = self.cache.get_or_insert(&key, || {
            self.replace_slow(val)
        });
        res.into()
    }

    /// replaces s with the replacement if s matches '^regex$'.
    ///
    /// s is returned as is if it doesn't match '^regex$'.
    pub fn replace_slow(&self, s: &str) -> String {
        // Slow path - regexp processing
        self.expand_capture_groups(&self.replacement, s)
    }

    pub fn expand_capture_groups(&self, template: &str, source: &str) -> String {
        if let Some(captures) = self.regex_anchored.captures(source) {
            let mut s = String::with_capacity(template.len() + 16);
            captures.expand(template, &mut s);
            return s
        }
        source.to_string()
    }
}