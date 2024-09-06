use metricsql_common::prelude::remove_start_end_anchors;
use regex::Regex;
use crate::common::{simplify, PromRegex};
use crate::relabel::{DEFAULT_ORIGINAL_REGEX_FOR_RELABEL_CONFIG, DEFAULT_REGEX_FOR_RELABEL_CONFIG};

const EMPTY_REGEX: &str = "";

pub(super) fn parse_regex(regex: Option<Regex>, strip_anchors: bool) -> Result<(Regex, Regex, PromRegex), String> {
    let reg_str = regex.as_ref().map_or(EMPTY_REGEX, |r| r.as_str());
    let (regex_anchored, regex_original_compiled, prom_regex) =
        if !is_empty_regex(&regex) && !is_default_regex(&reg_str) {
            let mut regex = reg_str;
            let mut regex_orig = regex;
            if strip_anchors {
                let stripped = remove_start_end_anchors(&regex);
                regex_orig = &*stripped.to_string();
                regex = format!("^(?:{stripped})$").as_str();
            }
            let regex_anchored =
                Regex::new(&regex).map_err(|e| format!("cannot parse `regex` {regex}: {:?}", e))?;

            let regex_original_compiled = Regex::new(&regex_orig)
                .map_err(|e| format!("cannot parse `regex` {regex_orig}: {:?}", e))?;

            let prom_regex = PromRegex::new(&regex_orig).map_err(|err| {
                format!("BUG: cannot parse already parsed regex {}: {:?}", regex_orig, err)
            })?;

        (regex_anchored, regex_original_compiled, prom_regex)
    } else {
        (
            DEFAULT_REGEX_FOR_RELABEL_CONFIG.clone(),
            DEFAULT_ORIGINAL_REGEX_FOR_RELABEL_CONFIG.clone(),
            PromRegex::new(".*").unwrap(),
        )
    };
    Ok((regex_anchored, regex_original_compiled, prom_regex))
}


fn is_default_regex(expr: &str) -> bool {
    match simplify(expr) {
        Ok((prefix, suffix)) => prefix == "" && suffix == ".*",
        _ => false,
    }
}

fn is_empty_regex(regex: &Option<Regex>) -> bool {
    regex.is_none() || regex.as_ref().unwrap().as_str() == ""
}