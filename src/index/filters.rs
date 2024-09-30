use crate::index::index_key::format_key_for_label_prefix;
use crate::index::timeseries_index::SetOperation;
use crate::index::ARTBitmap;
use blart::AsBytes;
use croaring::Bitmap64;
use metricsql_common::prelude::StringMatchHandler;
use metricsql_parser::label::{LabelFilterOp, Matchers};
use metricsql_runtime::{create_label_filter_matchers, LabelFilterVec, TagFilter};


pub fn process_equals_match(label_index: &ARTBitmap, key: &String, dest: &mut Bitmap64, op: SetOperation) {
    if let Some(map) = label_index.get(key.as_bytes()) {
        process_iterator(std::iter::once(map), dest, op);
    } else {
        dest.clear();
    }
}

fn process_filter(
    label_index: &ARTBitmap,
    filter: &TagFilter,
    dest: &mut Bitmap64,
    op: SetOperation,
    key_buf: &mut String,
) {
    use LabelFilterOp::*;

    #[inline]
    fn handle_match_all(label_index: &ARTBitmap, key_buf: &String, dest: &mut Bitmap64, op: SetOperation) {
        let iter = label_index.prefix(key_buf.as_bytes()).map(|(_, map)| map);
        process_iterator(iter, dest, op);
    }

    fn handle_match_none(dest: &mut Bitmap64, op: SetOperation) {
        if op == SetOperation::Intersection {
            dest.clear();
        }
    }

    fn handle_general_match(label_index: &ARTBitmap, filter: &TagFilter, dest: &mut Bitmap64, op: SetOperation, key_buf: &mut String) {
        format_key_for_label_prefix(key_buf, &filter.key);
        let start_pos = key_buf.len();
        let matcher = &filter.matcher;
        let is_negative = filter.is_negative;

        // handle AND separately so we can exit early on mismatch
        match op {
            SetOperation::Intersection => {
                for (key, map) in label_index.prefix(key_buf.as_bytes()) {
                    if filter_value_by_matcher(matcher, &key[start_pos..], is_negative) {
                        if dest.is_empty() {
                            dest.or_inplace(map);
                        } else {
                            dest.and_inplace(map);
                        }
                    } else {
                        dest.clear();
                        return;
                    }
                }
            }
            SetOperation::Union => {
                for (key, map) in label_index.prefix(key_buf.as_bytes()) {
                    if filter_value_by_matcher(matcher, &key[start_pos..], is_negative) {
                        dest.or_inplace(map);
                    }
                }
            }
        }
    }

    if filter.op() == Equal && !filter.is_negative {
        process_equals_match(label_index, key_buf, dest, op);
    } else {
        match filter.matcher {
            StringMatchHandler::Literal(_) if !filter.is_negative => {
                process_equals_match(label_index, &filter.key, dest, op);
            }
            StringMatchHandler::MatchNone if filter.is_negative => {
                format_key_for_label_prefix(key_buf, &filter.key);
                handle_match_all(label_index, key_buf, dest, op);
            }
            StringMatchHandler::MatchNone => {
                handle_match_none(dest, op);
            }
            StringMatchHandler::MatchAll if filter.is_negative => {
                handle_match_none(dest, op);
            }
            StringMatchHandler::MatchAll => {
                format_key_for_label_prefix(key_buf, &filter.key);
                handle_match_all(label_index, key_buf, dest, op);
            }
            _ => {
                handle_general_match(label_index, filter, dest, op, key_buf);
            }
        }
    }
}

pub fn filter_value_by_matcher(
    matcher: &StringMatchHandler,
    value: &[u8],
    is_negative: bool,
) -> bool {
    let res = match matcher {
        StringMatchHandler::Empty => value.is_empty(),
        StringMatchHandler::NotEmpty => !value.is_empty(),
        StringMatchHandler::StartsWith(prefix) => value.starts_with(prefix.as_bytes()),
        StringMatchHandler::EndsWith(suffix) => value.ends_with(suffix.as_bytes()),
        StringMatchHandler::Literal(literal) => value == literal.as_bytes(),
        _=> {
            let value = std::str::from_utf8(value).unwrap();
            matcher.matches(value)
        }
    };
    if is_negative {
        !res
    } else {
        res
    }
}

pub fn process_iterator<'a>(mut iter: impl Iterator<Item = &'a Bitmap64>, dest: &mut Bitmap64, op: SetOperation) {
    match op {
        SetOperation::Union => {
            for map in iter {
                dest.or_inplace(map);
            }
        }
        SetOperation::Intersection => {
            if dest.is_empty() {
                // we need at least one set to intersect with
                if let Some(first) = iter.next() {
                    dest.or_inplace(first);
                } else {
                    return;
                }
            }
            for map in iter {
                dest.and_inplace(map);
            }
        }
    }
}


// Compiles the given matchers to optimized matchers. Incurs some setup overhead, so use this in the following cases:
// * you are going to use the matchers multiple times
// * if the matchers are complex.
// * labels have high cardinality
pub fn get_ids_by_matchers_optimized(
    label_index: &ARTBitmap,
    matchers: &Matchers,
    dest: &mut Bitmap64
) {
    let mut key_buf = String::with_capacity(64);

    // todo: remove unwrap
    let filters = create_label_filter_matchers(matchers).unwrap();
    exec_label_matches(label_index, &filters, dest, &mut key_buf);
}

pub fn exec_label_matches(label_index: &ARTBitmap,
                    matchers: &LabelFilterVec,
                    dest: &mut Bitmap64,
                    key_buf: &mut String) {
    let mut intersects = Bitmap64::new();
    for filters in matchers.iter() {
        exec_filter_list(label_index, filters, &mut intersects, key_buf);
        dest.or_inplace(&intersects);
        intersects.clear();
    }
}

// execute ANDed list of filters
#[inline]
fn exec_filter_list(label_index: &ARTBitmap, filters: &[TagFilter], dest: &mut Bitmap64, key_buf: &mut String) {
    for filter in filters.iter() {
        process_filter(label_index, filter, dest, SetOperation::Intersection, key_buf);
        if dest.is_empty() {
            return;
        }
    }
}