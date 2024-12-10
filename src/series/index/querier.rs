use crate::error::{TsdbError, TsdbResult};
use crate::series::index::IdBitmap;
use crate::series::TimeseriesId;
use metricsql_common::hash::FastHashSet;
use metricsql_parser::label::{LabelFilterOp, Matcher};
use metricsql_runtime::RuntimeResult;
use smallvec::SmallVec;
use std::cmp::Ordering;
use crate::error_consts;

pub type SeriesRef = TimeseriesId;
pub type Postings = IdBitmap;


/// `IndexReader` provides read access to index data.
pub trait IndexReader {
    fn all_postings(&self) -> Postings;

    /// `label_values` returns possible label values which may not be sorted.
    fn label_values(&self, name: &str, matchers: &[Matcher]) -> RuntimeResult<Vec<String>>;

    /// `postings` returns the postings list iterator for the label pairs.
    /// The Postings here contain the ids to the series inside the index.
    /// Found IDs are not strictly required to point to a valid Series, e.g.
    /// during background garbage collections.
    fn postings(&self, name: &str, values: &[String]) -> RuntimeResult<Postings>;

    /// `postings_for_label_matching` returns postings having a label with the given name and a value
    /// for which match returns true. If no postings are found having at least one matching label,
    /// an empty iterator is returned.
    fn postings_for_label_matching(&self, name: &str, match_fn: fn(value: &str) -> bool) -> Postings;

    /// `postings_for_all_label_values` returns a sorted iterator over all postings having a label with the given name.
    /// If no postings are found with the label in question, an empty iterator is returned.
    fn postings_for_all_label_values(&self, name: &str) -> Postings;

    /// `label_names` returns all the unique label names present in the index in sorted order.
    fn label_names(&self, matchers: &[Matcher]) -> RuntimeResult<Vec<String>>;

    /// `label_value_for` returns label value for the given label name in the series referred to by ID.
    /// If the series couldn't be found or the series doesn't have the requested label a
    /// storage.ErrNotFound is returned as error.
    fn label_value_for(&self, id: SeriesRef, label: &str) -> RuntimeResult<String>;

    /// `label_names_for` returns all the label names for the series referred to by the postings.
    /// The names returned are sorted.
    fn label_names_for(&self, postings: Postings) -> RuntimeResult<Vec<String>>;
}


// `postings_for_matchers` assembles a single postings iterator against the index reader
// based on the given matchers. The resulting postings are not ordered by series.
pub fn postings_for_matchers(ix: &impl IndexReader, ms: &[Matcher]) -> TsdbResult<Postings> {
    if ms.len() == 1 && ms[0].label == "" && ms[0].value == "" {
        return Ok(ix.all_postings())
    }

    let mut sorted_matchers: SmallVec::<(&Matcher, bool, bool), 4> = SmallVec::new();
    let mut not_its= Postings::new();

    let mut has_subtracting_matchers = false;
    let mut has_intersecting_matchers = false;

    // See which label must be non-empty.
    // Optimization for case like {l=~".", l!="1"}.
    let mut label_must_be_set: FastHashSet<String> = FastHashSet::with_capacity(ms.len());
    for m in ms {
        let matches_empty = m.matches("");
        if !matches_empty {
            label_must_be_set.insert(m.label.clone());
        }
        let is_subtracting = is_subtracting_matcher(m, &label_must_be_set);

        has_subtracting_matchers |= is_subtracting;
        has_intersecting_matchers |= !is_subtracting;

        sorted_matchers.push((&m, matches_empty, is_subtracting))
    }

    let mut its = if has_subtracting_matchers && !has_intersecting_matchers {
        // If there's nothing to subtract from, add in everything and remove the not_its later.
        // We prefer to get AllPostings so that the base of subtraction (i.e. all_postings)
        // doesn't include series that may be added to the index reader during this function call.
        ix.all_postings()
    } else {
        Postings::new()
    };

    // Sort matchers to have the intersecting matchers first.
    // This way the base for subtraction is smaller and there is no chance that the set we subtract
    // from contains postings of series that didn't exist when we constructed the set we subtract by.
    sorted_matchers.sort_by(|i, j|-> Ordering {
        let is_i_subtracting = i.2;
        let is_j_subtracting = j.2;
        if !is_i_subtracting && is_j_subtracting {
            return Ordering::Less;
        }

        // i.cmp(&j)
        return Ordering::Greater;
    });

    for (m, matches_empty) in sorted_matchers {
        let value = &m.value;
        let name = &m.label;
        let typ = m.op;

        if name.is_empty() && value.is_empty() {
            // If the matchers for a label name selects an empty value, it selects all
            // the series which don't have the label name set too. See:
            //
            return Err(TsdbError::General(error_consts::MISSING_FILTER)) // todo: better error
        }

        if typ == LabelFilterOp::RegexEqual && value == ".*" {
            // .* regexp matches any string: do nothing.
            continue;
        }

        if typ == LabelFilterOp::RegexNotEqual && value == ".*" {
            return Ok(Postings::default())
        }

        if typ == LabelFilterOp::RegexEqual && value == ".+" {
            // .+ regexp matches any non-empty string: get postings for all label values.
            let it = ix.postings_for_all_label_values(&m.label);
            if it.is_empty() {
                return Ok(Postings::default())
            }
            its &= it;
        } else if typ == LabelFilterOp::RegexNotEqual && value == ".+" {
            // .+ regexp matches any non-empty string: get postings for all label values and remove them.
            let it = ix.postings_for_all_label_values(name);
            not_its |= it;
            //its = append(not_its, it)
        } else if label_must_be_set.contains(name) {
            // If this matcher must be non-empty, we can be smarter.
            let is_not = typ == LabelFilterOp::NotEqual || m.op == LabelFilterOp::RegexNotEqual;

            if is_not {
                let inverse = m.inverse()?;
                // If the label can't be empty and is a Not, then subtract it out at the end.
                if matches_empty { // l!="foo"
                    // If the label can't be empty and is a Not and the inner matcher
                    // doesn't match empty, then subtract it out at the end.
                    let it = postings_for_matcher(ix, inverse)?;
                    not_its |= it;
                } else {
                    // If the label can't be empty and is a Not, but the inner matcher can
                    // be empty we need to use inverse_postings_for_matcher.
                    let it = inverse_postings_for_matcher(ix, inverse)?;
                    if it.is_empty() {
                        return Ok(Postings::new())
                    }
                    its &= it;
                }
            } else {
                // l="a", l=~"a|b", l=~"a.b", etc.
                // Non-Not matcher, use normal `postings_for_matcher`.
                let it = postings_for_matcher(ix, m)?;
                if it.is_empty() {
                    return Ok(Postings::new())
                }
                its &= it;
            }

        } else { // l!=""
            // If the matchers for a label name selects an empty value, it selects all
            // the series which don't have the label name set too. See:
            // https://github.com/prometheus/prometheus/issues/3575 and
            // https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
            let it = inverse_postings_for_matcher(ix, m)?;
            not_its |= it;
        }
    }

    its -= &not_its;
    Ok(its)
}


fn is_subtracting_matcher(m: &Matcher, label_must_be_set: &FastHashSet<String>) -> bool {
    if !label_must_be_set.has(&m.label) {
        return true;
    }
    matches!(m.op, LabelFilterOp::NotEqual | LabelFilterOp::RegexNotEqual if m.is_match(""))
}

fn postings_for_matcher(ix: &impl IndexReader, m: &Matcher) -> Postings {
    if m.op == LabelFilterOp::Equal {
        return ix.postings_for_label_value(&m.label, &m.value);
    }
    if m.op == LabelFilterOp::RegexEqual {
        let set_matches = m.set_matches();
        if !set_matches.is_empty() {
            return ix.postings(&m.label, &set_matches);
        }
    }

    ix.postings_for_label_matching(&m.label, |s| m.matches(s))
}

fn inverse_postings_for_matcher(ix: &impl IndexReader, m: &Matcher) -> Postings {
    if m.op == LabelFilterOp::RegexNotEqual {
        let set_matches = m.set_matches();
        if !set_matches.is_empty() {
            return ix.postings(&m.label, &set_matches);
        }
    }

    if m.op == LabelFilterOp::NotEqual {
        return ix.postings(&m.label, &m.value);
    }

    if m.value.is_empty() && (m.op == LabelFilterOp::RegexEqual || m.op == LabelFilterOp::Equal) {
        return ix.postings_for_all_label_values(&m.label);
    }

    ix.postings_for_label_matching(&m.label, |s| !m.matches(s))
}

fn label_values_with_matchers(r: &impl IndexReader, name: &str, matchers: &[Matcher]) -> TsdbResult<Vec<String>> {
    let all_values = r.label_values(name)?;

    let mut filtered_values = Vec::new();
    for v in all_values {
        if matchers.iter().all(|m| m.matches(&v)) {
            filtered_values.push(v);
        }
    }

    if filtered_values.is_empty() {
        return Ok(Vec::new());
    }

    let p = postings_for_matchers(r, matchers)?;
    let values_postings = filtered_values.iter()
        .map(|value| r.postings(name, value))
        .collect::<Result<Vec<_>, _>>()?;

    let indexes = intersect(&values_postings);

    let mut values = Vec::new();
    for idx in indexes {
        values.push(filtered_values[idx].clone());
    }

    Ok(values)
}

fn label_names_with_matchers(r: &impl IndexReader, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
    let p = postings_for_matchers(r, matchers)?;
    r.label_names_for(p)
}

#[cfg(test)]
mod tests {

}