use crate::series::index::IdBitmap;
use crate::series::TimeseriesId;
use metricsql_parser::label::Matcher;
use metricsql_runtime::RuntimeResult;

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

#[cfg(test)]
mod tests {

}