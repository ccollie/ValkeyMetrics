use std::cmp::Ordering;
use std::collections::HashMap;
use metricsql_common::hash::FastHashSet;
use metricsql_parser::label::{LabelFilterOp, Matcher};
use metricsql_runtime::RuntimeResult;

// IndexReader provides read access to index data.
pub trait IndexReader {
    /// Symbols return an iterator over sorted string symbols that may occur in
    /// series' labels and indices. It is not safe to use the returned strings
    /// beyond the lifetime of the index reader.
    fn symbols(&self) -> StringIter;

    /// sorted_label_values returns sorted possible label values.
    fn sorted_label_values(&self, name: &str, matchers: &[Matcher]) -> RuntimeResult<Vec<String>>;

    /// label_values returns possible label values which may not be sorted.
    fn label_values(&self, name: &str, matchers: &[Matcher]) -> RuntimeResult<Vec<String>>;

    /// Postings returns the postings list iterator for the label pairs.
    /// The Postings here contain the ids to the series inside the index.
    /// Found IDs are not strictly required to point to a valid Series, e.g.
    /// during background garbage collections.
    fn postings(&self, name: &str, values: &[String]) -> RuntimeResult<Postings>;

    /// `postings_for_label_matching` returns a sorted iterator over postings having a label with the given name
    /// and a value for which match returns true. If no postings are found having at least one matching label,
    /// an empty iterator is returned.
    fn postings_for_label_matching(&self, name: &str, match_fn: fn(value: &str) -> bool) -> Postings;

    /// postings_for_all_label_values returns a sorted iterator over all postings having a label with the given name.
    /// If no postings are found with the label in question, an empty iterator is returned.
    fn postings_for_all_label_values(&self, name: &str) -> Postings;

    /// `sorted_postings` returns a postings list that is reordered to be sorted
    /// by the label set of the underlying series.
    fn sorted_postings(&self, postings: Postings) -> Postings;

    /// label_names returns all the unique label names present in the index in sorted order.
    fn label_names(&self, matchers: &[Matcher]) -> RuntimeResult<Vec<String>>;

    /// label_value_for returns label value for the given label name in the series referred to by ID.
    /// If the series couldn't be found or the series doesn't have the requested label a
    /// storage.ErrNotFound is returned as error.
    fn label_value_for(&self, id: SeriesRef, label: &str) -> RuntimeResult<String>;

    /// label_names_for returns all the label names for the series referred to by the postings.
    /// The names returned are sorted.
    fn label_names_for(&self, postings: Postings) -> RuntimeResult<Vec<String>>;
}

struct BlockBaseQuerier {
    block_id: String,
    index: IndexReader,
    mint: i64,
    maxt: i64,
}

impl BlockBaseQuerier {
    fn new(b: &BlockReader, mint: i64, maxt: i64) -> Result<Self, Error> {
        let indexr = b.index()?;
        let chunkr = b.chunks()?;

        Ok(Self {
            block_id: b.meta().ulid.clone(),
            mint,
            maxt,
            index: indexr,
            chunks: chunkr,
        })
    }

    fn label_values(&self, name: &str, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        let res = self.index.sorted_label_values(name, matchers)?;
        Ok(res)
    }

    fn label_names(&self, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        let res = self.index.label_names(matchers)?;
        Ok(res)
    }
}

struct BlockQuerier {
    block_base_querier: BlockBaseQuerier,
}

impl BlockQuerier {
    fn select(&self, sort_series: bool, hints: &SelectHints, ms: &[Matcher]) -> SeriesSet {
        select_series_set(sort_series, hints, ms, self.block_base_querier.mint, self.block_base_querier.maxt)
    }
}

fn select_series_set(sort_series: bool, hints: &SelectHints, ms: &[Matcher], index: &impl IndexReader, mint: i64, maxt: i64) -> SeriesSet {
    let disable_trimming = false;

    let p = postings_for_matchers(ctx, index, ms)?;
    if sort_series {
        p = index.sorted_postings(p);
    }

    new_block_series_set(index, chunks, p, mint, maxt, disable_trimming)
}

struct BlockChunkQuerier {
    block_base_querier: BlockBaseQuerier,
}

// PostingsForMatchers assembles a single postings iterator against the index reader
// based on the given matchers. The resulting postings are not ordered by series.
fn get_postings_for_matchers(ix: &impl IndexReader, ms: &[Matcher]) -> (index.Postings, error) {
    if ms.len() == 1 && ms[0].label == "" && ms[0].value == "" {
        k, v := index.AllPostingsKey()
        return ix.postings(ctx, k, v)
    }

    var its, notIts []index.Postings
    // See which label must be non-empty.
    // Optimization for case like {l=~".", l!="1"}.
    let label_must_be_set = HashSet<String>::with_capacity(ms.len());
    for m in ms {
        if !m.matches("") {
            label_must_be_set.add(m.label);
        }
    }

    let has_subtracting_matchers = ms.iter().any(|m| is_subtracting_matcher(m, &label_must_be_set));
    let has_intersecting_matchers = ms.iter().any(|m| !is_subtracting_matcher(m, &label_must_be_set));

    if has_subtracting_matchers && !has_intersecting_matchers {
        // If there's nothing to subtract from, add in everything and remove the notIts later.
        // We prefer to get AllPostings so that the base of subtraction (i.e. allPostings)
        // doesn't include series that may be added to the index reader during this function call.
        k, v = index.AllPostingsKey()
        let allPostings = ix.postings(k, v)?;
        its.push(allPostings)
    }

    // Sort matchers to have the intersecting matchers first.
    // This way the base for subtraction is smaller and
    // there is no chance that the set we subtract from
    // contains postings of series that didn't exist when
    // we constructed the set we subtract by.
    slices.SortStableFunc(ms, func(i, j *labels.Matcher) int {
        if !isSubtractingMatcher(i) && isSubtractingMatcher(j) {
            return -1
        }

        return +1
    })

    for m in ms {
        let value = &m.value;
        let name = &m.label;
        let typ = m.op;

        if name.is_empty() && value.is_empty() {
            // If the matchers for a labelname selects an empty value, it selects all
            // the series which don't have the label name set too. See:
            //
            return Err(errors.New("unexpected all postings"))
        }

        if typ == LabelFilterOp::RegexEqual && value == ".*" {
            // .* regexp matches any string: do nothing.
            continue;
        }

        if typ == LabelFilterOp::RegexNotEqual && value == ".*" {
            return Ok(index.ErrEmptyPostings())
        }

        if typ == LabelFilterOp::RegexEqual && value == ".+" {
            /// .+ regexp matches any non-empty string: get postings for all label values.
            let it = ix.postings_for_all_label_values(ctx, m.label)
            if index.IsEmptyPostingsType(it) {
                return Ok(index.EmptyPostings())
            }
            its.push(it)
        } else if typ == LabelFilterOp::RegexNotEqual && value == ".+" {
            // .+ regexp matches any non-empty string: get postings for all label values and remove them.
            its = append(notIts, ix.postings_for_all_label_values(name))
        } else if label_must_be_set.has(name) {
            // If this matcher must be non-empty, we can be smarter.
            let matchesEmpty = m.Matches("")
            let isNot = m.op == labels.NotEqual || m.op == labels.MatchNotRegexp;

            if isNot {
                let inverse = m.inverse()?;
                // If the label can't be empty and is a Not, then subtract it out at the end.
                if matchesEmpty { // l!="foo"
                    // If the label can't be empty and is a Not and the inner matcher
                    // doesn't match empty, then subtract it out at the end.
                    let it = postings_for_matcher(ix, inverse)?;
                    not_its.push(it);
                } else {
                    // If the label can't be empty and is a Not, but the inner matcher can
                    // be empty we need to use inverse_postings_for_matcher.
                    let it = inverse_postings_for_matcher(ix, inverse)?;
                    if index.is_empty_postings_type(it) {
                        return Ok(index.EmptyPostings());
                    }
                    its.push(it);
                }
            } else {
                // l="a", l=~"a|b", l=~"a.b", etc.
                // Non-Not matcher, use normal postingsForMatcher.
                let it = postings_for_matcher(ix, m)?;
                if index.IsEmptyPostingsType(it) {
                    return Err(index.EmptyPostings())
                }
                its.push(it);
            }

        } else { // l!=""
            // If the matchers for a labelname selects an empty value, it selects all
            // the series which don't have the label name set too. See:
            // https://github.com/prometheus/prometheus/issues/3575 and
            // https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
            let it = inverse_postings_for_matcher(ctx, ix, m)?;
            not_its.push(it);
        }
    }

    let mut it = index.Intersect(its...)

    for n in notIts {
        it = index.Without(it, n)
    }

    it
}


fn postings_for_matchers(ix: &impl IndexReader, ms: &[Matcher]) -> Result<Postings, Error> {
    let mut its = Vec::new();
    let mut not_its = Vec::new();
    let mut label_must_be_set = FastHashSet::with_capacity(ms.len() * 2); // todo: be more precise

    for m in ms {
        if !m.matches("") {
            label_must_be_set.insert(m.label.clone());
        }
    }

    let has_subtracting_matchers = ms.iter().any(|m| is_subtracting_matcher(m, &label_must_be_set));
    let has_intersecting_matchers = ms.iter().any(|m| !is_subtracting_matcher(m, &label_must_be_set));

    if has_subtracting_matchers && !has_intersecting_matchers {
        let all_postings = ix.postings( "", "")?;
        its.push(all_postings);
    }


    // Sort matchers to have the intersecting matchers first.
    // This way the base for subtraction is smaller and
    // there is no chance that the set we subtract from
    // contains postings of series that didn't exist when
    // we constructed the set we subtract by.
    ms.sort_by(|i, j|-> {
        if !is_subtracting_matcher(i, &label_must_be_set) && is_subtracting_matcher(j, &label_must_be_set) {
            return Ordering::Less;
        }

        // i.cmp(&j)
        return Ordering::Greater;
    })

    for m in ms {

        match m.label.as_str() {
            "" if m.value == "" => return Err(Error::new("unexpected all postings")),
            _ => {
                let it = match m.label.as_str() {
                    "" => postings_for_matcher(ctx, ix, m)?,
                    _ => inverse_postings_for_matcher(ctx, ix, m)?,
                };
                not_its.push(it);
            }
        }
    }

    let it = intersect(&its);
    Ok(it)
}

fn is_subtracting_matcher(m: &Matcher, label_must_be_set: &HashSet<String>) -> bool {
    if !label_must_be_set.has(&m.label) {
        return true;
    }
    match m.op {
        LabelFilterOp::NotEqual | LabelFilterOp::RegexNotEqual => m.is_match(""),
        _ => false,
    }
}

fn postings_for_matcher(ix: &impl IndexReader, m: &Matcher) -> Result<Postings, Error> {
    if m.op == LabelFilterOp::Equal {
        return ix.postings(&m.label, &[m.value]);
    }
    if m.op == LabelFilterOp::RegexEqual {
        let set_matches = m.set_matches();
        if !set_matches.is_empty() {
            return ix.postings(&m.label, &set_matches);
        }
    }

    let it = ix.postings_for_label_matching(&m.label, |s| m.matches(s));
    Ok(it)
}

fn inverse_postings_for_matcher(ix: &impl IndexReader, m: &Matcher) -> Result<Postings, Error> {
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
        let it = ix.postings_for_all_label_values(&m.label);
        return Ok(it);
    }

    let it = ix.postings_for_label_matching(&m.label, |s| !m.matches(s));
    Ok(it)
}

fn label_values_with_matchers(r: &impl IndexReader, name: &str, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
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
    let values_postings = filtered_values.iter().map(|value| r.postings(ctx, name, value)).collect::<Result<Vec<_>, _>>()?;
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

struct SeriesData {
    chks: Vec<ChunkMeta>,
    intervals: Vec<Interval>,
    labels: Labels,
}

impl SeriesData {
    fn labels(&self) -> &Labels {
        &self.labels
    }
}

struct BlockBaseSeriesSet {
    p: Postings,
    index: IndexReader,
    mint: i64,
    maxt: i64,
    curr: SeriesData,
    err: Option<Error>,
}

impl BlockBaseSeriesSet {
    fn next(&mut self) -> bool {
        while self.p.next() {
            if let Err(e) = self.index.series(self.p.at(), &mut self.builder, &mut self.buf_chks) {
                if e.is_not_found() {
                    continue;
                }
                self.err = Some(e);
                return false;
            }

            if self.buf_chks.is_empty() {
                continue;
            }

            let intervals = self.tombstones.get(self.p.at())?;

            let mut trim_front = false;
            let mut trim_back = false;

            let mut chks = Vec::new();
            for chk in &self.buf_chks {
                if chk.max_time < self.mint || chk.min_time > self.maxt {
                    continue;
                }
                if Interval { mint: chk.min_time, maxt: chk.max_time }.is_subrange(&intervals)
                {
                    continue;
                }
                chks.push(chk.clone());
            }

            if chks.is_empty() {
                continue;
            }

            if trim_front {
                intervals.push(Interval { mint: i64::MIN, maxt: self.mint - 1 });
            }
            if trim_back {
                intervals.push(Interval { mint: self.maxt + 1, maxt: i64::MAX });
            }

            self.curr.labels = self.builder.labels();
            self.curr.intervals = intervals;
            return true;
        }
        false
    }

    fn err(&self) -> Option<Error> {
        self.err.or_else(|| self.p.err())
    }

    fn warnings(&self) -> Vec<Annotation> {}
}
