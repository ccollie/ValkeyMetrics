// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::series::index::querier::{postings_for_matchers, SeriesRef};
use crate::series::TimeSeries;
use metricsql_common::label::Label;
use metricsql_parser::label::{LabelFilterOp, Matcher};
use std::collections::{HashMap, HashSet};
use crate::series::index::IndexInner;
// TODO(bwplotka): Replace those mocks with remote.concreteSeriesSet.

struct MockIndex {
    series: HashMap<SeriesRef, TimeSeries>,
    postings: HashMap<Label, Vec<SeriesRef>>,
    symbols: HashSet<String>,
}

fn newMockIndex() -> MockIndex {
    MockIndex {
        series: Default::default(),
        postings: Default::default(),
        symbols: Default::default(),
    }
}

fn labels_from_strings<S: Into<String>>(ss: &[S]) -> Vec<Label> {
    if ss.len() == 0 {
        return vec![];
    }
    if ss.len() % 2 != 0 {
        panic!("labels_from_strings: odd number of strings")
    }
    let mut labels = vec![];
    for i in (0..ss.len()).step_by(2) {
        let name = ss[i].into();
        let value = ss[i + 1].into();
        labels.push(Label {
            name,
            value,
        })
    }
    labels
}

fn add_series(ix: &mut IndexInner, series_ref: SeriesRef, labels: Vec<Label>) {
    for Label { name, value } in labels {
        ix.index_series_by_label(series_ref, name.as_str(), value.as_str());
    }
}

fn test_postings_for_matchers() {
    use LabelFilterOp::*;
    let mut ix = IndexInner::new();

    add_series(&mut ix, 0, labels_from_strings(&["n", "1"]));
    add_series(&mut ix, 0, labels_from_strings(&["n", "1", "i", "a"]));
    add_series(&mut ix, 0, labels_from_strings(&["n", "1", "i", "b"]));
    add_series(&mut ix, 0, labels_from_strings(&["n", "1", "i", "\n"]));
    add_series(&mut ix, 0, labels_from_strings(&["n", "2"]));
    add_series(&mut ix, 0, labels_from_strings(&["n", "2.5"]));

    struct TestCase {
        matchers: Vec<Matcher>,
        exp: Vec<Label>,
    }
    ;
    let cases = vec![
        TestCase {
            matchers: vec![Matcher::new(Equal, "n", "1").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(Equal, "i", "a").unwrap()
            ],
            exp: labels_from_strings(&["n", "1", "i", "a"]),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(Equal, "i", "missing").unwrap()
            ],
            exp: vec![],
        },
        TestCase {
            matchers: vec![Matcher::new(Equal, "missing", "").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![Matcher::new(NotEqual, "n", "1").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![Matcher::new(NotEqual, "i", "").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(NotEqual, "missing", "").unwrap()
            ],
            exp: vec![],
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(Equal, "i", "missing").unwrap(),
            ],
            exp: vec![],
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "missing", "").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        // Not equals.
        TestCase {
            matchers: vec![
                Matcher::new(NotEqual, "n", "1").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(NotEqual, "i", "").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![Matcher::new(NotEqual, "missing", "").unwrap()],
            exp: vec![],
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(NotEqual, "i", "a").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(NotEqual, "i", "").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
            ].iter().flatten().collect(),
        },
        // Regex.
        TestCase {
            matchers: vec![
                Matcher::new(RegexEqual, "n", "^1$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"])
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexEqual, "i", "^a$").unwrap()
            ],
            exp: labels_from_strings(&["n", "1", "i", "a"]),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexEqual, "i", "^a?$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(RegexEqual, "i", "^$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexEqual, "i", "^$").unwrap()
            ],
            exp: labels_from_strings(&["n", "1"])
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexEqual, "i", "^.*$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"])
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexEqual, "i", "^.+$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"])
            ].iter().flatten().collect(),
        },
        // Not regex.
        TestCase {
            matchers: vec![
                Matcher::new(RegexNotEqual, "i", "").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"])
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(RegexNotEqual, "n", "^1$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(RegexNotEqual, "n", "1").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(RegexNotEqual, "n", "1|2.5").unwrap()
            ],
            exp: labels_from_strings(&["n", "2"]),
        },
        TestCase {
            matchers: vec![
                Matcher::new(RegexNotEqual, "n", "(1|2.5)").unwrap()
            ],
            exp: labels_from_strings(&["n", "2"]),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexNotEqual, "i", "^a$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexNotEqual, "i", "^a?$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexNotEqual, "i", "^$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"])
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexNotEqual, "i", "^.*$").unwrap()
            ],
            exp: vec![],
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexNotEqual, "i", "^.+$").unwrap()
            ],
            exp: labels_from_strings(&["n", "1"]),
        },
        // Combinations.
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(NotEqual, "i", "").unwrap(),
                Matcher::new(Equal, "i", "a").unwrap()
            ],
            exp: labels_from_strings(&["n", "1", "i", "a"]),
        },
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(NotEqual, "i", "b").unwrap(),
                Matcher::new(RegexEqual, "i", "^(b|a).*$").unwrap()
            ],
            exp: labels_from_strings(&["n", "1", "i", "a"]),
        },
        // Set optimization for Regex.
        // Refer to https://github.com/prometheus/prometheus/issues/2651.
        TestCase {
            matchers: vec![
                Matcher::new(RegexEqual, "n", "1|2").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"])
                labels_from_strings(&["n", "2"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![Matcher::new(RegexEqual, "i", "a|b").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![Matcher::new(RegexEqual, "i", "(a|b)").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
            ]
        },
        TestCase {
            matchers: vec![Matcher::new(RegexEqual, "n", "x1|2").unwrap()],
            exp: labels_from_strings(&["n", "2"])
        },
        TestCase {
            matchers: vec![Matcher::new(RegexEqual, "n", "2|2\\.5").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        // Empty value.
        TestCase {
            matchers: vec![Matcher::new(RegexEqual, "i", "c||d").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        TestCase {
            matchers: vec![Matcher::new(RegexEqual, "i", "(c||d)").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        // Test shortcut for i=~".*"
        TestCase {
            matchers: vec![Matcher::new(RegexEqual, "i", ".*").unwrap()],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        // Test shortcut for n=~".*" and i=~"^.*$"
        TestCase {
            matchers: vec![
                Matcher::new(RegexEqual, "n", ".*").unwrap(),
                Matcher::new(RegexEqual, "i", "^.*$").unwrap()
            ],
            exp: vec![
                labels_from_strings(&["n", "1"]),
                labels_from_strings(&["n", "1", "i", "a"]),
                labels_from_strings(&["n", "1", "i", "b"]),
                labels_from_strings(&["n", "1", "i", "\n"]),
                labels_from_strings(&["n", "2"]),
                labels_from_strings(&["n", "2.5"]),
            ].iter().flatten().collect(),
        },
        // Test shortcut for n=~"^.*$"
        TestCase {
            matchers: vec![
                Matcher::new(RegexEqual, "n", "^.*$").unwrap(),
                Matcher::new(Equal, "i", "a").unwrap()
            ],
            exp: labels_from_strings(&["n", "1", "i", "a"])
        },
        // Test shortcut for i!~".*"
        TestCase {
            matchers: vec![
                Matcher::new(RegexNotEqual, "i", ".*").unwrap()
            ],
            exp: vec![],
        },
        // Test shortcut for n!~"^.*$",  i!~".*". First one triggers empty result.
        TestCase {
            matchers: vec![
                Matcher::new(RegexNotEqual, "n", "^.*$").unwrap(),
                Matcher::new(RegexNotEqual, "i", ".*").unwrap()
            ],
            exp: vec![],
        },
        // Test shortcut i!~".*"
        TestCase {
            matchers: vec![
                Matcher::new(RegexEqual, "n", ".*").unwrap(),
                Matcher::new(RegexNotEqual, "i", ".*").unwrap()
            ],
            exp: vec![],
        },
        // Test shortcut i!~"^.*$"
        TestCase {
            matchers: vec![
                Matcher::new(Equal, "n", "1").unwrap(),
                Matcher::new(RegexNotEqual, "i", "^.*$").unwrap()
            ],
            exp: vec ! [],
        },
    ];


for case in cases {
    let mut name: String = "";
    for (i, matcher) in case.matchers.iter().enumerate() {
        if i > 0 {
            name += ","
        }
        name += matcher.String()
    }
    let mut exp: HashSet < String > = HashSet::new();
    for label in case.exp {
        exp.insert(label.to_string());
    }

    let p = postings_for_matchers(ix, case.matchers).unwrap();

    let mut builder labels.ScratchBuilder
    for p.Next() {
require.NoError(t, ir.Series(p.At(), & builder))
    lbls: = builder.Labels()
    if _, ok: = exp[lbls.String()]; ! ok {
t.Errorf("Evaluating %v, unexpected result %s", c.matchers, lbls.String())
} else {
delete(exp, lbls.String())
}
}
require.Empty(t, exp, "Evaluating %v", c.matchers)
})
}
}

