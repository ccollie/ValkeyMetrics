use crate::common::regex_util::PromRegex;
use crate::common::types::Label;
use crate::rules::alerts::{AlertsError, AlertsResult};
use crate::rules::relabel::label_filter::to_canonical_label_name;
use crate::rules::relabel::{LabelFilter, LabelFilterOp, LabelMatchers};
use metricsql_parser::ast::Expr;
use metricsql_parser::prelude::MetricExpr;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// IfExpression represents PromQL-like label filters such as `metric_name{filters...}`.
///
/// It may contain either a single filter or multiple filters, which are executed with `or` operator.
///
/// Examples:
///
/// if: 'foo{bar="baz"}'
///
/// if:
/// - 'foo{bar="baz"}'
/// - '{x=~"y"}'
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IfExpression(pub Vec<IfExpressionMatcher>);

impl IfExpression {
    pub fn default() -> Self {
        IfExpression(vec![])
    }
    pub fn new(ies: Vec<IfExpressionMatcher>) -> Self {
        IfExpression(ies)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Parse parses ie from s.
    pub fn parse(s: &str) -> AlertsResult<Self> {
        let mut ies = vec![];
        let ie = IfExpressionMatcher::parse(s)?;
        ies.push(ie);
        Ok(IfExpression(ies))
    }

    /// Match returns true if labels match at least a single label filter inside ie.
    ///
    /// Match returns true for empty ie.
    pub fn is_match(&self, labels: &[Label]) -> bool {
        if self.is_empty() {
            return true;
        }
        self.0.iter().any(|ie| ie.is_match(labels))
    }
}

impl Display for IfExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() == 1 {
            return write!(f, "{}", &self.0[0]);
        }
        write!(f, "{:?}", self.0)
    }
}

type BaseLabelFilter = metricsql_parser::prelude::LabelFilter;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IfExpressionMatcher {
    s: String,
    lfss: Vec<LabelMatchers>,
}

impl IfExpressionMatcher {
    // todo: error type
    pub fn parse(s: &str) -> Result<Self, String> {
        let expr = metricsql_parser::prelude::parse(s)
            .map_err(|e| format!("cannot parse series selector: {}", e))?;

        match expr {
            Expr::MetricExpression(me) => {
                let lfss = metric_expr_to_label_filterss(&me)?;
                let ie = IfExpressionMatcher {
                    s: s.to_string(),
                    lfss,
                };
                Ok(ie)
            }
            _ => Err(format!(
                "expecting series selector; got {}",
                expr.return_type()
            )),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.lfss.is_empty()
    }

    /// Match returns true if ie matches the given labels.
    pub fn is_match(&self, labels: &[Label]) -> bool {
        if self.is_empty() {
            return true;
        }
        self.lfss.iter().any(|lfs| match_label_filters(lfs, labels))
    }
}

impl Display for IfExpressionMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.s)
    }
}

fn match_label_filters(lfs: &[LabelFilter], labels: &[Label]) -> bool {
    for lf in lfs {
        if !lf.matches(labels) {
            return false;
        }
    }
    return true;
}

fn metric_expr_to_label_filterss(me: &MetricExpr) -> AlertsResult<Vec<LabelMatchers>> {
    let mut lfss_new: Vec<LabelMatchers> = Vec::with_capacity(me.label_filters.len());
    for lfs in me.label_filters.iter() {
        let mut lfs_new: Vec<LabelFilter> = Vec::with_capacity(lfs.len());
        for filter in lfs.iter() {
            let lf = new_label_filter(filter).map_err(|e| {
                let msg = format!("cannot parse label filter {me}: {:?}", e);
                // todo: more specific error
                AlertsError::Generic(msg)
            })?;
            lfs_new.push(lf);
        }
        let matchers = LabelMatchers::new(lfs_new);
        lfss_new.push(matchers)
    }
    Ok(lfss_new)
}

fn new_label_filter(mlf: &BaseLabelFilter) -> AlertsResult<LabelFilter> {
    let mut lf = LabelFilter {
        label: to_canonical_label_name(&mlf.label).to_string(),
        op: get_filter_op(mlf),
        value: mlf.value.to_string(),
        re: None,
    };
    if lf.op.is_regex() {
        let re = PromRegex::new(&lf.value)
            .map_err(|e| format!("cannot parse regexp for {}: {}", mlf, e))?;
        lf.re = Some(re);
    }
    Ok(lf)
}

fn get_filter_op(mlf: &BaseLabelFilter) -> LabelFilterOp {
    if mlf.is_negative() {
        return if mlf.is_regexp() {
            LabelFilterOp::NotMatchRegexp
        } else {
            LabelFilterOp::NotEqual
        };
    }
    if mlf.is_regexp() {
        LabelFilterOp::MatchRegexp
    } else {
        LabelFilterOp::Equal
    }
}
