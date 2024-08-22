use crate::common::regex_util::PromRegex;
use crate::rules::alerts::{AlertsError, AlertsResult};
use crate::relabel::label_filter::to_canonical_label_name;
use crate::relabel::{LabelFilter, LabelFilterOp, LabelMatchers};
use metricsql_parser::ast::Expr;
use metricsql_parser::prelude::MetricExpr;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use crate::storage::Label;

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
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct IfExpression(Vec<IfExpressionMatcher>);

impl IfExpression {
    pub fn new(ies: Vec<IfExpressionMatcher>) -> Self {
        IfExpression(ies)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Parse parses ie from s.
    pub fn parse(s: &str) -> AlertsResult<Self> {
        let mut ies = vec![];
        // todo: more specific error enum
        let ie = IfExpressionMatcher::parse(s).map_err(|e| AlertsError::InvalidRule(e))?;
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

impl TryFrom<MetricExpr> for IfExpression {
    type Error = AlertsError;

    fn try_from(me: MetricExpr) -> Result<Self, Self::Error> {
        let matchers_list = metric_expr_to_label_filter_list(&me)
            .map_err(|e| AlertsError::InvalidRule(e.to_string()))?;
        let ie = IfExpressionMatcher {
            s: me.to_string(),
            matchers_list,
        };
        Ok(IfExpression(vec![ie]))
    }
}

type BaseLabelFilter = metricsql_parser::prelude::LabelFilter;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IfExpressionMatcher {
    s: String,
    matchers_list: Vec<LabelMatchers>,
}

impl IfExpressionMatcher {
    // todo: error type
    pub fn parse(s: &str) -> Result<Self, String> {
        let expr = metricsql_parser::prelude::parse(s)
            .map_err(|e| format!("cannot parse series selector: {}", e))?;

        match expr {
            Expr::MetricExpression(me) => {
                let matchers_list = metric_expr_to_label_filter_list(&me)
                    .map_err(|e| e.to_string())?;
                let ie = IfExpressionMatcher {
                    s: s.to_string(),
                    matchers_list,
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
        self.matchers_list.is_empty()
    }

    /// Match returns true if ie matches the given labels.
    pub fn is_match(&self, labels: &[Label]) -> bool {
        if self.is_empty() {
            return true;
        }
        self.matchers_list.iter().any(|lfs| match_label_filters(lfs, labels))
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
    true
}

fn metric_expr_to_label_filter_list(me: &MetricExpr) -> AlertsResult<Vec<LabelMatchers>> {
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
            .map_err(|e| {
                let msg = format!("cannot parse regexp for {}: {}", mlf, e);
                // todo: specific error
                AlertsError::Generic(msg)
            })?;
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
