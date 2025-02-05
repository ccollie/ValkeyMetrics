use metricsql_parser::binaryop::get_scalar_binop_handler;
use metricsql_parser::prelude::{BinopFunc, Operator as BaseOp};
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;
use valkey_module::ValkeyError;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum JoinReducer {
    AbsDiff,
    Add,
    And,
    Avg,
    Cmp,
    Default,
    Div,
    #[default]
    Eql,
    Mod,
    Mul,
    Pow,
    Sub,
    Gt,
    Gte,
    If,
    IfNot,
    Lt,
    Lte,
    Max,
    Min,
    NotEq,
    Or,
    PctChange,
    SgnDiff,
    Unless,
}

fn join_reducer_get(key: &str) -> Option<JoinReducer> {
    hashify::tiny_map_ignore_case! {
        key.as_bytes(),
        "+" => JoinReducer::Add,
        "-" => JoinReducer::Sub,
        "*" => JoinReducer::Mul,
        "/" => JoinReducer::Div,
        "%" => JoinReducer::Mod,
        "^" => JoinReducer::Pow,

        // cmp ops
        "==" => JoinReducer::Eql,
        "!=" => JoinReducer::NotEq,
        "<" => JoinReducer::Lt,
        ">" => JoinReducer::Gt,
        "<=" => JoinReducer::Lte,
        ">=" => JoinReducer::Gte,

        "abs_diff" => JoinReducer::AbsDiff,
        "add" => JoinReducer::Add,
        "cmp" => JoinReducer::Cmp,
        "eq" => JoinReducer::Eql,
        "gt" => JoinReducer::Gt,
        "gte" => JoinReducer::Gte,
        "sub" => JoinReducer::Sub,
        "mod" => JoinReducer::Mod,
        "mul" => JoinReducer::Mul,
        "ne"  => JoinReducer::NotEq,
        "lt" => JoinReducer::Lt,
        "lte" => JoinReducer::Lte,
        "div" => JoinReducer::Div,
        "pow" => JoinReducer::Pow,
        "sgn_diff" => JoinReducer::SgnDiff,
        "pct_change" => JoinReducer::PctChange,

        // logic set ops
        "and" => JoinReducer::And,
        "or" => JoinReducer::Or,
        "unless" => JoinReducer::Unless,

        "if" => JoinReducer::If,
        "ifnot" => JoinReducer::IfNot,
        "default" => JoinReducer::Default,

        "avg" => JoinReducer::Avg,
        "max" => JoinReducer::Max,
        "min" => JoinReducer::Min
    }
}


impl JoinReducer {
    pub const fn as_str(&self) -> &'static str {
        use JoinReducer::*;
        match self {
            AbsDiff => "abs_diff",
            Add => "+",
            And => "and",
            Cmp => "cmp",
            Default => "default",
            Div => "/",
            Eql => "==",
            Gt => ">",
            Gte => ">=",
            If => "if",
            IfNot => "ifNot",
            Mod => "%",
            Mul => "*",
            Lt => "<",
            Lte => "<=",
            NotEq => "!=",
            Or => "or",
            Pow => "^",
            SgnDiff => "sgn_diff",
            PctChange => "pct_change",
            Sub => "-",
            Unless => "unless",
            Avg => "avg",
            Max => "max",
            Min => "min",
        }
    }

    pub const fn get_handler(&self) -> BinopFunc {
        // cheat and use code from base library. We only need to handle max, min, avg

        const fn h(op: BaseOp) -> BinopFunc {
            get_scalar_binop_handler(op, true)
        }

        use JoinReducer::*;
        match self {
            Max => max,
            Min => min,
            Avg => avg,
            AbsDiff => abs_diff,
            Add => h(BaseOp::Add),
            And => h(BaseOp::And),
            Cmp => cmp,
            Default => h(BaseOp::Default),
            Div => h(BaseOp::Div),
            Eql => h(BaseOp::Eql),
            Mod => h(BaseOp::Mod),
            Mul => h(BaseOp::Mul),
            Pow => h(BaseOp::Pow),
            Sub => h(BaseOp::Sub),
            Gt => h(BaseOp::Gt),
            Gte => h(BaseOp::Gte),
            If => h(BaseOp::If),
            IfNot => h(BaseOp::IfNot),
            Lt => h(BaseOp::Lt),
            Lte => h(BaseOp::Lte),
            NotEq => h(BaseOp::NotEq),
            Or => h(BaseOp::Or),
            SgnDiff => sgn_diff,
            Unless => h(BaseOp::Unless),
            PctChange => pct_change,
        }
    }
}

impl FromStr for JoinReducer {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        JoinReducer::try_from(s)
    }
}

impl TryFrom<&str> for JoinReducer {
    type Error = ValkeyError;

    fn try_from(op: &str) -> Result<Self, Self::Error> {
        match join_reducer_get(op) {
            Some(operator) => Ok(operator),
            None => Err(ValkeyError::String(format!("Unknown binary op {}", op))),
        }
    }
}

impl fmt::Display for JoinReducer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())?;
        Ok(())
    }
}

fn cmp(x: f64, y: f64) -> f64 {
    if x.is_nan() && y.is_nan() {
        return 1.0;
    }
    if x.is_nan() {
        return -1.0;
    }
    if y.is_nan() {
        return 1.0;
    }
    match x.total_cmp(&y) {
        Ordering::Less => -1.0,
        Ordering::Equal => 0.0,
        Ordering::Greater => 1.0,
    }
}

const fn min(x: f64, y: f64) -> f64 {
    x.min(y)
}

const fn max(x: f64, y: f64) -> f64 {
    x.max(y)
}

const fn avg(x: f64, y: f64) -> f64 {
    (x + y) / 2.0
}

const fn abs_diff(x: f64, y: f64) -> f64 {
    (x - y).abs()
}

const fn sgn_diff(x: f64, y: f64) -> f64 {
    (x - y).signum()
}

const fn pct_change(x: f64, y: f64) -> f64 {
    if x == 0.0 {
        return 0.0;
    }
    (y - x) / x
}

#[cfg(test)]
mod tests {}
