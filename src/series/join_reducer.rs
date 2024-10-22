use metricsql_parser::binaryop::get_scalar_binop_handler;
use metricsql_parser::prelude::{BinopFunc, Operator as BaseOp};
use phf::phf_map;
use std::fmt;
use std::str::FromStr;
use valkey_module::ValkeyError;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum JoinReducer {
    AbsDiff,
    Add,
    And,
    Avg,
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
    Unless,
}

pub static BINARY_OPS_MAP: phf::Map<&'static str, JoinReducer> = phf_map! {
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

    "absdiff" => JoinReducer::AbsDiff,
    "add" => JoinReducer::Add,
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

    // logic set ops
    "and" => JoinReducer::And,
    "or" => JoinReducer::Or,
    "unless" => JoinReducer::Unless,

    "if" => JoinReducer::If,
    "ifnot" => JoinReducer::IfNot,
    "default" => JoinReducer::Default,

    "avg" => JoinReducer::Avg,
    "max" => JoinReducer::Max,
    "min" => JoinReducer::Min,
};

impl JoinReducer {
    pub const fn as_str(&self) -> &'static str {
        use JoinReducer::*;
        match self {
            AbsDiff => "absDiff",
            Add => "+",
            And => "and",
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
            Sub => "-",
            Unless => "unless",
            Avg => "avg",
            Max => "max",
            Min => "min"
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
            Unless => h(BaseOp::Unless),
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
        if let Some(ch) = op.chars().next() {
            let value = if !ch.is_alphabetic() {
                BINARY_OPS_MAP.get(op)
            } else {
                // slight optimization - don't lowercase if not needed (save allocation)
                BINARY_OPS_MAP.get(op).or_else(|| {
                    let lower = op.to_ascii_lowercase();
                    BINARY_OPS_MAP.get(&lower)
                })
            };
            if let Some(operator) = value {
                return Ok(*operator);
            }
        }
        Err(ValkeyError::String(format!("Unknown binary op {}", op)))
    }
}

impl fmt::Display for JoinReducer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())?;
        Ok(())
    }
}

fn min(x: f64, y: f64) -> f64 {
    x.min(y)
}

fn max(x: f64, y: f64) -> f64 {
    x.max(y)
}

fn avg(x: f64, y: f64) -> f64 {
    (x + y) / 2.0
}

fn abs_diff(x: f64, y: f64) -> f64 {
    (x - y).abs()
}

#[cfg(test)]
mod tests {
}
