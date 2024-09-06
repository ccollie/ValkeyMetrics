use metricsql_common::prelude::{remove_start_end_anchors, match_handlers::StringMatchHandler, FastRegexMatcher};
use regex::{Regex, Error as RegexError};
use regex_syntax::{
    escape as escape_regex,
    parse as parse_regex,
    hir::{Hir, HirKind}
};
use regex_syntax::hir::Class::{Unicode, Bytes};

const MAX_OR_VALUES: usize = 10;


/// get_or_values returns "or" values from the given regexp expr.
///
/// It ignores start and end anchors ('^') and ('$') at the start and the end of expr.
/// It returns ["foo", "bar"] for "foo|bar" regexp.
/// It returns ["foo"] for "foo" regexp.
/// It returns [""] for "" regexp.
/// It returns an empty list if it is impossible to extract "or" values from the regexp.
pub fn get_or_values(expr: &str) -> Vec<String> {
    if expr.is_empty() {
        return vec!["".to_string()]
    }
    let expr = remove_start_end_anchors(expr);
    let simplified = simplify(expr);
    if simplified.is_err() {
        // Cannot simplify the regexp. Use regexp for matching.
        return vec![expr.to_string()]
    }

    let (prefix, tail_expr) = simplified.unwrap();
    if tail_expr.is_empty() {
        return vec![prefix]
    }
    let sre = build_hir(&tail_expr);
    match sre {
        Ok(sre) => {
            let mut or_values = get_or_values_ext(&sre).unwrap_or_default();
            // Sort or_values for faster index seek later
            or_values.sort();
            if !prefix.is_empty() {
                // Add prefix to or_values
                for or_value in or_values.iter_mut() {
                    *or_value = format!("{prefix}{or_value}")
                }
            }
            or_values
        }
        Err(err) => {
            panic!("BUG: unexpected error when parsing verified tail_expr={tail_expr}: {:?}", err)
        }
    }
}

pub(crate) fn get_match_func_for_or_suffixes(or_values: Vec<String>) -> StringMatchHandler {
    if or_values.len() == 1 {
        let mut or_values = or_values;
        let v = or_values.remove(0);
        StringMatchHandler::equals(v)
    } else {
        // aho-corasick ?
        StringMatchHandler::Alternates(or_values, true)
    }
}

fn get_or_values_ext(sre: &Hir) -> Option<Vec<String>> {
    use HirKind::*;
    match sre.kind() {
        Empty => Some(vec!["".to_string()]),
        Capture(cap) => get_or_values_ext(cap.sub.as_ref()),
        Literal(literal) => {
            match String::from_utf8(literal.0.to_vec()) {
                Ok(s) => Some(vec![s]),
                Err(_) => None
            }
        },
        Alternation(alt) => {
            let mut a = Vec::with_capacity(alt.len());
            for sub in alt.iter() {
                let ca = get_or_values_ext(sub).unwrap_or_default();
                if ca.is_empty() {
                    return None;
                }
                a.extend(ca);
                if a.len() > MAX_OR_VALUES {
                    // It is cheaper to use regexp here.
                    return None;
                }
            }
            Some(a)
        }
        Concat(concat) => {
            if concat.is_empty() {
                return Some(vec!["".to_string()])
            }
            let prefixes = get_or_values_ext(&concat[0]).unwrap_or_default();
            if prefixes.is_empty() {
                return None;
            }
            if concat.len() == 1 {
                return Some(prefixes)
            }
            let subs = Vec::from(&concat[1..]);
            let concat = Hir::concat(subs);
            let suffixes = get_or_values_ext(&concat).unwrap_or_default();
            if suffixes.is_empty() {
                return None;
            }
            if prefixes.len() * suffixes.len() > MAX_OR_VALUES {
                // It is cheaper to use regexp here.
                return None;
            }
            let mut a = Vec::with_capacity(prefixes.len() * suffixes.len());
            for prefix in prefixes.iter() {
                for suffix in suffixes.iter() {
                    a.push(format!("{prefix}{suffix}"));
                }
            }
            Some(a)
        }
        Class(class) => {
            if let Some(literal) = class.literal() {
                return match String::from_utf8(literal.to_vec()) {
                    Ok(s) => Some(vec![s]),
                    Err(_) => None
                }
            }

            let mut a = Vec::with_capacity(32);
            match class {
                Unicode(uni) => {
                    for urange in uni.iter() {
                        let start = urange.start();
                        let end = urange.end();
                        for c in start..=end {
                            a.push(format!("{c}"));
                            if a.len() > MAX_OR_VALUES {
                                // It is cheaper to use regexp here.
                                return None;
                            }
                        }
                    }
                    Some(a)
                }
                Bytes(bytes) => {
                    for range in bytes.iter() {
                        let start = range.start();
                        let end = range.end();
                        for c in start..=end {
                            a.push(format!("{c}"));
                            if a.len() > MAX_OR_VALUES {
                                // It is cheaper to use regexp here.
                                return None;
                            }
                        }
                    }
                    Some(a)
                }
            }
        }
        _ => {
            None
        }
    }
}

fn is_literal(sre: &Hir) -> bool {
    match sre.kind() {
        HirKind::Literal(_) => true,
        HirKind::Capture(cap) => is_literal(cap.sub.as_ref()),
        _ => false
    }
}

/// simplifies the given expr.
///
/// It returns plaintext prefix and the remaining regular expression with dropped '^' and '$' anchors
/// at the beginning and the end of the regular expression.
///
/// The function removes capturing parens from the expr, so it cannot be used when capturing parens
/// are necessary.
pub fn simplify(expr: &str) -> Result<(String, String), RegexError> {
    if expr == ".*" || expr == ".+" {
        return Ok(("".to_string(), expr.to_string()))
    }

    let hir = match build_hir(expr) {
        Ok(hir) => hir,
        Err(_) => {
            return Ok(("".to_string(), "".to_string()))
        }
    };

    let mut sre = simplify_regexp(hir, false)?;

    if is_empty_regexp(&sre) {
        return Ok(("".to_string(), "".to_string()))
    }

    if is_literal(&sre) {
        return Ok((literal_to_string(&sre), "".to_string()))
    }

    let mut prefix: String = "".to_string();
    let mut sre_new: Option<Hir> = None;

    if let HirKind::Concat(concat) = sre.kind() {
        let head = &concat[0];
        let first_literal = is_literal(head);
        if first_literal {
            let lit = literal_to_string(head);
            prefix = lit.clone();
            match concat.len() {
                1 => return Ok((prefix, "".to_string())),
                2 => sre_new = Some( simplify_regexp(concat[1].clone(), true)? ),
                _ => {
                    let sub = Vec::from(&concat[1..]);
                    let temp = Hir::concat(sub);
                    sre_new = Some(temp);
                }
            }
        }
    }

    if sre_new.is_some() {
        sre = sre_new.unwrap();
    }

    if is_empty_regexp(&sre) {
        return Ok((prefix, "".to_string()))
    }

    let mut s = hir_to_string(&sre);

    if let Err(_) = Regex::new(&s) {
        // Cannot compile the regexp. Return it all as prefix.
        return Ok((expr.to_string(), "".to_string()))
    }

    s = s.replace( "(?:)", "");
    s = s.replace( "(?-s:.)", ".");
    s = s.replace("(?-m:$)", "$");
    Ok((prefix, s))
}

fn simplify_regexp(sre: Hir, has_prefix: bool) -> Result<Hir, RegexError> {
    if matches!(sre.kind(), HirKind::Empty) {
        return Ok(sre)
    }
    let mut sre = sre;
    loop {
        let sub = sre.clone();
        let hir_new = simplify_regexp_ext(sub, has_prefix, false);
        if hir_new == sre {
            return Ok(hir_new)
        }

        // build_hir(&s_new)?; // todo: this should panic

        sre = hir_new
    }
}

fn simplify_regexp_ext(sre: Hir, has_prefix: bool, has_suffix: bool) -> Hir {
    use HirKind::*;

    match sre.kind() {
        Alternation(alternate) => {
            // avoid clone if its all literal
            if alternate.iter().all(|hir| is_literal(hir)) {
                return sre
            }
            let mut sub = Vec::with_capacity(alternate.len());
            for hir in alternate.iter() {
                let simple = simplify_regexp_ext(hir.clone(), has_prefix, has_suffix);
                if !is_empty_regexp(&simple) {
                    sub.push(simple)
                }
            }

            if sub.len() == 1 {
                return sub.remove(0);
            }

            if sub.is_empty() {
                return Hir::empty()
            }

            Hir::alternation(sub)
        }
        Capture(cap) => {
            let sub = simplify_regexp_ext(cap.sub.as_ref().clone(), has_prefix, has_suffix);
            if is_empty_regexp(&sub) {
                return Hir::empty()
            }
            match sub.kind() {
                Concat(concat) => {
                    if concat.len() == 1 {
                        return concat[0].clone();
                    }
                }
                _ => {}
            }
            sub.clone()
        }
        Concat(concat) => {
            let mut sub = Vec::with_capacity(concat.len());
            for hir in concat.iter() {
                let simple = simplify_regexp_ext(hir.clone(), has_prefix, has_suffix);
                if !is_empty_regexp(&simple) {
                    sub.push(simple)
                }
            }

            if sub.len() == 1 {
                return sub.remove(0);
            }

            if sub.is_empty() {
                return Hir::empty()
            }

            Hir::concat(sub)
        }
        _=> {
            sre
        }
    }
}


fn hir_to_string(sre: &Hir) -> String {
    match sre.kind() {
        HirKind::Literal(lit) => {
            String::from_utf8(lit.0.to_vec()).unwrap_or_default()
        }
        HirKind::Concat(concat) => {
            let mut s = String::new();
            for hir in concat.iter() {
                s.push_str(&hir_to_string(hir));
            }
            s
        }
        HirKind::Alternation(alternate) => {
            // avoid extra allocation if its all literal
            if alternate.iter().all(|hir| is_literal(hir)) {
                return alternate
                    .iter()
                    .map(|hir| hir_to_string(hir))
                    .collect::<Vec<_>>()
                    .join("|")
            }
            let mut s = Vec::with_capacity(alternate.len());
            for hir in alternate.iter() {
                s.push(hir_to_string(hir));
            }
            s.join("|")
        }
        HirKind::Repetition(_repetition) => {
            if is_dot_star(sre) {
                return ".*".to_string();
            } else if is_dot_plus(sre) {
                return ".+".to_string();
            }
            sre.to_string()
        }
        _ => {
            sre.to_string()
        }
    }
}

fn literal_to_string(sre: &Hir) -> String {
    if let HirKind::Literal(lit) = sre.kind() {
        return String::from_utf8(lit.0.to_vec()).unwrap_or_default();
    }
    "".to_string()
}

fn dot_plus_matcher() -> StringMatchHandler {
    let match_fn = |needle, haystack| !needle.is_empty();
    StringMatchHandler::MatchFn(match_fn)
}

pub(super) fn get_prefix_matcher(prefix: &str) -> StringMatchHandler {
    if prefix == ".*" {
        return StringMatchHandler::MatchAll;
    }
    if prefix == ".+" {
        return dot_plus_matcher();
    }
    StringMatchHandler::StartsWith(prefix.to_string())
}

pub(super) fn get_suffix_matcher(suffix: &str) -> Result<StringMatchHandler, RegexError> {
    if !suffix.is_empty() {
        if suffix == ".*" {
            return Ok(StringMatchHandler::MatchAll);
        }
        if suffix == ".+" {
            return Ok(dot_plus_matcher());
        }
        if escape_regex(suffix) == suffix {
            // Fast path - pr contains only literal prefix such as 'foo'
            return Ok(StringMatchHandler::equals(suffix.to_string()));
        }
        let or_values = get_or_values(suffix);
        if !or_values.is_empty() {
            // Fast path - pr contains only alternate strings such as 'foo|bar|baz'
            return Ok(StringMatchHandler::Alternates(or_values));
        }
    }
    // It is expected that optimize returns valid regexp in suffix, so raise error if not.
    // Anchor suffix to the beginning and the end of the matching string.
    let suffix_expr = format!("^(?:{suffix})$");
    let re_suffix = Regex::new(&suffix_expr)?;
    Ok(StringMatchHandler::FastRegex(FastRegexMatcher::new(re_suffix)))
}

fn is_empty_regexp(sre: &Hir) -> bool {
    matches!(sre.kind(), HirKind::Empty)
}

fn is_dot_star(sre: &Hir) -> bool {
    match sre.kind() {
        HirKind::Capture(cap) => is_dot_star(cap.sub.as_ref()),
        HirKind::Alternation(alternate) => {
            alternate.iter().any(|re_sub| is_dot_star(re_sub))
        }
        HirKind::Repetition(repetition) => {
            repetition.min == 0 &&
                repetition.max.is_none() &&
                repetition.greedy == true &&
                sre.properties().is_literal() == false
        }
        _ => false,
    }
}

fn is_dot_plus(sre: &Hir) -> bool {
    match sre.kind() {
        HirKind::Capture(cap) => is_dot_plus(cap.sub.as_ref()),
        HirKind::Alternation(alternate) => {
            alternate.iter().any(|re_sub| is_dot_plus(re_sub))
        }
        HirKind::Repetition(repetition) => {
            repetition.min == 1 &&
                repetition.max.is_none() &&
                repetition.greedy == true &&
                sre.properties().is_literal() == false
        }
        _ => false,
    }
}

fn build_hir(pattern: &str) -> Result<Hir, RegexError> {
    parse_regex(pattern)
        .map_err(|err| RegexError::Syntax(err.to_string()))
}

#[cfg(test)]
mod test {
    use crate::common::simplify::{get_or_values, remove_start_end_anchors, simplify};

    #[test]
    fn test_get_or_values() {
        fn check(s: &str, values_expected: Vec<&str>) {
            let values = get_or_values(s);
            assert_eq!(
                values, values_expected,
                "unexpected values for s={:?}; got {:?}; want {:?}",
                s, values, values_expected
            )
        }

        check("", vec![""]);
        check("foo", vec!["foo"]);
        check("^foo$", vec!["foo"]);
        check("|foo", vec!["", "foo"]);
        check("|foo|", vec!["", "", "foo"]);
        check("foo.+", vec![]);
        check("foo.*", vec![]);
        check(".*", vec![]);
        check("foo|.*", vec![]);
        check("(fo((o)))|(bar)", vec!["bar", "foo"]);
        check("foobar", vec!["foobar"]);
        check("z|x|c", vec!["c", "x", "z"]);
        check("foo|bar", vec!["bar", "foo"]);
        check("(foo|bar)", vec!["bar", "foo"]);
        check("(foo|bar)baz", vec!["barbaz", "foobaz"]);
        check("[a-z][a-z]", vec![]);
        check("[a-d]", vec!["a", "b", "c", "d"]);
        check("x[a-d]we", vec!["xawe", "xbwe", "xcwe", "xdwe"]);
        check("foo(bar|baz)", vec!["foobar", "foobaz"]);
        check(
            "foo(ba[rz]|(xx|o))",
            vec!["foobar", "foobaz", "fooo", "fooxx"],
        );
        check(
            "foo(?:bar|baz)x(qwe|rt)",
            vec!["foobarxqwe", "foobarxrt", "foobazxqwe", "foobazxrt"],
        );
        check("foo(bar||baz)", vec!["foo", "foobar", "foobaz"]);
        check("(a|b|c)(d|e|f|0|1|2)(g|h|k|x|y|z)", vec![]);
        check("(?i)foo", vec![]);
        check("(?i)(foo|bar)", vec![]);
        check("^foo|bar$", vec!["bar", "foo"]);
        check("^(foo|bar)$", vec!["bar", "foo"]);
        check("^a(foo|b(?:a|r))$", vec!["aba", "abr", "afoo"]);
        check("^a(foo$|b(?:a$|r))$", vec!["aba", "abr", "afoo"]);
        check("^a(^foo|bar$)z$", vec![])
    }

    #[test]
    fn test_simplify() {
        fn check(s: &str, expected_prefix: &str, expected_suffix: &str) {
            let (prefix, suffix) = simplify(s).unwrap();
            assert_eq!(
                prefix, expected_prefix,
                "unexpected prefix for s={s}; got {prefix}; want {expected_prefix}");
            assert_eq!(
                suffix, expected_suffix,
                "unexpected suffix for s={s}; got {suffix}; want {expected_suffix}");
        }

        check("", "", "");
        check("^", "", "");
        check("$", "", "");
        check("^()$", "", "");
        check("^(?:)$", "", "");
        check("^foo|^bar$|baz", "", "foo|ba[rz]");
        check("^(foo$|^bar)$", "", "foo|bar");
        check("^a(foo$|bar)$", "a", "foo|bar");
        check("^a(^foo|bar$)z$", "a", "(?:\\Afoo|bar$)z");
        check("foobar", "foobar", "");
        check("foo$|^foobar", "foo", "|bar");
        check("^(foo$|^foobar)$", "foo", "|bar");
        check("foobar|foobaz", "fooba", "[rz]");
        check("(fo|(zar|bazz)|x)", "", "fo|zar|bazz|x");
        check("(тестЧЧ|тест)", "тест", "ЧЧ|");
        check("foo(bar|baz|bana)", "fooba", "[rz]|na");
        check("^foobar|foobaz", "fooba", "[rz]");
        check("^foobar|^foobaz$", "fooba", "[rz]");
        check("foobar|foobaz", "fooba", "[rz]");
        check("(?:^foobar|^foobaz)aa.*", "fooba", "[rz]aa.*");
        check("foo[bar]+", "foo", "[a-br]+");
        check("foo[a-z]+", "foo", "[a-z]+");
        check("foo[bar]*", "foo", "[a-br]*");
        check("foo[a-z]*", "foo", "[a-z]*");
        check("foo[x]+", "foo", "x+");
        check("foo[^x]+", "foo", "[^x]+");
        check("foo[x]*", "foo", "x*");
        check("foo[^x]*", "foo", "[^x]*");
        check("foo[x]*bar", "foo", "x*bar");
        check("fo\\Bo[x]*bar?", "fo", "\\Box*bar?");
        check("foo.+bar", "foo", ".+bar");
        check("a(b|c.*).+", "a", "(?:b|c.*).+");
        check("ab|ac", "a", "[b-c]");
        check("(?i)xyz", "", "(?i:XYZ)");
        check("(?i)foo|bar", "", "(?i:FOO)|(?i:BAR)");
        check("(?i)up.+x", "", "(?i:UP).+(?i:X)");
        check("(?smi)xy.*z$", "", "(?i:XY)(?s:.)*(?i:Z)(?m:$)");

        // test invalid regexps
        check("a(", "a(", "");
        check("a[", "a[", "");
        check("a[]", "a[]", "");
        check("a{", "a{", "");
        check("a{}", "a{}", "");
        check("invalid(regexp", "invalid(regexp", "");

        // The transformed regexp mustn't match aba
        check("a?(^ba|c)", "", "a?(?:\\Aba|c)");

        // The transformed regexp mustn't match barx
        check("(foo|bar$)x*", "", "(?:foo|bar$)x*");
    }

    #[test]
    fn test_remove_start_end_anchors() {
        fn f(s: &str, result_expected: &str) {
            let result = remove_start_end_anchors(s);
            assert_eq!(
                result, result_expected,
                "unexpected result for remove_start_end_anchors({s}); got {result}; want {}",
                result_expected
            );
        }

        f("", "");
        f("a", "a");
        f("^^abc", "abc");
        f("a^b$c", "a^b$c");
        f("$$abc^", "$$abc^");
        f("^abc|de$", "abc|de");
        f("abc\\$", "abc\\$");
        f("^abc\\$$$", "abc\\$");
        f("^a\\$b\\$$", "a\\$b\\$")
    }
}