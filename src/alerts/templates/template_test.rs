#[cfg(test)]
mod tests {
    use crate::alerts::templates::template_funcs;
    use gtmpl_value::Value;

    #[test]
    fn test_template_funcs_string_conversion() {
        let test_cases = vec![
            ("title", "foo bar", "Foo Bar"),
            ("to_upper", "foo", "FOO"),
            ("to_lower", "FOO", "foo"),
            ("path_escape", "foo/bar\n+baz", "foo%2Fbar%0A+baz"),
            ("query_escape", "foo+bar\n+baz", "foo%2Bbar%0A%2Bbaz"),
            (
                "json_escape",
                r#"foo{bar="baz"}\n + 1"#,
                r#""foo{bar=\"baz\"}\n + 1""#,
            ),
            (
                "quotes_escape",
                r#"foo{bar="baz"}\n + 1"#,
                r#"foo{bar=\"baz\"}\n + 1"#,
            ),
            ("html_escape", "foo < 10\nabc", "foo &lt; 10\nabc"),
            ("crlf_escape", "foo\nbar\rx", r#"foo\nbar\rx"#),
            ("strip_port", "foo", "foo"),
            ("strip_port", "foo:1234", "foo"),
            ("strip_domain", "foo.bar.baz", "foo"),
            ("strip_domain", "foo.bar:123", "foo:123"),
        ];

        let func_map = template_funcs();
        for (func_name, input, expected) in test_cases {
            let func = func_map.get(func_name).unwrap();
            let value: Value = input.into();
            let actual = func(&[value]).unwrap();
            assert_eq!(
                actual.to_string(),
                expected,
                "unexpected result for {}({})",
                func_name,
                input
            );
        }
    }

    #[test]
    fn test_template_funcs_match() {
        let func_map = template_funcs();
        let match_func = func_map["match"];

        let result = match_func(&["invalid[regexp".into(), "abc".into()]);
        assert!(result.is_err(), "expecting non-nil error on invalid regexp");

        let result = match_func(&["abc".into(), "def".into()]).unwrap();
        assert!(matches!(result, Value::Bool(v) if !v), "unexpected match");

        let result = match_func(&["a.+b".into(), "acsdb".into()]).unwrap();
        assert!(matches!(result, Value::Bool(v) if v), "unexpected mismatch");
    }

    #[test]
    fn test_template_funcs_formatting() {
        let test_cases = vec![
            ("humanize1024", 0.0, "0"),
            ("humanize1024", f64::INFINITY, "+Inf"),
            ("humanize1024", f64::NAN, "NaN"),
            ("humanize1024", 127087.0, "124.1ki"),
            ("humanize1024", 130137088.0, "124.1Mi"),
            ("humanize1024", 133260378112.0, "124.1Gi"),
            ("humanize1024", 136458627186688.0, "124.1Ti"),
            ("humanize1024", 139733634239168512.0, "124.1Pi"),
            ("humanize1024", 143087241460908556288.0, "124.1Ei"),
            ("humanize1024", 146521335255970361638912.0, "124.1Zi"),
            ("humanize1024", 150037847302113650318245888.0, "124.1Yi"),
            (
                "humanize1024",
                153638755637364377925883789312.0,
                "1.271e+05Yi",
            ),
            ("humanize", 127087.0, "127.1k"),
            ("humanize", 136458627186688.0, "136.5T"),
            ("humanizeDuration", 1.0, "0d 0h 0m 1s"),
            ("humanizeDuration", 0.2, "0d 0h 0m 0s"),
            ("humanizeDuration", 42000.0, "0d 11h 40m 0s"),
            ("humanizeDuration", 16790555.0, "194d 8h 2m 35s"),
            ("humanizePercentage", 1.0, "100.0%"),
            ("humanizePercentage", 0.8, "80.0%"),
            ("humanizePercentage", 0.015, "1.5%"),
            (
                "humanize_timestamp",
                1679055557.0,
                "2023-03-17T12:19:17+00:00",
            ),
        ];

        let func_map = template_funcs();
        for (func_name, input, expected) in test_cases {
            let func = func_map.get(func_name);
            if func.is_none() {
                panic!("function '{}' not found", func_name);
            }
            let func = func.unwrap();
            let value: Value = input.into();
            let actual = func(&[value]).unwrap().to_string();

            assert_eq!(
                actual, expected,
                "unexpected result for {}({})",
                func_name, input
            );
        }
    }
}
