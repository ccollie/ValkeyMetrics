use gtmpl::{Template, Value};
use crate::rules::alerts::template::{template_funcs, TextTemplate};

// see also https://github.com/prometheus/prometheus/blob/main/template/template_test.go
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmalert/templates/template_test.go
#[test]
fn test_template_funcs() {
    let funcs = template_funcs();
    fn f(func_name: &str, s: &str, result_expected: &str) {
        let funcs = template_funcs();
        let flocal = funcs.get(func_name).unwrap();
        let result = (flocal)(vec![s].into()).unwrap();
        assert_eq!(result, result_expected,
                   "unexpected result for {func_name}({s}); got\n{}\nwant\n{result_expected}", result);
    }

	f("title", "foo bar", "Foo Bar");
	f("toUpper", "foo", "FOO");
	f("toLower", "FOO", "foo");
	f("pathEscape", "foo/bar\n+baz", "foo%2Fbar%0A+baz");
	f("queryEscape", "foo+bar\n+baz", "foo%2Bbar%0A%2Bbaz");
	f("jsonEscape", r#"foo{bar="baz"}`+"\n + 1"#, "foo{bar=\"baz\"}\n + 1");
	f("quotesEscape", r#"foo{bar="baz"}`+"\n + 1"#, "foo{bar=\"baz\"}\n + 1");
	f("htmlEscape", "foo < 10\nabc", "foo &lt; 10\nabc");
	f("crlfEscape", "foo\nbar\rx", r#"foo\nbar\rx"#);
	f("stripPort", "foo", "foo");
	f("stripPort", "foo:1234", "foo");
	f("stripDomain", "foo.bar.baz", "foo");
	f("stripDomain", "foo.bar:123", "foo:123");

	// check "match" func
	let match_func = funcs.get("match").unwrap();
	if let Ok(_) = match_func(["invalid[regexp", "abc"].into()) {
		panic!("expecting non-None error on invalid regexp")
	}
	let ok = match_func(["abc", "def"].into()).expect("unexpected error");
	assert!(ok, "unexpected mismatch");
	let ok = match_func(["a.+b", "acsdb"].into()).expect("unexpected error");
	assert!(ok, "unexpected mismatch");

	fn formatting(func_name: &str, p: impl Into<Value>, result_expected: &str) {
		let funcs = template_funcs();
		let flocal = funcs.get(func_name).unwrap();
		let result = (flocal)(p).unwrap();
		assert_eq!(result, result_expected,
				   "unexpected result for {func_name}({p}); got\n{result}\nwant\n{result_expected}");
	}

	formatting("humanize1024", 0, "0");
	formatting("humanize1024", f64::INFINITY, "+Inf");
	formatting("humanize1024", f64::NAN, "NaN");
	formatting("humanize1024", 127087, "124.1ki");
	formatting("humanize1024", 130137088, "124.1Mi");
	formatting("humanize1024", 133260378112, "124.1Gi");
	formatting("humanize1024", 136458627186688, "124.1Ti");
	formatting("humanize1024", 139733634239168512, "124.1Pi");
	formatting("humanize1024", 143087241460908556288, "124.1Ei");
	formatting("humanize1024", 146521335255970361638912, "124.1Zi");
	formatting("humanize1024", 150037847302113650318245888, "124.1Yi");
	formatting("humanize1024", 153638755637364377925883789312, "1.271e+05Yi");

	formatting("humanize", 127087, "127.1k");
	formatting("humanize", 136458627186688, "136.5T");

	formatting("humanizeDuration", 1, "1s");
	formatting("humanizeDuration", 0.2, "200ms");
	formatting("humanizeDuration", 42000, "11h 40m 0s");
	formatting("humanizeDuration", 16790555, "194d 8h 2m 35s");

	formatting("humanizePercentage", 1, "100%");
	formatting("humanizePercentage", 0.8, "80%");
	formatting("humanizePercentage", 0.015, "1.5%");

	formatting("humanizeTimestamp", 1679055557, "2023-03-17 12:19:17 +0000 UTC")
}

fn mk_template(current: Option<&str>, replacement: Option<&str>) -> TextTemplate {
	let mut tmpl = TextTemplate::default();
	if let Some(current) = current {
		let mut tmp = Template::default();
		tmp.parse(current).unwrap();
		tmpl.current = tmp;
	}
	if let Some(replacement) = replacement {
		let mut tmp = Template::default();
		tmp.parse(replacement).unwrap();
		tmpl.replacement = tmp
	}
	return tmpl
}

fn equal_templates(tmpls: &[Template]) -> bool {
	let cmp: &Template;
	for (i, tmpl) in tmpls.iter().enumerate() {
		if i == 0 {
			cmp = tmpl
		} else {
			if cmp == None {
				if cmp != tmpl {
					return false
				}
				continue
			}
			if tmpls.len() != cmp.templates.len() {
				return false
			}
			for t in tmpl.templates() {
				let tp = cmp.Lookup(t.name);
				if tp == None {
					return false
				}
				if tp.root.String() != t.root.String() {
					return false
				}
			}
		}
	}
	true
}

struct LoadTestCase<'a> {
	name: &'a str,
	initial_template: TextTemplate,
	path_patterns: Vec<&'a str>,
	overwrite: bool,
	expected_template: TextTemplate,
	exp_err: &'a str
}

impl<'a> LoadTestCase<'a> {
	fn new(name: &'a str, initial_template: TextTemplate, path_patterns: Vec<&'a str>, overwrite: bool, expected_template: TextTemplate, exp_err: &'a str) -> Self {
		LoadTestCase {
			name,
			initial_template,
			path_patterns,
			overwrite,
			expected_template,
			exp_err,
		}
	}
}
#[test]
fn test_templates_load() {
	let test_cases: Vec<LoadTestCase> = vec![
		LoadTestCase::new(
			"non existing path undefined template override",
			mk_template(None, None),
			vec![
				"templates/non-existing/good-*.tpl",
				"templates/absent/good-*.tpl"
			],
			true,
			mk_template(Some(""), None),
			"",
		),
		LoadTestCase::new(
			"non existing path defined template override",
			mk_template(Some(r#"
				{{- define "test.1" -}}
					{{- printf "value" -}}
				{{- end -}}
			"#), None),
			vec![
				"templates/non-existing/good-*.tpl",
				"templates/absent/good-*.tpl"
			],
			true,
			mk_template(Option::from(""), None),
			""
		),
		LoadTestCase::new(
			"existing path undefined template override",
			mk_template(None, None),
			vec![
				"templates/other/nested/good0-*.tpl",
				"templates/test/good0-*.tpl"
			],
			false,
			mk_template(Some(r#"
				{{- define "good0-test.tpl" -}}{{- end -}}
				{{- define "test.0" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.1" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.2" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.3" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
			"#), None),
			""
		),
		LoadTestCase::new(
			"existing path defined template override",
			mk_template(Some(r#"
				{{- define "test.1" -}}
					{{ printf "Hello %s!" "world" }}
				{{- end -}}
			"#), None),
			vec![
				"templates/other/nested/good0-*.tpl",
				"templates/test/good0-*.tpl",
			],
			false,
			mk_template(Some(r##"
				{{- define "good0-test.tpl" -}}{{- end -}}
				{{- define "test.0" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.1" -}}
					{{ printf "Hello %s!" "world" }}
				{{- end -}}
				{{- define "test.2" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.3" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				"##, r##"
				{{- define "good0-test.tpl" -}}{{- end -}}
				{{- define "test.0" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.1" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.2" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
				{{- define "test.3" -}}
					{{ printf "Hello %s!" externalURL }}
				{{- end -}}
			"##),
			"",
		),
		LoadTestCase::new(
			"load template with syntax error",
			mk_template(r#"
				{{- define "test.1" -}}
					{{ printf "Hello %s!" "world" }}
				{{- end -}}
			"#, None),
			vec![
				"templates/other/nested/bad0-*.tpl",
				"templates/test/good0-*.tpl",
			],
			false,
			mk_template(r#"
				{{- define "test.1" -}}
					{{ printf "Hello %s!" "world" }}
				{{- end -}}
			"#, None),
			"failed to parse template glob",
		)
	];

	for tc in test_cases {
		let master_tmpl = tc.initial_template;
		err := Load(tc.path_patterns, tc.overwrite);
		if tc.exp_err == "" && err != None {
			t.Error("happened error that wasn't expected: %w", err)
		}
		if tc.exp_err != "" {
			t.Error("%+w", err);
			panic!("expected error that didn't happen")
		}
		if err != None && !err.to_string().contains(tc.exp_err) {
			panic!("expected string '{}' doesn't exist in error message '{:?}'", tc.exp_err, err)
		}
		if !equal_templates(&[master_tmpl.replacement, tc.expected_template.replacement]) {
			panic!("replacement template is not as expected")
		}
		if !equal_templates(&[master_tmpl.current, tc.expected_template.current]) {
			panic!("current template is not as expected")
		}
	}
}

struct ReloadTestCase<'a> {
	name:             &'a str,
	initial_template: TextTemplate,
	expected_template: TextTemplate,
}

impl<'a> ReloadTestCase<'a> {
	fn new(name: &'a str, initial_template: TextTemplate, expected_template: TextTemplate) -> Self {
		ReloadTestCase {
			name,
			initial_template,
			expected_template,
		}
	}
} 

#[test]
fn test_templates_reload() {
	let test_cases: Vec<ReloadTestCase> = vec![
		ReloadTestCase::new(
			"empty current and replacement templates",
			mk_template(None, None),
			mk_template(None, None),
		),
		ReloadTestCase::new(
			"empty current template only",
			mk_template(Some(r#"
				{{- define "test.1" -}}
					{{- printf "value" -}}
				{{- end -}}
			"#), None),
			mk_template(Some(r#"
				{{- define "test.1" -}}
					{{- printf "value" -}}
				{{- end -}}
			"#), None),
		),
		ReloadTestCase::new(
			"empty replacement template only",
			mk_template(None, Some(r#"
				{{- define "test.1" -}}
					{{- printf "value" -}}
				{{- end -}}
			"#)),
			mk_template(Some(r#"
				{{- define "test.1" -}}
					{{- printf "value" -}}
				{{- end -}}
			"#), None),
		),
		ReloadTestCase::new(
			"defined both templates",
			mk_template(Some(r#"
				{{- define "test.0" -}}
					{{- printf "value" -}}
				{{- end -}}
				{{- define "test.1" -}}
					{{- printf "before" -}}
				{{- end -}}
			"#), Some(r#"
				{{- define "test.1" -}}
					{{- printf "after" -}}
				{{- end -}}
			"#)),
			mk_template(Some(r#"
				{{- define "test.1" -}}
					{{- printf "after" -}}
				{{- end -}}
			"#), None),
		)
	];

	for tc in test_cases {
		let master_tmpl = tc.initial_template;
		Reload()
		if !equal_templates(master_tmpl.replacement, tc.expectedTemplate.replacement) {
			panic!("replacement template is not as expected")
		}
		if !equal_templates(master_tmpl.current, tc.expectedTemplate.current) {
			panic!("current template is not as expected")
		}
	}
}