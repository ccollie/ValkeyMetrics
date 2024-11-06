use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hasher;
use std::sync::OnceLock;
use std::time::Duration;
use xxhash_rust::xxh3::Xxh3;

use super::rule::RuleType;
use crate::alerts::{AlertsError, AlertsResult};
use crate::config::DEFAULT_RULE_UPDATE_ENTRIES_LIMIT;
/***
	rule_update_entries_limit = flag.Int("rule.updateEntriesLimit", 20, "Defines the max number of rule's state updates stored in-memory. "+
		"Rule's updates are available on rule's Details page and are used for debugging purposes. The number of stored updates can be overridden per rule via update_entries_limit param.")
	resendDelay = flag.Duration("rule.resendDelay", 0, "Minimum amount of time to wait before resending an alert to notifier")
	maxResolveDuration = flag.Duration("rule.maxResolveDuration", 0, "Limits the maximum duration for automatic alert expiration, "+
		"which by default is 4 times evaluationInterval of the parent group")
	evalDelay = flag.Duration("rule.evalDelay", 30*time.Second, "Adjustment of the `time` parameter for rule evaluation requests to compensate intentional data delay from the datasource."+
		"Normally, should be equal to `-search.latencyOffset` (cmd-line flag configured for VictoriaMetrics single-node or vmselect).")
	disableAlertGroupLabel = flag.Bool("disableAlertgroupLabel", false, "Whether to disable adding group's Name as label to generated alerts and time series.")
	remoteReadLookBack     = flag.Duration("remoteRead.lookback", time.Hour, "Lookback defines how far to look into past for alerts timeseries."+
		" For example, if lookback=1h then range from now() to now()-1h will be scanned.")
)
*/

/// ValidateTplFn must validate the given annotations
pub type ValidateTplFn = fn(annotations: &HashMap<String, String>) -> AlertsResult<()>;

/// `RuleConfig` describes entity that represent either recording rule or alerting rule.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleConfig {
    #[serde(skip)]
    pub id: u64,
    pub key: String,
    pub record: String,
    pub alert: String,
    pub expr: String,
    pub r#for: Duration,
    /// Alert will continue firing for this long even when the alerting expression no longer has results.
    pub keep_firing_for: Duration,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub debug: bool,
    /// update_entries_limit defines max number of rule's state updates stored in memory.
    /// Overrides `-rule.updateEntriesLimit`.
    pub update_entries_limit: Option<usize>,
}

impl RuleConfig {
    /// Hash returns unique hash of the RuleConfig
    pub fn hash(&self) -> u64 {
        hash_rule_config(self)
    }

    /// returns Rule name according to its type
    pub fn name(&self) -> &str {
        if !self.record.is_empty() {
            &self.record
        } else {
            &self.alert
        }
    }

    pub fn rule_type(&self) -> RuleType {
        if !self.record.is_empty() {
            RuleType::Recording
        } else {
            RuleType::Alerting
        }
    }

    pub fn update_entries_limit(&self) -> usize {
        // todo; this is a placeholder. use global config
        self.update_entries_limit.unwrap_or(DEFAULT_RULE_UPDATE_ENTRIES_LIMIT)
    }

    pub fn validate(&self) -> AlertsResult<()> {
        let name = self.name();

        let err = |msg: &str| -> AlertsResult<()> {
            return Err(AlertsError::InvalidRule(msg.to_string()));
        };

        if self.record.is_empty() && self.alert.is_empty() {
            let msg = format!("rule \"{name}\" must have either record or alert field set");
            return err(&msg);
        }
        if !self.record.is_empty() && !self.alert.is_empty() {
            let msg = format!("rule \"{name}\" should have either record or alert field set, not both");
            return err(&msg);
        }
        if self.expr.is_empty() {
            let msg = format!("rule \"{name}\" must have expression set");
            return err(&msg);
        }
        Ok(())
    }
}

impl Display for RuleConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut rule_type = "recording";
        if !self.alert.is_empty() {
            rule_type = "alerting"
        }
        write!(f, "{} rule {}; expr: {}", rule_type, self.name(), self.expr)?;
        let mut keys = self.labels.keys().collect::<Vec<_>>();
        keys.sort();

        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = self.labels.get(*key) {
                if i == 0 {
                    write!(f, "; labels:")?;
                }
                write!(f, " ")?;
                write!(f, "{}={}", key, value)?;
                if i < keys.len() - 1 {
                    write!(f, ",")?;
                }
            }
        }
        Ok(())
    }
}

/// SkipRandSleepOnGroupStart will skip random sleep delay in group first evaluation
pub static SKIP_RAND_SLEEP: OnceLock<bool> = OnceLock::new();

// todo: this is a placeholder. use global config
pub(crate) fn should_skip_rand_sleep_on_group_start() -> bool {
    *SKIP_RAND_SLEEP.get_or_init(|| {
        // get value from env
        let env_val = std::env::var("SKIP_RAND_SLEEP").unwrap_or_default();
        env_val == "true"
    })
}

/// Group contains list of Rules grouped into an entity with one name and evaluation interval
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GroupConfig {
    pub name: String,
    pub interval: Option<Duration>,
    pub eval_offset: Option<Duration>,
    pub eval_delay: Option<Duration>,
    pub limit: usize,
    pub rules: Vec<RuleConfig>,
    pub concurrency: usize,
    /// Labels is a set of label value pairs, that will be added to every rule.
    /// It has priority over the external labels.
    pub labels: HashMap<String, String>,
    /// Optional parameters added to each rule request
    pub params: Option<HashMap<String, String>>,
    /// optional headers sent to notifiers for generated notifications
    pub notifier_headers: Vec<Header>,
    /// eval_alignment will make the timestamp of group query requests be aligned with interval
    pub eval_alignment: Option<bool>,
    pub disabled: bool // change to paused ????
}

/// Header is a Key - Value struct for holding an HTTP header.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Header {
    pub key: String,
    pub value: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Headers(pub Vec<Header>);

impl Headers {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Header> {
        self.0.iter()
    }
}

impl From<&Headers> for HashMap<String, String> {
    fn from(h: &Headers) -> Self {
        let mut map = HashMap::with_capacity(h.0.len());
        for header in h.0.iter() {
            map.insert(header.key.clone(), header.value.clone());
        }
        map
    }
}

impl From<Headers> for HashMap<String, String> {
    fn from(h: Headers) -> Self {
        h.into()
    }
}
impl GroupConfig {
    pub fn validate(&self, validate_tpl_fn: ValidateTplFn, validate_expressions: bool) -> AlertsResult<()> {
        fn err(msg: &str) -> AlertsResult<()> {
            Err(AlertsError::InvalidConfiguration(msg.to_string()))
        }

        if self.name.is_empty() {
            return err("group name must be set");
        }
        
        if let Some(offset) = &self.eval_offset {
            if let Some(interval) = &self.interval {
                // if `eval_offset` is set, interval won't use global evaluationInterval flag and
                // must be bigger than offset.
                if offset > interval {
                    let msg = format!("eval_offset should be smaller than interval; now eval_offset: {}, interval: {}",
                                      offset.as_millis(), interval.as_millis());
                    return err(&msg);
                }
            }
        }

        let mut unique_rules = HashSet::with_capacity(self.rules.len());

        for r in self.rules.iter() {
            let rule_name = r.name();
            let id = r.id;
            if unique_rules.contains(&id) {
                return Err(AlertsError::InvalidConfiguration(format!("{} is a duplicate in group", r)));
            }
            unique_rules.insert(id);
            r.validate()?;

            if validate_expressions {
                validate_expr(&r.expr)
                    .map_err(|err| {
                        let msg = format!("invalid expression for rule {}: {:?}", rule_name, err);
                        AlertsError::InvalidRule(msg)
                    })?;
            }

            validate_tpl_fn(&r.annotations)
                .map_err(|err| {
                    let msg = format!("invalid annotations for rule {}: {:?}", rule_name, err);
                    AlertsError::InvalidRule(msg)
                })?;

            validate_tpl_fn(&r.labels)
                .map_err(|err| {
                    let msg = format!("invalid labels for rule {}: {:?}", rule_name, err);
                    AlertsError::InvalidRule(msg)
                })?;
        }
        Ok(())
    }
}

fn validate_expr(expr: &str) -> AlertsResult<()> {
    match metricsql_parser::parser::parse(expr) {
        Ok(_) => Ok(()),
        Err(err) => Err(AlertsError::InvalidConfiguration(format!("invalid expression: {:?}", err)))
    }
}

/// HashRule hashes significant Rule fields into unique hash that defines Rule uniqueness
fn hash_rule_config(r: &RuleConfig) -> u64 {
    let mut h = Xxh3::new();
    h.write(r.expr.as_bytes());
    if !r.record.is_empty() {
        h.write("recording".as_bytes());
        h.write(r.record.as_bytes());
    } else {
        h.write("alerting".as_bytes());
        h.write(r.alert.as_bytes());
    }
    let mut keys = r.labels.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        let v = r.labels.get(key).unwrap();
        h.write(key.as_bytes());
        h.write(v.as_bytes());
        h.write("\0xff".as_ref());
    }
    h.digest()
}
