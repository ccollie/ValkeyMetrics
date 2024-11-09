use std::cmp::Ordering;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::RwLock;
use crate::alerts::commands::api::{group_to_api, is_in_list};
use crate::alerts::commands::filtering::RulesFilter;
use crate::alerts::rules::MetricRule;

const PARAM_GROUP_ID: &str = "group_id";
const PARAM_ALERT_ID: &str = "alert_id";
const PARAM_RULE_ID: &str = "rule_id";
const RULE_TYPE_ALERTING: &str = "alerting";
const RULE_TYPE_RECORDING: &str = "recording";

#[derive(Debug, Clone)]
struct RequestHandler {
    m: Manager,
}

#[derive(Debug, Clone)]
struct Manager {
    groups_mu: RwLock<()>,
    groups: Vec<Group>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListGroupsResponse {
    status: String,
    data: ListGroupsData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListGroupsData {
    groups: Vec<ApiGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListAlertsResponse {
    status: String,
    data: ListAlertsData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListAlertsData {
    alerts: Vec<ApiAlert>,
}

impl RequestHandler {
    fn handler(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let path = req.path();
        let method = req.method();

        match path {
            "/vmalert/alerts" => self.list_alerts(req),
            "/vmalert/alert" => self.get_alert(req),
            "/vmalert/rule" => self.get_rule(req),
            "/vmalert/groups" => self.list_groups(req),
            "/vmalert/notifiers" => self.list_notifiers(req),
            "/rules" => self.list_groups(req),
            "/vmalert/api/v1/rules" => self.api_list_groups(req),
            "/vmalert/api/v1/alerts" => self.api_list_alerts(req),
            "/vmalert/api/v1/alert"  => self.api_get_alert(req),
            "/api/v1/rule" => self.api_get_rule(req),
            _ => Ok(HttpResponse::NotFound().body("Not Found")),
        }
    }

    fn list_alerts(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let g_alerts = self.group_alerts();
        Ok(HttpResponse::Ok().json(g_alerts))
    }

    fn get_alert(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let group_id = req.match_info().get(PARAM_GROUP_ID).unwrap().parse::<u64>().unwrap();
        let alert_id = req.match_info().get(PARAM_ALERT_ID).unwrap().parse::<u64>().unwrap();
        let alert = self.m.alert_api(group_id, alert_id).unwrap();
        Ok(HttpResponse::Ok().json(alert))
    }

    fn get_rule(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let group_id = req.match_info().get(PARAM_GROUP_ID).unwrap().parse::<u64>().unwrap();
        let rule_id = req.match_info().get(PARAM_RULE_ID).unwrap().parse::<u64>().unwrap();
        let rule = self.m.rule_api(group_id, rule_id).unwrap();
        Ok(HttpResponse::Ok().json(rule))
    }

    fn list_groups(&self, rf: RulesFilter) -> Result<HttpResponse, Error> {
        let groups = self.groups(rf);
        Ok(HttpResponse::Ok().json(groups))
    }

    fn api_list_groups(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let groups = self.groups(rf);
        let data = self.list_groups(rf).unwrap();
        Ok(HttpResponse::Ok().content_type("application/json").body(data))
    }

    fn api_list_alerts(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let data = self.list_alerts().unwrap();
        Ok(HttpResponse::Ok().content_type("application/json").body(data))
    }

    fn api_get_alert(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let alert = self.get_alert(req).unwrap();
        let data = serde_json::to_string(&alert).unwrap();
        Ok(HttpResponse::Ok().content_type("application/json").body(data))
    }

    fn api_get_rule(&self, req: HttpRequest) -> Result<HttpResponse, Error> {
        let rule = self.get_rule(req).unwrap();
        let rwu = ApiRuleWithUpdates {
            api_rule: rule,
            state_updates: rule.updates,
        };
        let data = serde_json::to_string(&rwu).unwrap();
        Ok(HttpResponse::Ok().content_type("application/json").body(data))
    }
    
    fn groups(&self, rf: RulesFilter) -> Vec<ApiGroup> {
        let _guard = self.m.groups_mu.read().unwrap();
        
        let mut groups = &self.m.groups
            .filter(|group| is_in_list(&rf.group_names, &group.name))
            .collect();
        
        groups.sort_by(|a, b| {
            let ordering = a.name.cmp(&b.name);
            if ordering == Ordering::Equal {
                let right_id = b.id();
                a.id().cmp(&right_id)
            } else {
                ordering
            }
        });
        
        let mut result = Vec::new();
        for group in groups.into_iter() {
            let g = group_to_api(group, Some(&rf));
            result.push(g);
        }
        
        result
    }

    fn list_groups(&self, rf: RulesFilter) -> Result<Vec<u8>, Error> {
        let lr = ListGroupsResponse {
            status: "success".to_string(),
            data: ListGroupsData {
                groups: self.groups(rf),
            },
        };
        let b = serde_json::to_vec(&lr).unwrap();
        Ok(b)
    }

    fn group_alerts(&self) -> Vec<GroupAlerts> {
        let _guard = self.m.groups_mu.read().unwrap();

        let mut g_alerts = Vec::new();
        for g in &self.m.groups {
            let mut alerts = Vec::new();
            for r in g.rules.iter()
                .filter_map(|r| match r {
                    MetricRule::AlertingRule(a) => Some(a),
                    _ => None,
                }) {
                alerts.extend(rule_to_api_alerts(a));
            }
            if !alerts.is_empty() {
                g_alerts.push(GroupAlerts {
                    group: group_to_api(g, None),
                    alerts,
                });
            }
        }

        g_alerts.sort_by(|a, b| a.group.name.cmp(&b.group.name));
        g_alerts
    }

    fn list_alerts(&self) -> Result<Vec<u8>, Error> {
        let _guard = self.m.groups_mu.read().unwrap();

        let mut lr = ListAlertsResponse {
            status: "success".to_string(),
            data: ListAlertsData {
                alerts: Vec::new(),
            },
        };

        for g in &self.m.groups {
            for r in &g.rules.iter() {
                if let MetricRule::AlertingRule(a) = r {
                    lr.data.alerts.extend(rule_to_api_alert(a));
                }
            }
        }

        lr.data.alerts.sort_by(|a, b| a.id.cmp(&b.id));

        let b = serde_json::to_vec(&lr).unwrap();
        Ok(b)
    }
}

#[derive(Debug, Clone)]
struct ApiRule {
    rule_type: String,
    name: String,
    alerts: Option<Vec<ApiAlert>>,
    // Other fields
}

#[derive(Debug, Clone)]
struct ApiAlert {
    // Fields
}

#[derive(Debug, Clone)]
struct ApiRuleWithUpdates {
    api_rule: ApiRule,
    state_updates: Vec<StateEntry>,
}

mod notifier {
    pub fn get_targets() -> Vec<String> {
        // Implementation here
        Vec::new()
    }
}