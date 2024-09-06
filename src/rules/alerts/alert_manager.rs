use std::sync::{Arc, RwLock};
use std::time::Duration;

use ahash::{AHashMap, HashMap};
use valkey_module::Context;

use crate::rules::alerts::{AlertsError, AlertsResult, Group, GroupConfig, Notifier, QuerierBuilder, WriteQueue};

/// manager controls group states
pub struct Manager {
    querier_builder: Box<dyn QuerierBuilder>,
    notifiers: Arc<Vec<Box<dyn Notifier>>>,
    rw: Arc<WriteQueue>,
    labels: AHashMap<String, String>,
    groups: RwLock<AHashMap<u64, Group>>,
    evaluation_interval: Duration,
}

impl Manager {

    fn start(&mut self, ctx: &Context, groups_cfg: &[GroupConfig]) -> AlertsResult<()> {
        self.update(ctx, groups_cfg, true)
    }

    pub fn close(&mut self) -> AlertsResult<()> {
        self.rw.close()?;
        let mut groups = self.groups.write().unwrap();
        for (_, g) in groups.iter_mut() {
            g.close()
        }
        Ok(())
    }

    fn start_group(&mut self, ctx: &Context, group: Group, restore: bool) -> AlertsResult<()> {
        let id = group.id();
        let mut group = group;
        if restore {
            group.start(ctx, self.notifiers, Arc::clone(&self.rw), self.querier_builder)
        } else {
            group.start(ctx, self.notifiers, Arc::clone(&self.rw), None)
        }
        let mut groups = self.groups.write().unwrap();
        groups.insert(id, group);
        Ok(())
    }

    fn update(&mut self, ctx: &Context, groups_cfg: &[GroupConfig], restore: bool) -> AlertsResult<()> {
        let mut rr_present = false;
        let mut ar_present = false;

        let mut groups_registry: HashMap<u64, Group> = HashMap::default();
        for cfg in groups_cfg {
            for r in cfg.rules {
                if rr_present && ar_present {
                    continue
                }
                if !r.record.is_empty() {
                    rr_present = true
                }
                if !r.alert.is_empty() {
                    ar_present = true
                }
            }
            let ng = Group::new(cfg, &self.querier_builder, self.evaluation_interval, &self.labels);
            groups_registry.insert(ng.ID(), ng)
        }

        if ar_present && self.notifiers.is_empty() {
            return Err(AlertsError::Configuration("config contains alerting rules but neither `-notifier.url` nor `-notifier.config` nor `-notifier.blackhole` aren't set".to_string()))
        }
        struct UpdateItem<'a> {
            old: &'a Group,
            new: &'a Group
        }

        let mut to_update = vec![];

        let mut groups = self.groups.write().unwrap();
        let to_delete = vec![];
        for (_, og) in groups.iter_mut() {
            let ng = groups_registry.get(og.ID());
            if ng.is_none() {
                // old group is not present in new list,
                // so must be stopped and deleted
                self.remove(og.ID());
                continue
            }
            let ng = ng.unwrap();
            groups_registry.remove(ng.ID());
            if og.checksum != ng.checksum {
                to_update.push(UpdateItem{old: &og, new: ng})
            }
        }
        for (_, ng) in groups_registry {
            self.start_group(ctx, ng, restore)?;
        }
        if !to_update.is_empty() {
            for item in to_update {
                old.update_with(new);
                item.old.interrupt_eval();
            }
        }
        Ok(())
    }
}

