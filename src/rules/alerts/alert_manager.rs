use std::sync::Arc;
use ahash::{AHashMap, HashMap};
use redis_module::Context;
use crate::rules::alerts::{AlertsResult, Group, GroupConfig, Notifier, QuerierBuilder, WriteQueue};

/// manager controls group states
pub struct Manager {
    querier_builder: Box<dyn QuerierBuilder>,
    notifiers: fn() -> Vec<Box<dyn Notifier>>,

    rw: Arc<WriteQueue>,
    // remote read builder.
    rr: Box<dyn QuerierBuilder>,

    labels: AHashMap<String, String>,
    groups: HashMap<u64, Group>
}

impl Manager {
    fn start(&self, ctx: &Context, groups_cfg: &[GroupConfig]) -> AlertsResult<()> {
        return self.update(ctx, groups_cfg, true)
    }

    fn start_group(&mut self, ctx: &Context, g: &Group, restore: bool) -> AlertsResult<()> {
        let id = g.ID();
        if restore {
            g.start(ctx, self.notifiers, self.rw, m.rr)
        } else {
            g.start(ctx, self.notifiers, self.rw, nil)
        }
        self.groups.insert(id, g);
        Ok(())
    }

    fn update(&self, ctx: &Context, groups_cfg: &[GroupConfig], restore: bool) -> AlertsResult<()> {
        let mut rr_present = false;
        let mut ar_present = false;

        let mut groups_registry: HashMap<u64, Group> = HashMap::default();
        for cfg in groups_cfg {
            for r in cfg.rules {
                if rr_present && ar_present {
                    continue
                }
                if r.record != "" {
                    rr_present = true
                }
                if r.alert != "" {
                    ar_present = true
                }
            }
            let ng = Group::new(cfg, self.querierBuilder, *evaluationInterval, &self.labels);
            groups_registry.insert(ng.ID(), ng)
        }

        if rr_present && m.rw == nil {
            return fmt.Errorf("config contains recording rules but `-remoteWrite.url` isn't set")
        }
        if ar_present && m.notifiers == nil {
            return fmt.Errorf("config contains alerting rules but neither `-notifier.url` nor `-notifier.config` nor `-notifier.blackhole` aren't set")
        }
        struct UpdateItem<'a> {
            old: &'a Group,
            new: &'a Group
        }

        let mut to_update = vec![];

        m.groupsMu.Lock();
        for (_, og) in self.groups {
            if let Some(ng) = groups_registry.get(og.ID()) {
                // old group is not present in new list,
                // so must be stopped and deleted
                self.remove(og.ID());
                continue
            }
            delete(groups_registry, ng.ID())
            if og.checksum != ng.Checksum {
                to_update.push(UpdateItem{old: og, new: ng})
            }
        }
        for (_, ng) in groups_registry {
            self.start_group(ctx, ng, restore)?;
        }
        if !to_update.is_empty() {
            for item in to_update {
                old.update_with(new);
                item.old.InterruptEval()
            }
        }
        Ok(())
    }
}

