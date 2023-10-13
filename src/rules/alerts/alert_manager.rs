use std::sync::Arc;
use ahash::{AHashMap, HashMap};
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
    fn start(&self, ctx: Context, groups_cfg: &[GroupConfig]) -> AlertsResult<()> {
        return self.update(ctx, groups_cfg, true)
    }

    fn start_group(&mut self, ctx: Context, g: &Group, restore: bool) -> AlertsResult<()> {
        let id = g.ID();
        if restore {
            g.start(ctx, m.notifiers, m.rw, m.rr)
        } else {
            g.Start(ctx, m.notifiers, m.rw, nil)
        }
        self.groups.insert(id, g);
        Ok(())
    }
}




func (m *manager) update(ctx: Context, groupsCfg: &[GroupConfig], restore: bool) -> AlertsResult<()> {
    let mut rrPresent = false;
    let mut arPresent = false;

    let mut groupsRegistry = make(map[uint64]*rule.Group)
    for cfg in groupsCfg {
        for r in cfg.rules {
            if rrPresent && arPresent {
                continue
            }
            if r.record != "" {
                rrPresent = true
            }
            if r.alert != "" {
                arPresent = true
            }
        }
        let ng = rule.NewGroup(cfg, m.querierBuilder, *evaluationInterval, m.labels)
        groupsRegistry.insert(ng.ID(), ng)
    }

    if rrPresent && m.rw == nil {
        return fmt.Errorf("config contains recording rules but `-remoteWrite.url` isn't set")
    }
    if arPresent && m.notifiers == nil {
        return fmt.Errorf("config contains alerting rules but neither `-notifier.url` nor `-notifier.config` nor `-notifier.blackhole` aren't set")
    }
    struct UpdateItem<'a> {
        old: &'a Group,
        new: &'a Group
    }

    var toUpdate []updateItem

    m.groupsMu.Lock()
    for (_, og) in m.groups {
        ng, ok := groupsRegistry[og.ID()]
if !ok {
// old group is not present in new list,
// so must be stopped and deleted
og.Close()
delete(m.groups, og.ID())
og = nil
continue
}
delete(groupsRegistry, ng.ID())
if og.Checksum != ng.Checksum {
toUpdate = append(toUpdate, updateItem{old: og, new: ng})
}
}
for (_, ng) in groupsRegistry {
    m.startGroup(ctx, ng, restore)?;
}
}

    if !toUpdate.is_empty() > 0 {
        for item in toUpdate {
            old.update_with(new)
            item.old.InterruptEval()
        }
    }
    return nil
}