use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::record::now_millis;

/// How long a member may go without a heartbeat before the group gives its
/// partitions to someone else.
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub member_id: String,
    pub generation: u64,
    pub partitions: Vec<TopicPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSummary {
    pub group: String,
    pub generation: u64,
    pub members: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberDescription {
    pub member_id: String,
    pub topics: Vec<String>,
    pub partitions: Vec<TopicPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupDescription {
    pub group: String,
    pub generation: u64,
    pub members: Vec<MemberDescription>,
}

struct Member {
    topics: Vec<String>,
    last_seen: Instant,
}

struct Group {
    generation: u64,
    members: HashMap<String, Member>,
    assignments: HashMap<String, Vec<TopicPartition>>,
}

/// Tracks who is in each consumer group and which partitions they own.
///
/// Membership changes bump a generation counter. A consumer learns it has been
/// rebalanced by seeing a generation it does not recognise come back from a
/// heartbeat, at which point it adopts the new assignment.
pub struct Coordinator {
    groups: Mutex<HashMap<String, Group>>,
    session_timeout: Duration,
    member_counter: AtomicU64,
}

impl Coordinator {
    pub fn new(session_timeout: Duration) -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            session_timeout,
            member_counter: AtomicU64::new(0),
        }
    }

    /// Add a member to a group and hand back its share of the partitions.
    pub fn join(
        &self,
        group_id: &str,
        topics: Vec<String>,
        partition_counts: &HashMap<String, u32>,
    ) -> Result<Assignment> {
        if topics.is_empty() {
            return Err(Error::Protocol(
                "a consumer must subscribe to at least one topic".to_string(),
            ));
        }

        let member_id = format!(
            "{}-{}",
            now_millis(),
            self.member_counter.fetch_add(1, Ordering::Relaxed)
        );

        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let group = groups.entry(group_id.to_string()).or_insert_with(|| Group {
            generation: 0,
            members: HashMap::new(),
            assignments: HashMap::new(),
        });

        Self::evict_expired(group, self.session_timeout);

        let mut sorted = topics;
        sorted.sort();
        sorted.dedup();
        group.members.insert(
            member_id.clone(),
            Member {
                topics: sorted,
                last_seen: Instant::now(),
            },
        );

        group.generation += 1;
        Self::rebalance(group, partition_counts);

        Ok(Assignment {
            partitions: group.assignments.get(&member_id).cloned().unwrap_or_default(),
            member_id,
            generation: group.generation,
        })
    }

    /// Refresh a member's lease and return its current assignment.
    ///
    /// An unknown member means it was evicted while it was away, so the caller
    /// has to rejoin rather than keep using a stale assignment.
    pub fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        partition_counts: &HashMap<String, u32>,
    ) -> Result<Assignment> {
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let group = groups
            .get_mut(group_id)
            .ok_or_else(|| Error::UnknownGroup(group_id.to_string()))?;

        let expired = Self::evict_expired(group, self.session_timeout);
        if expired > 0 {
            group.generation += 1;
            Self::rebalance(group, partition_counts);
        }

        let member = group
            .members
            .get_mut(member_id)
            .ok_or_else(|| Error::UnknownMember {
                group: group_id.to_string(),
                member: member_id.to_string(),
            })?;
        member.last_seen = Instant::now();

        Ok(Assignment {
            member_id: member_id.to_string(),
            generation: group.generation,
            partitions: group.assignments.get(member_id).cloned().unwrap_or_default(),
        })
    }

    /// Leave cleanly, so the group rebalances immediately instead of waiting
    /// out the session timeout.
    pub fn leave(
        &self,
        group_id: &str,
        member_id: &str,
        partition_counts: &HashMap<String, u32>,
    ) -> Result<()> {
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let group = groups
            .get_mut(group_id)
            .ok_or_else(|| Error::UnknownGroup(group_id.to_string()))?;

        if group.members.remove(member_id).is_none() {
            return Err(Error::UnknownMember {
                group: group_id.to_string(),
                member: member_id.to_string(),
            });
        }
        group.assignments.remove(member_id);
        group.generation += 1;

        if group.members.is_empty() {
            groups.remove(group_id);
        } else {
            Self::rebalance(group, partition_counts);
        }
        Ok(())
    }

    pub fn list(&self) -> Vec<GroupSummary> {
        let groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let mut summaries: Vec<GroupSummary> = groups
            .iter()
            .map(|(group, state)| GroupSummary {
                group: group.clone(),
                generation: state.generation,
                members: state.members.len(),
            })
            .collect();
        summaries.sort_by(|a, b| a.group.cmp(&b.group));
        summaries
    }

    pub fn describe(&self, group_id: &str) -> Result<GroupDescription> {
        let groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let group = groups
            .get(group_id)
            .ok_or_else(|| Error::UnknownGroup(group_id.to_string()))?;

        let mut members: Vec<MemberDescription> = group
            .members
            .iter()
            .map(|(id, member)| MemberDescription {
                member_id: id.clone(),
                topics: member.topics.clone(),
                partitions: group.assignments.get(id).cloned().unwrap_or_default(),
            })
            .collect();
        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        Ok(GroupDescription {
            group: group_id.to_string(),
            generation: group.generation,
            members,
        })
    }

    fn evict_expired(group: &mut Group, timeout: Duration) -> usize {
        let now = Instant::now();
        let dead: Vec<String> = group
            .members
            .iter()
            .filter(|(_, member)| now.duration_since(member.last_seen) > timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for id in &dead {
            tracing::info!("evicting member {} after session timeout", id);
            group.members.remove(id);
            group.assignments.remove(id);
        }
        dead.len()
    }

    fn rebalance(group: &mut Group, partition_counts: &HashMap<String, u32>) {
        Self::rebalance_with_known_counts(group, partition_counts);
    }

    /// Round robin assignment, dealt per topic so a member only ever receives
    /// partitions of topics it actually subscribed to.
    ///
    /// Members are sorted by id and partitions by number, so every broker thread
    /// computing this for the same membership lands on the same answer.
    fn rebalance_with_known_counts(group: &mut Group, partition_counts: &HashMap<String, u32>) {
        let mut assignments: HashMap<String, Vec<TopicPartition>> = group
            .members
            .keys()
            .map(|id| (id.clone(), Vec::new()))
            .collect();

        let subscribed: BTreeSet<String> = group
            .members
            .values()
            .flat_map(|m| m.topics.iter().cloned())
            .collect();

        for topic in subscribed {
            let Some(&count) = partition_counts.get(&topic) else {
                continue;
            };

            let mut eligible: Vec<&String> = group
                .members
                .iter()
                .filter(|(_, m)| m.topics.iter().any(|t| t == &topic))
                .map(|(id, _)| id)
                .collect();
            eligible.sort();
            if eligible.is_empty() {
                continue;
            }

            for partition in 0..count {
                let owner = eligible[partition as usize % eligible.len()];
                assignments
                    .get_mut(owner)
                    .expect("eligible members are always in the assignment map")
                    .push(TopicPartition {
                        topic: topic.clone(),
                        partition,
                    });
            }
        }

        group.assignments = assignments;
    }
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new(DEFAULT_SESSION_TIMEOUT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(pairs: &[(&str, u32)]) -> HashMap<String, u32> {
        pairs.iter().map(|(t, n)| (t.to_string(), *n)).collect()
    }

    #[test]
    fn a_lone_member_takes_every_partition() {
        let coordinator = Coordinator::default();
        let counts = counts(&[("orders", 4)]);

        let assignment = coordinator
            .join("g", vec!["orders".to_string()], &counts)
            .unwrap();

        assert_eq!(assignment.generation, 1);
        assert_eq!(assignment.partitions.len(), 4);
    }

    #[test]
    fn partitions_split_evenly_and_do_not_overlap() {
        let coordinator = Coordinator::default();
        let counts = counts(&[("orders", 6)]);

        let a = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        let b = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        let c = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();

        // The first two joined before the others, so re-read their current view.
        let a = coordinator.heartbeat("g", &a.member_id, &counts).unwrap();
        let b = coordinator.heartbeat("g", &b.member_id, &counts).unwrap();
        let c = coordinator.heartbeat("g", &c.member_id, &counts).unwrap();

        assert_eq!(a.partitions.len(), 2);
        assert_eq!(b.partitions.len(), 2);
        assert_eq!(c.partitions.len(), 2);

        let mut all: Vec<u32> = a
            .partitions
            .iter()
            .chain(&b.partitions)
            .chain(&c.partitions)
            .map(|tp| tp.partition)
            .collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn more_members_than_partitions_leaves_some_idle() {
        let coordinator = Coordinator::default();
        let counts = counts(&[("orders", 2)]);

        let members: Vec<Assignment> = (0..4)
            .map(|_| coordinator.join("g", vec!["orders".to_string()], &counts).unwrap())
            .collect();

        let held: Vec<usize> = members
            .iter()
            .map(|m| {
                coordinator
                    .heartbeat("g", &m.member_id, &counts)
                    .unwrap()
                    .partitions
                    .len()
            })
            .collect();

        assert_eq!(held.iter().sum::<usize>(), 2);
        assert_eq!(held.iter().filter(|n| **n == 0).count(), 2);
    }

    #[test]
    fn leaving_hands_the_partitions_back() {
        let coordinator = Coordinator::default();
        let counts = counts(&[("orders", 4)]);

        let a = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        let b = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        assert_eq!(
            coordinator.heartbeat("g", &a.member_id, &counts).unwrap().partitions.len(),
            2
        );

        coordinator.leave("g", &b.member_id, &counts).unwrap();

        let after = coordinator.heartbeat("g", &a.member_id, &counts).unwrap();
        assert_eq!(after.partitions.len(), 4);
        assert!(after.generation > a.generation);
    }

    #[test]
    fn the_last_member_leaving_removes_the_group() {
        let coordinator = Coordinator::default();
        let counts = counts(&[("orders", 2)]);

        let a = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        coordinator.leave("g", &a.member_id, &counts).unwrap();

        assert!(coordinator.list().is_empty());
        assert!(matches!(
            coordinator.describe("g"),
            Err(Error::UnknownGroup(_))
        ));
    }

    #[test]
    fn a_silent_member_is_evicted_and_its_work_reassigned() {
        let coordinator = Coordinator::new(Duration::from_millis(0));
        let counts = counts(&[("orders", 4)]);

        let stale = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        std::thread::sleep(Duration::from_millis(5));

        // Joining sweeps expired members first, so the newcomer inherits everything.
        let fresh = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        assert_eq!(fresh.partitions.len(), 4);

        assert!(matches!(
            coordinator.heartbeat("g", &stale.member_id, &counts),
            Err(Error::UnknownMember { .. })
        ));
    }

    #[test]
    fn members_only_get_topics_they_subscribed_to() {
        let coordinator = Coordinator::default();
        let counts = counts(&[("orders", 2), ("payments", 2)]);

        let orders = coordinator.join("g", vec!["orders".to_string()], &counts).unwrap();
        let payments = coordinator
            .join("g", vec!["payments".to_string()], &counts)
            .unwrap();

        let orders = coordinator.heartbeat("g", &orders.member_id, &counts).unwrap();
        let payments = coordinator.heartbeat("g", &payments.member_id, &counts).unwrap();

        assert!(orders.partitions.iter().all(|tp| tp.topic == "orders"));
        assert!(payments.partitions.iter().all(|tp| tp.topic == "payments"));
        assert_eq!(orders.partitions.len(), 2);
        assert_eq!(payments.partitions.len(), 2);
    }

    #[test]
    fn subscribing_to_nothing_is_rejected() {
        let coordinator = Coordinator::default();
        assert!(matches!(
            coordinator.join("g", vec![], &HashMap::new()),
            Err(Error::Protocol(_))
        ));
    }
}
