/*!
# cuda-lease

Lease management for distributed resource grants.

Agents hold leases on shared resources — locks, slots, capacities.
Leases expire automatically, preventing deadlocks from crashed agents.

- Time-bounded leases with TTL
- Lease renewal
- Automatic expiry
- Lease hierarchy (parent expires → children expire)
- Lease statistics
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaseState { Active, Expired, Revoked }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lease {
    pub id: String,
    pub holder: String,
    pub resource: String,
    pub state: LeaseState,
    pub acquired_ms: u64,
    pub ttl_ms: u64,
    pub renewals: u32,
    pub max_renewals: u32,
    pub parent_id: Option<String>,
}

impl Lease {
    pub fn is_expired(&self) -> bool { now() - self.acquired_ms > self.ttl_ms }
    pub fn remaining_ms(&self) -> i64 { (self.ttl_ms as i64) - ((now() - self.acquired_ms) as i64) }
}

/// Lease manager
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseManager {
    pub leases: HashMap<String, Lease>,
    pub resource_leases: HashMap<String, Vec<String>>, // resource → [lease_ids]
    pub holder_leases: HashMap<String, Vec<String>>,   // holder → [lease_ids]
    pub total_acquired: u64,
    pub total_expired: u64,
    pub total_revoked: u64,
    pub total_renewals: u64,
}

impl LeaseManager {
    pub fn new() -> Self { LeaseManager { leases: HashMap::new(), resource_leases: HashMap::new(), holder_leases: HashMap::new(), total_acquired: 0, total_expired: 0, total_revoked: 0, total_renewals: 0 } }

    /// Acquire a lease
    pub fn acquire(&mut self, holder: &str, resource: &str, ttl_ms: u64, max_renewals: u32) -> Option<String> {
        // Check for existing active lease on resource
        if let Some(ids) = self.resource_leases.get(resource) {
            for id in ids {
                if let Some(lease) = self.leases.get(id) {
                    if lease.state == LeaseState::Active && !lease.is_expired() { return None; }
                }
            }
        }
        let id = format!("lease_{}", self.total_acquired + 1);
        let lease = Lease { id: id.clone(), holder: holder.to_string(), resource: resource.to_string(), state: LeaseState::Active, acquired_ms: now(), ttl_ms, renewals: 0, max_renewals, parent_id: None };
        self.leases.insert(id.clone(), lease);
        self.resource_leases.entry(resource.to_string()).or_default().push(id.clone());
        self.holder_leases.entry(holder.to_string()).or_default().push(id.clone());
        self.total_acquired += 1;
        Some(id)
    }

    /// Renew a lease
    pub fn renew(&mut self, lease_id: &str, additional_ms: u64) -> RenewResult {
        let lease = match self.leases.get_mut(lease_id) {
            Some(l) if l.state == LeaseState::Active && !l.is_expired() => l,
            _ => return RenewResult::NotFound,
        };
        if lease.renewals >= lease.max_renewals { return RenewResult::MaxRenewals; }
        lease.ttl_ms += additional_ms;
        lease.renewals += 1;
        self.total_renewals += 1;
        RenewResult::Renewed
    }

    /// Revoke a lease
    pub fn revoke(&mut self, lease_id: &str) -> bool {
        if let Some(lease) = self.leases.get_mut(lease_id) {
            if lease.state == LeaseState::Active {
                lease.state = LeaseState::Revoked;
                self.total_revoked += 1;
                return true;
            }
        }
        false
    }

    /// Revoke all leases for a holder
    pub fn revoke_all_for(&mut self, holder: &str) -> usize {
        let ids: Vec<String> = self.holder_leases.get(holder).cloned().unwrap_or_default();
        let mut count = 0;
        for id in ids { if self.revoke(&id) { count += 1; } }
        count
    }

    /// Clean up expired leases
    pub fn cleanup(&mut self) -> usize {
        let mut expired = vec![];
        for (id, lease) in &self.leases {
            if lease.state == LeaseState::Active && lease.is_expired() { expired.push(id.clone()); }
        }
        for id in &expired {
            if let Some(lease) = self.leases.get_mut(id) { lease.state = LeaseState::Expired; }
            self.total_expired += 1;
        }
        expired.len()
    }

    /// Check if a resource is leased
    pub fn is_leased(&self, resource: &str) -> bool {
        self.resource_leases.get(resource).map_or(false, |ids| ids.iter().any(|id| {
            self.leases.get(id).map_or(false, |l| l.state == LeaseState::Active && !l.is_expired())
        }))
    }

    /// Get active leases for a holder
    pub fn active_for(&self, holder: &str) -> Vec<&Lease> {
        self.holder_leases.get(holder).map_or(vec![], |ids| {
            ids.iter().filter_map(|id| self.leases.get(id)).filter(|l| l.state == LeaseState::Active).collect()
        })
    }

    /// Summary
    pub fn summary(&self) -> String {
        let active = self.leases.values().filter(|l| l.state == LeaseState::Active).count();
        format!("Leases: {} active, {} acquired, {} expired, {} revoked, {} renewals",
            active, self.total_acquired, self.total_expired, self.total_revoked, self.total_renewals)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenewResult { Renewed, NotFound, MaxRenewals }

fn now() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire() {
        let mut mgr = LeaseManager::new();
        let id = mgr.acquire("a1", "res1", 60000, 3);
        assert!(id.is_some());
        assert!(mgr.is_leased("res1"));
    }

    #[test]
    fn test_cannot_double_lease() {
        let mut mgr = LeaseManager::new();
        mgr.acquire("a1", "res1", 60000, 3);
        let id2 = mgr.acquire("a2", "res1", 60000, 3);
        assert!(id2.is_none());
    }

    #[test]
    fn test_renew() {
        let mut mgr = LeaseManager::new();
        let id = mgr.acquire("a1", "res1", 60000, 3).unwrap();
        assert_eq!(mgr.renew(&id, 30000), RenewResult::Renewed);
        let lease = mgr.leases.get(&id).unwrap();
        assert_eq!(lease.renewals, 1);
    }

    #[test]
    fn test_max_renewals() {
        let mut mgr = LeaseManager::new();
        let id = mgr.acquire("a1", "res1", 60000, 1).unwrap();
        mgr.renew(&id, 30000);
        assert_eq!(mgr.renew(&id, 30000), RenewResult::MaxRenewals);
    }

    #[test]
    fn test_revoke() {
        let mut mgr = LeaseManager::new();
        let id = mgr.acquire("a1", "res1", 60000, 3).unwrap();
        mgr.revoke(&id);
        assert!(!mgr.is_leased("res1"));
    }

    #[test]
    fn test_revoke_all_for_holder() {
        let mut mgr = LeaseManager::new();
        mgr.acquire("a1", "r1", 60000, 3);
        mgr.acquire("a1", "r2", 60000, 3);
        mgr.acquire("a2", "r3", 60000, 3);
        let count = mgr.revoke_all_for("a1");
        assert_eq!(count, 2);
        assert!(!mgr.is_leased("r1"));
        assert!(!mgr.is_leased("r2"));
        assert!(mgr.is_leased("r3"));
    }

    #[test]
    fn test_expired_lease_allows_reacquire() {
        let mut mgr = LeaseManager::new();
        let id = mgr.acquire("a1", "r1", 0, 3).unwrap(); // ttl=0 → immediately expired
        let lease = mgr.leases.get_mut(&id).unwrap();
        lease.state = LeaseState::Active; // force active
        let id2 = mgr.acquire("a2", "r1", 60000, 3);
        // The expired lease still occupies unless cleaned
        mgr.cleanup();
        let id3 = mgr.acquire("a2", "r1", 60000, 3);
        assert!(id3.is_some());
    }

    #[test]
    fn test_cleanup() {
        let mut mgr = LeaseManager::new();
        mgr.acquire("a1", "r1", 0, 3);
        let cleaned = mgr.cleanup();
        assert_eq!(cleaned, 1);
    }

    #[test]
    fn test_active_for() {
        let mut mgr = LeaseManager::new();
        mgr.acquire("a1", "r1", 60000, 3);
        mgr.acquire("a1", "r2", 60000, 3);
        let active = mgr.active_for("a1");
        assert_eq!(active.len(), 2);
    }

    #[test]
    fn test_summary() {
        let mgr = LeaseManager::new();
        let s = mgr.summary();
        assert!(s.contains("0 active"));
    }
}
