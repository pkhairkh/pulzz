//! UCB1 route selector with provable regret bound.
//!
//! Reference: Auer, Cesa-Bianchi & Fischer 2002, "Finite-time Analysis of the
//! Multiarmed Bandit Problem", Machine Learning 47(2-3):235-256.
//!
//! Regret: `O(sqrt(N log N))` per arm (Auer 2002 Theorem 1).

use std::collections::HashMap;

use crate::chpmt::ControllerRouteFamily;

#[derive(Debug)]
pub struct Ucb1RouteSelector {
    arm_stats: HashMap<ControllerRouteFamily, ArmStats>,
    total_pulls: u64,
}

#[derive(Default, Clone, Copy, Debug)]
struct ArmStats {
    successes: u32,
    failures: u32,
}

impl ArmStats {
    const fn pulls(&self) -> u64 {
        (self.successes as u64).saturating_add(self.failures as u64)
    }
}

impl Default for Ucb1RouteSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl Ucb1RouteSelector {
    pub fn new() -> Self {
        Self {
            arm_stats: HashMap::new(),
            total_pulls: 0,
        }
    }

    pub fn record_outcome(&mut self, family: ControllerRouteFamily, success: bool) {
        let arm = self.arm_stats.entry(family).or_default();
        if success {
            arm.successes = arm.successes.saturating_add(1);
        } else {
            arm.failures = arm.failures.saturating_add(1);
        }
        self.total_pulls = self.total_pulls.saturating_add(1);
    }

    pub fn record_outcomes(
        &mut self,
        family: ControllerRouteFamily,
        successes: u32,
        failures: u32,
    ) {
        let arm = self.arm_stats.entry(family).or_default();
        arm.successes = arm.successes.saturating_add(successes);
        arm.failures = arm.failures.saturating_add(failures);
        let added = (successes as u64).saturating_add(failures as u64);
        self.total_pulls = self.total_pulls.saturating_add(added);
    }

    pub fn pulls_for(&self, family: ControllerRouteFamily) -> u64 {
        self.arm_stats.get(&family).map_or(0, ArmStats::pulls)
    }

    pub fn score(&self, family: ControllerRouteFamily) -> Option<f64> {
        let arm = self.arm_stats.get(&family)?;
        let n = arm.pulls();
        if n == 0 {
            return Some(f64::INFINITY);
        }
        let mean = arm.successes as f64 / n as f64;
        if self.total_pulls <= 1 {
            return Some(mean);
        }
        let bonus = (2.0 * (self.total_pulls as f64).ln() / n as f64).sqrt();
        Some(mean + bonus)
    }

    pub fn pick<'a>(
        &self,
        candidates: &'a [ControllerRouteFamily],
    ) -> Option<&'a ControllerRouteFamily> {
        candidates.iter().max_by(|a, b| {
            let sa = self.score(**a).unwrap_or(f64::INFINITY);
            let sb = self.score(**b).unwrap_or(f64::INFINITY);
            sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_selector_picks_none() {
        let selector = Ucb1RouteSelector::new();
        assert!(selector.pick(&[]).is_none());
    }

    #[test]
    fn untracked_arm_score_is_none() {
        let selector = Ucb1RouteSelector::new();
        assert_eq!(selector.score(ControllerRouteFamily::DirectState), None);
    }

    #[test]
    fn pick_prefers_higher_mean_when_both_arms_well_pulled() {
        let mut selector = Ucb1RouteSelector::new();
        selector.record_outcomes(ControllerRouteFamily::ExactAtom, 7, 3);
        selector.record_outcomes(ControllerRouteFamily::DirectState, 2, 8);
        let winner = selector
            .pick(&[
                ControllerRouteFamily::DirectState,
                ControllerRouteFamily::ExactAtom,
            ])
            .copied()
            .expect("non-empty candidate list");
        assert_eq!(winner, ControllerRouteFamily::ExactAtom);
    }
}
