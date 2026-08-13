//! Prediction Suffix Tree (PST) — integer-only variable-order Markov model.
//!
//! Wave 4 Task 4-a: implements a PST per Begleiter, El-Yaniv & Yona 2004,
//! "On Prediction Using Variable Order Markov Models" (arxiv 1107.0051).

use std::collections::HashMap;

pub const DEFAULT_MAX_DEPTH: usize = 8;
pub const DEFAULT_THETA: f64 = 0.05;
pub const PROB_SCALE: u64 = 1_000_000;

pub type Token = u8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Prediction {
    pub token: Token,
    pub confidence: u32,
}

#[derive(Debug, Clone)]
pub struct PredictionSuffixTree {
    nodes: HashMap<Vec<Token>, HashMap<Token, u64>>,
    max_depth: usize,
    theta: f64,
    marginal: HashMap<Token, u64>,
    total_tokens: u64,
}

impl Default for PredictionSuffixTree {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_DEPTH, DEFAULT_THETA)
    }
}

impl PredictionSuffixTree {
    pub fn new(max_depth: usize, theta: f64) -> Self {
        Self {
            nodes: HashMap::new(),
            max_depth,
            theta,
            marginal: HashMap::new(),
            total_tokens: 0,
        }
    }

    pub fn train(&mut self, sequence: &[Token]) {
        for &tok in sequence {
            *self.marginal.entry(tok).or_insert(0) += 1;
        }
        self.total_tokens += sequence.len() as u64;

        for i in 1..sequence.len() {
            let next_token = sequence[i];
            for depth in 1..=self.max_depth.min(i) {
                let start = i - depth;
                let context: Vec<Token> = sequence[start..i].to_vec();
                let node = self.nodes.entry(context).or_default();
                *node.entry(next_token).or_insert(0) += 1;
            }
        }
    }

    pub fn prune(&mut self) {
        let total = self.total_tokens as f64;
        if total == 0.0 {
            return;
        }
        let marginal_p: HashMap<Token, f64> = self
            .marginal
            .iter()
            .map(|(&tok, &count)| (tok, count as f64 / total))
            .collect();

        let theta = self.theta;
        let nodes = std::mem::take(&mut self.nodes);
        self.nodes = nodes
            .into_iter()
            .filter_map(|(context, mut counts)| {
                let total_count: u64 = counts.values().sum();
                if total_count == 0 {
                    return None;
                }
                let retains = counts.iter().any(|(&tok, &count)| {
                    let cond_p = count as f64 / total_count as f64;
                    let marg = marginal_p.get(&tok).copied().unwrap_or(0.0);
                    cond_p - marg > theta
                });
                if retains {
                    counts.retain(|_, &mut count| count > 0);
                    Some((context, counts))
                } else {
                    None
                }
            })
            .collect();
    }

    pub fn predict(&self, context: &[Token]) -> Option<Prediction> {
        if self.total_tokens == 0 || context.is_empty() {
            return None;
        }
        for depth in (1..=self.max_depth.min(context.len())).rev() {
            let suffix: Vec<Token> = context[context.len() - depth..].to_vec();
            if let Some(node) = self.nodes.get(&suffix) {
                let total_count: u64 = node.values().sum();
                if total_count == 0 {
                    continue;
                }
                let (best_tok, best_count) = node.iter().max_by_key(|(_, c)| **c)?;
                let cond_p = *best_count as f64 / total_count as f64;
                let confidence = (cond_p * PROB_SCALE as f64) as u32;
                return Some(Prediction {
                    token: *best_tok,
                    confidence,
                });
            }
        }
        let (best_tok, best_count) = self.marginal.iter().max_by_key(|(_, c)| **c)?;
        let marg_p = *best_count as f64 / self.total_tokens as f64;
        let confidence = (marg_p * PROB_SCALE as f64) as u32;
        Some(Prediction {
            token: *best_tok,
            confidence,
        })
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn total_tokens(&self) -> u64 {
        self.total_tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_pst_predicts_none() {
        let pst = PredictionSuffixTree::default();
        assert!(pst.predict(&[1, 2, 3]).is_none());
    }

    #[test]
    fn trains_on_simple_sequence() {
        let mut pst = PredictionSuffixTree::new(3, 0.01);
        let seq: Vec<u8> = (0..20).map(|i| (i % 2) as u8).collect();
        pst.train(&seq);
        pst.prune();
        let pred_after_0 = pst.predict(&[0]).expect("must predict");
        assert_eq!(pred_after_0.token, 1);
        let pred_after_1 = pst.predict(&[1]).expect("must predict");
        assert_eq!(pred_after_1.token, 0);
    }

    #[test]
    fn longer_context_outperforms_shorter() {
        let mut pst = PredictionSuffixTree::new(4, 0.01);
        let seq: Vec<u8> = (0..30).map(|i| match i % 3 {
            0 => 0,
            1 => 0,
            2 => 1,
            _ => unreachable!(),
        }).collect();
        pst.train(&seq);
        pst.prune();
        let pred = pst.predict(&[0, 0]).expect("must predict");
        assert_eq!(pred.token, 1);
        assert!(pred.confidence > 800_000);
    }
}
