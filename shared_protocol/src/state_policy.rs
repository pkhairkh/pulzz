use serde::{Deserialize, Serialize};

use crate::{
    AssemblyAdmissibilityFailure, AssemblyAdmissibilityInput, AssemblyAdmissibilityPolicy,
    AssemblyExtractionCandidate, AssemblyExtractionConfig, BlockId, BundleId,
    CatalogPrefixCandidate, ContextHash, ContextTreeGovernor, ContextTreeSymbol,
    ControllerRouteFamily, DelimiterClass, DependencyClosureGate, EpisodeCandidatePolicy,
    EpisodeCompletionCandidate, EpisodeMemory, ExactStateMaterial, HybridRoute,
    HybridRouteComponent, LagBucket, ObjectDependency, ObjectKind, ObjectLifecycleMeta,
    PromotionLevel, RefKind, ResidualSizeBucket, RouteGovernorSignal, RouteScore, RouteScoreInputs,
    RouteStatistics, SchemaCandidatePolicy, SchemaGraph, SchemaKind, SchemaRouteCandidate,
    SharedBlockCatalog, SlotShapeBucket, SourceKind, SparseCue, StateOp, StateProgram, StateRef,
    SubstrateCapabilityMask, SubstrateObject, SubstrateRouteGraph, SyncStateBucket,
    TransformBasisMask, TransformBasisRef, TransformCandidate, TransformClass,
    TransformDependencyClosure, TransformId, TransformInstance, TransformKind,
    TransformOutputContract, TransformParameterField, TransformParameterSchema,
    TransformRoutePrior, TransformSupportMetrics, TransformTransducerFamily,
    assembly_route_eligibility, bitset_overlap_count, delimiter_scan,
    extract_catalog_assembly_candidates, generate_episode_completion_candidates,
    generate_schema_route_candidates, gf2_fingerprint, keyed_permute_indices, popcount_bytes,
    score_route, summarize_route_feedback,
};

// S2.1: Route family dependency inventory — what each route family actually
// requires the receiver to have installed before it can decode/apply.
//
// | Route Family   | Required Peer Objects                    | Dependency Closure Source         |
// |----------------|------------------------------------------|-----------------------------------|
// | ExactAtom      | SubstrateRef (blocks/bundles/ranges)     | Derived from SubstrateRef in PRG  |
// | Hybrid         | SubstrateRef, AssemblyRef, Dictionary,   | Union of component-level deps     |
// |                | Schema (via PRG sub-graph)               |                                   |
// | Assembly       | AssemblyDef, Schema, Dictionary refs     | From assembly body extraction     |
// | Transform      | TransformClass (def), basis refs         | From TransformInstance deps       |
// | SchemaExp.     | SchemaDef, slot-fill substrate           | From SchemaGraph dependency_closure|
// | EpisodeComp.   | Episode hint objects (assembly/schema)   | From episode candidate refs       |
// | DirectState    | None (self-contained)                    | Empty                             |
//
// Gap analysis (S2.1.c):
// - ExactAtom: previously had empty dependency vectors → NOW derived from SubstrateRef (S2.2)
// - Hybrid: previously had empty dependency_closure → NOW derived from components (S2.3)
// - Transform: DEMOTED from active architecture. Candidate generation is retained for future reactivation but the planner does not select transform routes. See extend.md §6 demotion.
// - Schema/Episode: dependency_closure comes from extraction/generation, appears complete
//
// Satisfaction sources (S2.1.d):
// - Confirmed peer state: assembly_sync_versions, peer_schema_ids, peer_dictionary_versions,
//   peer_transform_ids, peer_catalog (blocks/bundles)
// - Inline staged definitions: inline_assembly_defs, inline_schema_defs, inline_dictionaries
//   (validated and committed only after dispatch success per S1.3)
// - Local-only materialization: server-side entries, local_catalog — never appear in deps

pub const TINY_STATE_PAYLOAD_BYTES: usize = 16;
pub const MAX_COMPOSITE_STATE_REFS: usize = 8;
pub const MAX_COMPOSITE_CANDIDATES_PER_STEP: usize = 8;
pub const MIN_COMPOSITE_COMPONENT_BYTES: usize = 8;
pub const MIN_RANGE_REUSE_BYTES: usize = 12;
pub const MAX_SUFFIX_RESIDUAL_BYTES: usize = 48;
pub const MAX_PATCH_RESIDUAL_BYTES: usize = 96;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkipCompressionReason {
    ControlOrCoordinationRecord,
    CatalogSync,
    Repair,
    TinyPayload,
    StateDescriptionNotCheaper,
    HighNovelty,
    UncertainBasis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportRepresentationDecision {
    State {
        program: StateProgram,
        description_bytes: usize,
        basis: StateBasis,
    },
    ExactCopy {
        skip_compression: SkipCompressionReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateBasis {
    Block(BlockId),
    Bundle(BundleId),
    Range {
        block_id: BlockId,
        start: u32,
        len: u32,
    },
    Composite {
        refs: u16,
        residual_bytes: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactAtomPlan {
    pub basis: StateBasis,
    pub output_len: u32,
    pub capability_mask: SubstrateCapabilityMask,
    pub substrate_graph: SubstrateRouteGraph,
}

#[derive(Debug, Clone)]
struct StateCandidate {
    program: StateProgram,
    description_bytes: usize,
    basis: StateBasis,
}

#[derive(Debug, Clone)]
struct CompositeRefs {
    refs: Vec<StateRef>,
    covered_len: usize,
}

impl CompositeRefs {
    fn ref_count(&self) -> u16 {
        self.refs.len().min(u16::MAX as usize) as u16
    }
}

#[derive(Debug, Clone)]
struct WindowMatchCandidate {
    state_ref: StateRef,
    payload_start: usize,
    matched_len: usize,
    total_payload_len: usize,
}

#[derive(Debug, Clone, Copy)]
struct WindowMatch {
    payload_start: usize,
    base_start: u32,
    matched_len: usize,
}

impl TransportRepresentationDecision {
    pub fn skips_compression(&self) -> Option<SkipCompressionReason> {
        match self {
            Self::State { .. } => None,
            Self::ExactCopy { skip_compression } => Some(*skip_compression),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredictiveTransportDecision {
    State {
        program: StateProgram,
        description_bytes: usize,
        basis: StateBasis,
    },
    Transform {
        class: TransformClass,
        instance: TransformInstance,
        description_bytes: usize,
    },
    ExactCopy {
        skip_compression: SkipCompressionReason,
    },
}

pub fn choose_predictive_transport_representation(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> PredictiveTransportDecision {
    let state_choice = choose_transport_representation(current, peer_catalog);
    let transform_choice = generate_transform_candidates(current, peer_catalog)
        .into_iter()
        .min_by_key(|candidate| candidate.estimated_wire_bytes);

    match (state_choice, transform_choice) {
        (
            TransportRepresentationDecision::State {
                description_bytes, ..
            },
            Some(candidate),
        ) if (candidate.estimated_wire_bytes as usize) < description_bytes => {
            PredictiveTransportDecision::Transform {
                class: candidate.class,
                instance: candidate.instance,
                description_bytes: candidate.estimated_wire_bytes as usize,
            }
        }
        (
            TransportRepresentationDecision::State {
                program,
                description_bytes,
                basis,
            },
            _,
        ) => PredictiveTransportDecision::State {
            program,
            description_bytes,
            basis,
        },
        (TransportRepresentationDecision::ExactCopy { .. }, Some(candidate)) => {
            PredictiveTransportDecision::Transform {
                class: candidate.class,
                instance: candidate.instance,
                description_bytes: candidate.estimated_wire_bytes as usize,
            }
        }
        (TransportRepresentationDecision::ExactCopy { skip_compression }, None) => {
            PredictiveTransportDecision::ExactCopy { skip_compression }
        }
    }
}

/// DEMOTED: Transform candidate generation. These candidates are generated but the
/// planner does not select transform routes for emission. Retained for potential future
/// reactivation with confirmed transform-class synchronization.
pub fn generate_transform_candidates(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> Vec<TransformCandidate> {
    let source_kind = current.source_kind;
    let cue = crate::derive_sparse_cue_from_bytes(source_kind, &current.exact_bytes);
    let mut out = Vec::new();
    let exact_bases = exact_transform_bases(peer_catalog);

    if let Some(match_candidate) = best_window_match(&current.exact_bytes, peer_catalog) {
        if match_candidate.matched_len >= MIN_RANGE_REUSE_BYTES {
            let basis_ref = TransformBasisRef {
                object_kind: match match_candidate.state_ref.kind {
                    RefKind::Bundle => ObjectKind::ExactBundle,
                    _ => ObjectKind::ExactBlock,
                },
                // S2.6: Canonical object-ID grammar uses "block:<id>" and "bundle:<id>" prefixes.
                // All emitters and receivers must use this same form. See ISSUES.md I8.
                object_id: match match_candidate.state_ref.kind {
                    RefKind::Bundle => {
                        format!("bundle:{}", match_candidate.state_ref.base_id)
                    }
                    _ => format!("block:{}", match_candidate.state_ref.base_id),
                },
                start: match_candidate.state_ref.start,
                len: match_candidate.state_ref.len,
            };
            if let Some(basis_material) =
                materialize_transform_basis(&match_candidate.state_ref, peer_catalog)
            {
                let salience = match_candidate.matched_len.min(u32::MAX as usize) as u32;
                let basis_refs = vec![basis_ref.clone()];

                if let Some((start, remove_len, replacement)) =
                    bounded_substitution_delta(&basis_material, &current.exact_bytes)
                {
                    let output_contract = TransformOutputContract {
                        min_output_len: current.exact_bytes.len() as u32,
                        max_output_len: current.exact_bytes.len() as u32,
                        preserves_delimiters: false,
                        deterministic: true,
                    };
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                101,
                                TransformKind::BoundedSubstitution,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![
                                        TransformParameterField {
                                            name: "start".into(),
                                            min_value: 0,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                        TransformParameterField {
                                            name: "remove_len".into(),
                                            min_value: 0,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                    ],
                                    max_basis_count: 1,
                                    residual_allowed: true,
                                },
                                output_contract,
                                false,
                                replacement.len() as u32,
                                2,
                                6,
                                salience,
                            ),
                            instance: make_transform_instance(
                                101,
                                basis_refs.clone(),
                                vec![start as i32, remove_len as i32],
                                vec![start as u32],
                                vec![replacement.clone()],
                                output_contract,
                                false,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: replacement.len() as u32 + 12,
                        },
                    );

                    if let Some((gap_start, gap_len, gap_fill)) =
                        copy_with_gap_plan(&basis_material, &current.exact_bytes)
                    {
                        replace_or_push_transform_candidate(
                            &mut out,
                            TransformCandidate {
                                class: make_transform_class(
                                    source_kind,
                                    cue,
                                    113,
                                    TransformKind::CopyWithGap,
                                    &basis_refs,
                                    TransformParameterSchema {
                                        parameters: vec![
                                            TransformParameterField {
                                                name: "start".into(),
                                                min_value: 0,
                                                max_value: i32::MAX,
                                                required: true,
                                            },
                                            TransformParameterField {
                                                name: "gap_len".into(),
                                                min_value: 0,
                                                max_value: i32::MAX,
                                                required: true,
                                            },
                                        ],
                                        max_basis_count: 1,
                                        residual_allowed: true,
                                    },
                                    output_contract,
                                    false,
                                    gap_fill.len() as u32,
                                    2,
                                    8,
                                    salience,
                                ),
                                instance: make_transform_instance(
                                    113,
                                    basis_refs.clone(),
                                    vec![gap_start as i32, gap_len as i32],
                                    vec![gap_start as u32],
                                    vec![gap_fill],
                                    output_contract,
                                    false,
                                    current.exact_bytes.len() as u32,
                                ),
                                estimated_wire_bytes: replacement.len() as u32 + 10,
                            },
                        );
                    }
                }

                if delimiter_inventory(&basis_material) == delimiter_inventory(&current.exact_bytes)
                    && basis_material != current.exact_bytes
                {
                    let output_contract = TransformOutputContract {
                        min_output_len: current.exact_bytes.len() as u32,
                        max_output_len: current.exact_bytes.len() as u32,
                        preserves_delimiters: true,
                        deterministic: true,
                    };
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                102,
                                TransformKind::DelimiterPreservingRewrite,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![],
                                    max_basis_count: 1,
                                    residual_allowed: true,
                                },
                                output_contract,
                                false,
                                current.exact_bytes.len() as u32,
                                3,
                                10,
                                salience,
                            ),
                            instance: make_transform_instance(
                                102,
                                basis_refs.clone(),
                                vec![],
                                vec![0],
                                vec![current.exact_bytes.clone()],
                                output_contract,
                                false,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: current.exact_bytes.len() as u32,
                        },
                    );
                }

                if let Some((delimiter, base_segments, current_segments)) =
                    reordered_delimited_segments(&basis_material, &current.exact_bytes)
                {
                    let output_contract = TransformOutputContract {
                        min_output_len: current.exact_bytes.len() as u32,
                        max_output_len: current.exact_bytes.len() as u32,
                        preserves_delimiters: true,
                        deterministic: true,
                    };
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                103,
                                TransformKind::BoundedReorder,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![
                                        TransformParameterField {
                                            name: "start".into(),
                                            min_value: 0,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                        TransformParameterField {
                                            name: "len".into(),
                                            min_value: 0,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                        TransformParameterField {
                                            name: "delimiter".into(),
                                            min_value: 0,
                                            max_value: 255,
                                            required: true,
                                        },
                                    ],
                                    max_basis_count: 1,
                                    residual_allowed: true,
                                },
                                output_contract,
                                true,
                                current.exact_bytes.len() as u32,
                                4,
                                14,
                                salience,
                            ),
                            instance: make_transform_instance(
                                103,
                                basis_refs.clone(),
                                vec![0, basis_material.len() as i32, delimiter as i32],
                                vec![0],
                                vec![current.exact_bytes.clone()],
                                output_contract,
                                true,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: (current.exact_bytes.len() as u32)
                                .saturating_sub(
                                    base_segments.len().min(current_segments.len()) as u32
                                ),
                        },
                    );
                }

                if let Some((motif_start, motif_len, repeats)) =
                    motif_duplicate_plan(&basis_material, &current.exact_bytes)
                {
                    let output_contract = TransformOutputContract {
                        min_output_len: current.exact_bytes.len() as u32,
                        max_output_len: current.exact_bytes.len() as u32,
                        preserves_delimiters: delimiter_inventory(&basis_material)
                            == delimiter_inventory(&current.exact_bytes),
                        deterministic: true,
                    };
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                104,
                                TransformKind::MotifDuplicateCompress,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![
                                        TransformParameterField {
                                            name: "motif_start".into(),
                                            min_value: 0,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                        TransformParameterField {
                                            name: "motif_len".into(),
                                            min_value: 1,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                        TransformParameterField {
                                            name: "repeat_count".into(),
                                            min_value: 1,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                    ],
                                    max_basis_count: 1,
                                    residual_allowed: false,
                                },
                                output_contract,
                                true,
                                0,
                                3,
                                18,
                                salience,
                            ),
                            instance: make_transform_instance(
                                104,
                                basis_refs.clone(),
                                vec![motif_start as i32, motif_len as i32, repeats as i32],
                                Vec::new(),
                                Vec::new(),
                                output_contract,
                                true,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: 12,
                        },
                    );
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                117,
                                TransformKind::BoundedDuplication,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![
                                        TransformParameterField {
                                            name: "start".into(),
                                            min_value: 0,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                        TransformParameterField {
                                            name: "len".into(),
                                            min_value: 1,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                        TransformParameterField {
                                            name: "repeat_count".into(),
                                            min_value: 1,
                                            max_value: i32::MAX,
                                            required: true,
                                        },
                                    ],
                                    max_basis_count: 1,
                                    residual_allowed: false,
                                },
                                output_contract,
                                true,
                                0,
                                3,
                                16,
                                salience,
                            ),
                            instance: make_transform_instance(
                                117,
                                basis_refs.clone(),
                                vec![motif_start as i32, motif_len as i32, repeats as i32],
                                Vec::new(),
                                Vec::new(),
                                output_contract,
                                true,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: 12,
                        },
                    );
                }

                let mirrored_basis = [
                    basis_material.clone(),
                    basis_material.iter().rev().copied().collect::<Vec<u8>>(),
                ]
                .concat();
                if current.exact_bytes == mirrored_basis {
                    let output_contract = TransformOutputContract {
                        min_output_len: current.exact_bytes.len() as u32,
                        max_output_len: current.exact_bytes.len() as u32,
                        preserves_delimiters: delimiter_inventory(&basis_material)
                            == delimiter_inventory(&current.exact_bytes),
                        deterministic: true,
                    };
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                105,
                                TransformKind::MirrorSymmetry,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![],
                                    max_basis_count: 1,
                                    residual_allowed: false,
                                },
                                output_contract,
                                true,
                                0,
                                2,
                                16,
                                salience,
                            ),
                            instance: make_transform_instance(
                                105,
                                basis_refs.clone(),
                                Vec::new(),
                                Vec::new(),
                                Vec::new(),
                                output_contract,
                                true,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: 8,
                        },
                    );
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                116,
                                TransformKind::LocalMirror,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![],
                                    max_basis_count: 1,
                                    residual_allowed: false,
                                },
                                output_contract,
                                true,
                                0,
                                2,
                                14,
                                salience,
                            ),
                            instance: make_transform_instance(
                                116,
                                basis_refs.clone(),
                                Vec::new(),
                                Vec::new(),
                                Vec::new(),
                                output_contract,
                                true,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: 8,
                        },
                    );
                }

                if let Some((delimiter, repeats)) =
                    delimited_repeat_plan(&basis_material, &current.exact_bytes)
                {
                    let output_contract = TransformOutputContract {
                        min_output_len: current.exact_bytes.len() as u32,
                        max_output_len: current.exact_bytes.len() as u32,
                        preserves_delimiters: true,
                        deterministic: true,
                    };
                    replace_or_push_transform_candidate(
                        &mut out,
                        TransformCandidate {
                            class: make_transform_class(
                                source_kind,
                                cue,
                                106,
                                TransformKind::SchemaConditionedExpansion,
                                &basis_refs,
                                TransformParameterSchema {
                                    parameters: vec![TransformParameterField {
                                        name: "repeat_count".into(),
                                        min_value: 1,
                                        max_value: i32::MAX,
                                        required: true,
                                    }],
                                    max_basis_count: 1,
                                    residual_allowed: true,
                                },
                                output_contract,
                                false,
                                1,
                                3,
                                12,
                                salience,
                            ),
                            instance: make_transform_instance(
                                106,
                                basis_refs.clone(),
                                vec![repeats as i32],
                                vec![basis_material.len() as u32],
                                vec![vec![delimiter]],
                                output_contract,
                                false,
                                current.exact_bytes.len() as u32,
                            ),
                            estimated_wire_bytes: 9,
                        },
                    );
                }
            }
        }
    }

    for (basis_ref, basis_material) in &exact_bases {
        let salience = basis_material.len().min(u32::MAX as usize) as u32;
        let basis_refs = vec![basis_ref.clone()];

        if let Some(prefix) = prefix_insert_plan(basis_material, &current.exact_bytes) {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: delimiter_inventory(basis_material)
                    == delimiter_inventory(&current.exact_bytes),
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        107,
                        TransformKind::PrefixInsert,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![],
                            max_basis_count: 1,
                            residual_allowed: true,
                        },
                        output_contract,
                        false,
                        prefix.len() as u32,
                        1,
                        10,
                        salience,
                    ),
                    instance: make_transform_instance(
                        107,
                        basis_refs.clone(),
                        Vec::new(),
                        vec![0],
                        vec![prefix.clone()],
                        output_contract,
                        false,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: prefix.len() as u32 + 4,
                },
            );
        }

        if let Some(suffix) = suffix_insert_plan(basis_material, &current.exact_bytes) {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: delimiter_inventory(basis_material)
                    == delimiter_inventory(&current.exact_bytes),
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        108,
                        TransformKind::SuffixInsert,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![],
                            max_basis_count: 1,
                            residual_allowed: true,
                        },
                        output_contract,
                        false,
                        suffix.len() as u32,
                        1,
                        10,
                        salience,
                    ),
                    instance: make_transform_instance(
                        108,
                        basis_refs.clone(),
                        Vec::new(),
                        vec![basis_material.len() as u32],
                        vec![suffix.clone()],
                        output_contract,
                        false,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: suffix.len() as u32 + 4,
                },
            );
        }

        if let Some((wrap_prefix, wrap_suffix)) = wrap_plan(basis_material, &current.exact_bytes) {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: delimiter_inventory(basis_material)
                    == delimiter_inventory(&current.exact_bytes),
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        109,
                        TransformKind::Wrap,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![],
                            max_basis_count: 1,
                            residual_allowed: true,
                        },
                        output_contract,
                        false,
                        (wrap_prefix.len() + wrap_suffix.len()) as u32,
                        2,
                        12,
                        salience,
                    ),
                    instance: make_transform_instance(
                        109,
                        basis_refs.clone(),
                        Vec::new(),
                        vec![0, (wrap_prefix.len() + basis_material.len()) as u32],
                        vec![wrap_prefix.clone(), wrap_suffix.clone()],
                        output_contract,
                        false,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: (wrap_prefix.len() + wrap_suffix.len()) as u32 + 8,
                },
            );
        }

        if let Some((offset, inserted)) =
            bounded_interior_insert_plan(basis_material, &current.exact_bytes)
        {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: false,
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        110,
                        TransformKind::BoundedInteriorInsert,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![TransformParameterField {
                                name: "offset".into(),
                                min_value: 0,
                                max_value: i32::MAX,
                                required: true,
                            }],
                            max_basis_count: 1,
                            residual_allowed: true,
                        },
                        output_contract,
                        false,
                        inserted.len() as u32,
                        2,
                        12,
                        salience,
                    ),
                    instance: make_transform_instance(
                        110,
                        basis_refs.clone(),
                        vec![offset as i32],
                        vec![offset as u32],
                        vec![inserted.clone()],
                        output_contract,
                        false,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: inserted.len() as u32 + 8,
                },
            );
        }

        if let Some((start, delete_len)) = bounded_delete_plan(basis_material, &current.exact_bytes)
        {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: delimiter_inventory(basis_material)
                    == delimiter_inventory(&current.exact_bytes),
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        111,
                        TransformKind::BoundedDelete,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![
                                TransformParameterField {
                                    name: "start".into(),
                                    min_value: 0,
                                    max_value: i32::MAX,
                                    required: true,
                                },
                                TransformParameterField {
                                    name: "delete_len".into(),
                                    min_value: 1,
                                    max_value: i32::MAX,
                                    required: true,
                                },
                            ],
                            max_basis_count: 1,
                            residual_allowed: false,
                        },
                        output_contract,
                        true,
                        0,
                        2,
                        10,
                        salience,
                    ),
                    instance: make_transform_instance(
                        111,
                        basis_refs.clone(),
                        vec![start as i32, delete_len as i32],
                        Vec::new(),
                        Vec::new(),
                        output_contract,
                        true,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: 6,
                },
            );
        }

        if let Some(repeats) = repeated_motif_expansion_plan(basis_material, &current.exact_bytes) {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: delimiter_inventory(basis_material)
                    == delimiter_inventory(&current.exact_bytes),
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        112,
                        TransformKind::RepeatedMotifExpansion,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![TransformParameterField {
                                name: "repeat_count".into(),
                                min_value: 1,
                                max_value: i32::MAX,
                                required: true,
                            }],
                            max_basis_count: 1,
                            residual_allowed: false,
                        },
                        output_contract,
                        true,
                        0,
                        2,
                        14,
                        salience,
                    ),
                    instance: make_transform_instance(
                        112,
                        basis_refs.clone(),
                        vec![repeats as i32],
                        Vec::new(),
                        Vec::new(),
                        output_contract,
                        true,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: 6,
                },
            );
        }

        if let Some(patches) = slot_substitution_plan(basis_material, &current.exact_bytes) {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: delimiter_inventory(basis_material)
                    == delimiter_inventory(&current.exact_bytes),
                deterministic: true,
            };
            let integer_parameters = patches
                .iter()
                .flat_map(|(start, len, _)| [*start as i32, *len as i32])
                .collect::<Vec<_>>();
            let residual_offsets = patches
                .iter()
                .map(|(start, _, _)| *start as u32)
                .collect::<Vec<_>>();
            let residual_bytes = patches
                .iter()
                .map(|(_, _, replacement)| replacement.clone())
                .collect::<Vec<_>>();
            let residual_len = residual_bytes
                .iter()
                .map(|bytes| bytes.len() as u32)
                .sum::<u32>();
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        114,
                        TransformKind::SlotSubstitution,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![TransformParameterField {
                                name: "slot_patch".into(),
                                min_value: 0,
                                max_value: i32::MAX,
                                required: true,
                            }],
                            max_basis_count: 1,
                            residual_allowed: true,
                        },
                        output_contract,
                        false,
                        residual_len,
                        (patches.len() as u32).saturating_add(1),
                        18,
                        salience,
                    ),
                    instance: make_transform_instance(
                        114,
                        basis_refs.clone(),
                        integer_parameters.clone(),
                        residual_offsets.clone(),
                        residual_bytes.clone(),
                        output_contract,
                        false,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: residual_len + patches.len() as u32 * 4,
                },
            );
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        120,
                        TransformKind::SchemaSlotFill,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![TransformParameterField {
                                name: "slot_fill".into(),
                                min_value: 0,
                                max_value: i32::MAX,
                                required: true,
                            }],
                            max_basis_count: 1,
                            residual_allowed: true,
                        },
                        output_contract,
                        false,
                        residual_len,
                        (patches.len() as u32).saturating_add(1),
                        18,
                        salience,
                    ),
                    instance: make_transform_instance(
                        120,
                        basis_refs.clone(),
                        integer_parameters,
                        residual_offsets,
                        residual_bytes,
                        output_contract,
                        false,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: residual_len + patches.len() as u32 * 4,
                },
            );
        }

        if let Some(width) = role_permutation_plan(basis_material, &current.exact_bytes) {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: delimiter_inventory(basis_material)
                    == delimiter_inventory(&current.exact_bytes),
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        115,
                        TransformKind::RolePermutation,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![TransformParameterField {
                                name: "chunk_width".into(),
                                min_value: 1,
                                max_value: i32::MAX,
                                required: true,
                            }],
                            max_basis_count: 1,
                            residual_allowed: false,
                        },
                        output_contract,
                        true,
                        0,
                        2,
                        16,
                        salience,
                    ),
                    instance: make_transform_instance(
                        115,
                        basis_refs.clone(),
                        vec![width as i32],
                        Vec::new(),
                        Vec::new(),
                        output_contract,
                        true,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: 6,
                },
            );
        }

        if let Some((offset, stride)) = strided_selection_plan(basis_material, &current.exact_bytes)
        {
            let output_contract = TransformOutputContract {
                min_output_len: current.exact_bytes.len() as u32,
                max_output_len: current.exact_bytes.len() as u32,
                preserves_delimiters: false,
                deterministic: true,
            };
            replace_or_push_transform_candidate(
                &mut out,
                TransformCandidate {
                    class: make_transform_class(
                        source_kind,
                        cue,
                        118,
                        TransformKind::StridedSelection,
                        &basis_refs,
                        TransformParameterSchema {
                            parameters: vec![
                                TransformParameterField {
                                    name: "offset".into(),
                                    min_value: 0,
                                    max_value: i32::MAX,
                                    required: true,
                                },
                                TransformParameterField {
                                    name: "stride".into(),
                                    min_value: 1,
                                    max_value: i32::MAX,
                                    required: true,
                                },
                            ],
                            max_basis_count: 1,
                            residual_allowed: false,
                        },
                        output_contract,
                        true,
                        0,
                        2,
                        14,
                        salience,
                    ),
                    instance: make_transform_instance(
                        118,
                        basis_refs.clone(),
                        vec![offset as i32, stride as i32],
                        Vec::new(),
                        Vec::new(),
                        output_contract,
                        true,
                        current.exact_bytes.len() as u32,
                    ),
                    estimated_wire_bytes: 6,
                },
            );
        }
    }

    if let Some((left_ref, right_ref, left_len, right_start)) =
        splice_from_two_bases_plan(&current.exact_bytes, &exact_bases)
    {
        let output_contract = TransformOutputContract {
            min_output_len: current.exact_bytes.len() as u32,
            max_output_len: current.exact_bytes.len() as u32,
            preserves_delimiters: false,
            deterministic: true,
        };
        let basis_refs = vec![left_ref.clone(), right_ref.clone()];
        replace_or_push_transform_candidate(
            &mut out,
            TransformCandidate {
                class: make_transform_class(
                    source_kind,
                    cue,
                    119,
                    TransformKind::SpliceFromTwoBases,
                    &basis_refs,
                    TransformParameterSchema {
                        parameters: vec![
                            TransformParameterField {
                                name: "left_len".into(),
                                min_value: 1,
                                max_value: i32::MAX,
                                required: true,
                            },
                            TransformParameterField {
                                name: "right_start".into(),
                                min_value: 0,
                                max_value: i32::MAX,
                                required: true,
                            },
                        ],
                        max_basis_count: 2,
                        residual_allowed: false,
                    },
                    output_contract,
                    true,
                    0,
                    2,
                    16,
                    current.exact_bytes.len() as u32,
                ),
                instance: make_transform_instance(
                    119,
                    basis_refs,
                    vec![left_len as i32, right_start as i32],
                    Vec::new(),
                    Vec::new(),
                    output_contract,
                    true,
                    current.exact_bytes.len() as u32,
                ),
                estimated_wire_bytes: 8,
            },
        );
    }

    let baseline_total_cost = current.exact_bytes.len().min(u32::MAX as usize) as u32 + 1;
    out.retain_mut(|candidate| {
        candidate.estimated_wire_bytes = transform_total_cost(candidate, baseline_total_cost);
        candidate.estimated_wire_bytes < baseline_total_cost
    });
    let _route_fingerprint = gf2_fingerprint(&current.exact_bytes);
    let _stable_basis_order = keyed_permute_indices(
        peer_catalog.block_count().min(16),
        current.exact_bytes.len() as u64,
    );
    out.sort_by_key(|candidate| {
        (
            candidate.estimated_wire_bytes,
            u32::MAX.saturating_sub(candidate.class.route_prior.planner_gain),
            candidate.class.mean_residual_bytes,
        )
    });
    out
}

fn transform_lifecycle(salience: u32) -> ObjectLifecycleMeta {
    ObjectLifecycleMeta {
        support_count: 1,
        success_count: 0,
        failure_count: 0,
        salience,
        creation_tick: 0,
        last_seen_tick: 0,
        promotion_level: PromotionLevel::Cold,
        consolidation_count: 0,
        last_consolidated_tick: 0,
        ontology_family_id: None,
        ontology_subfamily_id: None,
    }
}

fn transform_dependency_closure_from_refs(
    basis_refs: &[TransformBasisRef],
) -> TransformDependencyClosure {
    let mut substrate_object_ids = basis_refs
        .iter()
        .map(|basis| basis.object_id.clone())
        .collect::<Vec<_>>();
    substrate_object_ids.sort();
    substrate_object_ids.dedup();
    TransformDependencyClosure {
        substrate_object_ids,
        assembly_ids: Vec::new(),
        transform_ids: Vec::new(),
        schema_ids: Vec::new(),
        max_decode_depth: 3,
    }
}

fn make_transform_class(
    source_kind: SourceKind,
    cue: SparseCue,
    transform_id: u64,
    transform_kind: TransformKind,
    basis_refs: &[TransformBasisRef],
    parameter_schema: TransformParameterSchema,
    output_contract: TransformOutputContract,
    invertible: bool,
    residual_bytes: u32,
    decode_steps: u32,
    planner_gain: u32,
    salience: u32,
) -> TransformClass {
    TransformClass {
        transform_id: TransformId(transform_id),
        source_kind,
        transform_kind,
        transducer_family: TransformTransducerFamily::default_for_kind(transform_kind),
        parameter_schema,
        basis_mask: TransformBasisMask::substrate_only(),
        output_contract,
        invertible,
        dependency_closure: transform_dependency_closure_from_refs(basis_refs),
        support_metrics: TransformSupportMetrics {
            mean_residual_bytes: residual_bytes,
            mean_decode_steps: decode_steps,
            last_residual_bytes: residual_bytes,
            last_decode_steps: decode_steps,
        },
        route_prior: TransformRoutePrior {
            planner_gain,
            route_win_count: 0,
            route_loss_count: 0,
            last_route_win_tick: 0,
        },
        lifecycle: transform_lifecycle(salience),
        mean_residual_bytes: residual_bytes,
        mean_decode_steps: decode_steps,
        reuse_savings: 0,
        stability_score: 0,
        failure_score: 0,
        cue,
    }
}

fn make_transform_instance(
    class_id: u64,
    basis_refs: Vec<TransformBasisRef>,
    integer_parameters: Vec<i32>,
    residual_offsets: Vec<u32>,
    residual_bytes: Vec<Vec<u8>>,
    output_contract: TransformOutputContract,
    invertibility_expected: bool,
    output_len: u32,
) -> TransformInstance {
    let dependency_closure = transform_dependency_closure_from_refs(&basis_refs);
    TransformInstance {
        class_id: TransformId(class_id),
        basis_refs,
        integer_parameters,
        residual_offsets,
        residual_bytes,
        dependency_closure,
        output_contract,
        invertibility_expected,
        output_len,
    }
}

fn replace_or_push_transform_candidate(
    out: &mut Vec<TransformCandidate>,
    candidate: TransformCandidate,
) {
    if let Some(existing) = out
        .iter_mut()
        .find(|existing| existing.class.transform_kind == candidate.class.transform_kind)
    {
        if candidate.estimated_wire_bytes < existing.estimated_wire_bytes {
            *existing = candidate;
        }
    } else {
        out.push(candidate);
    }
}

fn exact_transform_bases(peer_catalog: &SharedBlockCatalog) -> Vec<(TransformBasisRef, Vec<u8>)> {
    let mut out = Vec::new();
    for block in peer_catalog.blocks_iter() {
        if block.material.is_empty() {
            continue;
        }
        out.push((
            TransformBasisRef {
                object_kind: ObjectKind::ExactBlock,
                object_id: format!("block:{}", block.block_id.0),
                start: 0,
                len: block.material.len() as u32,
            },
            block.material.clone(),
        ));
    }
    for bundle in peer_catalog.bundles_iter() {
        let Ok(material) = peer_catalog.materialize_bundle(bundle.bundle_id) else {
            continue;
        };
        if material.is_empty() {
            continue;
        }
        out.push((
            TransformBasisRef {
                object_kind: ObjectKind::ExactBundle,
                object_id: format!("bundle:{}", bundle.bundle_id.0),
                start: 0,
                len: material.len() as u32,
            },
            material,
        ));
    }
    out
}

fn prefix_insert_plan(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    if target.len() <= base.len() || !target.ends_with(base) {
        return None;
    }
    let prefix = target[..target.len().saturating_sub(base.len())].to_vec();
    (!prefix.is_empty()).then_some(prefix)
}

fn suffix_insert_plan(base: &[u8], target: &[u8]) -> Option<Vec<u8>> {
    if target.len() <= base.len() || !target.starts_with(base) {
        return None;
    }
    let suffix = target[base.len()..].to_vec();
    (!suffix.is_empty()).then_some(suffix)
}

fn wrap_plan(base: &[u8], target: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    if target.len() <= base.len().saturating_add(1) || base.is_empty() {
        return None;
    }
    for start in 1..target.len().saturating_sub(base.len()) {
        let end = start + base.len();
        if target.get(start..end) == Some(base) && end < target.len() {
            return Some((target[..start].to_vec(), target[end..].to_vec()));
        }
    }
    None
}

fn bounded_interior_insert_plan(base: &[u8], target: &[u8]) -> Option<(usize, Vec<u8>)> {
    if target.len() <= base.len() || base.len() < 2 {
        return None;
    }
    let prefix = common_prefix_len(base, target);
    if prefix == 0 || prefix >= base.len() {
        return None;
    }
    let mut base_end = base.len();
    let mut target_end = target.len();
    while base_end > prefix && target_end > prefix && base[base_end - 1] == target[target_end - 1] {
        base_end -= 1;
        target_end -= 1;
    }
    if base_end != prefix || target_end <= prefix {
        return None;
    }
    Some((prefix, target[prefix..target_end].to_vec()))
}

fn bounded_delete_plan(base: &[u8], target: &[u8]) -> Option<(usize, usize)> {
    if base.len() <= target.len() {
        return None;
    }
    let prefix = common_prefix_len(base, target);
    let mut base_end = base.len();
    let mut target_end = target.len();
    while base_end > prefix && target_end > prefix && base[base_end - 1] == target[target_end - 1] {
        base_end -= 1;
        target_end -= 1;
    }
    if target_end != prefix || base_end <= prefix {
        return None;
    }
    Some((prefix, base_end.saturating_sub(prefix)))
}

fn repeated_motif_expansion_plan(base: &[u8], target: &[u8]) -> Option<usize> {
    if base.is_empty() || target.len() <= base.len() || target.len() % base.len() != 0 {
        return None;
    }
    let repeats = target.len() / base.len();
    (repeats > 1 && target.chunks(base.len()).all(|chunk| chunk == base)).then_some(repeats)
}

fn copy_with_gap_plan(base: &[u8], target: &[u8]) -> Option<(usize, usize, Vec<u8>)> {
    let (start, gap_len, replacement) = bounded_substitution_delta(base, target)?;
    if gap_len == 0 || replacement.is_empty() {
        return None;
    }
    if start == 0 || start.saturating_add(gap_len) >= base.len() {
        return None;
    }
    Some((start, gap_len, replacement))
}

fn slot_substitution_plan(base: &[u8], target: &[u8]) -> Option<Vec<(usize, usize, Vec<u8>)>> {
    for delimiter in [b',', b';', b':', b'|', b'/', b' ', b'\n', b'\t'] {
        let base_ranges = segment_ranges(base, delimiter);
        let target_ranges = segment_ranges(target, delimiter);
        if base_ranges.len() < 2 || base_ranges.len() != target_ranges.len() {
            continue;
        }
        let mut patches = Vec::new();
        for ((base_start, base_len), (target_start, target_len)) in
            base_ranges.iter().zip(target_ranges.iter())
        {
            let base_seg = &base[*base_start..(*base_start + *base_len)];
            let target_seg = &target[*target_start..(*target_start + *target_len)];
            if base_seg != target_seg {
                patches.push((*base_start, *base_len, target_seg.to_vec()));
            }
        }
        if !patches.is_empty() {
            return Some(patches);
        }
    }
    None
}

fn segment_ranges(bytes: &[u8], delimiter: u8) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    for (idx, byte) in bytes.iter().enumerate() {
        if *byte == delimiter {
            out.push((start, idx.saturating_sub(start)));
            start = idx + 1;
        }
    }
    out.push((start, bytes.len().saturating_sub(start)));
    out
}

fn role_permutation_plan(base: &[u8], target: &[u8]) -> Option<usize> {
    if base.len() != target.len() || base.len() < 2 {
        return None;
    }
    for width in 1..=base.len().min(32) {
        let chunks = base
            .chunks(width)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        if chunks.len() < 2 {
            continue;
        }
        let mut reversed = chunks.clone();
        reversed.reverse();
        if reversed.concat() == target {
            return Some(width);
        }
    }
    None
}

fn strided_selection_plan(base: &[u8], target: &[u8]) -> Option<(usize, usize)> {
    if target.is_empty() || target.len() >= base.len() {
        return None;
    }
    for stride in 2..=base.len().min(32) {
        for offset in 0..stride.min(base.len()) {
            let candidate = base
                .iter()
                .skip(offset)
                .step_by(stride)
                .copied()
                .collect::<Vec<_>>();
            if candidate == target {
                return Some((offset, stride));
            }
        }
    }
    None
}

fn splice_from_two_bases_plan(
    target: &[u8],
    bases: &[(TransformBasisRef, Vec<u8>)],
) -> Option<(TransformBasisRef, TransformBasisRef, usize, usize)> {
    let mut best: Option<(TransformBasisRef, TransformBasisRef, usize, usize, usize)> = None;
    for split in 1..target.len() {
        let left_target = &target[..split];
        let right_target = &target[split..];
        for (left_ref, left_bytes) in bases {
            if !left_bytes.starts_with(left_target) {
                continue;
            }
            for (right_ref, right_bytes) in bases {
                if left_ref.object_id == right_ref.object_id {
                    continue;
                }
                if !right_bytes.ends_with(right_target) {
                    continue;
                }
                let right_start = right_bytes.len().saturating_sub(right_target.len());
                let metadata_span = left_bytes.len().saturating_add(right_bytes.len());
                match &best {
                    Some((_, _, _, _, current_span)) if *current_span <= metadata_span => {}
                    _ => {
                        best = Some((
                            left_ref.clone(),
                            right_ref.clone(),
                            split,
                            right_start,
                            metadata_span,
                        ));
                    }
                }
            }
        }
    }
    best.map(|(left_ref, right_ref, left_len, right_start, _)| {
        (left_ref, right_ref, left_len, right_start)
    })
}

fn transform_total_cost(candidate: &TransformCandidate, baseline_total_cost: u32) -> u32 {
    let transform_id_cost = 4_u32;
    let parameter_cost = candidate
        .instance
        .integer_parameters
        .len()
        .min(u32::MAX as usize) as u32
        * 2;
    let metadata_cost = candidate
        .instance
        .residual_offsets
        .len()
        .min(u32::MAX as usize) as u32
        * 2
        + candidate.instance.basis_refs.len().min(u32::MAX as usize) as u32 * 2;
    let decode_burden_cost = candidate
        .class
        .support_metrics
        .mean_decode_steps
        .max(candidate.class.mean_decode_steps)
        .min(32);
    let exact_gain_credit = candidate
        .class
        .route_prior
        .planner_gain
        .min(baseline_total_cost / 2);
    candidate
        .estimated_wire_bytes
        .saturating_add(transform_id_cost)
        .saturating_add(parameter_cost)
        .saturating_add(metadata_cost)
        .saturating_add(decode_burden_cost)
        .saturating_sub(exact_gain_credit)
}

fn materialize_transform_basis(
    reference: &StateRef,
    peer_catalog: &SharedBlockCatalog,
) -> Option<Vec<u8>> {
    match reference.kind {
        RefKind::Block | RefKind::Range => peer_catalog
            .materialize_block_range(BlockId(reference.base_id), reference.start, reference.len)
            .ok(),
        RefKind::Bundle => peer_catalog
            .materialize_bundle(BundleId(reference.base_id))
            .ok()
            .and_then(|bytes| {
                let start = reference.start as usize;
                let len = reference.len as usize;
                bytes
                    .get(start..start.saturating_add(len))
                    .map(|slice| slice.to_vec())
            }),
        _ => None,
    }
}

fn bounded_substitution_delta(base: &[u8], target: &[u8]) -> Option<(usize, usize, Vec<u8>)> {
    if base == target {
        return None;
    }
    let mut start = 0;
    while start < base.len().min(target.len()) && base[start] == target[start] {
        start += 1;
    }
    let mut base_end = base.len();
    let mut target_end = target.len();
    while base_end > start && target_end > start && base[base_end - 1] == target[target_end - 1] {
        base_end -= 1;
        target_end -= 1;
    }
    Some((
        start,
        base_end.saturating_sub(start),
        target[start..target_end].to_vec(),
    ))
}

fn delimiter_inventory(bytes: &[u8]) -> Vec<u8> {
    let mut delimiters = bytes
        .iter()
        .copied()
        .filter(|byte| {
            matches!(
                *byte,
                b',' | b';' | b':' | b'|' | b'/' | b' ' | b'\n' | b'\t'
            )
        })
        .collect::<Vec<_>>();
    delimiters.sort_unstable();
    delimiters
}

fn reordered_delimited_segments(
    base: &[u8],
    target: &[u8],
) -> Option<(u8, Vec<Vec<u8>>, Vec<Vec<u8>>)> {
    let delimiter = [b',', b';', b':', b'|', b'/', b' ']
        .into_iter()
        .find(|candidate| {
            delimiter_scan(base, *candidate).len() >= 2
                && delimiter_scan(target, *candidate).len() >= 2
        })?;
    let base_segments = base
        .split(|byte| *byte == delimiter)
        .map(|segment| segment.to_vec())
        .collect::<Vec<_>>();
    let target_segments = target
        .split(|byte| *byte == delimiter)
        .map(|segment| segment.to_vec())
        .collect::<Vec<_>>();
    if base_segments.len() != target_segments.len() || base_segments.len() < 2 {
        return None;
    }
    let mut left = base_segments.clone();
    let mut right = target_segments.clone();
    left.sort();
    right.sort();
    if left == right && base_segments != target_segments {
        Some((delimiter, base_segments, target_segments))
    } else {
        None
    }
}

fn motif_duplicate_plan(base: &[u8], target: &[u8]) -> Option<(usize, usize, usize)> {
    if target.len() <= base.len() || !target.starts_with(base) {
        return None;
    }
    let motif_overlap = bitset_overlap_count(base, &target[..base.len()]);
    let motif_density = popcount_bytes(base);
    if motif_overlap == 0 && motif_density == 0 {
        return None;
    }
    for motif_len in (1..=base.len().min(32)).rev() {
        let motif_start = base.len().saturating_sub(motif_len);
        let motif = &base[motif_start..];
        let suffix = &target[base.len()..];
        if suffix.is_empty() || suffix.len() % motif_len != 0 {
            continue;
        }
        let repeats = suffix.len() / motif_len;
        if repeats > 0 && suffix.chunks(motif_len).all(|chunk| chunk == motif) {
            return Some((motif_start, motif_len, repeats));
        }
    }
    None
}

fn delimited_repeat_plan(base: &[u8], target: &[u8]) -> Option<(u8, usize)> {
    for delimiter in [b'|', b',', b';', b':', b' '] {
        let delimiter_positions = delimiter_scan(target, delimiter);
        let parts = target.split(|byte| *byte == delimiter).collect::<Vec<_>>();
        if delimiter_positions.is_empty() {
            continue;
        }
        if parts.len() < 2 {
            continue;
        }
        if parts.iter().all(|part| *part == base) {
            return Some((delimiter, parts.len()));
        }
    }
    None
}

pub fn should_skip_compression_for_coordination(
    kind: crate::RecordType,
) -> Option<SkipCompressionReason> {
    match kind {
        crate::RecordType::Repair => Some(SkipCompressionReason::Repair),
        crate::RecordType::Rekey
        | crate::RecordType::Resync
        | crate::RecordType::Close
        | crate::RecordType::SourceMeta
        | crate::RecordType::PredictiveConfirm
        | crate::RecordType::PredictiveCorrect
        | crate::RecordType::AssemblyDef
        | crate::RecordType::TransformDef
        | crate::RecordType::SchemaDef
        | crate::RecordType::EpisodeHint
        | crate::RecordType::ReplayHint
        | crate::RecordType::MemoryRetire
        | crate::RecordType::TransformCorrect
        | crate::RecordType::MemoryAck => Some(SkipCompressionReason::ControlOrCoordinationRecord),
        crate::RecordType::ExactState | crate::RecordType::BatchEnvelope => None,
    }
}

pub fn choose_transport_representation(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> TransportRepresentationDecision {
    if current.exact_bytes.len() <= TINY_STATE_PAYLOAD_BYTES {
        return TransportRepresentationDecision::ExactCopy {
            skip_compression: SkipCompressionReason::TinyPayload,
        };
    }

    let copy_len = 1 + current.exact_bytes.len();
    let mut best: Option<StateCandidate> = None;

    consider_candidate(&mut best, exact_bundle_state(current, peer_catalog));
    consider_candidate(&mut best, exact_block_state(current, peer_catalog));
    consider_candidate(&mut best, exact_composite_state(current, peer_catalog));
    consider_candidate(
        &mut best,
        composite_with_suffix_residual_state(current, peer_catalog),
    );
    consider_candidate(
        &mut best,
        range_patch_state(current, peer_catalog, MAX_PATCH_RESIDUAL_BYTES),
    );

    match best {
        Some(candidate) if candidate.description_bytes < copy_len => {
            TransportRepresentationDecision::State {
                program: candidate.program,
                description_bytes: candidate.description_bytes,
                basis: candidate.basis,
            }
        }
        Some(_) => TransportRepresentationDecision::ExactCopy {
            skip_compression: SkipCompressionReason::StateDescriptionNotCheaper,
        },
        None => TransportRepresentationDecision::ExactCopy {
            skip_compression: SkipCompressionReason::HighNovelty,
        },
    }
}

fn consider_candidate(best: &mut Option<StateCandidate>, candidate: Option<StateCandidate>) {
    let Some(candidate) = candidate else {
        return;
    };

    match best {
        Some(current_best) if current_best.description_bytes <= candidate.description_bytes => {}
        _ => *best = Some(candidate),
    }
}

fn exact_bundle_state(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> Option<StateCandidate> {
    let bundle = peer_catalog.exact_bundle_for_material(&current.exact_bytes)?;
    let program = StateProgram::new(
        current.source_kind,
        vec![StateRef::bundle(bundle.bundle_id, 0, bundle.byte_len)],
        Vec::new(),
        Vec::new(),
    );
    encoded_candidate(program, StateBasis::Bundle(bundle.bundle_id))
}

fn exact_block_state(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> Option<StateCandidate> {
    let block = peer_catalog.exact_block_for_material(&current.exact_bytes)?;
    let program = StateProgram::new(
        current.source_kind,
        vec![StateRef::catalog_block(block.block_id, 0, block.byte_len)],
        Vec::new(),
        Vec::new(),
    );
    encoded_candidate(program, StateBasis::Block(block.block_id))
}

fn exact_composite_state(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> Option<StateCandidate> {
    let refs = best_prefix_refs(
        &current.exact_bytes,
        peer_catalog,
        MAX_COMPOSITE_STATE_REFS,
        0,
    )?;
    if refs.covered_len != current.exact_bytes.len() || refs.refs.len() < 2 {
        return None;
    }

    let program = StateProgram::new(
        current.source_kind,
        refs.refs.clone(),
        vec![StateOp::Concat],
        Vec::new(),
    );
    encoded_candidate(
        program,
        StateBasis::Composite {
            refs: refs.ref_count(),
            residual_bytes: 0,
        },
    )
}

fn composite_with_suffix_residual_state(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> Option<StateCandidate> {
    let refs = best_prefix_refs(
        &current.exact_bytes,
        peer_catalog,
        MAX_COMPOSITE_STATE_REFS,
        MAX_SUFFIX_RESIDUAL_BYTES,
    )?;
    if refs.refs.is_empty() || refs.covered_len >= current.exact_bytes.len() {
        return None;
    }

    let residual = current.exact_bytes[refs.covered_len..].to_vec();
    if residual.is_empty() || residual.len() > MAX_SUFFIX_RESIDUAL_BYTES {
        return None;
    }

    let program = StateProgram::new(current.source_kind, refs.refs.clone(), Vec::new(), residual);
    encoded_candidate(
        program,
        StateBasis::Composite {
            refs: refs.ref_count(),
            residual_bytes: current
                .exact_bytes
                .len()
                .saturating_sub(refs.covered_len)
                .min(u16::MAX as usize) as u16,
        },
    )
}

fn range_patch_state(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
    max_residual_bytes: usize,
) -> Option<StateCandidate> {
    let match_candidate = best_window_match(&current.exact_bytes, peer_catalog)?;
    if match_candidate.matched_len < MIN_RANGE_REUSE_BYTES {
        return None;
    }

    let total_residual_bytes = residual_bytes_for_match(&match_candidate);
    if total_residual_bytes == 0 || total_residual_bytes > max_residual_bytes {
        return None;
    }

    let prefix_len = match_candidate.payload_start;
    let suffix_start = prefix_len + match_candidate.matched_len;
    if suffix_start > current.exact_bytes.len() {
        return None;
    }

    let prefix = &current.exact_bytes[..prefix_len];
    let suffix = &current.exact_bytes[suffix_start..];

    let mut residual = Vec::with_capacity(prefix.len() + suffix.len());
    residual.extend_from_slice(prefix);
    residual.extend_from_slice(suffix);

    let mut ops = Vec::new();
    if !prefix.is_empty() {
        ops.push(StateOp::Patch {
            offset: 0,
            remove_len: 0,
            residual_offset: 0,
            residual_len: prefix.len() as u32,
        });
    }
    if !suffix.is_empty() {
        ops.push(StateOp::Patch {
            offset: (prefix_len + match_candidate.matched_len) as u32,
            remove_len: 0,
            residual_offset: prefix.len() as u32,
            residual_len: suffix.len() as u32,
        });
    }

    let program = StateProgram::new(
        current.source_kind,
        vec![match_candidate.state_ref.clone()],
        ops,
        residual,
    );

    encoded_candidate(
        program,
        StateBasis::Composite {
            refs: 1,
            residual_bytes: total_residual_bytes.min(u16::MAX as usize) as u16,
        },
    )
}

fn encoded_candidate(program: StateProgram, basis: StateBasis) -> Option<StateCandidate> {
    let description_bytes = program.encode().ok()?.len();
    Some(StateCandidate {
        program,
        description_bytes,
        basis,
    })
}

fn best_prefix_refs(
    payload: &[u8],
    peer_catalog: &SharedBlockCatalog,
    max_refs: usize,
    allowed_trailing_residual: usize,
) -> Option<CompositeRefs> {
    let mut best = None;
    let mut stack = Vec::new();
    search_prefix_refs(
        payload,
        peer_catalog,
        0,
        max_refs,
        allowed_trailing_residual,
        &mut stack,
        &mut best,
    );
    best
}

fn search_prefix_refs(
    payload: &[u8],
    peer_catalog: &SharedBlockCatalog,
    cursor: usize,
    max_refs: usize,
    allowed_trailing_residual: usize,
    stack: &mut Vec<StateRef>,
    best: &mut Option<CompositeRefs>,
) {
    let covered_len = cursor;

    if payload.len().saturating_sub(covered_len) <= allowed_trailing_residual {
        let candidate = CompositeRefs {
            refs: stack.clone(),
            covered_len,
        };
        if better_prefix_solution(
            best.as_ref(),
            &candidate,
            allowed_trailing_residual,
            payload.len(),
        ) {
            *best = Some(candidate);
        }
    }

    if covered_len >= payload.len() || stack.len() >= max_refs {
        return;
    }

    let remaining = &payload[covered_len..];
    let candidates = peer_catalog.prefix_candidates(
        remaining,
        MIN_COMPOSITE_COMPONENT_BYTES,
        MAX_COMPOSITE_CANDIDATES_PER_STEP,
    );

    for candidate in candidates {
        let (state_ref, advance) = match candidate {
            CatalogPrefixCandidate::Block { block_id, byte_len } => (
                StateRef::catalog_block(block_id, 0, byte_len),
                byte_len as usize,
            ),
            CatalogPrefixCandidate::Bundle {
                bundle_id,
                byte_len,
            } => (StateRef::bundle(bundle_id, 0, byte_len), byte_len as usize),
        };

        if advance == 0 {
            continue;
        }

        stack.push(state_ref);
        search_prefix_refs(
            payload,
            peer_catalog,
            covered_len + advance,
            max_refs,
            allowed_trailing_residual,
            stack,
            best,
        );
        stack.pop();
    }
}

fn better_prefix_solution(
    current: Option<&CompositeRefs>,
    candidate: &CompositeRefs,
    allowed_trailing_residual: usize,
    payload_len: usize,
) -> bool {
    match current {
        None => true,
        Some(current) => {
            let current_residual = payload_len.saturating_sub(current.covered_len);
            let candidate_residual = payload_len.saturating_sub(candidate.covered_len);

            if candidate.covered_len != current.covered_len {
                return candidate.covered_len > current.covered_len;
            }
            if allowed_trailing_residual > 0 && candidate_residual != current_residual {
                return candidate_residual < current_residual;
            }
            if candidate.refs.len() != current.refs.len() {
                return candidate.refs.len() < current.refs.len();
            }

            estimated_refs_bytes(&candidate.refs) < estimated_refs_bytes(&current.refs)
        }
    }
}

fn estimated_refs_bytes(refs: &[StateRef]) -> usize {
    refs.iter()
        .map(|reference| {
            let indices_bytes = reference.indices.len() * std::mem::size_of::<u32>();
            1 + 8 + 4 + 4 + 2 + 4 + indices_bytes
        })
        .sum()
}

fn best_window_match(
    payload: &[u8],
    peer_catalog: &SharedBlockCatalog,
) -> Option<WindowMatchCandidate> {
    let mut best = None;

    for block in peer_catalog.blocks_iter() {
        consider_window_match(
            &mut best,
            find_best_window_in_base(payload, &block.material).map(|window| WindowMatchCandidate {
                state_ref: StateRef::range(
                    block.block_id,
                    window.base_start,
                    window.matched_len as u32,
                ),
                payload_start: window.payload_start,
                matched_len: window.matched_len,
                total_payload_len: payload.len(),
            }),
        );
    }

    for bundle in peer_catalog.bundles_iter() {
        let material = peer_catalog.materialize_bundle(bundle.bundle_id).ok()?;
        consider_window_match(
            &mut best,
            find_best_window_in_base(payload, &material).map(|window| WindowMatchCandidate {
                state_ref: StateRef::bundle(
                    bundle.bundle_id,
                    window.base_start,
                    window.matched_len as u32,
                ),
                payload_start: window.payload_start,
                matched_len: window.matched_len,
                total_payload_len: payload.len(),
            }),
        );
    }

    best
}

fn consider_window_match(
    best: &mut Option<WindowMatchCandidate>,
    candidate: Option<WindowMatchCandidate>,
) {
    let Some(candidate) = candidate else {
        return;
    };

    match best {
        Some(current) if !better_window_match(&candidate, current) => {}
        _ => *best = Some(candidate),
    }
}

fn better_window_match(candidate: &WindowMatchCandidate, current: &WindowMatchCandidate) -> bool {
    if candidate.matched_len != current.matched_len {
        return candidate.matched_len > current.matched_len;
    }

    let candidate_residual = residual_bytes_for_match(candidate);
    let current_residual = residual_bytes_for_match(current);
    if candidate_residual != current_residual {
        return candidate_residual < current_residual;
    }

    if candidate.payload_start != current.payload_start {
        return candidate.payload_start < current.payload_start;
    }

    let candidate_ref_cost = estimated_refs_bytes(std::slice::from_ref(&candidate.state_ref));
    let current_ref_cost = estimated_refs_bytes(std::slice::from_ref(&current.state_ref));
    if candidate_ref_cost != current_ref_cost {
        return candidate_ref_cost < current_ref_cost;
    }

    false
}

fn residual_bytes_for_match(candidate: &WindowMatchCandidate) -> usize {
    candidate
        .total_payload_len
        .saturating_sub(candidate.matched_len)
}

fn find_best_window_in_base(payload: &[u8], base: &[u8]) -> Option<WindowMatch> {
    if payload.is_empty() || base.is_empty() {
        return None;
    }

    let mut best: Option<WindowMatch> = None;

    for payload_start in 0..payload.len() {
        let max_candidate_len = payload.len() - payload_start;
        if let Some(current_best) = best {
            if max_candidate_len <= current_best.matched_len {
                continue;
            }
        }

        for base_start in 0..base.len() {
            let match_len = common_prefix_len(&payload[payload_start..], &base[base_start..]);
            if match_len < MIN_RANGE_REUSE_BYTES {
                continue;
            }

            let candidate = WindowMatch {
                payload_start,
                base_start: base_start as u32,
                matched_len: match_len,
            };

            match best {
                Some(current) if !better_window_match_raw(candidate, current) => {}
                _ => best = Some(candidate),
            }
        }
    }

    best
}

fn better_window_match_raw(candidate: WindowMatch, current: WindowMatch) -> bool {
    if candidate.matched_len != current.matched_len {
        return candidate.matched_len > current.matched_len;
    }
    if candidate.payload_start != current.payload_start {
        return candidate.payload_start < current.payload_start;
    }
    candidate.base_start < current.base_start
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(a, b)| a == b)
        .count()
}

pub fn state_ref_requires_catalog(reference: &StateRef) -> bool {
    matches!(
        reference.kind,
        RefKind::Block
            | RefKind::Range
            | RefKind::StridedRange
            | RefKind::IndexSet
            | RefKind::Bundle
    )
}

#[cfg(test)]
mod tests {
    use crate::{BlockCatalogVersion, BundleMember, SourceKind};

    use super::*;

    fn block(bytes: &[u8]) -> ExactStateMaterial {
        ExactStateMaterial::copy_exact(SourceKind::Text, bytes)
    }

    #[test]
    fn state_is_chosen_for_known_bundle_when_cheaper() {
        let mut catalog = SharedBlockCatalog::default();
        let left_bytes = b"known known known known known known ".to_vec();
        let right_bytes = b"bundle bundle bundle bundle bundle bundle".to_vec();

        let left = catalog
            .insert_block(SourceKind::Text, left_bytes.clone())
            .unwrap();
        let right = catalog
            .insert_block(SourceKind::Text, right_bytes.clone())
            .unwrap();

        catalog
            .define_bundle(vec![
                BundleMember::block(left, 0, left_bytes.len() as u32),
                BundleMember::block(right, 0, right_bytes.len() as u32),
            ])
            .unwrap();

        let bundle_id = catalog.bundles_iter().next().unwrap().bundle_id;
        let sync = catalog
            .sync_payload(BlockCatalogVersion(1), &[], &[bundle_id])
            .unwrap();

        let mut peer = SharedBlockCatalog::default();
        peer.apply_sync(sync).unwrap();

        let mut exact = left_bytes;
        exact.extend_from_slice(&right_bytes);

        let decision = choose_transport_representation(&block(&exact), &peer);
        assert!(matches!(
            decision,
            TransportRepresentationDecision::State {
                basis: StateBasis::Bundle(_),
                ..
            }
        ));
    }

    #[test]
    fn composite_state_is_chosen_for_multiple_shared_blocks_without_bundle() {
        let mut catalog = SharedBlockCatalog::default();
        let left_bytes = b"shared prefix ".repeat(48);
        let right_bytes = b"shared suffix ".repeat(48);

        let left = catalog
            .insert_block(SourceKind::Text, left_bytes.clone())
            .unwrap();
        let right = catalog
            .insert_block(SourceKind::Text, right_bytes.clone())
            .unwrap();

        let sync = catalog
            .sync_payload(BlockCatalogVersion(1), &[left, right], &[])
            .unwrap();

        let mut peer = SharedBlockCatalog::default();
        peer.apply_sync(sync).unwrap();

        let mut exact = left_bytes;
        exact.extend_from_slice(&right_bytes);

        let decision = choose_transport_representation(&block(&exact), &peer);
        match decision {
            TransportRepresentationDecision::State {
                basis:
                    StateBasis::Composite {
                        refs,
                        residual_bytes,
                    },
                program,
                ..
            } => {
                assert_eq!(refs, 2);
                assert_eq!(residual_bytes, 0);
                assert_eq!(program.refs.len(), 2);
                assert_eq!(program.ops, vec![StateOp::Concat]);
            }
            other => panic!("expected composite state, got {other:?}"),
        }
    }

    #[test]
    fn range_patch_state_is_chosen_for_internal_shared_window() {
        let mut catalog = SharedBlockCatalog::default();
        let reusable_center = b"highly reusable shared center ".repeat(8);

        let mut base = b"---- ".to_vec();
        base.extend_from_slice(&reusable_center);
        base.extend_from_slice(b" ----");

        let block_id = catalog.insert_block(SourceKind::Text, base).unwrap();

        let sync = catalog
            .sync_payload(BlockCatalogVersion(1), &[block_id], &[])
            .unwrap();

        let mut peer = SharedBlockCatalog::default();
        peer.apply_sync(sync).unwrap();

        let mut payload = b"prefix ".to_vec();
        payload.extend_from_slice(&reusable_center);
        payload.extend_from_slice(b" suffix");

        let decision = choose_transport_representation(&block(&payload), &peer);
        match decision {
            TransportRepresentationDecision::State {
                basis:
                    StateBasis::Composite {
                        refs,
                        residual_bytes,
                    },
                program,
                ..
            } => {
                assert_eq!(refs, 1);
                assert!(residual_bytes > 0);
                assert_eq!(program.refs.len(), 1);
                assert!(matches!(
                    program.refs[0].kind,
                    RefKind::Range | RefKind::Bundle
                ));
                assert!(!program.ops.is_empty());
            }
            other => panic!("expected patched range state, got {other:?}"),
        }
    }

    #[test]
    fn bounded_search_prefers_more_complete_cover_over_first_prefix() {
        let mut catalog = SharedBlockCatalog::default();

        let short = catalog
            .insert_block(SourceKind::Text, b"abc".repeat(4))
            .unwrap();
        let mid = catalog
            .insert_block(SourceKind::Text, b"abcabcxyz".to_vec())
            .unwrap();
        let tail = catalog
            .insert_block(SourceKind::Text, b"tailtailtail".to_vec())
            .unwrap();

        let sync = catalog
            .sync_payload(BlockCatalogVersion(1), &[short, mid, tail], &[])
            .unwrap();

        let mut peer = SharedBlockCatalog::default();
        peer.apply_sync(sync).unwrap();

        let payload = b"abcabcxyztailtailtail";
        let refs = best_prefix_refs(payload, &peer, MAX_COMPOSITE_STATE_REFS, 0).unwrap();
        assert_eq!(refs.covered_len, payload.len());
        assert_eq!(refs.refs.len(), 2);
    }

    #[test]
    fn high_novelty_uses_exact_copy_and_skips_compression() {
        let peer = SharedBlockCatalog::default();
        let decision = choose_transport_representation(
            &block(b"this payload has no shared basis in the catalog"),
            &peer,
        );
        assert_eq!(
            decision,
            TransportRepresentationDecision::ExactCopy {
                skip_compression: SkipCompressionReason::HighNovelty,
            }
        );
    }

    #[test]
    fn tiny_payload_skips_state_and_compression() {
        let peer = SharedBlockCatalog::default();
        let decision = choose_transport_representation(&block(b"tiny"), &peer);
        assert_eq!(
            decision,
            TransportRepresentationDecision::ExactCopy {
                skip_compression: SkipCompressionReason::TinyPayload,
            }
        );
    }

    #[test]
    fn exact_atom_plan_supports_exact_composite_catalog_reuse() {
        let mut catalog = SharedBlockCatalog::default();
        let left_bytes = b"left-left-left-".repeat(4);
        let right_bytes = b"right-right-right".repeat(4);
        let left = catalog
            .insert_block(SourceKind::Text, left_bytes.clone())
            .unwrap();
        let right = catalog
            .insert_block(SourceKind::Text, right_bytes.clone())
            .unwrap();
        let sync = catalog
            .sync_payload(BlockCatalogVersion(1), &[left, right], &[])
            .unwrap();
        let mut peer = SharedBlockCatalog::default();
        peer.apply_sync(sync).unwrap();
        let mut exact = left_bytes;
        exact.extend_from_slice(&right_bytes);
        let plan = exact_atom_plan(&block(&exact), &peer).expect("exact atom plan");
        assert!(matches!(
            plan.basis,
            StateBasis::Composite {
                refs: 2,
                residual_bytes: 0
            }
        ));
        assert_eq!(
            plan.substrate_graph
                .nodes
                .iter()
                .filter(|node| matches!(node.kind, crate::SubstrateRouteNodeKind::Material))
                .count(),
            2
        );
        assert!(
            plan.substrate_graph
                .nodes
                .iter()
                .filter_map(|node| node.substrate.as_ref())
                .all(|object| matches!(
                    object.reference.object_kind,
                    ObjectKind::ExactRange | ObjectKind::ExactBundle | ObjectKind::ExactBlock
                ))
        );
        assert!(
            plan.capability_mask.supports(ObjectKind::ExactRange)
                || plan.capability_mask.supports(ObjectKind::ExactBundle)
                || plan.capability_mask.supports(ObjectKind::ExactBlock)
        );
        assert_eq!(plan.output_len as usize, exact.len());
    }
}

pub fn choose_admissible_assembly_candidates(
    source_kind: SourceKind,
    bytes: &[u8],
    extraction: AssemblyExtractionConfig,
    policy: AssemblyAdmissibilityPolicy,
    ambiguity_score: u32,
    dependencies_available: bool,
) -> Vec<AssemblyExtractionCandidate> {
    extract_catalog_assembly_candidates(source_kind, bytes, extraction)
        .into_iter()
        .filter(|candidate| {
            let dependencies_available =
                dependencies_available || candidate.dependency_closure.dependencies.is_empty();
            assembly_route_eligibility(
                policy,
                candidate.admissibility_input(ambiguity_score, dependencies_available),
            )
            .is_ok()
        })
        .collect()
}

pub fn check_assembly_route_admissibility(
    policy: AssemblyAdmissibilityPolicy,
    input: AssemblyAdmissibilityInput,
) -> Result<(), AssemblyAdmissibilityFailure> {
    assembly_route_eligibility(policy, input)
}

pub fn generate_episode_route_candidates(
    memory: &EpisodeMemory,
    policy: EpisodeCandidatePolicy,
) -> Vec<EpisodeCompletionCandidate> {
    generate_episode_completion_candidates(memory, policy)
}

pub fn episode_hint_dependencies(
    candidates: &[EpisodeCompletionCandidate],
) -> Vec<ObjectDependency> {
    let mut dependencies = candidates
        .iter()
        .map(|candidate| ObjectDependency {
            object_kind: candidate.object_ref.object_kind,
            object_id: candidate.object_ref.object_id.clone(),
            required_revision: 0,
        })
        .collect::<Vec<_>>();
    dependencies.sort_by(|l, r| {
        (l.object_kind as u8)
            .cmp(&(r.object_kind as u8))
            .then_with(|| l.object_id.cmp(&r.object_id))
    });
    dependencies.dedup();
    dependencies
}

pub fn generate_schema_candidates_for_route(
    schemas: &[SchemaGraph],
    cue: SparseCue,
    source_kind: SourceKind,
    context_hash: Option<ContextHash>,
    policy: SchemaCandidatePolicy,
) -> Vec<SchemaRouteCandidate> {
    generate_schema_route_candidates(schemas, cue, source_kind, context_hash, policy)
}

pub fn episode_candidates_for_context(
    memory: &EpisodeMemory,
    context: &[ContextHash],
    policy: EpisodeCandidatePolicy,
) -> Vec<EpisodeCompletionCandidate> {
    let mut staged = memory.clone();
    staged
        .working_trace
        .events
        .retain(|event| context.contains(&event.context_hash));
    generate_episode_completion_candidates(&staged, policy)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControllerRoutePlan {
    DirectState {
        score: RouteScore,
    },
    ExactAtom {
        score: RouteScore,
        plan: ExactAtomPlan,
    },
    Assembly {
        score: RouteScore,
        candidate: AssemblyExtractionCandidate,
        dependency_closure: Vec<ObjectDependency>,
    },
    Transform {
        score: RouteScore,
        class: TransformClass,
        instance: TransformInstance,
    },
    EpisodeCompletion {
        score: RouteScore,
        candidate: EpisodeCompletionCandidate,
    },
    SchemaExpansion {
        score: RouteScore,
        candidate: SchemaRouteCandidate,
    },
    Hybrid {
        score: RouteScore,
        route: HybridRoute,
    },
}

impl ControllerRoutePlan {
    pub const fn route_family(&self) -> ControllerRouteFamily {
        match self {
            Self::DirectState { .. } => ControllerRouteFamily::DirectState,
            Self::ExactAtom { .. } => ControllerRouteFamily::ExactAtom,
            Self::Assembly { .. } => ControllerRouteFamily::DirectState,
            Self::Transform { .. } => ControllerRouteFamily::DirectState,
            Self::EpisodeCompletion { .. } => ControllerRouteFamily::DirectState,
            Self::SchemaExpansion { .. } => ControllerRouteFamily::DirectState,
            Self::Hybrid { .. } => ControllerRouteFamily::Hybrid,
        }
    }

    pub const fn score(&self) -> RouteScore {
        match self {
            Self::DirectState { score }
            | Self::ExactAtom { score, .. }
            | Self::Assembly { score, .. }
            | Self::Transform { score, .. }
            | Self::EpisodeCompletion { score, .. }
            | Self::SchemaExpansion { score, .. }
            | Self::Hybrid { score, .. } => *score,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteAdmissibilityPolicy {
    pub max_sync_risk: u32,
    pub max_ambiguity: u32,
}

impl RouteAdmissibilityPolicy {
    pub const fn bounded_default() -> Self {
        Self {
            max_sync_risk: 64,
            max_ambiguity: 64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteSelectionContext {
    pub cue: SparseCue,
    pub source_kind: SourceKind,
    pub context_hash: Option<ContextHash>,
    pub tick: u64,
    #[serde(default)]
    pub governor: ContextTreeGovernor,
    #[serde(default)]
    pub prior_schema_kind: Option<SchemaKind>,
    #[serde(default)]
    pub prior_transform_family: Option<TransformTransducerFamily>,
    #[serde(default)]
    pub lag_bucket: LagBucket,
}

impl RouteSelectionContext {
    fn governor_signal(
        &self,
        route_family: ControllerRouteFamily,
        slot_shape: SlotShapeBucket,
        residual_bytes: u32,
        sync_risk: u32,
    ) -> RouteGovernorSignal {
        self.governor.signal_for(ContextTreeSymbol {
            route_family,
            delimiter_class: DelimiterClass::from_cue(self.cue),
            slot_shape,
            prior_schema_kind: self.prior_schema_kind,
            prior_transform_family: self.prior_transform_family,
            lag_bucket: self.lag_bucket,
            residual_bucket: ResidualSizeBucket::from_bytes(residual_bytes),
            sync_state: SyncStateBucket::from_sync_risk(sync_risk),
        })
    }
}

pub fn route_context_symbol_for_plan(
    plan: &ControllerRoutePlan,
    selection: &RouteSelectionContext,
) -> ContextTreeSymbol {
    match plan {
        ControllerRoutePlan::DirectState { .. } => ContextTreeSymbol {
            route_family: ControllerRouteFamily::DirectState,
            delimiter_class: DelimiterClass::from_cue(selection.cue),
            slot_shape: SlotShapeBucket::None,
            prior_schema_kind: selection.prior_schema_kind,
            prior_transform_family: selection.prior_transform_family,
            lag_bucket: selection.lag_bucket,
            residual_bucket: ResidualSizeBucket::from_bytes(0),
            sync_state: SyncStateBucket::Stable,
        },
        ControllerRoutePlan::ExactAtom { .. } => ContextTreeSymbol {
            route_family: ControllerRouteFamily::ExactAtom,
            delimiter_class: DelimiterClass::from_cue(selection.cue),
            slot_shape: SlotShapeBucket::None,
            prior_schema_kind: selection.prior_schema_kind,
            prior_transform_family: selection.prior_transform_family,
            lag_bucket: selection.lag_bucket,
            residual_bucket: ResidualSizeBucket::None,
            sync_state: SyncStateBucket::from_sync_risk(4),
        },
        ControllerRoutePlan::Assembly { candidate, .. } => ContextTreeSymbol {
            route_family: ControllerRouteFamily::DirectState,
            delimiter_class: DelimiterClass::from_cue(selection.cue),
            slot_shape: SlotShapeBucket::from_slot_count(candidate.slots.len()),
            prior_schema_kind: selection.prior_schema_kind,
            prior_transform_family: selection.prior_transform_family,
            lag_bucket: selection.lag_bucket,
            residual_bucket: ResidualSizeBucket::from_bytes(candidate.residual_bytes()),
            sync_state: SyncStateBucket::from_sync_risk(candidate.dependency_sync_risk()),
        },
        ControllerRoutePlan::Transform { class, .. } => ContextTreeSymbol {
            route_family: ControllerRouteFamily::DirectState,
            delimiter_class: DelimiterClass::from_cue(selection.cue),
            slot_shape: SlotShapeBucket::None,
            prior_schema_kind: selection.prior_schema_kind,
            prior_transform_family: Some(class.transducer_family),
            lag_bucket: selection.lag_bucket,
            residual_bucket: ResidualSizeBucket::from_bytes(class.mean_residual_bytes),
            sync_state: SyncStateBucket::from_sync_risk(8),
        },
        ControllerRoutePlan::EpisodeCompletion { candidate, .. } => ContextTreeSymbol {
            route_family: ControllerRouteFamily::DirectState,
            delimiter_class: DelimiterClass::from_cue(selection.cue),
            slot_shape: SlotShapeBucket::None,
            prior_schema_kind: selection.prior_schema_kind,
            prior_transform_family: selection.prior_transform_family,
            lag_bucket: candidate.lag_bucket,
            residual_bucket: ResidualSizeBucket::None,
            sync_state: SyncStateBucket::from_sync_risk(18),
        },
        ControllerRoutePlan::SchemaExpansion { candidate, .. } => ContextTreeSymbol {
            route_family: ControllerRouteFamily::DirectState,
            delimiter_class: DelimiterClass::from_cue(selection.cue),
            slot_shape: SlotShapeBucket::None,
            prior_schema_kind: Some(candidate.schema_kind),
            prior_transform_family: selection.prior_transform_family,
            lag_bucket: selection.lag_bucket,
            residual_bucket: ResidualSizeBucket::from_bytes(candidate.decode_burden),
            sync_state: SyncStateBucket::from_sync_risk(
                candidate.dependency_closure.dependencies.len() as u32 * 4,
            ),
        },
        ControllerRoutePlan::Hybrid { .. } => ContextTreeSymbol {
            route_family: ControllerRouteFamily::Hybrid,
            delimiter_class: DelimiterClass::from_cue(selection.cue),
            slot_shape: SlotShapeBucket::None,
            prior_schema_kind: selection.prior_schema_kind,
            prior_transform_family: selection.prior_transform_family,
            lag_bucket: selection.lag_bucket,
            residual_bucket: ResidualSizeBucket::Small,
            sync_state: SyncStateBucket::from_sync_risk(10),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SchemaGuidance<'a> {
    strongest: Option<&'a SchemaRouteCandidate>,
}

impl<'a> SchemaGuidance<'a> {
    fn gain_for_assembly(
        self,
        candidate: &AssemblyExtractionCandidate,
        selection: &RouteSelectionContext,
    ) -> u32 {
        let Some(schema) = self.strongest else {
            return 0;
        };
        let cue_gain = selection
            .cue
            .overlap_score(schema.cue)
            .saturating_add(candidate.cue.overlap_score(schema.cue))
            / 2;
        let slot_gain = if !candidate.slots.is_empty() {
            schema.branch_consistency.min(24)
        } else {
            schema.branch_consistency.min(8)
        };
        cue_gain
            .saturating_add(slot_gain)
            .saturating_add(schema.cross_episode_support.min(16))
            .saturating_sub(schema.contradiction_burden.min(12))
    }

    fn gain_for_transform(self, class: &TransformClass, selection: &RouteSelectionContext) -> u32 {
        let Some(schema) = self.strongest else {
            return 0;
        };
        let cue_gain = selection.cue.overlap_score(schema.cue);
        let family_gain =
            if schema_prefers_transform_family(schema.schema_kind, class.transducer_family) {
                schema.branch_consistency.min(24)
            } else {
                0
            };
        cue_gain
            .saturating_add(family_gain)
            .saturating_add(schema.cross_episode_support.min(16))
            .saturating_sub(schema.contradiction_burden.min(12))
    }

    fn mismatch_penalty_for_transform(self, class: &TransformClass) -> u32 {
        let Some(schema) = self.strongest else {
            return 0;
        };
        if schema.cue_overlap >= 16
            && !schema_prefers_transform_family(schema.schema_kind, class.transducer_family)
        {
            schema
                .contradiction_burden
                .saturating_add(schema.decode_burden / 2)
                .min(24)
        } else {
            0
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResonanceState {
    route_family: ControllerRouteFamily,
    agreement_gain: u32,
    contradiction_burden: u32,
    decode_cost: u32,
    sync_risk: u32,
    episodic_gain: u32,
    schema_gain: u32,
    transform_fit: u32,
    novelty_penalty: u32,
    resonance_total: i32,
}

fn schema_prefers_transform_family(
    schema_kind: SchemaKind,
    family: TransformTransducerFamily,
) -> bool {
    matches!(
        (schema_kind, family),
        (
            SchemaKind::Sequence,
            TransformTransducerFamily::BoundedReorder
        ) | (SchemaKind::Template, TransformTransducerFamily::SlotBind)
            | (
                SchemaKind::Alternation,
                TransformTransducerFamily::DelimiterPreservingRewrite
            )
            | (
                SchemaKind::Permutation,
                TransformTransducerFamily::RolePermutation
            )
            | (
                SchemaKind::Expansion,
                TransformTransducerFamily::SchemaConditionedExpansion
            )
    )
}

fn schema_guidance<'a>(schemas: &'a [SchemaRouteCandidate]) -> SchemaGuidance<'a> {
    SchemaGuidance {
        strongest: schemas.first(),
    }
}

fn resonance_state_for_plan(
    plan: &ControllerRoutePlan,
    selection: &RouteSelectionContext,
) -> ResonanceState {
    match plan {
        ControllerRoutePlan::DirectState { score } => ResonanceState {
            route_family: ControllerRouteFamily::DirectState,
            agreement_gain: 0,
            contradiction_burden: 0,
            decode_cost: 1,
            sync_risk: 0,
            episodic_gain: 0,
            schema_gain: 0,
            transform_fit: 0,
            novelty_penalty: score.breakdown.base_penalties / 6,
            resonance_total: score.total,
        },
        ControllerRoutePlan::ExactAtom { score, plan } => ResonanceState {
            route_family: ControllerRouteFamily::ExactAtom,
            agreement_gain: plan.output_len / 2,
            contradiction_burden: 0,
            decode_cost: 2,
            sync_risk: plan.substrate_graph.dependency_closure.len() as u32 * 4,
            episodic_gain: 0,
            schema_gain: 0,
            transform_fit: 0,
            novelty_penalty: 0,
            resonance_total: score.total,
        },
        ControllerRoutePlan::Assembly {
            score,
            candidate,
            dependency_closure,
        } => ResonanceState {
            route_family: ControllerRouteFamily::DirectState,
            agreement_gain: candidate
                .agreement_score()
                .saturating_add(candidate.support_vote_count()),
            contradiction_burden: candidate.ambiguity_score(),
            decode_cost: candidate.estimated_decode_cost(),
            sync_risk: route_dependency_gate(dependency_closure, candidate.dependency_sync_risk())
                .sync_risk,
            episodic_gain: 0,
            schema_gain: selection.cue.overlap_score(candidate.cue),
            transform_fit: 0,
            novelty_penalty: candidate
                .residual_bytes()
                .saturating_div(8)
                .saturating_add(candidate.ambiguity_score() / 2),
            resonance_total: score.total,
        },
        ControllerRoutePlan::Transform { score, class, .. } => ResonanceState {
            route_family: ControllerRouteFamily::DirectState,
            agreement_gain: class.lifecycle.support_count / 2,
            contradiction_burden: class.failure_score,
            decode_cost: class.mean_decode_steps,
            sync_risk: 8,
            episodic_gain: 0,
            schema_gain: selection.cue.overlap_score(class.cue),
            transform_fit: class.stability_score,
            novelty_penalty: class
                .failure_score
                .saturating_add(class.mean_residual_bytes / 8),
            resonance_total: score.total,
        },
        ControllerRoutePlan::EpisodeCompletion { score, candidate } => ResonanceState {
            route_family: ControllerRouteFamily::DirectState,
            agreement_gain: candidate.transition_match,
            contradiction_burden: candidate.branch_rank.0 as u32,
            decode_cost: 6,
            sync_risk: 18,
            episodic_gain: candidate
                .transition_count
                .0
                .saturating_add(candidate.recency_score / 2),
            schema_gain: candidate.cue_overlap / 2,
            transform_fit: 0,
            novelty_penalty: candidate.lag_bucket.0 as u32,
            resonance_total: score.total,
        },
        ControllerRoutePlan::SchemaExpansion { score, candidate } => ResonanceState {
            route_family: ControllerRouteFamily::DirectState,
            agreement_gain: candidate
                .cross_episode_support
                .saturating_add(candidate.branch_consistency / 2),
            contradiction_burden: candidate.contradiction_burden,
            decode_cost: candidate.decode_burden.max(candidate.depth_cap as u32),
            sync_risk: candidate.dependency_closure.dependencies.len() as u32 * 4,
            episodic_gain: candidate.cross_episode_support / 2,
            schema_gain: candidate
                .cue_overlap
                .saturating_add(candidate.branch_consistency),
            transform_fit: 0,
            novelty_penalty: candidate.fanout_cap as u32,
            resonance_total: score.total,
        },
        ControllerRoutePlan::Hybrid { score, route } => ResonanceState {
            route_family: ControllerRouteFamily::Hybrid,
            agreement_gain: route.output_len / 4,
            contradiction_burden: 0,
            decode_cost: 6,
            sync_risk: 10,
            episodic_gain: 0,
            schema_gain: 0,
            transform_fit: 0,
            novelty_penalty: route.components.len() as u32 * 2,
            resonance_total: score.total,
        },
    }
}

fn route_families_incompatible(left: ControllerRouteFamily, right: ControllerRouteFamily) -> bool {
    left != right
        && !matches!(
            (left, right),
            (
                ControllerRouteFamily::DirectState,
                ControllerRouteFamily::DirectState
            )
        )
}

fn inhibition_penalty(winner: ResonanceState, loser: ResonanceState) -> i32 {
    if !route_families_incompatible(winner.route_family, loser.route_family) {
        return 0;
    }
    let winner_drive = winner
        .agreement_gain
        .saturating_add(winner.episodic_gain)
        .saturating_add(winner.schema_gain)
        .saturating_add(winner.transform_fit / 2);
    let loser_vulnerability = loser
        .contradiction_burden
        .saturating_add(loser.novelty_penalty)
        .saturating_add(loser.sync_risk / 2);
    (winner_drive.min(64) as i32) - (loser_vulnerability.min(32) as i32 / 2)
}

fn choose_by_resonance(
    candidates: Vec<ControllerRoutePlan>,
    selection: &RouteSelectionContext,
) -> ControllerRoutePlan {
    if candidates.is_empty() {
        return ControllerRoutePlan::DirectState {
            score: score_route(
                RouteScoreInputs {
                    wire_cost: 0,
                    decode_cost: 1,
                    residual_cost: 0,
                    sync_risk: 0,
                    ambiguity: 0,
                    novelty: 0,
                    support_gain: 0,
                    predictive_match_gain: 0,
                    temporal_continuation_gain: 0,
                    schema_reuse_gain: 0,
                    adaptive_prior_gain: 0,
                    adaptive_failure_penalty: 0,
                    adaptive_promotion_gain: 0,
                    adaptive_suppression_penalty: 0,
                },
                crate::PrecisionBand::Exact,
            ),
        };
    }
    let mut states = candidates
        .iter()
        .map(|plan| resonance_state_for_plan(plan, selection))
        .collect::<Vec<_>>();
    for _round in 0..3 {
        for state in &mut states {
            let excitation = state
                .agreement_gain
                .saturating_add(state.episodic_gain)
                .saturating_add(state.schema_gain)
                .saturating_add(state.transform_fit / 2) as i32;
            let burden = state
                .contradiction_burden
                .saturating_add(state.decode_cost)
                .saturating_add(state.sync_risk / 2)
                .saturating_add(state.novelty_penalty) as i32;
            state.resonance_total += excitation - burden;
        }
        let Some((winner_idx, winner_state)) = states
            .iter()
            .enumerate()
            .max_by(|(left_idx, left), (right_idx, right)| {
                left.resonance_total
                    .cmp(&right.resonance_total)
                    .then_with(|| {
                        candidates[*left_idx]
                            .score()
                            .total
                            .cmp(&candidates[*right_idx].score().total)
                    })
            })
            .map(|(idx, state)| (idx, *state))
        else {
            break;
        };
        for (idx, state) in states.iter_mut().enumerate() {
            if idx != winner_idx {
                state.resonance_total -= inhibition_penalty(winner_state, *state);
            }
        }
    }
    candidates
        .into_iter()
        .zip(states)
        .max_by(|(left_plan, left_state), (right_plan, right_state)| {
            left_state
                .resonance_total
                .cmp(&right_state.resonance_total)
                .then_with(|| left_plan.score().total.cmp(&right_plan.score().total))
                .then_with(|| {
                    (right_plan.route_family().route_family() as u8)
                        .cmp(&(left_plan.route_family().route_family() as u8))
                })
        })
        .map(|(plan, _)| plan)
        .unwrap()
}

fn score_direct_state_route(
    current: &ExactStateMaterial,
    selection: &RouteSelectionContext,
    route_statistics: &[RouteStatistics],
) -> RouteScore {
    let feedback = summarize_route_feedback(
        ControllerRouteFamily::DirectState,
        selection.source_kind,
        selection.context_hash,
        selection.tick,
        route_statistics.iter(),
    );
    let signal = selection.governor_signal(
        ControllerRouteFamily::DirectState,
        SlotShapeBucket::None,
        current.exact_bytes.len().min(u32::MAX as usize) as u32,
        0,
    );
    score_route(
        RouteScoreInputs {
            wire_cost: current.exact_bytes.len() as u32,
            decode_cost: 1,
            residual_cost: 0,
            sync_risk: 0,
            ambiguity: 0,
            novelty: (current.exact_bytes.len() / 4) as u32,
            support_gain: 0,
            predictive_match_gain: 0,
            temporal_continuation_gain: 0,
            schema_reuse_gain: 0,
            adaptive_prior_gain: feedback
                .planner_prior_gain()
                .saturating_add(signal.prior_gain),
            adaptive_failure_penalty: feedback
                .planner_failure_penalty()
                .saturating_add(signal.failure_penalty),
            adaptive_promotion_gain: feedback.success_promotion_gain(),
            adaptive_suppression_penalty: feedback
                .suppression_penalty()
                .saturating_add(signal.suppression_penalty),
        },
        crate::PrecisionBand::Exact,
    )
}

fn exact_atom_ref_to_substrate_object(
    reference: &StateRef,
    peer_catalog: &SharedBlockCatalog,
) -> Option<SubstrateObject> {
    match reference.kind {
        RefKind::Block if reference.start == 0 && reference.indices.is_empty() => {
            if reference.len == 0 {
                let entry = peer_catalog.block(BlockId(reference.base_id))?;
                let output_len = entry.material.len() as u32;
                SubstrateObject::try_new(
                    crate::SubstrateRef::new(
                        ObjectKind::ExactBlock,
                        reference.base_id,
                        0,
                        output_len,
                        crate::ObjectVersion::default(),
                        Vec::new(),
                    ),
                    output_len,
                )
            } else {
                SubstrateObject::try_new(
                    crate::SubstrateRef::new(
                        ObjectKind::ExactRange,
                        reference.base_id,
                        0,
                        reference.len,
                        crate::ObjectVersion::default(),
                        Vec::new(),
                    ),
                    reference.len,
                )
            }
        }
        RefKind::Range => SubstrateObject::try_new(
            crate::SubstrateRef::new(
                ObjectKind::ExactRange,
                reference.base_id,
                reference.start,
                reference.len,
                crate::ObjectVersion::default(),
                Vec::new(),
            ),
            reference.len,
        ),
        RefKind::Bundle if reference.start == 0 && reference.indices.is_empty() => {
            let output_len = if reference.len != 0 {
                reference.len
            } else {
                peer_catalog
                    .materialize_bundle(BundleId(reference.base_id))
                    .ok()?
                    .len() as u32
            };
            SubstrateObject::try_new(
                crate::SubstrateRef::new(
                    ObjectKind::ExactBundle,
                    reference.base_id,
                    0,
                    output_len,
                    crate::ObjectVersion::default(),
                    Vec::new(),
                ),
                output_len,
            )
        }
        _ => None,
    }
}

fn exact_atom_output_len(substrates: &[SubstrateObject]) -> Option<u32> {
    let mut total = 0_u32;
    for substrate in substrates {
        total = total.checked_add(substrate.output_len)?;
    }
    Some(total)
}

fn exact_atom_plan_from_objects(
    basis: StateBasis,
    substrate_objects: Vec<SubstrateObject>,
) -> Option<ExactAtomPlan> {
    if substrate_objects.is_empty() {
        return None;
    }
    let output_len = exact_atom_output_len(&substrate_objects)?;
    let substrate_graph = SubstrateRouteGraph::from_objects(substrate_objects);
    Some(ExactAtomPlan {
        basis,
        output_len,
        capability_mask: substrate_graph.capability_mask,
        substrate_graph,
    })
}

fn exact_atom_plan(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> Option<ExactAtomPlan> {
    // S2.2: Helper to build a proper substrate ref with derived dependencies.
    // Instead of passing Vec::new() dependencies, we derive the dependency
    // from the substrate object being referenced (block/bundle/range).
    let make_substrate_ref = |object_kind: ObjectKind, object_id: u64, start: u32, len: u32| {
        let dependency = ObjectDependency {
            object_kind,
            object_id: match object_kind {
                ObjectKind::ExactBlock => format!("block:{}", object_id),
                ObjectKind::ExactBundle => format!("bundle:{}", object_id),
                ObjectKind::ExactRange => format!("range:{}", object_id),
                _ => object_id.to_string(),
            },
            required_revision: 0,
        };
        crate::SubstrateRef::new(
            object_kind,
            object_id,
            start,
            len,
            crate::ObjectVersion::default(),
            vec![dependency],
        )
    };

    for block in peer_catalog.blocks_iter() {
        if block.material == current.exact_bytes {
            let object = SubstrateObject::try_new(
                make_substrate_ref(ObjectKind::ExactBlock, block.block_id.0, 0, block.material.len() as u32),
                block.material.len() as u32,
            )?;
            return exact_atom_plan_from_objects(StateBasis::Block(block.block_id), vec![object]);
        }
    }
    for bundle in peer_catalog.bundles_iter() {
        if let Ok(material) = peer_catalog.materialize_bundle(bundle.bundle_id) {
            if material == current.exact_bytes {
                let object = SubstrateObject::try_new(
                    make_substrate_ref(ObjectKind::ExactBundle, bundle.bundle_id.0, 0, material.len() as u32),
                    material.len() as u32,
                )?;
                return exact_atom_plan_from_objects(
                    StateBasis::Bundle(bundle.bundle_id),
                    vec![object],
                );
            }
        }
    }
    for block in peer_catalog.blocks_iter() {
        if let Some(window) = find_best_window_in_base(&current.exact_bytes, &block.material) {
            if window.payload_start == 0 && window.matched_len == current.exact_bytes.len() {
                let object = SubstrateObject::try_new(
                    make_substrate_ref(ObjectKind::ExactRange, block.block_id.0, window.base_start, window.matched_len as u32),
                    window.matched_len as u32,
                )?;
                return exact_atom_plan_from_objects(
                    StateBasis::Range {
                        block_id: block.block_id,
                        start: window.base_start,
                        len: window.matched_len as u32,
                    },
                    vec![object],
                );
            }
        }
    }
    let refs = best_prefix_refs(
        &current.exact_bytes,
        peer_catalog,
        MAX_COMPOSITE_STATE_REFS,
        0,
    )?;
    if refs.covered_len != current.exact_bytes.len() || refs.refs.len() < 2 {
        return None;
    }
    let substrate_objects = refs
        .refs
        .iter()
        .map(|reference| exact_atom_ref_to_substrate_object(reference, peer_catalog))
        .collect::<Option<Vec<_>>>()?;
    exact_atom_plan_from_objects(
        StateBasis::Composite {
            refs: refs.ref_count(),
            residual_bytes: 0,
        },
        substrate_objects,
    )
}

fn build_hybrid_route(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
) -> Option<HybridRoute> {
    let best = best_window_match(&current.exact_bytes, peer_catalog)?;
    if best.matched_len < MIN_RANGE_REUSE_BYTES {
        return None;
    }
    let object_kind = match best.state_ref.kind {
        RefKind::Bundle => ObjectKind::ExactBundle,
        RefKind::Range => ObjectKind::ExactRange,
        _ => ObjectKind::ExactBlock,
    };
    // S2.3: Derive dependency closure from the actual substrate reference
    // instead of using an empty Vec.
    let dependency = ObjectDependency {
        object_kind,
        object_id: match object_kind {
            ObjectKind::ExactBundle => format!("bundle:{}", best.state_ref.base_id),
            ObjectKind::ExactRange => format!("range:{}", best.state_ref.base_id),
            _ => format!("block:{}", best.state_ref.base_id),
        },
        required_revision: 0,
    };
    let substrate = crate::SubstrateRef::new(
        object_kind,
        best.state_ref.base_id,
        best.state_ref.start,
        best.matched_len as u32,
        crate::ObjectVersion::default(),
        vec![dependency.clone()],
    );
    let components = if best.matched_len == current.exact_bytes.len() {
        vec![HybridRouteComponent::Substrate(substrate)]
    } else {
        vec![HybridRouteComponent::Literal(current.exact_bytes.clone())]
    };
    Some(HybridRoute {
        route_family: ControllerRouteFamily::Hybrid,
        precision_band: crate::PrecisionBand::Balanced,
        assembly_mode: None,
        output_len: current.exact_bytes.len() as u32,
        dependency_closure: vec![dependency],
        components,
    })
}

pub fn route_dependency_gate(
    dependency_closure: &[ObjectDependency],
    sync_risk: u32,
) -> DependencyClosureGate {
    DependencyClosureGate {
        dependencies_present: dependency_closure
            .iter()
            .all(|dependency| !dependency.object_id.is_empty()),
        version_compatible: true,
        sync_risk,
    }
}

// Route choice is adaptive rather than telemetry-only.
// `route_statistics` are folded into route-family/source/context feedback summaries,
// then converted into explicit planner score components and suppression gates.
pub fn choose_route_by_family(
    current: &ExactStateMaterial,
    peer_catalog: &SharedBlockCatalog,
    assemblies: &[AssemblyExtractionCandidate],
    transforms: &[TransformCandidate],
    episodes: &[EpisodeCompletionCandidate],
    schemas: &[SchemaRouteCandidate],
    route_statistics: &[RouteStatistics],
    selection: RouteSelectionContext,
    policy: RouteAdmissibilityPolicy,
) -> ControllerRoutePlan {
    let mut candidates = Vec::new();
    let schema_guidance = schema_guidance(schemas);
    candidates.push(ControllerRoutePlan::DirectState {
        score: score_direct_state_route(current, &selection, route_statistics),
    });
    if let Some(plan) = exact_atom_plan(current, peer_catalog) {
        let signal = selection.governor_signal(
            ControllerRouteFamily::ExactAtom,
            SlotShapeBucket::None,
            0,
            4,
        );
        candidates.push(ControllerRoutePlan::ExactAtom {
            score: {
                let feedback = summarize_route_feedback(
                    ControllerRouteFamily::ExactAtom,
                    selection.source_kind,
                    selection.context_hash,
                    selection.tick,
                    route_statistics.iter(),
                );
                score_route(
                    RouteScoreInputs {
                        wire_cost: 4,
                        decode_cost: 2,
                        residual_cost: 0,
                        sync_risk: 4,
                        ambiguity: 0,
                        novelty: 0,
                        support_gain: 24,
                        predictive_match_gain: 48,
                        temporal_continuation_gain: 0,
                        schema_reuse_gain: 0,
                        adaptive_prior_gain: feedback
                            .planner_prior_gain()
                            .saturating_add(signal.prior_gain),
                        adaptive_failure_penalty: feedback
                            .planner_failure_penalty()
                            .saturating_add(signal.failure_penalty),
                        adaptive_promotion_gain: feedback.success_promotion_gain(),
                        adaptive_suppression_penalty: feedback
                            .suppression_penalty()
                            .saturating_add(signal.suppression_penalty),
                    },
                    crate::PrecisionBand::Exact,
                )
            },
            plan,
        });
    }
    for candidate in assemblies.iter().take(4) {
        let feedback = summarize_route_feedback(
            ControllerRouteFamily::DirectState,
            selection.source_kind,
            selection.context_hash,
            selection.tick,
            route_statistics.iter(),
        );
        let residual_cost = candidate.residual_bytes();
        let decode_cost = candidate
            .estimated_decode_cost()
            .saturating_add(candidate.support_vote_count().saturating_sub(1));
        let ambiguity = candidate.ambiguity_score();
        let agreement_score = candidate.agreement_score();
        let support_gain = candidate.support_gain().saturating_add(agreement_score / 2);
        let schema_gain = schema_guidance.gain_for_assembly(candidate, &selection);
        let dependency_closure = candidate.dependency_closure.dependencies.clone();
        let sync_risk = candidate.dependency_sync_risk();
        let signal = selection.governor_signal(
            ControllerRouteFamily::DirectState,
            SlotShapeBucket::from_slot_count(candidate.slots.len()),
            residual_cost,
            sync_risk,
        );
        let score = score_route(
            RouteScoreInputs {
                wire_cost: candidate
                    .estimated_route_wire_cost()
                    .saturating_add(residual_cost / 2),
                decode_cost,
                residual_cost: residual_cost.saturating_add(signal.residual_penalty),
                sync_risk: sync_risk.saturating_add(signal.sync_penalty),
                ambiguity,
                novelty: 0,
                support_gain,
                predictive_match_gain: selection
                    .cue
                    .overlap_score(candidate.cue)
                    .saturating_add(agreement_score),
                temporal_continuation_gain: 0,
                schema_reuse_gain: schema_gain,
                adaptive_prior_gain: feedback
                    .planner_prior_gain()
                    .saturating_add(signal.prior_gain),
                adaptive_failure_penalty: feedback
                    .planner_failure_penalty()
                    .saturating_add(signal.failure_penalty),
                adaptive_promotion_gain: feedback.success_promotion_gain(),
                adaptive_suppression_penalty: feedback
                    .suppression_penalty()
                    .saturating_add(signal.suppression_penalty),
            },
            crate::PrecisionBand::Balanced,
        );
        let gate = route_dependency_gate(&dependency_closure, sync_risk);
        if gate.admissible(policy.max_sync_risk)
            && ambiguity <= policy.max_ambiguity
            && candidate.support_count() > 0
            && score.breakdown.adaptive_suppression_penalty < 72
        {
            candidates.push(ControllerRoutePlan::Assembly {
                score,
                candidate: candidate.clone(),
                dependency_closure,
            });
        }
    }
    for candidate in transforms.iter() {
        let feedback = summarize_route_feedback(
            ControllerRouteFamily::DirectState,
            selection.source_kind,
            selection.context_hash,
            selection.tick,
            route_statistics.iter(),
        );
        let schema_gain = schema_guidance.gain_for_transform(&candidate.class, &selection);
        let mismatch_penalty = schema_guidance.mismatch_penalty_for_transform(&candidate.class);
        let signal = selection.governor_signal(
            ControllerRouteFamily::DirectState,
            SlotShapeBucket::None,
            candidate.class.mean_residual_bytes,
            8,
        );
        let score = score_route(
            RouteScoreInputs {
                wire_cost: candidate.estimated_wire_bytes,
                decode_cost: candidate.class.mean_decode_steps,
                residual_cost: candidate
                    .class
                    .mean_residual_bytes
                    .saturating_add(signal.residual_penalty),
                sync_risk: 8_u32.saturating_add(signal.sync_penalty),
                ambiguity: 0,
                novelty: mismatch_penalty,
                support_gain: candidate.class.lifecycle.support_count,
                predictive_match_gain: selection.cue.overlap_score(candidate.class.cue),
                temporal_continuation_gain: 0,
                schema_reuse_gain: schema_gain,
                adaptive_prior_gain: feedback
                    .planner_prior_gain()
                    .saturating_add(candidate.class.route_prior.planner_gain)
                    .saturating_add(signal.prior_gain),
                adaptive_failure_penalty: feedback
                    .planner_failure_penalty()
                    .saturating_add(
                        candidate
                            .class
                            .route_prior
                            .route_loss_count
                            .saturating_mul(2),
                    )
                    .saturating_add(signal.failure_penalty),
                adaptive_promotion_gain: feedback.success_promotion_gain(),
                adaptive_suppression_penalty: feedback
                    .suppression_penalty()
                    .saturating_add(signal.suppression_penalty),
            },
            crate::PrecisionBand::Tight,
        );
        if score.breakdown.adaptive_suppression_penalty < 72 {
            candidates.push(ControllerRoutePlan::Transform {
                score,
                class: candidate.class.clone(),
                instance: candidate.instance.clone(),
            });
        }
    }
    for candidate in episodes.iter().take(4) {
        let feedback = summarize_route_feedback(
            ControllerRouteFamily::DirectState,
            selection.source_kind,
            selection.context_hash,
            selection.tick,
            route_statistics.iter(),
        );
        let sync_risk = 18;
        let signal = selection.governor_signal(
            ControllerRouteFamily::DirectState,
            SlotShapeBucket::None,
            0,
            sync_risk,
        );
        let score = score_route(
            RouteScoreInputs {
                wire_cost: 6_u32.saturating_add(candidate.lag_bucket.0 as u32),
                decode_cost: 6,
                residual_cost: 0,
                sync_risk: sync_risk.saturating_add(signal.sync_penalty),
                ambiguity: candidate.branch_rank.0 as u32
                    + if candidate.cue_overlap == 0 { 8 } else { 0 },
                novelty: 0,
                support_gain: candidate
                    .transition_count
                    .0
                    .saturating_add(candidate.route_support / 4),
                predictive_match_gain: candidate.cue_overlap,
                temporal_continuation_gain: candidate
                    .transition_count
                    .0
                    .saturating_add(candidate.transition_match)
                    .saturating_add(candidate.recency_score / 2),
                schema_reuse_gain: 0,
                adaptive_prior_gain: feedback
                    .planner_prior_gain()
                    .saturating_add(signal.prior_gain),
                adaptive_failure_penalty: feedback
                    .planner_failure_penalty()
                    .saturating_add(signal.failure_penalty),
                adaptive_promotion_gain: feedback.success_promotion_gain(),
                adaptive_suppression_penalty: feedback
                    .suppression_penalty()
                    .saturating_add(signal.suppression_penalty),
            },
            candidate.precision_band,
        );
        let gate = route_dependency_gate(&[], sync_risk);
        if candidate.admissible
            && gate.admissible(policy.max_sync_risk)
            && score.breakdown.adaptive_suppression_penalty < 72
        {
            candidates.push(ControllerRoutePlan::EpisodeCompletion {
                score,
                candidate: candidate.clone(),
            });
        }
    }
    for candidate in schemas.iter().take(4) {
        let feedback = summarize_route_feedback(
            ControllerRouteFamily::DirectState,
            selection.source_kind,
            selection.context_hash,
            selection.tick,
            route_statistics.iter(),
        );
        let sync_risk = candidate.dependency_closure.dependencies.len() as u32 * 4;
        let signal = selection.governor_signal(
            ControllerRouteFamily::DirectState,
            SlotShapeBucket::None,
            candidate.decode_burden,
            sync_risk,
        );
        let score = score_route(
            RouteScoreInputs {
                wire_cost: candidate.output_len / 8,
                decode_cost: candidate.decode_burden.max(candidate.depth_cap as u32),
                residual_cost: 0,
                sync_risk: sync_risk.saturating_add(signal.sync_penalty),
                ambiguity: candidate.fanout_cap as u32 + candidate.contradiction_burden / 2,
                novelty: candidate
                    .decode_burden
                    .saturating_div(4)
                    .saturating_add(signal.residual_penalty),
                support_gain: candidate
                    .entry_condition
                    .min_support_count
                    .saturating_add(candidate.cross_episode_support)
                    .saturating_add(candidate.branch_consistency / 2),
                predictive_match_gain: candidate.cue_overlap,
                temporal_continuation_gain: 0,
                schema_reuse_gain: candidate
                    .cue_overlap
                    .saturating_add(candidate.branch_consistency)
                    .saturating_sub(
                        candidate
                            .contradiction_burden
                            .min(candidate.cue_overlap / 2),
                    ),
                adaptive_prior_gain: feedback
                    .planner_prior_gain()
                    .saturating_add(signal.prior_gain),
                adaptive_failure_penalty: feedback
                    .planner_failure_penalty()
                    .saturating_add(signal.failure_penalty),
                adaptive_promotion_gain: feedback.success_promotion_gain(),
                adaptive_suppression_penalty: feedback
                    .suppression_penalty()
                    .saturating_add(signal.suppression_penalty),
            },
            candidate.precision_band,
        );
        let gate = route_dependency_gate(&candidate.dependency_closure.dependencies, sync_risk);
        if gate.admissible(policy.max_sync_risk)
            && score.breakdown.adaptive_suppression_penalty < 72
        {
            candidates.push(ControllerRoutePlan::SchemaExpansion {
                score,
                candidate: candidate.clone(),
            });
        }
    }
    if let Some(route) = build_hybrid_route(current, peer_catalog) {
        let hybrid_suppressed = {
            let feedback = summarize_route_feedback(
                ControllerRouteFamily::Hybrid,
                selection.source_kind,
                selection.context_hash,
                selection.tick,
                route_statistics.iter(),
            );
            feedback.suppressed()
        };
        if !hybrid_suppressed {
            let signal = selection.governor_signal(
                ControllerRouteFamily::Hybrid,
                SlotShapeBucket::None,
                8,
                10,
            );
            candidates.push(ControllerRoutePlan::Hybrid {
                score: {
                    let feedback = summarize_route_feedback(
                        ControllerRouteFamily::Hybrid,
                        selection.source_kind,
                        selection.context_hash,
                        selection.tick,
                        route_statistics.iter(),
                    );
                    score_route(
                        RouteScoreInputs {
                            wire_cost: 16,
                            decode_cost: 6,
                            residual_cost: 8_u32.saturating_add(signal.residual_penalty),
                            sync_risk: 10_u32.saturating_add(signal.sync_penalty),
                            ambiguity: 0,
                            novelty: 0,
                            support_gain: 16,
                            predictive_match_gain: selection.cue.overlap_score(selection.cue),
                            temporal_continuation_gain: 0,
                            schema_reuse_gain: 0,
                            adaptive_prior_gain: feedback
                                .planner_prior_gain()
                                .saturating_add(signal.prior_gain),
                            adaptive_failure_penalty: feedback
                                .planner_failure_penalty()
                                .saturating_add(signal.failure_penalty),
                            adaptive_promotion_gain: feedback.success_promotion_gain(),
                            adaptive_suppression_penalty: feedback
                                .suppression_penalty()
                                .saturating_add(signal.suppression_penalty),
                        },
                        route.precision_band,
                    )
                },
                route,
            });
        }
    }
    choose_by_resonance(candidates, &selection)
}
