use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    Assembly, AssemblyBody, AssemblyBodyNode, AssemblyComponentRef, AssemblyId, AssemblyRouteMode,
    AtomSubstrateObject, BlockId, BundleId, ContextHash, ControllerRouteFamily, DictionaryId,
    EpisodeCompletionCandidate, EpisodeObjectRef, ExactStateMaterial, ItemId, LagBucket,
    ObjectDependency, ObjectKind, ObjectVersion, PrecisionBand, RecordType, RouteFamily,
    SchemaDependencyClosure, SchemaGraph, SchemaId, SchemaNode, SchemaSlotDescriptor,
    SharedDictionary, SourceKind, SubstrateCapability, SubstrateCapabilityMask, TransformBasisRef,
    TransformClass, TransformDependencyClosure, TransformId, TransformInstance, TransformKind,
    TransformOutputContract, TransformTransducerFamily, assembly_reuse_signature_from_parts,
    slot_mask_allows,
};

pub const STATE_PROGRAM_VERSION: u8 = 1;
pub const MAX_STATE_REFS: usize = 1024;
pub const MAX_STATE_OPS: usize = 1024;
pub const MAX_STATE_INDICES_PER_REF: usize = 32 * 1024;

const STATE_PROGRAM_HEADER_LEN: usize = 1 + 1 + 2 + 2 + 4;
const STATE_PROGRAM_REF_FIXED_LEN: usize = 1 + 8 + 4 + 4 + 2 + 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubstrateRef {
    pub object_kind: ObjectKind,
    pub object_id: u64,
    #[serde(default)]
    pub start: u32,
    #[serde(default)]
    pub len: u32,
    pub version: ObjectVersion,
    pub dependencies: Vec<ObjectDependency>,
}

impl SubstrateRef {
    // S2.2.d: Callers should use the dependency-deriving constructor instead of
    // providing ad hoc dependency lists. Empty dependency vectors for substrate-
    // referencing objects are a protocol contract violation.
    pub fn new(
        object_kind: ObjectKind,
        object_id: u64,
        start: u32,
        len: u32,
        version: ObjectVersion,
        dependencies: Vec<ObjectDependency>,
    ) -> Self {
        Self {
            object_kind,
            object_id,
            start,
            len,
            version,
            dependencies,
        }
    }

    // S2.2.d: Callers should use the dependency-deriving constructor instead of
    // providing ad hoc dependency lists. Empty dependency vectors for substrate-
    // referencing objects are a protocol contract violation.
    pub fn opaque(object_kind: ObjectKind, object_id: u64) -> Self {
        Self::new(
            object_kind,
            object_id,
            0,
            0,
            ObjectVersion::default(),
            Vec::new(), // S2.2.d: empty dependency vector — opaque refs have no known dependencies
        )
    }

    pub fn from_substrate(substrate: &AtomSubstrateObject, object_id: u64) -> Self {
        match substrate {
            AtomSubstrateObject::AtomFragment(fragment) => Self::new(
                ObjectKind::AtomFragment,
                object_id,
                fragment.byte_offset,
                fragment.byte_len,
                fragment.version_meta.version,
                fragment.version_meta.dependencies.clone(),
            ),
            AtomSubstrateObject::ExactBlock(block) => Self::new(
                ObjectKind::ExactBlock,
                object_id,
                0,
                block.byte_len,
                block.version_meta.version,
                block.version_meta.dependencies.clone(),
            ),
            AtomSubstrateObject::ExactBundle(bundle) => Self::new(
                ObjectKind::ExactBundle,
                object_id,
                0,
                bundle.byte_len,
                bundle.version_meta.version,
                bundle.version_meta.dependencies.clone(),
            ),
            AtomSubstrateObject::ExactRange(range) => Self::new(
                ObjectKind::ExactRange,
                object_id,
                range.start,
                range.byte_len,
                range.version_meta.version,
                range.version_meta.dependencies.clone(),
            ),
            AtomSubstrateObject::ResidualBuffer(buffer) => Self::new(
                ObjectKind::ResidualBuffer,
                object_id,
                0,
                buffer.byte_len,
                buffer.version_meta.version,
                buffer.version_meta.dependencies.clone(),
            ),
        }
    }

    // S2.2.d: Callers should use the dependency-deriving constructor instead of
    // providing ad hoc dependency lists. Empty dependency vectors for substrate-
    // referencing objects are a protocol contract violation.
    pub fn from_dependency(dependency: &ObjectDependency) -> Self {
        Self::new(
            dependency.object_kind,
            dependency.object_id.parse().unwrap_or_default(),
            0,
            0,
            ObjectVersion {
                schema_version: 0,
                object_revision: dependency.required_revision,
            },
            Vec::new(), // S2.2.d: empty dependency vector — dependency-derived refs carry no transitive closure
        )
    }

    pub const fn capability(&self) -> Option<SubstrateCapability> {
        self.object_kind.substrate_capability()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSubstrateBinding {
    pub substrate: AtomSubstrateObject,
    pub reference: SubstrateRef,
}

impl StateSubstrateBinding {
    pub fn from_substrate(substrate: AtomSubstrateObject, object_id: u64) -> Self {
        let reference = SubstrateRef::from_substrate(&substrate, object_id);
        Self {
            reference,
            substrate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubstrateObject {
    pub capability: SubstrateCapability,
    pub reference: SubstrateRef,
    pub output_len: u32,
    pub version: ObjectVersion,
    pub dependency_closure: Vec<ObjectDependency>,
}

impl SubstrateObject {
    pub fn try_new(reference: SubstrateRef, output_len: u32) -> Option<Self> {
        let capability = reference.capability()?;
        let version = reference.version;
        let dependency_closure = reference.dependencies.clone();
        Some(Self {
            capability,
            reference,
            output_len,
            version,
            dependency_closure,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum SubstrateRouteNodeKind {
    #[default]
    Material = 1,
    Concat = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SubstrateRouteNode {
    pub node_id: u32,
    pub kind: SubstrateRouteNodeKind,
    pub substrate: Option<SubstrateObject>,
    pub child_ids: Vec<u32>,
    pub output_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SubstrateRouteGraph {
    pub version: u8,
    pub capability_mask: SubstrateCapabilityMask,
    pub output_len: u32,
    pub root_node_id: u32,
    pub dependency_closure: Vec<ObjectDependency>,
    pub nodes: Vec<SubstrateRouteNode>,
}

impl SubstrateRouteGraph {
    pub const VERSION: u8 = 1;

    pub fn from_objects(objects: Vec<SubstrateObject>) -> Self {
        let mut capability_mask = SubstrateCapabilityMask::empty();
        let mut dependency_closure = Vec::new();
        let output_len = objects.iter().fold(0_u32, |total, object| {
            total.saturating_add(object.output_len)
        });
        for object in &objects {
            capability_mask.insert(object.capability);
            dependency_closure.extend(object.dependency_closure.clone());
        }
        if objects.len() > 1 {
            capability_mask.insert(SubstrateCapability::CompositeGraph);
        }
        dependency_closure.sort_by(|left, right| {
            (left.object_kind as u8)
                .cmp(&(right.object_kind as u8))
                .then_with(|| left.object_id.cmp(&right.object_id))
                .then_with(|| left.required_revision.cmp(&right.required_revision))
        });
        dependency_closure.dedup();

        if objects.is_empty() {
            return Self {
                version: Self::VERSION,
                capability_mask,
                output_len: 0,
                root_node_id: 0,
                dependency_closure,
                nodes: Vec::new(),
            };
        }

        if objects.len() == 1 {
            let object = objects.into_iter().next().unwrap();
            return Self {
                version: Self::VERSION,
                capability_mask,
                output_len,
                root_node_id: 1,
                dependency_closure,
                nodes: vec![SubstrateRouteNode {
                    node_id: 1,
                    kind: SubstrateRouteNodeKind::Material,
                    substrate: Some(object),
                    child_ids: Vec::new(),
                    output_len,
                }],
            };
        }

        let mut nodes = Vec::with_capacity(objects.len() + 1);
        nodes.push(SubstrateRouteNode {
            node_id: 0,
            kind: SubstrateRouteNodeKind::Concat,
            substrate: None,
            child_ids: (1..=objects.len() as u32).collect(),
            output_len,
        });
        for (index, object) in objects.into_iter().enumerate() {
            nodes.push(SubstrateRouteNode {
                node_id: index as u32 + 1,
                kind: SubstrateRouteNodeKind::Material,
                output_len: object.output_len,
                substrate: Some(object),
                child_ids: Vec::new(),
            });
        }
        Self {
            version: Self::VERSION,
            capability_mask,
            output_len,
            root_node_id: 0,
            dependency_closure,
            nodes,
        }
    }

    pub fn validate(&self) -> Result<(), StateProgramError> {
        if self.version != Self::VERSION {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                format!("invalid substrate graph version {}", self.version),
            ));
        }
        if self.nodes.is_empty() {
            return Err(StateProgramError::PrgMissingRoot(self.root_node_id));
        }
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.root_node_id)
        {
            return Err(StateProgramError::PrgMissingRoot(self.root_node_id));
        }
        for node in &self.nodes {
            match node.kind {
                SubstrateRouteNodeKind::Material => {
                    let object = node.substrate.as_ref().ok_or_else(|| {
                        StateProgramError::InvalidPredictiveDispatchContract(format!(
                            "substrate material node {} is missing its substrate object",
                            node.node_id
                        ))
                    })?;
                    if !self.capability_mask.contains(object.capability)
                        && !self
                            .capability_mask
                            .contains(SubstrateCapability::CompositeGraph)
                    {
                        return Err(StateProgramError::InvalidPredictiveDispatchContract(
                            format!(
                                "substrate material node {} advertises unsupported capability {:?}",
                                node.node_id, object.capability
                            ),
                        ));
                    }
                }
                SubstrateRouteNodeKind::Concat => {
                    if node.child_ids.is_empty() {
                        return Err(StateProgramError::InvalidPredictiveDispatchContract(
                            format!(
                                "substrate concat node {} is missing child nodes",
                                node.node_id
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn execute_substrate_route_graph<FSub>(
    graph: &SubstrateRouteGraph,
    mut resolve_substrate: FSub,
) -> Result<Vec<u8>, StateProgramError>
where
    FSub: FnMut(&SubstrateRef) -> Option<Vec<u8>>,
{
    graph.validate()?;
    let mut active = BTreeSet::new();
    let out = execute_substrate_route_node(
        graph.root_node_id,
        graph,
        &mut active,
        &mut resolve_substrate,
    )?;
    if out.len() != graph.output_len as usize {
        return Err(StateProgramError::PrgOutputLenMismatch {
            expected: graph.output_len,
            actual: out.len() as u32,
        });
    }
    Ok(out)
}

fn execute_substrate_route_node<FSub>(
    node_id: u32,
    graph: &SubstrateRouteGraph,
    active: &mut BTreeSet<u32>,
    resolve_substrate: &mut FSub,
) -> Result<Vec<u8>, StateProgramError>
where
    FSub: FnMut(&SubstrateRef) -> Option<Vec<u8>>,
{
    if !active.insert(node_id) {
        return Err(StateProgramError::PrgCycleDetected(node_id));
    }
    let node = graph
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .ok_or(StateProgramError::PrgMissingNode(node_id))?;
    let out = match node.kind {
        SubstrateRouteNodeKind::Material => {
            let object = node.substrate.as_ref().ok_or_else(|| {
                StateProgramError::InvalidPredictiveDispatchContract(format!(
                    "substrate material node {} is missing its substrate object",
                    node.node_id
                ))
            })?;
            resolve_substrate(&object.reference).ok_or_else(|| {
                StateProgramError::PrgMissingDependency(object.reference.object_id.to_string())
            })?
        }
        SubstrateRouteNodeKind::Concat => {
            let mut out = Vec::new();
            for child_id in &node.child_ids {
                out.extend_from_slice(&execute_substrate_route_node(
                    *child_id,
                    graph,
                    active,
                    resolve_substrate,
                )?);
            }
            out
        }
    };
    active.remove(&node_id);
    if out.len() != node.output_len as usize {
        return Err(StateProgramError::PrgNodeOutputLenMismatch {
            node_id: node.node_id,
            expected: node.output_len,
            actual: out.len() as u32,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssemblyRef {
    pub assembly_id: AssemblyId,
    pub version: ObjectVersion,
    pub dependency_closure: Vec<ObjectDependency>,
    pub output_len: u32,
    #[serde(default)]
    pub body_shape_hash: u64,
    #[serde(default)]
    pub dependency_fingerprint: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconstructionRef {
    Substrate(SubstrateRef),
    Assembly(AssemblyRef),
    Transform(TransformId, u32),
}

impl ReconstructionRef {
    pub const fn object_kind(&self) -> ObjectKind {
        match self {
            Self::Substrate(reference) => reference.object_kind,
            Self::Assembly(_) => ObjectKind::Assembly,
            Self::Transform(_, _) => ObjectKind::Transform,
        }
    }

    pub const fn output_len(&self) -> u32 {
        match self {
            Self::Substrate(_) => 0,
            Self::Assembly(reference) => reference.output_len,
            Self::Transform(_, output_len) => *output_len,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconstructionGraphNode {
    pub node_id: u32,
    pub reference: ReconstructionRef,
    pub dependencies: Vec<ReconstructionRef>,
    pub output_len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum PrgNodeKind {
    Literal = 1,
    AtomRef = 2,
    RangeRef = 3,
    BundleRef = 4,
    AssemblyRef = 5,
    TransformRef = 6,
    EpisodeRef = 7,
    SchemaRef = 8,
    Concat = 9,
    Patch = 10,
    Repeat = 11,
    Select = 12,
    Permute = 13,
    SlotFill = 14,
    Expand = 15,
    Guard = 16,
    Branch = 17,
    Commit = 18,
    #[default]
    Noop = 255,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PrgDependencyContract {
    pub dependency_ids: Vec<u32>,
    pub required_object_ids: Vec<String>,
    pub max_depth: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PrgNode {
    pub node_id: u32,
    pub kind: PrgNodeKind,
    pub output_len: u32,
    pub dependency_contract: PrgDependencyContract,
    pub literal_bytes: Vec<u8>,
    pub substrate_ref: Option<SubstrateRef>,
    pub assembly_ref: Option<AssemblyRef>,
    pub transform_ref: Option<TransformId>,
    pub episode_ref: Option<EpisodeObjectRef>,
    pub schema_ref: Option<SchemaId>,
    pub child_ids: Vec<u32>,
    pub select_index: Option<u16>,
    pub repeat_count: Option<u16>,
    pub permutation: Vec<u16>,
    pub slot_id: Option<u16>,
    pub basis_node_id: Option<u32>,
    pub patch_offset: Option<u32>,
    pub patch_remove_len: Option<u32>,
    pub guard_precision: Option<PrecisionBand>,
    pub branch_target: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PredictiveReconstructionGraph {
    pub version: u8,
    pub node_count: u16,
    pub depth_cap: u8,
    pub output_len: u32,
    pub root_node_id: u32,
    pub nodes: Vec<PrgNode>,
    pub dependency_ids: Vec<String>,
}

impl PredictiveReconstructionGraph {
    pub const VERSION: u8 = 1;

    pub fn new(
        root_node_id: u32,
        depth_cap: u8,
        output_len: u32,
        nodes: Vec<PrgNode>,
        dependency_ids: Vec<String>,
    ) -> Self {
        Self {
            version: Self::VERSION,
            node_count: nodes.len().min(u16::MAX as usize) as u16,
            depth_cap,
            output_len,
            root_node_id,
            nodes,
            dependency_ids,
        }
    }

    // S2.2.e: serde_json::to_vec serializes all derived fields including any
    // dependency data within PRG nodes. No dependency_closure is silently
    // dropped or truncated during encode.
    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::PrgSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let graph: Self = serde_json::from_slice(bytes)
            .map_err(|error| StateProgramError::PrgSerialization(error.to_string()))?;
        if graph.version != Self::VERSION {
            return Err(StateProgramError::InvalidPrgVersion(graph.version));
        }
        graph.validate()?;
        Ok(graph)
    }

    pub fn validate(&self) -> Result<(), StateProgramError> {
        if self.node_count as usize != self.nodes.len() {
            return Err(StateProgramError::PrgNodeCountMismatch {
                declared: self.node_count as usize,
                actual: self.nodes.len(),
            });
        }
        if self.nodes.is_empty() {
            return Err(StateProgramError::PrgMissingRoot(self.root_node_id));
        }
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.root_node_id)
        {
            return Err(StateProgramError::PrgMissingRoot(self.root_node_id));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaDefPayload {
    pub version: u8,
    pub schema: SchemaGraph,
    pub node_table: Vec<PrgNode>,
    pub edge_table: Vec<(u32, u32)>,
    pub slot_table: Vec<SchemaSlotDescriptor>,
    pub dependency_closure: SchemaDependencyClosure,
    pub decode_max_nodes: u16,
    pub decode_max_depth: u8,
}

impl SchemaDefPayload {
    pub const VERSION: u8 = 1;

    pub fn new(schema: SchemaGraph, graph: &PredictiveReconstructionGraph) -> Self {
        let edge_table = graph
            .nodes
            .iter()
            .flat_map(|node| {
                node.child_ids
                    .iter()
                    .copied()
                    .map(move |child| (node.node_id, child))
            })
            .collect();
        Self {
            version: Self::VERSION,
            node_table: graph.nodes.clone(),
            edge_table,
            slot_table: schema.slots.clone(),
            dependency_closure: schema.dependency_closure.clone(),
            decode_max_nodes: schema.decode_max_nodes,
            decode_max_depth: schema.decode_max_depth,
            schema,
        }
    }

    // S2.2.e: serde_json::to_vec preserves dependency_closure (SchemaDependencyClosure)
    // in full. No silent truncation occurs during encode.
    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::SchemaPayloadSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| StateProgramError::SchemaPayloadSerialization(error.to_string()))?;
        if payload.version != Self::VERSION {
            return Err(StateProgramError::InvalidSchemaPayloadVersion(
                payload.version,
            ));
        }
        Ok(payload)
    }

    pub fn install_plan(&self) -> SchemaInstallPlan {
        SchemaInstallPlan {
            schema_id: self.schema.schema_id,
            dependency_closure: self.dependency_closure.dependencies.clone(),
            node_count: self.node_table.len().min(u16::MAX as usize) as u16,
            depth_cap: self.decode_max_depth,
            output_len: self.schema.output_len,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaInstallPlan {
    pub schema_id: SchemaId,
    pub dependency_closure: Vec<ObjectDependency>,
    pub node_count: u16,
    pub depth_cap: u8,
    pub output_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SharedDictionaryDefPayload {
    pub version: u8,
    pub dictionary: SharedDictionary,
}

impl SharedDictionaryDefPayload {
    pub const VERSION: u8 = 1;

    pub fn new(dictionary: SharedDictionary) -> Self {
        Self {
            version: Self::VERSION,
            dictionary,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::PredictiveDispatchSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let payload: Self = serde_json::from_slice(bytes).map_err(|error| {
            StateProgramError::PredictiveDispatchSerialization(error.to_string())
        })?;
        if payload.version != Self::VERSION {
            return Err(StateProgramError::InvalidPredictiveDispatchVersion(
                payload.version,
            ));
        }
        Ok(payload)
    }
}

pub fn schema_ref_from_payload(payload: &SchemaDefPayload) -> SchemaId {
    payload.schema.schema_id
}

pub fn reconstruct_graph_from_schema(schema: &SchemaGraph) -> PredictiveReconstructionGraph {
    let nodes = schema
        .nodes
        .iter()
        .map(prg_node_from_schema_node)
        .collect::<Vec<_>>();
    let root_node_id = schema
        .nodes
        .first()
        .map(|node| node.node_id.0)
        .unwrap_or_default();
    let dependency_ids = schema
        .dependency_closure
        .dependencies
        .iter()
        .map(|dependency| dependency.object_id.clone())
        .collect();
    PredictiveReconstructionGraph::new(
        root_node_id,
        schema.decode_max_depth,
        schema.output_len,
        nodes,
        dependency_ids,
    )
}

pub fn prg_node_from_schema_node(node: &SchemaNode) -> PrgNode {
    let mut prg = PrgNode {
        node_id: node.node_id.0,
        kind: PrgNodeKind::Literal,
        output_len: node.output_len,
        dependency_contract: PrgDependencyContract::default(),
        literal_bytes: Vec::new(),
        substrate_ref: None,
        assembly_ref: None,
        transform_ref: None,
        episode_ref: None,
        schema_ref: None,
        child_ids: Vec::new(),
        select_index: None,
        repeat_count: None,
        permutation: Vec::new(),
        slot_id: node.slot_binding,
        basis_node_id: None,
        patch_offset: None,
        patch_remove_len: None,
        guard_precision: None,
        branch_target: None,
    };
    match node.kind {
        ObjectKind::Assembly => {
            prg.kind = PrgNodeKind::AssemblyRef;
            if let Some(object_ref) = &node.object_ref {
                let assembly_id = object_ref
                    .object_id
                    .rsplit(':')
                    .next()
                    .and_then(|id| id.parse().ok())
                    .unwrap_or_default();
                prg.assembly_ref = Some(AssemblyRef {
                    assembly_id: AssemblyId(assembly_id),
                    version: ObjectVersion::default(),
                    dependency_closure: Vec::new(), // S2.2.d: empty closure — schema-derived assembly refs lack transitive closure
                    output_len: node.output_len,
                    body_shape_hash: 0,
                    dependency_fingerprint: 0,
                });
            }
        }
        ObjectKind::Transform => {
            prg.kind = PrgNodeKind::TransformRef;
            if let Some(object_ref) = &node.object_ref {
                let transform_id = object_ref
                    .object_id
                    .rsplit(':')
                    .next()
                    .and_then(|id| id.parse().ok())
                    .unwrap_or_default();
                prg.transform_ref = Some(TransformId(transform_id));
            }
        }
        ObjectKind::Schema => {
            prg.kind = PrgNodeKind::SchemaRef;
            if let Some(object_ref) = &node.object_ref {
                let schema_id = object_ref
                    .object_id
                    .rsplit(':')
                    .next()
                    .and_then(|id| id.parse().ok())
                    .unwrap_or_default();
                prg.schema_ref = Some(SchemaId(schema_id));
            }
        }
        ObjectKind::EpisodeHint | ObjectKind::ReplayHint => {
            prg.kind = PrgNodeKind::EpisodeRef;
            if let Some(object_ref) = &node.object_ref {
                prg.episode_ref = Some(EpisodeObjectRef {
                    object_kind: object_ref.object_kind,
                    object_id: object_ref.object_id.clone(),
                });
            }
        }
        _ => {
            prg.kind = match node.kind {
                ObjectKind::ExactRange => PrgNodeKind::RangeRef,
                ObjectKind::ExactBundle => PrgNodeKind::BundleRef,
                _ => PrgNodeKind::AtomRef,
            };
            if let Some(object_ref) = &node.object_ref {
                prg.substrate_ref = Some(SubstrateRef::opaque(
                    object_ref.object_kind,
                    object_ref.object_id.parse().unwrap_or_default(),
                ));
            }
        }
    }
    prg
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridRouteComponent {
    Literal(Vec<u8>),
    Substrate(SubstrateRef),
    SubstrateGraph(SubstrateRouteGraph),
    Assembly(AssemblyRef),
    Transform(TransformId),
    DictionaryTokens {
        dictionary_id: DictionaryId,
        token_ids: Vec<u16>,
    },
    Schema(PredictiveReconstructionGraph),
    ResidualPatch {
        offset: u32,
        remove_len: u32,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HybridRoute {
    pub route_family: ControllerRouteFamily,
    pub precision_band: PrecisionBand,
    #[serde(default)]
    pub assembly_mode: Option<AssemblyRouteMode>,
    pub output_len: u32,
    pub dependency_closure: Vec<ObjectDependency>,
    pub components: Vec<HybridRouteComponent>,
}

impl HybridRoute {
    pub fn validate_dependencies<F>(&self, mut available: F) -> bool
    where
        F: FnMut(ObjectKind, &str, u32) -> bool,
    {
        self.dependency_closure.iter().all(|dependency| {
            available(
                dependency.object_kind,
                dependency.object_id.as_str(),
                dependency.required_revision,
            )
        })
    }

    /// S2.4: Returns true if this hybrid route has at least one component
    /// that provides actual substrate/object reuse (not just literal data).
    pub fn has_reuse_component(&self) -> bool {
        self.components.iter().any(|component| {
            matches!(
                component,
                HybridRouteComponent::Substrate(_)
                    | HybridRouteComponent::SubstrateGraph(_)
                    | HybridRouteComponent::Assembly(_)
                    | HybridRouteComponent::Transform(_)
                    | HybridRouteComponent::DictionaryTokens { .. }
                    | HybridRouteComponent::Schema(_)
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteGraphSyncRequirements {
    pub dependency_count: u16,
    pub sync_risk: u32,
    pub requires_synchronized_memory: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteGraphContradictionContract {
    pub contradiction_bytes_len: u32,
    pub exact_contradiction_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteGraphContract {
    pub route_graph_id: u64,
    pub node_count: u16,
    pub output_len: u32,
    pub sync_requirements: RouteGraphSyncRequirements,
    pub contradiction_contract: RouteGraphContradictionContract,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PredictiveRouteDispatchPayload {
    pub version: u8,
    pub route_family: ControllerRouteFamily,
    pub route_kind: RouteFamily,
    #[serde(default)]
    pub route_source_kind: Option<SourceKind>,
    #[serde(default)]
    pub assembly_mode: Option<AssemblyRouteMode>,
    pub precision_band: PrecisionBand,
    pub dependency_closure: Vec<ObjectDependency>,
    pub sync_risk: u32,
    pub literal_bytes: Vec<u8>,
    pub assembly_ref: Option<AssemblyRef>,
    #[serde(default)]
    pub inline_assembly_defs: Vec<AssemblyDefPayload>,
    #[serde(default)]
    pub inline_schema_defs: Vec<SchemaDefPayload>,
    #[serde(default)]
    pub inline_dictionaries: Vec<SharedDictionaryDefPayload>,
    #[serde(default)]
    pub inline_episode_hints: Vec<EpisodeHintPayload>,
    #[serde(default)]
    pub route_graph: RouteGraphContract,
    #[serde(default)]
    pub contradiction_bytes: Vec<u8>,
    pub prg: Option<PredictiveReconstructionGraph>,
    pub hybrid_route: Option<HybridRoute>,
}

impl PredictiveRouteDispatchPayload {
    pub const VERSION: u8 = 1;

    // S2.2.e: serde_json::to_vec preserves all dependency_closure fields in full,
    // including the top-level dependency_closure, SubstrateRouteGraph dependency_closure
    // within HybridRoute components, SubstrateObject dependency_closure, and
    // SchemaDefPayload dependency_closure. No silent truncation occurs during encode.
    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::PredictiveDispatchSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let payload: Self = serde_json::from_slice(bytes).map_err(|error| {
            StateProgramError::PredictiveDispatchSerialization(error.to_string())
        })?;
        if payload.version != Self::VERSION {
            return Err(StateProgramError::InvalidPredictiveDispatchVersion(
                payload.version,
            ));
        }
        payload.validate_route_contract()?;
        Ok(payload)
    }

    pub fn validate_route_contract(&self) -> Result<(), StateProgramError> {
        let expected = self.route_family.route_family();
        if self.route_kind != expected {
            return Err(StateProgramError::InvalidPredictiveRouteContract {
                route_family: self.route_family,
                route_kind: self.route_kind,
                expected_route_kind: expected,
            });
        }
        if self.route_family == ControllerRouteFamily::Assembly {
            let derived = self.derived_assembly_mode();
            if let (Some(mode), Some(derived_mode)) = (self.assembly_mode, derived) {
                if mode != derived_mode {
                    return Err(StateProgramError::InvalidPredictiveDispatchContract(
                        format!(
                            "assembly mode mismatch: declared {:?}, derived {:?}",
                            mode, derived_mode
                        ),
                    ));
                }
            }
            if let Some(route) = &self.hybrid_route {
                if let (Some(mode), Some(route_mode)) = (self.assembly_mode, route.assembly_mode) {
                    if mode != route_mode {
                        return Err(StateProgramError::InvalidPredictiveDispatchContract(
                            format!(
                                "hybrid assembly mode mismatch: declared {:?}, hybrid {:?}",
                                mode, route_mode
                            ),
                        ));
                    }
                }
            }
        }
        if let Some(route) = &self.hybrid_route {
            for component in &route.components {
                if let HybridRouteComponent::SubstrateGraph(graph) = component {
                    graph.validate()?;
                }
            }
        }
        let derived_node_count = self.derived_node_count();
        let derived_output_len = self.derived_output_len();
        if self.route_graph.node_count != derived_node_count {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                format!(
                    "route graph node count mismatch: declared {}, derived {}",
                    self.route_graph.node_count, derived_node_count
                ),
            ));
        }
        if self.route_graph.output_len != derived_output_len {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                format!(
                    "route graph output length mismatch: declared {}, derived {}",
                    self.route_graph.output_len, derived_output_len
                ),
            ));
        }
        if self.route_graph.sync_requirements.dependency_count
            != self.dependency_closure.len().min(u16::MAX as usize) as u16
        {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                "route graph dependency count does not match dependency closure".into(),
            ));
        }
        if self.route_graph.sync_requirements.sync_risk != self.sync_risk {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                "route graph sync risk does not match payload sync risk".into(),
            ));
        }
        if self
            .route_graph
            .contradiction_contract
            .contradiction_bytes_len
            != self.contradiction_bytes.len().min(u32::MAX as usize) as u32
        {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                "route graph contradiction length does not match contradiction payload".into(),
            ));
        }
        if self.route_graph.route_graph_id != self.derived_route_graph_id() {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                "route graph id does not match derived contract".into(),
            ));
        }
        Ok(())
    }
}

impl PredictiveRouteDispatchPayload {
    pub fn installs_assembly_defs(&self) -> bool {
        !self.inline_assembly_defs.is_empty()
    }

    pub fn reuses_synchronized_assembly(&self) -> bool {
        self.assembly_ref.is_some() && self.inline_assembly_defs.is_empty()
    }

    pub fn derived_assembly_mode(&self) -> Option<AssemblyRouteMode> {
        if self.route_family != ControllerRouteFamily::Assembly {
            return None;
        }
        if self.installs_assembly_defs() {
            return Some(AssemblyRouteMode::DefineAndActivate);
        }
        if let Some(route) = &self.hybrid_route {
            let has_assembly = route
                .components
                .iter()
                .any(|component| matches!(component, HybridRouteComponent::Assembly(_)));
            let has_non_assembly = route
                .components
                .iter()
                .any(|component| !matches!(component, HybridRouteComponent::Assembly(_)));
            if has_assembly && !has_non_assembly {
                return Some(AssemblyRouteMode::ReuseReference);
            }
            if has_non_assembly {
                return Some(AssemblyRouteMode::StructuralHybrid);
            }
        }
        if self.reuses_synchronized_assembly() {
            Some(AssemblyRouteMode::ReuseReference)
        } else {
            None
        }
    }

    fn derived_node_count_for(
        literal_bytes: &[u8],
        assembly_ref: Option<&AssemblyRef>,
        prg: Option<&PredictiveReconstructionGraph>,
        hybrid_route: Option<&HybridRoute>,
    ) -> u16 {
        if let Some(prg) = prg {
            return prg.nodes.len().min(u16::MAX as usize) as u16;
        }
        if let Some(route) = hybrid_route {
            return route.components.len().min(u16::MAX as usize) as u16;
        }
        if assembly_ref.is_some() {
            return 1;
        }
        if !literal_bytes.is_empty() {
            return 1;
        }
        0
    }

    fn derived_output_len_for(
        literal_bytes: &[u8],
        assembly_ref: Option<&AssemblyRef>,
        prg: Option<&PredictiveReconstructionGraph>,
        hybrid_route: Option<&HybridRoute>,
    ) -> u32 {
        if let Some(prg) = prg {
            return prg.output_len;
        }
        if let Some(route) = hybrid_route {
            return route.output_len;
        }
        if let Some(reference) = assembly_ref {
            return reference.output_len;
        }
        literal_bytes.len().min(u32::MAX as usize) as u32
    }

    fn derived_route_graph_id_for(
        route_family: ControllerRouteFamily,
        route_kind: RouteFamily,
        dependency_closure: &[ObjectDependency],
        sync_risk: u32,
        literal_bytes: &[u8],
        assembly_ref: Option<&AssemblyRef>,
        prg: Option<&PredictiveReconstructionGraph>,
        hybrid_route: Option<&HybridRoute>,
    ) -> u64 {
        let derived_output_len =
            Self::derived_output_len_for(literal_bytes, assembly_ref, prg, hybrid_route);
        let derived_node_count =
            Self::derived_node_count_for(literal_bytes, assembly_ref, prg, hybrid_route);
        let mut hash = 0x517cc1b727220a95_u64;
        hash ^= (route_family as u8 as u64).rotate_left(5);
        hash ^= (route_kind as u8 as u64).rotate_left(13);
        hash ^= (derived_output_len as u64).rotate_left(23);
        hash ^= (derived_node_count as u64).rotate_left(31);
        hash ^= (sync_risk as u64).rotate_left(41);
        for dependency in dependency_closure {
            hash ^= (dependency.object_kind as u8 as u64).rotate_left(7);
            for byte in dependency.object_id.as_bytes().iter().copied().take(24) {
                hash = hash.rotate_left(9) ^ byte as u64;
            }
            hash ^= (dependency.required_revision as u64).rotate_left(19);
        }
        if let Some(reference) = assembly_ref {
            hash ^= reference.assembly_id.0.rotate_left(11);
            hash ^= reference.output_len as u64;
            hash ^= reference.body_shape_hash.rotate_left(17);
        }
        if let Some(prg) = prg {
            hash ^= (prg.root_node_id as u64).rotate_left(29);
            for node in &prg.nodes {
                hash ^= (node.node_id as u64).rotate_left(3);
                hash ^= (node.kind as u8 as u64).rotate_left(37);
                hash ^= (node.output_len as u64).rotate_left(43);
            }
        }
        if let Some(route) = hybrid_route {
            hash ^= (route.components.len().min(u16::MAX as usize) as u64).rotate_left(47);
            for component in &route.components {
                let tag = match component {
                    HybridRouteComponent::Literal(_) => 1_u64,
                    HybridRouteComponent::Substrate(_) => 2_u64,
                    HybridRouteComponent::SubstrateGraph(_) => 3_u64,
                    HybridRouteComponent::Assembly(_) => 4_u64,
                    HybridRouteComponent::Transform(_) => 5_u64,
                    HybridRouteComponent::DictionaryTokens { .. } => 6_u64,
                    HybridRouteComponent::Schema(_) => 7_u64,
                    HybridRouteComponent::ResidualPatch { .. } => 8_u64,
                };
                hash ^= tag.rotate_left(53);
                hash = hash.rotate_left(11).wrapping_mul(0x100000001b3_u64);
            }
        }
        hash
    }

    pub fn derive_route_graph_contract_for(
        route_family: ControllerRouteFamily,
        route_kind: RouteFamily,
        dependency_closure: &[ObjectDependency],
        sync_risk: u32,
        contradiction_bytes: &[u8],
        literal_bytes: &[u8],
        assembly_ref: Option<&AssemblyRef>,
        prg: Option<&PredictiveReconstructionGraph>,
        hybrid_route: Option<&HybridRoute>,
    ) -> RouteGraphContract {
        let node_count =
            Self::derived_node_count_for(literal_bytes, assembly_ref, prg, hybrid_route);
        let output_len =
            Self::derived_output_len_for(literal_bytes, assembly_ref, prg, hybrid_route);
        RouteGraphContract {
            route_graph_id: Self::derived_route_graph_id_for(
                route_family,
                route_kind,
                dependency_closure,
                sync_risk,
                literal_bytes,
                assembly_ref,
                prg,
                hybrid_route,
            ),
            node_count,
            output_len,
            sync_requirements: RouteGraphSyncRequirements {
                dependency_count: dependency_closure.len().min(u16::MAX as usize) as u16,
                sync_risk,
                requires_synchronized_memory: sync_risk > 0 || !dependency_closure.is_empty(),
            },
            contradiction_contract: RouteGraphContradictionContract {
                contradiction_bytes_len: contradiction_bytes.len().min(u32::MAX as usize) as u32,
                exact_contradiction_only: true,
            },
        }
    }

    pub fn derived_node_count(&self) -> u16 {
        Self::derived_node_count_for(
            &self.literal_bytes,
            self.assembly_ref.as_ref(),
            self.prg.as_ref(),
            self.hybrid_route.as_ref(),
        )
    }

    pub fn derived_output_len(&self) -> u32 {
        Self::derived_output_len_for(
            &self.literal_bytes,
            self.assembly_ref.as_ref(),
            self.prg.as_ref(),
            self.hybrid_route.as_ref(),
        )
    }

    pub fn with_derived_route_graph(mut self) -> Self {
        self.route_graph = self.derive_route_graph_contract();
        self
    }

    pub fn derive_route_graph_contract(&self) -> RouteGraphContract {
        Self::derive_route_graph_contract_for(
            self.route_family,
            self.route_kind,
            &self.dependency_closure,
            self.sync_risk,
            &self.contradiction_bytes,
            &self.literal_bytes,
            self.assembly_ref.as_ref(),
            self.prg.as_ref(),
            self.hybrid_route.as_ref(),
        )
    }

    fn derived_route_graph_id(&self) -> u64 {
        Self::derived_route_graph_id_for(
            self.route_family,
            self.route_kind,
            &self.dependency_closure,
            self.sync_risk,
            &self.literal_bytes,
            self.assembly_ref.as_ref(),
            self.prg.as_ref(),
            self.hybrid_route.as_ref(),
        )
    }
}

pub fn execute_hybrid_route<FSub, FAsm, FTx, FDict, FEp, FSchema>(
    route: &HybridRoute,
    mut resolve_substrate: FSub,
    mut resolve_assembly: FAsm,
    mut resolve_transform: FTx,
    mut resolve_dictionary: FDict,
    mut resolve_episode: FEp,
    mut resolve_schema: FSchema,
) -> Result<Vec<u8>, StateProgramError>
where
    FSub: FnMut(&SubstrateRef) -> Option<Vec<u8>>,
    FAsm: FnMut(&AssemblyRef) -> Option<Vec<u8>>,
    FTx: FnMut(TransformId) -> Option<Vec<u8>>,
    FDict: FnMut(DictionaryId, &[u16]) -> Option<Vec<u8>>,
    FEp: FnMut(&EpisodeObjectRef) -> Option<Vec<u8>>,
    FSchema: FnMut(SchemaId) -> Option<Vec<u8>>,
{
    let mut out = Vec::new();
    for component in &route.components {
        match component {
            HybridRouteComponent::Literal(bytes) => out.extend_from_slice(bytes),
            HybridRouteComponent::Substrate(reference) => {
                let bytes = resolve_substrate(reference).ok_or_else(|| {
                    StateProgramError::PrgMissingDependency(reference.object_id.to_string())
                })?;
                out.extend_from_slice(&bytes);
            }
            HybridRouteComponent::SubstrateGraph(graph) => {
                let bytes =
                    execute_substrate_route_graph(graph, |reference| resolve_substrate(reference))?;
                out.extend_from_slice(&bytes);
            }
            HybridRouteComponent::Assembly(reference) => {
                let bytes = resolve_assembly(reference).ok_or_else(|| {
                    StateProgramError::PrgMissingDependency(reference.assembly_id.0.to_string())
                })?;
                out.extend_from_slice(&bytes);
            }
            HybridRouteComponent::Transform(transform_id) => {
                let bytes = resolve_transform(*transform_id).ok_or_else(|| {
                    StateProgramError::PrgMissingDependency(transform_id.0.to_string())
                })?;
                out.extend_from_slice(&bytes);
            }
            HybridRouteComponent::DictionaryTokens {
                dictionary_id,
                token_ids,
            } => {
                let bytes = resolve_dictionary(*dictionary_id, token_ids).ok_or_else(|| {
                    StateProgramError::PrgMissingDependency(dictionary_id.0.to_string())
                })?;
                out.extend_from_slice(&bytes);
            }
            HybridRouteComponent::Schema(graph) => {
                let bytes = execute_prg(
                    graph,
                    |reference| resolve_substrate(reference),
                    |reference| resolve_assembly(reference),
                    |transform_id| resolve_transform(transform_id),
                    |episode_ref| resolve_episode(episode_ref),
                    |schema_id| resolve_schema(schema_id),
                )?;
                out.extend_from_slice(&bytes);
            }
            HybridRouteComponent::ResidualPatch {
                offset,
                remove_len,
                bytes,
            } => {
                let range = checked_patch_range(out.len(), *offset, *remove_len)?;
                out.splice(range, bytes.clone());
            }
        }
    }
    if out.len() != route.output_len as usize {
        return Err(StateProgramError::PrgOutputLenMismatch {
            expected: route.output_len,
            actual: out.len() as u32,
        });
    }
    Ok(out)
}

pub fn exact_contradiction_mask(predicted: &[u8], actual: &[u8]) -> Vec<u8> {
    if predicted.len() != actual.len() {
        return actual.to_vec();
    }
    let mut mask = Vec::with_capacity(predicted.len());
    let mut any = false;
    for (predicted_byte, actual_byte) in predicted.iter().zip(actual.iter()) {
        let delta = predicted_byte ^ actual_byte;
        any |= delta != 0;
        mask.push(delta);
    }
    if any { mask } else { Vec::new() }
}

pub fn apply_exact_contradiction_mask_to_bytes(
    mut predicted: Vec<u8>,
    contradiction_bytes: &[u8],
) -> Result<Vec<u8>, StateProgramError> {
    if contradiction_bytes.is_empty() {
        return Ok(predicted);
    }
    if predicted.len() != contradiction_bytes.len() {
        return Err(StateProgramError::InvalidPredictiveDispatchContract(
            "exact contradiction mask length does not match predicted output".into(),
        ));
    }
    for (byte, delta) in predicted.iter_mut().zip(contradiction_bytes.iter()) {
        *byte ^= *delta;
    }
    Ok(predicted)
}

pub fn execute_prg<FSub, FAsm, FTx, FEp, FSchema>(
    graph: &PredictiveReconstructionGraph,
    mut resolve_substrate: FSub,
    mut resolve_assembly: FAsm,
    mut resolve_transform: FTx,
    mut resolve_episode: FEp,
    mut resolve_schema: FSchema,
) -> Result<Vec<u8>, StateProgramError>
where
    FSub: FnMut(&SubstrateRef) -> Option<Vec<u8>>,
    FAsm: FnMut(&AssemblyRef) -> Option<Vec<u8>>,
    FTx: FnMut(TransformId) -> Option<Vec<u8>>,
    FEp: FnMut(&EpisodeObjectRef) -> Option<Vec<u8>>,
    FSchema: FnMut(SchemaId) -> Option<Vec<u8>>,
{
    graph.validate()?;
    let mut stack = BTreeSet::new();
    let out = execute_prg_node(
        graph.root_node_id,
        0,
        graph,
        &mut stack,
        &mut resolve_substrate,
        &mut resolve_assembly,
        &mut resolve_transform,
        &mut resolve_episode,
        &mut resolve_schema,
    )?;
    if out.len() != graph.output_len as usize {
        return Err(StateProgramError::PrgOutputLenMismatch {
            expected: graph.output_len,
            actual: out.len() as u32,
        });
    }
    Ok(out)
}

fn execute_prg_node<FSub, FAsm, FTx, FEp, FSchema>(
    node_id: u32,
    depth: u8,
    graph: &PredictiveReconstructionGraph,
    stack: &mut BTreeSet<u32>,
    resolve_substrate: &mut FSub,
    resolve_assembly: &mut FAsm,
    resolve_transform: &mut FTx,
    resolve_episode: &mut FEp,
    resolve_schema: &mut FSchema,
) -> Result<Vec<u8>, StateProgramError>
where
    FSub: FnMut(&SubstrateRef) -> Option<Vec<u8>>,
    FAsm: FnMut(&AssemblyRef) -> Option<Vec<u8>>,
    FTx: FnMut(TransformId) -> Option<Vec<u8>>,
    FEp: FnMut(&EpisodeObjectRef) -> Option<Vec<u8>>,
    FSchema: FnMut(SchemaId) -> Option<Vec<u8>>,
{
    if depth > graph.depth_cap {
        return Err(StateProgramError::PrgDepthExceeded {
            depth,
            cap: graph.depth_cap,
        });
    }
    let node = graph
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .ok_or(StateProgramError::PrgMissingNode(node_id))?;
    if !stack.insert(node_id) {
        return Err(StateProgramError::PrgCycleDetected(node_id));
    }
    let result = match node.kind {
        PrgNodeKind::Literal => node.literal_bytes.clone(),
        PrgNodeKind::AtomRef | PrgNodeKind::RangeRef | PrgNodeKind::BundleRef => {
            let reference =
                node.substrate_ref
                    .as_ref()
                    .ok_or(StateProgramError::PrgMissingReference(
                        node.node_id,
                        node.kind,
                    ))?;
            resolve_substrate(reference).ok_or_else(|| {
                StateProgramError::PrgMissingDependency(reference.object_id.to_string())
            })?
        }
        PrgNodeKind::AssemblyRef | PrgNodeKind::Expand => {
            let reference =
                node.assembly_ref
                    .as_ref()
                    .ok_or(StateProgramError::PrgMissingReference(
                        node.node_id,
                        node.kind,
                    ))?;
            resolve_assembly(reference).ok_or_else(|| {
                StateProgramError::PrgMissingDependency(reference.assembly_id.0.to_string())
            })?
        }
        PrgNodeKind::TransformRef => {
            let reference = node
                .transform_ref
                .ok_or(StateProgramError::PrgMissingReference(
                    node.node_id,
                    node.kind,
                ))?;
            resolve_transform(reference)
                .ok_or_else(|| StateProgramError::PrgMissingDependency(reference.0.to_string()))?
        }
        PrgNodeKind::EpisodeRef => {
            let reference =
                node.episode_ref
                    .as_ref()
                    .ok_or(StateProgramError::PrgMissingReference(
                        node.node_id,
                        node.kind,
                    ))?;
            resolve_episode(reference).ok_or_else(|| {
                StateProgramError::PrgMissingDependency(reference.object_id.clone())
            })?
        }
        PrgNodeKind::SchemaRef => {
            let reference = node
                .schema_ref
                .ok_or(StateProgramError::PrgMissingReference(
                    node.node_id,
                    node.kind,
                ))?;
            resolve_schema(reference)
                .ok_or_else(|| StateProgramError::PrgMissingDependency(reference.0.to_string()))?
        }
        PrgNodeKind::Concat | PrgNodeKind::Commit | PrgNodeKind::Guard | PrgNodeKind::Branch => {
            let mut out = Vec::new();
            for child in &node.child_ids {
                out.extend_from_slice(&execute_prg_node(
                    *child,
                    depth.saturating_add(1),
                    graph,
                    stack,
                    resolve_substrate,
                    resolve_assembly,
                    resolve_transform,
                    resolve_episode,
                    resolve_schema,
                )?);
            }
            out
        }
        PrgNodeKind::Repeat => {
            let child = node
                .child_ids
                .first()
                .copied()
                .ok_or(StateProgramError::PrgMissingChild(node.node_id))?;
            let times = node.repeat_count.unwrap_or(0);
            let bytes = execute_prg_node(
                child,
                depth.saturating_add(1),
                graph,
                stack,
                resolve_substrate,
                resolve_assembly,
                resolve_transform,
                resolve_episode,
                resolve_schema,
            )?;
            let mut out = Vec::new();
            for _ in 0..times {
                out.extend_from_slice(&bytes);
            }
            out
        }
        PrgNodeKind::Select => {
            let ix = node.select_index.unwrap_or(0) as usize;
            let child = node
                .child_ids
                .get(ix)
                .copied()
                .ok_or(StateProgramError::PrgMissingChild(node.node_id))?;
            execute_prg_node(
                child,
                depth.saturating_add(1),
                graph,
                stack,
                resolve_substrate,
                resolve_assembly,
                resolve_transform,
                resolve_episode,
                resolve_schema,
            )?
        }
        PrgNodeKind::Permute => {
            let mut parts = Vec::new();
            for child in &node.child_ids {
                parts.push(execute_prg_node(
                    *child,
                    depth.saturating_add(1),
                    graph,
                    stack,
                    resolve_substrate,
                    resolve_assembly,
                    resolve_transform,
                    resolve_episode,
                    resolve_schema,
                )?);
            }
            let mut out = Vec::new();
            for ix in &node.permutation {
                let part = parts
                    .get(*ix as usize)
                    .ok_or(StateProgramError::PrgMissingChild(node.node_id))?;
                out.extend_from_slice(part);
            }
            out
        }
        PrgNodeKind::SlotFill => node.literal_bytes.clone(),
        PrgNodeKind::Patch => {
            let child = node
                .child_ids
                .first()
                .copied()
                .ok_or(StateProgramError::PrgMissingChild(node.node_id))?;
            let mut out = execute_prg_node(
                child,
                depth.saturating_add(1),
                graph,
                stack,
                resolve_substrate,
                resolve_assembly,
                resolve_transform,
                resolve_episode,
                resolve_schema,
            )?;
            let offset = node.patch_offset.unwrap_or(0);
            let remove_len = node.patch_remove_len.unwrap_or(0);
            let range = checked_patch_range(out.len(), offset, remove_len)?;
            out.splice(range, node.literal_bytes.clone());
            out
        }
        PrgNodeKind::Noop => Vec::new(),
    };
    stack.remove(&node_id);
    if node.output_len != 0 && result.len() != node.output_len as usize {
        return Err(StateProgramError::PrgNodeOutputLenMismatch {
            node_id,
            expected: node.output_len,
            actual: result.len() as u32,
        });
    }
    Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssemblyDefPayload {
    pub version: u8,
    pub assembly: Assembly,
}

impl AssemblyDefPayload {
    pub const VERSION: u8 = 1;

    pub fn new(mut assembly: Assembly) -> Self {
        if assembly.dependency_closure.version == ObjectVersion::default() {
            let signature = crate::assembly_reuse_signature(&assembly);
            assembly.dependency_closure.version = ObjectVersion {
                schema_version: 1,
                object_revision: (signature.body_shape_hash
                    ^ signature.dependency_fingerprint
                    ^ signature.output_len as u64) as u32,
            };
        }
        Self {
            version: Self::VERSION,
            assembly,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::AssemblyPayloadSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| StateProgramError::AssemblyPayloadSerialization(error.to_string()))?;
        if payload.version != Self::VERSION {
            return Err(StateProgramError::InvalidAssemblyPayloadVersion(
                payload.version,
            ));
        }
        Ok(payload)
    }

    pub fn install_plan(&self) -> AssemblyInstallPlan {
        AssemblyInstallPlan {
            assembly_id: self.assembly.assembly_id,
            dependency_closure: self.assembly.dependency_closure.dependencies.clone(),
            output_len: self.assembly.body.output_len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssemblyInstallPlan {
    pub assembly_id: AssemblyId,
    pub dependency_closure: Vec<ObjectDependency>,
    pub output_len: u32,
}

pub fn assembly_ref_from_payload(payload: &AssemblyDefPayload) -> AssemblyRef {
    let signature = assembly_reuse_signature_from_parts(
        payload.assembly.dependency_closure.version,
        &payload.assembly.body,
        &payload.assembly.dependency_closure.dependencies,
    );
    AssemblyRef {
        assembly_id: payload.assembly.assembly_id,
        version: payload.assembly.dependency_closure.version,
        dependency_closure: payload.assembly.dependency_closure.dependencies.clone(),
        output_len: payload.assembly.body.output_len(),
        body_shape_hash: signature.body_shape_hash,
        dependency_fingerprint: signature.dependency_fingerprint,
    }
}

pub fn expand_assembly_body<F>(
    body: &AssemblyBody,
    mut resolve: F,
) -> Result<Vec<u8>, StateProgramError>
where
    F: FnMut(&AssemblyComponentRef) -> Option<Vec<u8>>,
{
    let mut out = Vec::new();
    for node in &body.nodes {
        match node {
            AssemblyBodyNode::DelimiterAnchor { bytes }
            | AssemblyBodyNode::LiteralIsland { bytes }
            | AssemblyBodyNode::SlotPlaceholder { bytes, .. }
            | AssemblyBodyNode::MotifLink { bytes, .. }
            | AssemblyBodyNode::TypedBoundary { bytes, .. } => out.extend_from_slice(bytes),
            AssemblyBodyNode::SubstrateSpan { reference } => {
                let bytes = resolve(reference).ok_or_else(|| {
                    StateProgramError::MissingAssemblyComponent {
                        object_kind: reference.object_kind(),
                        object_id: match reference {
                            AssemblyComponentRef::AtomFragment { object_id, .. }
                            | AssemblyComponentRef::ExactBlock { object_id, .. }
                            | AssemblyComponentRef::ExactBundle { object_id, .. }
                            | AssemblyComponentRef::ExactRange { object_id, .. }
                            | AssemblyComponentRef::ResidualBuffer { object_id, .. } => {
                                object_id.clone()
                            }
                        },
                    }
                })?;
                out.extend_from_slice(&bytes);
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformInstallPlan {
    pub class_id: TransformId,
    pub transform_kind: TransformKind,
    pub transducer_family: TransformTransducerFamily,
    pub parameter_count: u16,
    pub max_basis_count: u8,
    pub basis_mask: crate::TransformBasisMask,
    pub output_contract: TransformOutputContract,
    pub dependency_closure: TransformDependencyClosure,
    pub invertible: bool,
}

impl TransformInstallPlan {
    pub fn from_class(class: &TransformClass) -> Self {
        Self {
            class_id: class.transform_id,
            transform_kind: class.transform_kind,
            transducer_family: class.transducer_family,
            parameter_count: class
                .parameter_schema
                .parameters
                .len()
                .min(u16::MAX as usize) as u16,
            max_basis_count: class.parameter_schema.max_basis_count,
            basis_mask: class.basis_mask,
            output_contract: class.output_contract,
            dependency_closure: class.dependency_closure.clone(),
            invertible: class.invertible,
        }
    }

    fn matches_class(&self, class: &TransformClass) -> bool {
        self.class_id == class.transform_id
            && self.transform_kind == class.transform_kind
            && self.transducer_family == class.transducer_family
            && self.max_basis_count == class.parameter_schema.max_basis_count
            && self.parameter_count as usize == class.parameter_schema.parameters.len()
            && self.basis_mask == class.basis_mask
            && self.output_contract == class.output_contract
            && self.dependency_closure == class.dependency_closure
            && self.invertible == class.invertible
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformDefPayload {
    pub version: u8,
    pub class: TransformClass,
    pub install_plan: TransformInstallPlan,
}

impl TransformDefPayload {
    pub const VERSION: u8 = 1;

    pub fn new(class: TransformClass) -> Self {
        let install_plan = TransformInstallPlan::from_class(&class);
        Self {
            version: Self::VERSION,
            class,
            install_plan,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::TransformPayloadSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| StateProgramError::TransformPayloadSerialization(error.to_string()))?;
        if payload.version != Self::VERSION {
            return Err(StateProgramError::InvalidTransformPayloadVersion(
                payload.version,
            ));
        }
        if !payload.install_plan.matches_class(&payload.class) {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                "transform install plan does not match class contract".into(),
            ));
        }
        Ok(payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformApplyPlan {
    pub class_id: TransformId,
    pub transform_kind: TransformKind,
    pub transducer_family: TransformTransducerFamily,
    pub basis_refs: Vec<TransformBasisRef>,
    pub dependency_closure: TransformDependencyClosure,
    pub output_contract: TransformOutputContract,
    pub invertibility_expected: bool,
    pub output_len: u32,
    pub parameter_count: u16,
    pub residual_count: u16,
}

impl TransformApplyPlan {
    pub fn from_parts(class: &TransformClass, instance: &TransformInstance) -> Self {
        Self {
            class_id: instance.class_id,
            transform_kind: class.transform_kind,
            transducer_family: class.transducer_family,
            basis_refs: instance.basis_refs.clone(),
            dependency_closure: instance.dependency_closure.clone(),
            output_contract: instance.output_contract,
            invertibility_expected: instance.invertibility_expected,
            output_len: instance.output_len,
            parameter_count: instance.integer_parameters.len().min(u16::MAX as usize) as u16,
            residual_count: instance.residual_bytes.len().min(u16::MAX as usize) as u16,
        }
    }

    fn matches_payload(&self, payload: &TransformInstancePayload) -> bool {
        self.class_id == payload.class_id
            && self.transform_kind == payload.transform_kind
            && self.transducer_family == payload.transducer_family
            && self.basis_refs == payload.basis_refs
            && self.dependency_closure == payload.dependency_closure
            && self.output_contract == payload.output_contract
            && self.invertibility_expected == payload.invertibility_expected
            && self.output_len == payload.output_len
            && self.parameter_count as usize == payload.integer_parameters.len()
            && self.residual_count as usize == payload.residual_bytes.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformInstancePayload {
    pub version: u8,
    pub transform_kind: TransformKind,
    pub class_id: TransformId,
    pub transducer_family: TransformTransducerFamily,
    pub basis_refs: Vec<TransformBasisRef>,
    pub integer_parameters: Vec<i32>,
    pub residual_offsets: Vec<u32>,
    pub residual_bytes: Vec<Vec<u8>>,
    pub dependency_closure: TransformDependencyClosure,
    pub output_contract: TransformOutputContract,
    pub invertibility_expected: bool,
    pub output_len: u32,
    pub apply_plan: TransformApplyPlan,
}

impl TransformInstancePayload {
    pub const VERSION: u8 = 1;

    pub fn new(class: &TransformClass, instance: TransformInstance) -> Self {
        let mut payload = Self {
            version: Self::VERSION,
            transform_kind: class.transform_kind,
            class_id: instance.class_id,
            transducer_family: class.transducer_family,
            basis_refs: instance.basis_refs,
            integer_parameters: instance.integer_parameters,
            residual_offsets: instance.residual_offsets,
            residual_bytes: instance.residual_bytes,
            dependency_closure: instance.dependency_closure,
            output_contract: instance.output_contract,
            invertibility_expected: instance.invertibility_expected,
            output_len: instance.output_len,
            apply_plan: TransformApplyPlan::default(),
        };
        payload.apply_plan = TransformApplyPlan::from_parts(class, &payload.instance());
        payload
    }

    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::TransformPayloadSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| StateProgramError::TransformPayloadSerialization(error.to_string()))?;
        if payload.version != Self::VERSION {
            return Err(StateProgramError::InvalidTransformPayloadVersion(
                payload.version,
            ));
        }
        if payload.residual_offsets.len() != payload.residual_bytes.len() {
            return Err(StateProgramError::TransformResidualContractMismatch {
                offsets: payload.residual_offsets.len(),
                payloads: payload.residual_bytes.len(),
            });
        }
        if !payload.apply_plan.matches_payload(&payload) {
            return Err(StateProgramError::InvalidPredictiveDispatchContract(
                "transform apply plan does not match payload contract".into(),
            ));
        }
        Ok(payload)
    }

    pub fn instance(&self) -> TransformInstance {
        TransformInstance {
            class_id: self.class_id,
            basis_refs: self.basis_refs.clone(),
            integer_parameters: self.integer_parameters.clone(),
            residual_offsets: self.residual_offsets.clone(),
            residual_bytes: self.residual_bytes.clone(),
            dependency_closure: self.dependency_closure.clone(),
            output_contract: self.output_contract,
            invertibility_expected: self.invertibility_expected,
            output_len: self.output_len,
        }
    }
}

pub fn apply_transform_instance<F>(
    class: &TransformClass,
    instance: &TransformInstance,
    mut resolve_basis: F,
) -> Result<Vec<u8>, StateProgramError>
where
    F: FnMut(&TransformBasisRef) -> Option<Vec<u8>>,
{
    if class.transform_id != instance.class_id {
        return Err(StateProgramError::InvalidPredictiveDispatchContract(
            "transform instance class id does not match installed class".into(),
        ));
    }
    if instance.residual_offsets.len() != instance.residual_bytes.len() {
        return Err(StateProgramError::TransformResidualContractMismatch {
            offsets: instance.residual_offsets.len(),
            payloads: instance.residual_bytes.len(),
        });
    }
    if instance.output_contract != class.output_contract {
        return Err(StateProgramError::InvalidPredictiveDispatchContract(
            "transform instance output contract does not match class".into(),
        ));
    }
    if instance.dependency_closure != class.dependency_closure {
        return Err(StateProgramError::InvalidPredictiveDispatchContract(
            "transform dependency closure does not match installed class".into(),
        ));
    }
    if instance.invertibility_expected && !class.invertible {
        return Err(StateProgramError::InvalidPredictiveDispatchContract(
            "transform instance requires invertible class semantics".into(),
        ));
    }
    if instance
        .basis_refs
        .iter()
        .any(|basis| !class.basis_mask.admits(basis.object_kind))
    {
        return Err(StateProgramError::TransformBasisKindMismatch(
            class.transform_kind,
        ));
    }
    let bases = instance
        .basis_refs
        .iter()
        .map(|basis| {
            resolve_basis(basis).ok_or_else(|| StateProgramError::MissingTransformBasis {
                object_kind: basis.object_kind,
                object_id: basis.object_id.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let out = match class.transform_kind {
        TransformKind::PrefixInsert => {
            if bases.len() != 1 || instance.residual_bytes.len() != 1 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            [instance.residual_bytes[0].clone(), bases[0].clone()].concat()
        }
        TransformKind::SuffixInsert => {
            if bases.len() != 1 || instance.residual_bytes.len() != 1 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            [bases[0].clone(), instance.residual_bytes[0].clone()].concat()
        }
        TransformKind::Wrap => {
            if bases.len() != 1 || instance.residual_bytes.len() != 2 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            [
                instance.residual_bytes[0].clone(),
                bases[0].clone(),
                instance.residual_bytes[1].clone(),
            ]
            .concat()
        }
        TransformKind::BoundedInteriorInsert => {
            if bases.len() != 1
                || instance.residual_bytes.len() != 1
                || instance.integer_parameters.len() < 1
            {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let offset = instance.integer_parameters[0].max(0) as usize;
            if offset > bases[0].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            [
                bases[0][..offset].to_vec(),
                instance.residual_bytes[0].clone(),
                bases[0][offset..].to_vec(),
            ]
            .concat()
        }
        TransformKind::BoundedDelete => {
            if bases.len() != 1 || instance.integer_parameters.len() < 2 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let start = instance.integer_parameters[0].max(0) as usize;
            let delete_len = instance.integer_parameters[1].max(0) as usize;
            if start > bases[0].len() || start.saturating_add(delete_len) > bases[0].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            [
                bases[0][..start].to_vec(),
                bases[0][start + delete_len..].to_vec(),
            ]
            .concat()
        }
        TransformKind::BoundedSubstitution => {
            if bases.len() != 1
                || instance.residual_bytes.len() != 1
                || instance.integer_parameters.len() < 2
            {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let start = instance.integer_parameters[0].max(0) as usize;
            let remove_len = instance.integer_parameters[1].max(0) as usize;
            if start > bases[0].len() || start.saturating_add(remove_len) > bases[0].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            [
                bases[0][..start].to_vec(),
                instance.residual_bytes[0].clone(),
                bases[0][start + remove_len..].to_vec(),
            ]
            .concat()
        }
        TransformKind::RepeatedMotifExpansion => {
            if bases.len() != 1 || instance.integer_parameters.len() < 1 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let repeats = instance.integer_parameters[0].max(0) as usize;
            let mut out = Vec::new();
            for _ in 0..repeats {
                out.extend_from_slice(&bases[0]);
            }
            out
        }
        TransformKind::CopyWithGap => {
            if bases.len() != 1
                || instance.integer_parameters.len() < 2
                || instance.residual_bytes.len() != 1
            {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let start = instance.integer_parameters[0].max(0) as usize;
            let gap_len = instance.integer_parameters[1].max(0) as usize;
            if start > bases[0].len() || start.saturating_add(gap_len) > bases[0].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            [
                bases[0][..start].to_vec(),
                instance.residual_bytes[0].clone(),
                bases[0][start + gap_len..].to_vec(),
            ]
            .concat()
        }
        TransformKind::SlotSubstitution | TransformKind::SchemaSlotFill => {
            if bases.len() != 1
                || instance.integer_parameters.is_empty()
                || instance.integer_parameters.len() != instance.residual_bytes.len() * 2
            {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let mut out = bases[0].clone();
            for (idx, replacement) in instance.residual_bytes.iter().enumerate().rev() {
                let start = instance.integer_parameters[idx * 2].max(0) as usize;
                let len = instance.integer_parameters[idx * 2 + 1].max(0) as usize;
                if !kernel_slot_mask_check(out.len(), start, len) {
                    return Err(StateProgramError::InvalidTransformParameter(
                        class.transform_kind,
                    ));
                }
                kernel_apply_patch(&mut out, start..start + len, replacement);
            }
            out
        }
        TransformKind::RolePermutation => {
            if bases.len() != 1 || instance.integer_parameters.is_empty() {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let width = instance.integer_parameters[0].max(1) as usize;
            let mut chunks = bases[0]
                .chunks(width)
                .map(|c| c.to_vec())
                .collect::<Vec<_>>();
            chunks.reverse();
            chunks.concat()
        }
        TransformKind::DelimiterPreservingRewrite => {
            if bases.len() != 1 || instance.residual_bytes.len() != 1 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let basis_delimiters = transform_delimiter_inventory(&bases[0]);
            let target = instance.residual_bytes[0].clone();
            if basis_delimiters != transform_delimiter_inventory(&target) {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            target
        }
        TransformKind::LocalMirror => {
            if bases.len() != 1 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let mut out = bases[0].clone();
            out.extend(bases[0].iter().rev().copied());
            out
        }
        TransformKind::BoundedDuplication => {
            if bases.len() != 1 || instance.integer_parameters.len() < 3 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let start = instance.integer_parameters[0].max(0) as usize;
            let len = instance.integer_parameters[1].max(0) as usize;
            let repeats = instance.integer_parameters[2].max(0) as usize;
            if start > bases[0].len() || start.saturating_add(len) > bases[0].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            let motif = bases[0][start..start + len].to_vec();
            let mut out = bases[0].clone();
            for _ in 0..repeats {
                out.extend_from_slice(&motif);
            }
            out
        }
        TransformKind::StridedSelection => {
            if bases.len() != 1 || instance.integer_parameters.len() < 2 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let offset = instance.integer_parameters[0].max(0) as usize;
            let stride = instance.integer_parameters[1].max(1) as usize;
            bases[0]
                .iter()
                .skip(offset)
                .step_by(stride)
                .copied()
                .collect()
        }
        TransformKind::SpliceFromTwoBases => {
            if bases.len() != 2 || instance.integer_parameters.len() < 2 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let left_len = instance.integer_parameters[0].max(0) as usize;
            let right_start = instance.integer_parameters[1].max(0) as usize;
            if left_len > bases[0].len() || right_start > bases[1].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            [
                bases[0][..left_len].to_vec(),
                bases[1][right_start..].to_vec(),
            ]
            .concat()
        }
        TransformKind::BoundedReorder => {
            if bases.len() != 1
                || instance.integer_parameters.len() < 3
                || instance.residual_bytes.len() != 1
            {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let start = instance.integer_parameters[0].max(0) as usize;
            let len = instance.integer_parameters[1].max(0) as usize;
            let delimiter = instance.integer_parameters[2].clamp(0, 255) as u8;
            if start > bases[0].len() || start.saturating_add(len) > bases[0].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            let mut out = bases[0].clone();
            let replacement = instance.residual_bytes[0].clone();
            if replacement.len() != len {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            let base_segments = bases[0][start..start + len]
                .split(|byte| *byte == delimiter)
                .map(|segment| segment.to_vec())
                .collect::<Vec<_>>();
            let replacement_segments = replacement
                .split(|byte| *byte == delimiter)
                .map(|segment| segment.to_vec())
                .collect::<Vec<_>>();
            if base_segments.len() != replacement_segments.len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            let mut sorted_base = base_segments.clone();
            let mut sorted_replacement = replacement_segments.clone();
            sorted_base.sort();
            sorted_replacement.sort();
            if sorted_base != sorted_replacement {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            out.splice(start..start + len, replacement);
            out
        }
        TransformKind::MotifDuplicateCompress => {
            if bases.len() != 1 || instance.integer_parameters.len() < 3 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let start = instance.integer_parameters[0].max(0) as usize;
            let len = instance.integer_parameters[1].max(0) as usize;
            let repeats = instance.integer_parameters[2].max(0) as usize;
            if start > bases[0].len() || start.saturating_add(len) > bases[0].len() {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            let motif = bases[0][start..start + len].to_vec();
            let mut out = bases[0].clone();
            for _ in 0..repeats {
                out.extend_from_slice(&motif);
            }
            out
        }
        TransformKind::MirrorSymmetry => {
            if bases.len() != 1 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let mut out = bases[0].clone();
            out.extend(bases[0].iter().rev().copied());
            out
        }
        TransformKind::SchemaConditionedExpansion => {
            if bases.len() != 1 || instance.integer_parameters.len() < 1 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let repeats = instance.integer_parameters[0].max(1) as usize;
            let delimiter = instance
                .residual_bytes
                .first()
                .cloned()
                .unwrap_or_else(|| vec![b'|']);
            let mut out = Vec::new();
            for idx in 0..repeats {
                if idx > 0 {
                    out.extend_from_slice(&delimiter);
                }
                out.extend_from_slice(&bases[0]);
            }
            out
        }
        TransformKind::RecursiveMacroExpansion => {
            if bases.len() != 1 || instance.integer_parameters.len() < 2 {
                return Err(StateProgramError::InvalidTransformArity(
                    class.transform_kind,
                ));
            }
            let depth = instance.integer_parameters[0].max(0) as usize;
            let repeat = instance.integer_parameters[1].max(1) as usize;
            if depth > instance.dependency_closure.max_decode_depth as usize {
                return Err(StateProgramError::InvalidTransformParameter(
                    class.transform_kind,
                ));
            }
            let mut out = bases[0].clone();
            for _ in 0..depth {
                let mut next = Vec::new();
                for idx in 0..repeat {
                    if idx > 0 {
                        next.extend_from_slice(
                            instance
                                .residual_bytes
                                .first()
                                .map(Vec::as_slice)
                                .unwrap_or(&[]),
                        );
                    }
                    next.extend_from_slice(&out);
                }
                out = next;
            }
            out
        }
        TransformKind::TokenDictionarySubstitution => {
            return Err(StateProgramError::InvalidTransformParameter(
                class.transform_kind,
            ));
        }
    };
    if out.len() as u32 != instance.output_len {
        return Err(StateProgramError::TransformOutputLenMismatch {
            expected: instance.output_len,
            actual: out.len() as u32,
        });
    }
    if (out.len() as u32) < class.output_contract.min_output_len
        || (out.len() as u32) > class.output_contract.max_output_len
    {
        return Err(StateProgramError::TransformOutputLenContract {
            min: class.output_contract.min_output_len,
            max: class.output_contract.max_output_len,
            actual: out.len() as u32,
        });
    }
    Ok(out)
}

fn transform_delimiter_inventory(bytes: &[u8]) -> Vec<u8> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeHintPayload {
    pub version: u8,
    pub context_hash: ContextHash,
    pub lag_bucket: LagBucket,
    pub precision_band: PrecisionBand,
    pub dependencies: Vec<ObjectDependency>,
    pub candidates: Vec<EpisodeCompletionCandidate>,
}

impl EpisodeHintPayload {
    pub const VERSION: u8 = 1;

    pub fn new(
        context_hash: ContextHash,
        lag_bucket: LagBucket,
        precision_band: PrecisionBand,
        dependencies: Vec<ObjectDependency>,
        candidates: Vec<EpisodeCompletionCandidate>,
    ) -> Self {
        Self {
            version: Self::VERSION,
            context_hash,
            lag_bucket,
            precision_band,
            dependencies,
            candidates,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        serde_json::to_vec(self)
            .map_err(|error| StateProgramError::EpisodePayloadSerialization(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| StateProgramError::EpisodePayloadSerialization(error.to_string()))?;
        if payload.version != Self::VERSION {
            return Err(StateProgramError::InvalidEpisodePayloadVersion(
                payload.version,
            ));
        }
        Ok(payload)
    }

    pub fn hinted_object_refs(&self) -> Vec<EpisodeObjectRef> {
        self.candidates
            .iter()
            .map(|candidate| candidate.object_ref.clone())
            .collect()
    }
}

pub fn predictive_record_uses_chpmt_routing(kind: RecordType) -> bool {
    matches!(
        kind,
        RecordType::PredictiveConfirm
            | RecordType::PredictiveCorrect
            | RecordType::AssemblyDef
            | RecordType::TransformDef
            | RecordType::SchemaDef
            | RecordType::EpisodeHint
            | RecordType::ReplayHint
            | RecordType::ExactState
            | RecordType::MemoryRetire
            | RecordType::TransformCorrect
            | RecordType::MemoryAck
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum RefKind {
    Block = 1,
    Range = 2,
    StridedRange = 3,
    IndexSet = 4,
    Bundle = 5,
    PriorState = 6,
}

impl RefKind {
    pub const fn tag(self) -> u8 {
        self as u8
    }

    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Block),
            2 => Some(Self::Range),
            3 => Some(Self::StridedRange),
            4 => Some(Self::IndexSet),
            5 => Some(Self::Bundle),
            6 => Some(Self::PriorState),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRef {
    pub kind: RefKind,
    pub base_id: u64,
    pub start: u32,
    pub len: u32,
    pub step: u16,
    pub indices: Vec<u32>,
}

impl StateRef {
    pub fn block(item_id: ItemId, start: u32, len: u32) -> Self {
        Self {
            kind: RefKind::Block,
            base_id: item_id.0,
            start,
            len,
            step: 1,
            indices: Vec::new(),
        }
    }

    pub fn catalog_block(block_id: BlockId, start: u32, len: u32) -> Self {
        Self {
            kind: RefKind::Block,
            base_id: block_id.0,
            start,
            len,
            step: 1,
            indices: Vec::new(),
        }
    }

    pub fn range(block_id: BlockId, start: u32, len: u32) -> Self {
        Self {
            kind: RefKind::Range,
            base_id: block_id.0,
            start,
            len,
            step: 1,
            indices: Vec::new(),
        }
    }

    pub fn strided_range(block_id: BlockId, start: u32, len: u32, step: u16) -> Self {
        Self {
            kind: RefKind::StridedRange,
            base_id: block_id.0,
            start,
            len,
            step,
            indices: Vec::new(),
        }
    }

    pub fn bundle(bundle_id: BundleId, start: u32, len: u32) -> Self {
        Self {
            kind: RefKind::Bundle,
            base_id: bundle_id.0,
            start,
            len,
            step: 1,
            indices: Vec::new(),
        }
    }

    pub fn prior_state(item_id: ItemId, start: u32, len: u32) -> Self {
        Self {
            kind: RefKind::PriorState,
            base_id: item_id.0,
            start,
            len,
            step: 1,
            indices: Vec::new(),
        }
    }

    pub fn index_set(item_id: ItemId, start: u32, indices: Vec<u32>) -> Self {
        Self {
            kind: RefKind::IndexSet,
            base_id: item_id.0,
            start,
            len: indices.len() as u32,
            step: 1,
            indices,
        }
    }

    pub fn encoded_len_estimate(&self) -> usize {
        STATE_PROGRAM_REF_FIXED_LEN + (self.indices.len() * 4)
    }

    pub fn is_catalog_backed(&self) -> bool {
        matches!(
            self.kind,
            RefKind::Block
                | RefKind::Range
                | RefKind::StridedRange
                | RefKind::IndexSet
                | RefKind::Bundle
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateOp {
    Concat,
    Patch {
        offset: u32,
        remove_len: u32,
        residual_offset: u32,
        residual_len: u32,
    },
    Take {
        start: u32,
        len: u32,
    },
    Drop {
        start: u32,
        len: u32,
    },
}

impl StateOp {
    pub fn encoded_len_estimate(&self) -> usize {
        match self {
            StateOp::Concat => 1,
            StateOp::Patch { .. } => 1 + 4 + 4 + 4 + 4,
            StateOp::Take { .. } | StateOp::Drop { .. } => 1 + 4 + 4,
        }
    }

    pub fn patch(offset: u32, remove_len: u32, residual_offset: u32, residual_len: u32) -> Self {
        Self::Patch {
            offset,
            remove_len,
            residual_offset,
            residual_len,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateProgram {
    pub version: u8,
    pub source_kind: SourceKind,
    pub refs: Vec<StateRef>,
    pub ops: Vec<StateOp>,
    pub residual: Vec<u8>,
}

impl StateProgram {
    pub fn new(
        source_kind: SourceKind,
        refs: Vec<StateRef>,
        ops: Vec<StateOp>,
        residual: Vec<u8>,
    ) -> Self {
        Self {
            version: STATE_PROGRAM_VERSION,
            source_kind,
            refs,
            ops,
            residual,
        }
    }

    pub fn residual_only(source_kind: SourceKind, bytes: &[u8]) -> Self {
        Self::new(source_kind, Vec::new(), Vec::new(), bytes.to_vec())
    }

    pub fn concat_only(source_kind: SourceKind, refs: Vec<StateRef>) -> Self {
        Self::new(source_kind, refs, vec![StateOp::Concat], Vec::new())
    }

    pub fn concat_with_residual(
        source_kind: SourceKind,
        refs: Vec<StateRef>,
        residual: Vec<u8>,
    ) -> Self {
        Self::new(source_kind, refs, Vec::new(), residual)
    }

    pub fn single_ref_patch(
        source_kind: SourceKind,
        reference: StateRef,
        ops: Vec<StateOp>,
        residual: Vec<u8>,
    ) -> Self {
        Self::new(source_kind, vec![reference], ops, residual)
    }

    pub fn encoded_len_estimate(&self) -> usize {
        let refs_len = self
            .refs
            .iter()
            .map(StateRef::encoded_len_estimate)
            .sum::<usize>();
        let ops_len = self
            .ops
            .iter()
            .map(StateOp::encoded_len_estimate)
            .sum::<usize>();
        STATE_PROGRAM_HEADER_LEN + refs_len + ops_len + self.residual.len()
    }

    pub fn referenced_catalog_blocks(&self) -> Vec<BlockId> {
        let mut ids = BTreeSet::new();
        for reference in &self.refs {
            if matches!(
                reference.kind,
                RefKind::Block | RefKind::Range | RefKind::StridedRange | RefKind::IndexSet
            ) {
                ids.insert(BlockId(reference.base_id));
            }
        }
        ids.into_iter().collect()
    }

    pub fn referenced_bundles(&self) -> Vec<BundleId> {
        let mut ids = BTreeSet::new();
        for reference in &self.refs {
            if matches!(reference.kind, RefKind::Bundle) {
                ids.insert(BundleId(reference.base_id));
            }
        }
        ids.into_iter().collect()
    }

    pub fn referenced_prior_states(&self) -> Vec<ItemId> {
        let mut ids = BTreeSet::new();
        for reference in &self.refs {
            if matches!(reference.kind, RefKind::PriorState) {
                ids.insert(ItemId(reference.base_id));
            }
        }
        ids.into_iter().collect()
    }

    pub fn has_catalog_dependencies(&self) -> bool {
        self.refs.iter().any(StateRef::is_catalog_backed)
    }

    pub fn has_prior_state_dependencies(&self) -> bool {
        self.refs
            .iter()
            .any(|reference| matches!(reference.kind, RefKind::PriorState))
    }

    pub fn encode(&self) -> Result<Vec<u8>, StateProgramError> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.encoded_len_estimate());
        out.push(self.version);
        out.push(self.source_kind.tag());
        out.extend_from_slice(&(self.refs.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.ops.len() as u16).to_le_bytes());
        out.extend_from_slice(&(self.residual.len() as u32).to_le_bytes());

        for reference in &self.refs {
            out.push(reference.kind.tag());
            out.extend_from_slice(&reference.base_id.to_le_bytes());
            out.extend_from_slice(&reference.start.to_le_bytes());
            out.extend_from_slice(&reference.len.to_le_bytes());
            out.extend_from_slice(&reference.step.to_le_bytes());
            out.extend_from_slice(&(reference.indices.len() as u32).to_le_bytes());
            for index in &reference.indices {
                out.extend_from_slice(&index.to_le_bytes());
            }
        }

        for op in &self.ops {
            match op {
                StateOp::Concat => out.push(1),
                StateOp::Patch {
                    offset,
                    remove_len,
                    residual_offset,
                    residual_len,
                } => {
                    out.push(2);
                    out.extend_from_slice(&offset.to_le_bytes());
                    out.extend_from_slice(&remove_len.to_le_bytes());
                    out.extend_from_slice(&residual_offset.to_le_bytes());
                    out.extend_from_slice(&residual_len.to_le_bytes());
                }
                StateOp::Take { start, len } => {
                    out.push(3);
                    out.extend_from_slice(&start.to_le_bytes());
                    out.extend_from_slice(&len.to_le_bytes());
                }
                StateOp::Drop { start, len } => {
                    out.push(4);
                    out.extend_from_slice(&start.to_le_bytes());
                    out.extend_from_slice(&len.to_le_bytes());
                }
            }
        }

        out.extend_from_slice(&self.residual);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StateProgramError> {
        if bytes.len() < STATE_PROGRAM_HEADER_LEN {
            return Err(StateProgramError::TruncatedPayload {
                minimum: STATE_PROGRAM_HEADER_LEN,
                actual: bytes.len(),
            });
        }

        let mut cursor = 0_usize;
        let version = read_u8(bytes, &mut cursor)?;
        if version != STATE_PROGRAM_VERSION {
            return Err(StateProgramError::InvalidVersion(version));
        }
        let source_kind_tag = read_u8(bytes, &mut cursor)?;
        let source_kind = SourceKind::from_tag(source_kind_tag)
            .map_err(|_| StateProgramError::UnknownSourceKind(source_kind_tag))?;
        let refs_len = read_u16(bytes, &mut cursor)? as usize;
        let ops_len = read_u16(bytes, &mut cursor)? as usize;
        let residual_len = read_u32(bytes, &mut cursor)? as usize;

        if refs_len > MAX_STATE_REFS {
            return Err(StateProgramError::TooManyRefs(refs_len));
        }
        if ops_len > MAX_STATE_OPS {
            return Err(StateProgramError::TooManyOps(ops_len));
        }

        let mut refs = Vec::with_capacity(refs_len);
        for _ in 0..refs_len {
            let kind_tag = read_u8(bytes, &mut cursor)?;
            let kind =
                RefKind::from_tag(kind_tag).ok_or(StateProgramError::UnknownRefKind(kind_tag))?;
            let base_id = read_u64(bytes, &mut cursor)?;
            let start = read_u32(bytes, &mut cursor)?;
            let len = read_u32(bytes, &mut cursor)?;
            let step = read_u16(bytes, &mut cursor)?;
            let indices_len = read_u32(bytes, &mut cursor)? as usize;
            if indices_len > MAX_STATE_INDICES_PER_REF {
                return Err(StateProgramError::TooManyIndices(indices_len));
            }
            let mut indices = Vec::with_capacity(indices_len);
            for _ in 0..indices_len {
                indices.push(read_u32(bytes, &mut cursor)?);
            }
            refs.push(StateRef {
                kind,
                base_id,
                start,
                len,
                step,
                indices,
            });
        }

        let mut ops = Vec::with_capacity(ops_len);
        for _ in 0..ops_len {
            let tag = read_u8(bytes, &mut cursor)?;
            let op = match tag {
                1 => StateOp::Concat,
                2 => StateOp::Patch {
                    offset: read_u32(bytes, &mut cursor)?,
                    remove_len: read_u32(bytes, &mut cursor)?,
                    residual_offset: read_u32(bytes, &mut cursor)?,
                    residual_len: read_u32(bytes, &mut cursor)?,
                },
                3 => StateOp::Take {
                    start: read_u32(bytes, &mut cursor)?,
                    len: read_u32(bytes, &mut cursor)?,
                },
                4 => StateOp::Drop {
                    start: read_u32(bytes, &mut cursor)?,
                    len: read_u32(bytes, &mut cursor)?,
                },
                other => return Err(StateProgramError::UnknownOp(other)),
            };
            ops.push(op);
        }

        if bytes.len().saturating_sub(cursor) < residual_len {
            return Err(StateProgramError::TruncatedPayload {
                minimum: cursor + residual_len,
                actual: bytes.len(),
            });
        }
        let residual = bytes[cursor..cursor + residual_len].to_vec();
        cursor += residual_len;
        if cursor != bytes.len() {
            return Err(StateProgramError::TrailingBytes(bytes.len() - cursor));
        }

        let program = Self {
            version,
            source_kind,
            refs,
            ops,
            residual,
        };
        program.validate()?;
        Ok(program)
    }

    pub fn validate(&self) -> Result<(), StateProgramError> {
        if self.version != STATE_PROGRAM_VERSION {
            return Err(StateProgramError::InvalidVersion(self.version));
        }
        if self.refs.len() > MAX_STATE_REFS {
            return Err(StateProgramError::TooManyRefs(self.refs.len()));
        }
        if self.ops.len() > MAX_STATE_OPS {
            return Err(StateProgramError::TooManyOps(self.ops.len()));
        }

        for reference in &self.refs {
            if reference.indices.len() > MAX_STATE_INDICES_PER_REF {
                return Err(StateProgramError::TooManyIndices(reference.indices.len()));
            }
            if matches!(reference.kind, RefKind::StridedRange) && reference.step == 0 {
                return Err(StateProgramError::InvalidStep);
            }
            if matches!(reference.kind, RefKind::IndexSet)
                && reference.len != 0
                && reference.len as usize > reference.indices.len()
            {
                return Err(StateProgramError::InvalidRefRange {
                    start: reference.start,
                    len: reference.len,
                    base_len: reference.indices.len(),
                });
            }
        }

        for op in &self.ops {
            match *op {
                StateOp::Concat => {}
                StateOp::Patch {
                    residual_offset,
                    residual_len,
                    ..
                } => {
                    checked_range(self.residual.len(), residual_offset, residual_len).ok_or(
                        StateProgramError::InvalidResidualRange {
                            offset: residual_offset,
                            len: residual_len,
                        },
                    )?;
                }
                StateOp::Take { .. } | StateOp::Drop { .. } => {}
            }
        }

        Ok(())
    }

    pub fn decode_exact_bytes<F>(&self, mut resolve: F) -> Result<Vec<u8>, StateProgramError>
    where
        F: FnMut(RefKind, u64) -> Option<Vec<u8>>,
    {
        self.validate()?;
        let mut materialized_refs = Vec::with_capacity(self.refs.len());
        for reference in &self.refs {
            let base = resolve(reference.kind, reference.base_id)
                .ok_or(StateProgramError::MissingBase(reference.base_id))?;
            materialized_refs.push(materialize_ref(reference, &base)?);
        }

        let mut output = concat_refs(&materialized_refs);
        if self.ops.is_empty() {
            output.extend_from_slice(&self.residual);
            return Ok(output);
        }

        for op in &self.ops {
            match *op {
                StateOp::Concat => {
                    output = concat_refs(&materialized_refs);
                }
                StateOp::Patch {
                    offset,
                    remove_len,
                    residual_offset,
                    residual_len,
                } => {
                    apply_patch_op(
                        &mut output,
                        &self.residual,
                        offset,
                        remove_len,
                        residual_offset,
                        residual_len,
                    )?;
                }
                StateOp::Take { start, len } => {
                    let range = checked_range(output.len(), start, len)
                        .ok_or(StateProgramError::InvalidTakeRange { start, len })?;
                    output = output[range].to_vec();
                }
                StateOp::Drop { start, len } => {
                    let range = checked_range(output.len(), start, len)
                        .ok_or(StateProgramError::InvalidDropRange { start, len })?;
                    output.drain(range);
                }
            }
        }
        Ok(output)
    }

    pub fn decode_exact_state_material<F>(
        &self,
        resolve: F,
    ) -> Result<ExactStateMaterial, StateProgramError>
    where
        F: FnMut(RefKind, u64) -> Option<Vec<u8>>,
    {
        Ok(ExactStateMaterial::new(
            self.source_kind,
            self.decode_exact_bytes(resolve)?,
        ))
    }
}

pub fn reconstruction_node_for_assembly(
    node_id: u32,
    assembly: &Assembly,
) -> ReconstructionGraphNode {
    ReconstructionGraphNode {
        node_id,
        reference: ReconstructionRef::Assembly(AssemblyRef {
            assembly_id: assembly.assembly_id,
            version: assembly.dependency_closure.version,
            dependency_closure: assembly.dependency_closure.dependencies.clone(),
            output_len: assembly.body.output_len(),
            body_shape_hash: assembly.body.body_shape_hash(),
            dependency_fingerprint: crate::assembly_dependency_fingerprint(
                &assembly.dependency_closure.dependencies,
            ),
        }),
        dependencies: assembly
            .dependency_closure
            .dependencies
            .iter()
            .cloned()
            .map(|dependency| {
                ReconstructionRef::Substrate(SubstrateRef::from_dependency(&dependency))
            })
            .collect(),
        output_len: assembly.body.output_len(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StateProgramError {
    #[error("state program payload is truncated: expected at least {minimum}, actual {actual}")]
    TruncatedPayload { minimum: usize, actual: usize },
    #[error("state program has trailing bytes: {0}")]
    TrailingBytes(usize),
    #[error("transform payload serialization failed: {0}")]
    TransformPayloadSerialization(String),
    #[error("invalid transform payload version: {0}")]
    InvalidTransformPayloadVersion(u8),
    #[error("transform residual contract mismatch: offsets={offsets}, payloads={payloads}")]
    TransformResidualContractMismatch { offsets: usize, payloads: usize },
    #[error("missing transform basis: kind={object_kind:?}, id={object_id}")]
    MissingTransformBasis {
        object_kind: ObjectKind,
        object_id: String,
    },
    #[error("transform basis kind mismatch for {0:?}")]
    TransformBasisKindMismatch(TransformKind),
    #[error("invalid transform arity for {0:?}")]
    InvalidTransformArity(TransformKind),
    #[error("invalid transform parameter for {0:?}")]
    InvalidTransformParameter(TransformKind),
    #[error("transform output length mismatch: expected={expected}, actual={actual}")]
    TransformOutputLenMismatch { expected: u32, actual: u32 },
    /// S4.7: Predictive route payload inflates beyond the logical content it
    /// represents. This means the inline definitions, dependency closures,
    /// and PRG graphs make the wire representation larger than the raw data.
    /// The caller should fall back to direct-state transmission instead.
    #[error("predictive route inflation: payload={payload_len} > logical_content={logical_content_len}")]
    PredictiveRouteInflation { payload_len: usize, logical_content_len: usize },
    #[error("transform output violates length contract: min={min}, max={max}, actual={actual}")]
    TransformOutputLenContract { min: u32, max: u32, actual: u32 },
    #[error("invalid state program version: {0}")]
    InvalidVersion(u8),
    #[error("unknown state source kind tag: {0}")]
    UnknownSourceKind(u8),
    #[error("unknown state ref kind tag: {0}")]
    UnknownRefKind(u8),
    #[error("unknown state op tag: {0}")]
    UnknownOp(u8),
    #[error("state program has too many refs: {0}")]
    TooManyRefs(usize),
    #[error("state program has too many ops: {0}")]
    TooManyOps(usize),
    #[error("state ref has too many indices: {0}")]
    TooManyIndices(usize),
    #[error("state ref has invalid zero step")]
    InvalidStep,
    #[error("state base is missing: {0}")]
    MissingBase(u64),
    #[error("state ref kind is not supported by the decoder: {0:?}")]
    UnsupportedRefKind(RefKind),
    #[error("invalid state ref byte range: start={start}, len={len}, base_len={base_len}")]
    InvalidRefRange {
        start: u32,
        len: u32,
        base_len: usize,
    },
    #[error("invalid state ref index: index={index}, base_len={base_len}")]
    InvalidRefIndex { index: u32, base_len: usize },
    #[error("invalid state patch range: offset={offset}, remove_len={remove_len}")]
    InvalidPatchRange { offset: u32, remove_len: u32 },
    #[error("invalid state residual range: offset={offset}, len={len}")]
    InvalidResidualRange { offset: u32, len: u32 },
    #[error("invalid state take range: start={start}, len={len}")]
    InvalidTakeRange { start: u32, len: u32 },
    #[error("invalid state drop range: start={start}, len={len}")]
    InvalidDropRange { start: u32, len: u32 },
    #[error("invalid PRG payload version: {0}")]
    InvalidPrgVersion(u8),
    #[error("PRG serialization failed: {0}")]
    PrgSerialization(String),
    #[error("PRG node count mismatch: declared={declared}, actual={actual}")]
    PrgNodeCountMismatch { declared: usize, actual: usize },
    #[error("PRG missing root node {0}")]
    PrgMissingRoot(u32),
    #[error("PRG missing node {0}")]
    PrgMissingNode(u32),
    #[error("PRG missing child for node {0}")]
    PrgMissingChild(u32),
    #[error("PRG missing dependency {0}")]
    PrgMissingDependency(String),
    #[error("PRG missing reference for node {0} of kind {1:?}")]
    PrgMissingReference(u32, PrgNodeKind),
    #[error("PRG cycle detected at node {0}")]
    PrgCycleDetected(u32),
    #[error("PRG depth exceeded: depth={depth}, cap={cap}")]
    PrgDepthExceeded { depth: u8, cap: u8 },
    #[error(
        "PRG node output length mismatch: node={node_id}, expected={expected}, actual={actual}"
    )]
    PrgNodeOutputLenMismatch {
        node_id: u32,
        expected: u32,
        actual: u32,
    },
    #[error("PRG output length mismatch: expected={expected}, actual={actual}")]
    PrgOutputLenMismatch { expected: u32, actual: u32 },
    #[error("invalid schema payload version: {0}")]
    InvalidSchemaPayloadVersion(u8),
    #[error("schema payload serialization failed: {0}")]
    SchemaPayloadSerialization(String),
    #[error("invalid assembly payload version: {0}")]
    InvalidAssemblyPayloadVersion(u8),
    #[error("assembly payload serialization failed: {0}")]
    AssemblyPayloadSerialization(String),
    #[error("invalid episode payload version: {0}")]
    InvalidEpisodePayloadVersion(u8),
    #[error("episode payload serialization failed: {0}")]
    EpisodePayloadSerialization(String),
    #[error("missing assembly component: kind={object_kind:?}, id={object_id}")]
    MissingAssemblyComponent {
        object_kind: ObjectKind,
        object_id: String,
    },
    #[error("invalid predictive dispatch payload version: {0}")]
    InvalidPredictiveDispatchVersion(u8),
    #[error(
        "invalid predictive route contract: family={route_family:?}, kind={route_kind:?}, expected={expected_route_kind:?}"
    )]
    InvalidPredictiveRouteContract {
        route_family: ControllerRouteFamily,
        route_kind: RouteFamily,
        expected_route_kind: RouteFamily,
    },
    #[error("invalid predictive dispatch contract: {0}")]
    InvalidPredictiveDispatchContract(String),
    #[error("predictive dispatch payload serialization failed: {0}")]
    PredictiveDispatchSerialization(String),
}

fn materialize_ref(reference: &StateRef, base: &[u8]) -> Result<Vec<u8>, StateProgramError> {
    match reference.kind {
        RefKind::Block | RefKind::Range | RefKind::PriorState | RefKind::Bundle => {
            let range = checked_range(base.len(), reference.start, reference.len).ok_or(
                StateProgramError::InvalidRefRange {
                    start: reference.start,
                    len: reference.len,
                    base_len: base.len(),
                },
            )?;
            Ok(base[range].to_vec())
        }
        RefKind::StridedRange => {
            if reference.step == 0 {
                return Err(StateProgramError::InvalidStep);
            }
            let mut out = Vec::with_capacity(reference.len as usize);
            for index in 0..reference.len {
                let offset = reference
                    .start
                    .checked_add(index.saturating_mul(reference.step as u32))
                    .ok_or(StateProgramError::InvalidRefRange {
                        start: reference.start,
                        len: reference.len,
                        base_len: base.len(),
                    })?;
                let byte = base
                    .get(offset as usize)
                    .ok_or(StateProgramError::InvalidRefIndex {
                        index: offset,
                        base_len: base.len(),
                    })?;
                out.push(*byte);
            }
            Ok(out)
        }
        RefKind::IndexSet => {
            let count = if reference.len == 0 {
                reference.indices.len()
            } else {
                reference.len as usize
            };
            if count > reference.indices.len() {
                return Err(StateProgramError::InvalidRefRange {
                    start: reference.start,
                    len: reference.len,
                    base_len: base.len(),
                });
            }
            let mut out = Vec::with_capacity(count);
            for index in reference.indices.iter().take(count) {
                let offset = reference.start.checked_add(*index).ok_or(
                    StateProgramError::InvalidRefIndex {
                        index: *index,
                        base_len: base.len(),
                    },
                )?;
                let byte = base
                    .get(offset as usize)
                    .ok_or(StateProgramError::InvalidRefIndex {
                        index: offset,
                        base_len: base.len(),
                    })?;
                out.push(*byte);
            }
            Ok(out)
        }
    }
}

fn concat_refs(refs: &[Vec<u8>]) -> Vec<u8> {
    let total_len = refs.iter().map(Vec::len).sum();
    let mut out = Vec::with_capacity(total_len);
    for bytes in refs {
        out.extend_from_slice(bytes);
    }
    out
}

fn apply_patch_op(
    output: &mut Vec<u8>,
    residual: &[u8],
    offset: u32,
    remove_len: u32,
    residual_offset: u32,
    residual_len: u32,
) -> Result<(), StateProgramError> {
    let output_range = checked_patch_range(output.len(), offset, remove_len)?;
    let residual_range = checked_range(residual.len(), residual_offset, residual_len).ok_or(
        StateProgramError::InvalidResidualRange {
            offset: residual_offset,
            len: residual_len,
        },
    )?;
    kernel_apply_patch(output, output_range, &residual[residual_range]);
    Ok(())
}

fn kernel_apply_patch(output: &mut Vec<u8>, range: std::ops::Range<usize>, replacement: &[u8]) {
    output.splice(range, replacement.iter().copied());
}

fn kernel_slot_mask_check(total_len: usize, start: usize, len: usize) -> bool {
    if start > total_len || start.saturating_add(len) > total_len {
        return false;
    }
    if total_len <= 64 {
        let start = start.min(63) as u8;
        let end = start.saturating_add(len.min(64) as u8);
        for slot in start..end {
            if !slot_mask_allows(u64::MAX, slot) {
                return false;
            }
        }
    }
    true
}

fn checked_patch_range(
    total_len: usize,
    offset: u32,
    remove_len: u32,
) -> Result<std::ops::Range<usize>, StateProgramError> {
    checked_range(total_len, offset, remove_len)
        .ok_or(StateProgramError::InvalidPatchRange { offset, remove_len })
}

fn checked_range(total_len: usize, start: u32, len: u32) -> Option<std::ops::Range<usize>> {
    let start = start as usize;
    let len = len as usize;
    let end = start.checked_add(len)?;
    if start <= total_len && end <= total_len {
        Some(start..end)
    } else {
        None
    }
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, StateProgramError> {
    if bytes.len().saturating_sub(*cursor) < 1 {
        return Err(StateProgramError::TruncatedPayload {
            minimum: *cursor + 1,
            actual: bytes.len(),
        });
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, StateProgramError> {
    if bytes.len().saturating_sub(*cursor) < 2 {
        return Err(StateProgramError::TruncatedPayload {
            minimum: *cursor + 2,
            actual: bytes.len(),
        });
    }
    let value = u16::from_le_bytes(bytes[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, StateProgramError> {
    if bytes.len().saturating_sub(*cursor) < 4 {
        return Err(StateProgramError::TruncatedPayload {
            minimum: *cursor + 4,
            actual: bytes.len(),
        });
    }
    let value = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, StateProgramError> {
    if bytes.len().saturating_sub(*cursor) < 8 {
        return Err(StateProgramError::TruncatedPayload {
            minimum: *cursor + 8,
            actual: bytes.len(),
        });
    }
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{BundleMember, SharedBlockCatalog};

    use super::*;

    #[test]
    fn residual_only_state_round_trips_exact_bytes() {
        let program = StateProgram::residual_only(SourceKind::Json, b"{\"ok\":true}");
        let encoded = program.encode().unwrap();
        let decoded = StateProgram::decode(&encoded).unwrap();
        let bytes = decoded.decode_exact_bytes(|_, _| None).unwrap();
        assert_eq!(bytes, b"{\"ok\":true}");
    }

    #[test]
    fn concat_state_uses_shared_blocks() {
        let mut blocks = HashMap::new();
        blocks.insert(7, b"hello ".to_vec());
        blocks.insert(9, b"world".to_vec());
        let program = StateProgram::concat_only(
            SourceKind::Text,
            vec![
                StateRef::block(ItemId(7), 0, 6),
                StateRef::block(ItemId(9), 0, 5),
            ],
        );

        let bytes = StateProgram::decode(&program.encode().unwrap())
            .unwrap()
            .decode_exact_bytes(|_, base_id| blocks.get(&base_id).cloned())
            .unwrap();
        assert_eq!(bytes, b"hello world");
    }

    #[test]
    fn patch_state_replaces_bytes_from_residual() {
        let mut blocks = HashMap::new();
        blocks.insert(1, b"{\"count\":41,\"ok\":true}".to_vec());
        let program = StateProgram::single_ref_patch(
            SourceKind::Json,
            StateRef::prior_state(ItemId(1), 0, 22),
            vec![StateOp::patch(10, 1, 0, 1)],
            b"2".to_vec(),
        );

        let bytes = program
            .decode_exact_bytes(|_, base_id| blocks.get(&base_id).cloned())
            .unwrap();
        assert_eq!(bytes, b"{\"count\":42,\"ok\":true}");
    }

    #[test]
    fn index_set_state_selects_stable_byte_positions() {
        let mut blocks = HashMap::new();
        blocks.insert(4, b"abcdef".to_vec());
        let program = StateProgram::new(
            SourceKind::Binary,
            vec![StateRef::index_set(ItemId(4), 0, vec![5, 3, 1])],
            vec![StateOp::Concat],
            Vec::new(),
        );

        let bytes = program
            .decode_exact_bytes(|_, base_id| blocks.get(&base_id).cloned())
            .unwrap();
        assert_eq!(bytes, b"fdb");
    }

    #[test]
    fn bundle_ref_materializes_over_catalog_bundle() {
        let mut catalog = SharedBlockCatalog::default();
        let left = catalog
            .insert_block(SourceKind::Text, b"multi ".to_vec())
            .unwrap();
        let right = catalog
            .insert_block(SourceKind::Text, b"block".to_vec())
            .unwrap();
        let bundle = catalog
            .define_bundle(vec![
                BundleMember::block(left, 0, 6),
                BundleMember::block(right, 0, 5),
            ])
            .unwrap();
        let program = StateProgram::new(
            SourceKind::Text,
            vec![StateRef::bundle(bundle, 0, 11)],
            vec![StateOp::Concat],
            Vec::new(),
        );

        let bytes = program
            .decode_exact_bytes(|kind, base_id| match kind {
                RefKind::Bundle => catalog.materialize_bundle(BundleId(base_id)).ok(),
                _ => None,
            })
            .unwrap();
        assert_eq!(bytes, b"multi block");
    }

    #[test]
    fn composite_state_with_suffix_residual_decodes_exactly() {
        let mut blocks = HashMap::new();
        blocks.insert(11, b"shared prefix ".to_vec());
        blocks.insert(13, b"and middle ".to_vec());

        let program = StateProgram::concat_with_residual(
            SourceKind::Text,
            vec![
                StateRef::block(ItemId(11), 0, 14),
                StateRef::block(ItemId(13), 0, 11),
            ],
            b"tail".to_vec(),
        );

        let bytes = program
            .decode_exact_bytes(|_, base_id| blocks.get(&base_id).cloned())
            .unwrap();
        assert_eq!(bytes, b"shared prefix and middle tail");
    }

    #[test]
    fn range_patch_state_decodes_exactly() {
        let mut blocks = HashMap::new();
        blocks.insert(21, b"123shared-middle456".to_vec());

        let program = StateProgram::single_ref_patch(
            SourceKind::Text,
            StateRef::range(BlockId(21), 3, 13),
            vec![StateOp::patch(0, 0, 0, 7), StateOp::patch(20, 0, 7, 6)],
            b"prefix-suffix".to_vec(),
        );

        let bytes = program
            .decode_exact_bytes(|_, base_id| blocks.get(&base_id).cloned())
            .unwrap();
        assert_eq!(bytes, b"prefix-shared-middlesuffix");
    }

    #[test]
    fn referenced_catalog_dependencies_are_extracted() {
        let program = StateProgram::new(
            SourceKind::Text,
            vec![
                StateRef::catalog_block(BlockId(7), 0, 5),
                StateRef::range(BlockId(8), 3, 7),
                StateRef::bundle(BundleId(9), 0, 12),
                StateRef::bundle(BundleId(9), 0, 12),
                StateRef::prior_state(ItemId(77), 0, 4),
            ],
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(
            program.referenced_catalog_blocks(),
            vec![BlockId(7), BlockId(8)]
        );
        assert_eq!(program.referenced_bundles(), vec![BundleId(9)]);
        assert_eq!(program.referenced_prior_states(), vec![ItemId(77)]);
    }

    #[test]
    fn dependency_flags_are_reported() {
        let program = StateProgram::new(
            SourceKind::Text,
            vec![
                StateRef::catalog_block(BlockId(1), 0, 4),
                StateRef::prior_state(ItemId(2), 0, 4),
            ],
            Vec::new(),
            Vec::new(),
        );
        assert!(program.has_catalog_dependencies());
        assert!(program.has_prior_state_dependencies());
    }

    #[test]
    fn missing_bundle_ref_fails_explicitly() {
        let program = StateProgram::new(
            SourceKind::Text,
            vec![StateRef::bundle(BundleId(99), 0, 1)],
            Vec::new(),
            Vec::new(),
        );
        let error = program.decode_exact_bytes(|_, _| None).unwrap_err();
        assert_eq!(error, StateProgramError::MissingBase(99));
    }

    #[test]
    fn invalid_index_set_len_is_rejected() {
        let program = StateProgram::new(
            SourceKind::Text,
            vec![StateRef {
                kind: RefKind::IndexSet,
                base_id: 1,
                start: 0,
                len: 3,
                step: 1,
                indices: vec![0, 1],
            }],
            Vec::new(),
            Vec::new(),
        );
        assert!(matches!(
            program.validate().unwrap_err(),
            StateProgramError::InvalidRefRange { .. }
        ));
    }
}
