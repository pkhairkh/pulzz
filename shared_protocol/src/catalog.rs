use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ExactStateMaterial, ObjectDependency, ObjectKind, ObjectVersion, PromotionLevel, SourceKind,
};

pub const CATALOG_SYNC_PAYLOAD_VERSION: u8 = 1;
pub const REPAIR_PAYLOAD_VERSION: u8 = 1;
pub const MAX_CATALOG_SYNC_BLOCKS: usize = 4096;
pub const MAX_CATALOG_SYNC_BUNDLES: usize = 1024;
pub const MAX_BUNDLE_MEMBERS: usize = 1024;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct BlockId(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct BundleId(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct BlockCatalogVersion(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ResidualBufferId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SubstrateVersionMeta {
    pub version: ObjectVersion,
    pub dependencies: Vec<ObjectDependency>,
    pub promotion_level: PromotionLevel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtomFragment {
    pub fragment_id: BlockId,
    pub version_meta: SubstrateVersionMeta,
    pub source_kind: SourceKind,
    pub byte_offset: u32,
    pub byte_len: u32,
    pub content_hash: [u8; 32],
    pub material: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactBlockObject {
    pub block_id: BlockId,
    pub version_meta: SubstrateVersionMeta,
    pub source_kind: SourceKind,
    pub byte_len: u32,
    pub content_hash: [u8; 32],
    pub material: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactBundleObject {
    pub bundle_id: BundleId,
    pub version_meta: SubstrateVersionMeta,
    pub members: Vec<BundleMember>,
    pub byte_len: u32,
    pub content_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactRangeObject {
    pub range_id: u64,
    pub version_meta: SubstrateVersionMeta,
    pub base_block_id: BlockId,
    pub start: u32,
    pub byte_len: u32,
    pub content_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualBufferObject {
    pub residual_id: ResidualBufferId,
    pub version_meta: SubstrateVersionMeta,
    pub byte_len: u32,
    pub content_hash: [u8; 32],
    pub material: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AtomSubstrateObject {
    AtomFragment(AtomFragment),
    ExactBlock(ExactBlockObject),
    ExactBundle(ExactBundleObject),
    ExactRange(ExactRangeObject),
    ResidualBuffer(ResidualBufferObject),
}

impl AtomSubstrateObject {
    pub const fn object_kind(&self) -> ObjectKind {
        match self {
            Self::AtomFragment(_) => ObjectKind::AtomFragment,
            Self::ExactBlock(_) => ObjectKind::ExactBlock,
            Self::ExactBundle(_) => ObjectKind::ExactBundle,
            Self::ExactRange(_) => ObjectKind::ExactRange,
            Self::ResidualBuffer(_) => ObjectKind::ResidualBuffer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockCatalogEntry {
    pub block_id: BlockId,
    pub source_kind: SourceKind,
    pub byte_len: u32,
    pub content_hash: [u8; 32],
    pub material: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleMember {
    pub block_id: BlockId,
    pub start: u32,
    pub len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleCatalogEntry {
    pub bundle_id: BundleId,
    pub members: Vec<BundleMember>,
    pub byte_len: u32,
    pub content_hash: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogPrefixCandidate {
    Block { block_id: BlockId, byte_len: u32 },
    Bundle { bundle_id: BundleId, byte_len: u32 },
}

impl CatalogPrefixCandidate {
    pub fn byte_len(self) -> u32 {
        match self {
            Self::Block { byte_len, .. } | Self::Bundle { byte_len, .. } => byte_len,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogWindowCandidate {
    Block {
        block_id: BlockId,
        base_start: u32,
        byte_len: u32,
    },
    Bundle {
        bundle_id: BundleId,
        base_start: u32,
        byte_len: u32,
    },
}

impl CatalogWindowCandidate {
    pub fn base_start(self) -> u32 {
        match self {
            Self::Block { base_start, .. } | Self::Bundle { base_start, .. } => base_start,
        }
    }

    pub fn byte_len(self) -> u32 {
        match self {
            Self::Block { byte_len, .. } | Self::Bundle { byte_len, .. } => byte_len,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSyncPayload {
    pub payload_version: u8,
    pub catalog_version: BlockCatalogVersion,
    pub blocks: Vec<BlockCatalogEntry>,
    pub bundles: Vec<BundleCatalogEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CatalogSyncSummary {
    pub catalog_version: BlockCatalogVersion,
    pub block_count: usize,
    pub bundle_count: usize,
    pub exact_block_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairCause {
    MissingBasis,
    CatalogVersionMismatch {
        local: BlockCatalogVersion,
        required: BlockCatalogVersion,
    },
    UnsupportedStateReference {
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairRequest {
    pub catalog_version: BlockCatalogVersion,
    pub missing_blocks: Vec<BlockId>,
    pub missing_bundles: Vec<BundleId>,
    pub cause: RepairCause,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairPayload {
    Request {
        payload_version: u8,
        request: RepairRequest,
    },
    Response {
        payload_version: u8,
        sync: CatalogSyncPayload,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedBlockCatalog {
    version: BlockCatalogVersion,
    blocks: BTreeMap<BlockId, BlockCatalogEntry>,
    bundles: BTreeMap<BundleId, BundleCatalogEntry>,
}

impl Default for SharedBlockCatalog {
    fn default() -> Self {
        Self {
            version: BlockCatalogVersion(0),
            blocks: BTreeMap::new(),
            bundles: BTreeMap::new(),
        }
    }
}

impl AtomFragment {
    pub fn from_bytes(
        source_kind: SourceKind,
        byte_offset: u32,
        material: Vec<u8>,
    ) -> Result<Self, CatalogError> {
        let byte_len = checked_u32_len(material.len())?;
        let content_hash = content_hash(&material);
        Ok(Self {
            fragment_id: derive_block_id(source_kind, &content_hash),
            version_meta: SubstrateVersionMeta::default(),
            source_kind,
            byte_offset,
            byte_len,
            content_hash,
            material,
        })
    }
}

impl ExactBlockObject {
    pub fn from_bytes(source_kind: SourceKind, material: Vec<u8>) -> Result<Self, CatalogError> {
        let byte_len = checked_u32_len(material.len())?;
        let content_hash = content_hash(&material);
        Ok(Self {
            block_id: derive_block_id(source_kind, &content_hash),
            version_meta: SubstrateVersionMeta::default(),
            source_kind,
            byte_len,
            content_hash,
            material,
        })
    }
}

impl ExactRangeObject {
    pub fn new(base_block_id: BlockId, start: u32, material: &[u8]) -> Result<Self, CatalogError> {
        let byte_len = checked_u32_len(material.len())?;
        let content_hash = content_hash(material);
        Ok(Self {
            range_id: derive_range_id(base_block_id, start, byte_len, &content_hash),
            version_meta: SubstrateVersionMeta::default(),
            base_block_id,
            start,
            byte_len,
            content_hash,
        })
    }
}

impl ResidualBufferObject {
    pub fn from_bytes(material: Vec<u8>) -> Result<Self, CatalogError> {
        let byte_len = checked_u32_len(material.len())?;
        let content_hash = content_hash(&material);
        Ok(Self {
            residual_id: ResidualBufferId(derive_residual_id(&content_hash)),
            version_meta: SubstrateVersionMeta::default(),
            byte_len,
            content_hash,
            material,
        })
    }
}

impl BlockCatalogEntry {
    pub fn from_bytes(source_kind: SourceKind, material: Vec<u8>) -> Result<Self, CatalogError> {
        let byte_len = checked_u32_len(material.len())?;
        let content_hash = content_hash(&material);
        let block_id = derive_block_id(source_kind, &content_hash);
        Ok(Self {
            block_id,
            source_kind,
            byte_len,
            content_hash,
            material,
        })
    }

    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.block_id.0 == 0 {
            return Err(CatalogError::InvalidBlockId(self.block_id));
        }
        if self.byte_len as usize != self.material.len() {
            return Err(CatalogError::BlockLenMismatch {
                block_id: self.block_id,
                expected: self.byte_len as usize,
                actual: self.material.len(),
            });
        }
        let content_hash = content_hash(&self.material);
        if content_hash != self.content_hash {
            return Err(CatalogError::BlockHashMismatch(self.block_id));
        }
        let expected_id = derive_block_id(self.source_kind, &self.content_hash);
        if expected_id != self.block_id {
            return Err(CatalogError::BlockIdMismatch {
                expected: expected_id,
                actual: self.block_id,
            });
        }
        Ok(())
    }
}

impl BundleMember {
    pub fn block(block_id: BlockId, start: u32, len: u32) -> Self {
        Self {
            block_id,
            start,
            len,
        }
    }
}

impl BundleCatalogEntry {
    pub fn from_members(
        members: Vec<BundleMember>,
        catalog: &SharedBlockCatalog,
    ) -> Result<Self, CatalogError> {
        if members.is_empty() {
            return Err(CatalogError::EmptyBundle);
        }
        if members.len() > MAX_BUNDLE_MEMBERS {
            return Err(CatalogError::TooManyBundleMembers(members.len()));
        }
        let material = catalog.materialize_members(&members)?;
        let byte_len = checked_u32_len(material.len())?;
        let content_hash = content_hash(&material);
        let bundle_id = derive_bundle_id(&members, &content_hash);
        Ok(Self {
            bundle_id,
            members,
            byte_len,
            content_hash,
        })
    }

    pub fn validate_with_catalog(&self, catalog: &SharedBlockCatalog) -> Result<(), CatalogError> {
        if self.bundle_id.0 == 0 {
            return Err(CatalogError::InvalidBundleId(self.bundle_id));
        }
        if self.members.is_empty() {
            return Err(CatalogError::EmptyBundle);
        }
        if self.members.len() > MAX_BUNDLE_MEMBERS {
            return Err(CatalogError::TooManyBundleMembers(self.members.len()));
        }
        let material = catalog.materialize_members(&self.members)?;
        if self.byte_len as usize != material.len() {
            return Err(CatalogError::BundleLenMismatch {
                bundle_id: self.bundle_id,
                expected: self.byte_len as usize,
                actual: material.len(),
            });
        }
        let content_hash = content_hash(&material);
        if content_hash != self.content_hash {
            return Err(CatalogError::BundleHashMismatch(self.bundle_id));
        }
        let expected_id = derive_bundle_id(&self.members, &self.content_hash);
        if expected_id != self.bundle_id {
            return Err(CatalogError::BundleIdMismatch {
                expected: expected_id,
                actual: self.bundle_id,
            });
        }
        Ok(())
    }
}

impl CatalogSyncPayload {
    pub fn new(
        catalog_version: BlockCatalogVersion,
        blocks: Vec<BlockCatalogEntry>,
        bundles: Vec<BundleCatalogEntry>,
    ) -> Self {
        Self {
            payload_version: CATALOG_SYNC_PAYLOAD_VERSION,
            catalog_version,
            blocks,
            bundles,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        self.validate_basic()?;
        serde_json::to_vec(self).map_err(|error| CatalogError::Serde(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| CatalogError::Serde(error.to_string()))?;
        payload.validate_basic()?;
        Ok(payload)
    }

    pub fn validate_basic(&self) -> Result<(), CatalogError> {
        if self.payload_version != CATALOG_SYNC_PAYLOAD_VERSION {
            return Err(CatalogError::InvalidCatalogPayloadVersion(
                self.payload_version,
            ));
        }
        if self.blocks.len() > MAX_CATALOG_SYNC_BLOCKS {
            return Err(CatalogError::TooManySyncBlocks(self.blocks.len()));
        }
        if self.bundles.len() > MAX_CATALOG_SYNC_BUNDLES {
            return Err(CatalogError::TooManySyncBundles(self.bundles.len()));
        }
        let mut block_ids = BTreeSet::new();
        for block in &self.blocks {
            block.validate()?;
            if !block_ids.insert(block.block_id) {
                return Err(CatalogError::DuplicateBlock(block.block_id));
            }
        }
        let mut bundle_ids = BTreeSet::new();
        for bundle in &self.bundles {
            if !bundle_ids.insert(bundle.bundle_id) {
                return Err(CatalogError::DuplicateBundle(bundle.bundle_id));
            }
        }
        Ok(())
    }
}

impl RepairRequest {
    pub fn missing_basis(
        catalog_version: BlockCatalogVersion,
        missing_blocks: Vec<BlockId>,
        missing_bundles: Vec<BundleId>,
    ) -> Self {
        Self {
            catalog_version,
            missing_blocks,
            missing_bundles,
            cause: RepairCause::MissingBasis,
        }
    }

    pub fn validate(&self) -> Result<(), CatalogError> {
        ensure_unique_blocks(&self.missing_blocks)?;
        ensure_unique_bundles(&self.missing_bundles)?;
        if self.missing_blocks.is_empty()
            && self.missing_bundles.is_empty()
            && matches!(self.cause, RepairCause::MissingBasis)
        {
            return Err(CatalogError::EmptyRepairRequest);
        }
        Ok(())
    }
}

impl RepairPayload {
    pub fn request(request: RepairRequest) -> Self {
        Self::Request {
            payload_version: REPAIR_PAYLOAD_VERSION,
            request,
        }
    }

    pub fn response(sync: CatalogSyncPayload) -> Self {
        Self::Response {
            payload_version: REPAIR_PAYLOAD_VERSION,
            sync,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| CatalogError::Serde(error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogError> {
        let payload: Self = serde_json::from_slice(bytes)
            .map_err(|error| CatalogError::Serde(error.to_string()))?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<(), CatalogError> {
        match self {
            RepairPayload::Request {
                payload_version,
                request,
            } => {
                if *payload_version != REPAIR_PAYLOAD_VERSION {
                    return Err(CatalogError::InvalidRepairPayloadVersion(*payload_version));
                }
                request.validate()
            }
            RepairPayload::Response {
                payload_version,
                sync,
            } => {
                if *payload_version != REPAIR_PAYLOAD_VERSION {
                    return Err(CatalogError::InvalidRepairPayloadVersion(*payload_version));
                }
                sync.validate_basic()
            }
        }
    }
}

impl SharedBlockCatalog {
    pub fn version(&self) -> BlockCatalogVersion {
        self.version
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn bundle_count(&self) -> usize {
        self.bundles.len()
    }

    pub fn contains_block(&self, block_id: BlockId) -> bool {
        self.blocks.contains_key(&block_id)
    }

    pub fn contains_bundle(&self, bundle_id: BundleId) -> bool {
        self.bundles.contains_key(&bundle_id)
    }

    pub fn block(&self, block_id: BlockId) -> Option<&BlockCatalogEntry> {
        self.blocks.get(&block_id)
    }

    pub fn bundle(&self, bundle_id: BundleId) -> Option<&BundleCatalogEntry> {
        self.bundles.get(&bundle_id)
    }

    pub fn blocks_iter(&self) -> impl Iterator<Item = &BlockCatalogEntry> {
        self.blocks.values()
    }

    pub fn bundles_iter(&self) -> impl Iterator<Item = &BundleCatalogEntry> {
        self.bundles.values()
    }

    pub fn exact_block_for_material(&self, material: &[u8]) -> Option<&BlockCatalogEntry> {
        self.blocks
            .values()
            .find(|entry| entry.material == material)
    }

    pub fn exact_bundle_for_material(&self, material: &[u8]) -> Option<&BundleCatalogEntry> {
        self.bundles.values().find(|bundle| {
            bundle.byte_len as usize == material.len()
                && self
                    .materialize_bundle(bundle.bundle_id)
                    .map(|bytes| bytes == material)
                    .unwrap_or(false)
        })
    }

    pub fn prefix_candidates(
        &self,
        prefix: &[u8],
        min_len: usize,
        limit: usize,
    ) -> Vec<CatalogPrefixCandidate> {
        let mut out = Vec::new();

        for block in self.blocks.values() {
            if block.material.len() >= min_len && prefix.starts_with(&block.material) {
                out.push(CatalogPrefixCandidate::Block {
                    block_id: block.block_id,
                    byte_len: block.byte_len,
                });
            }
        }

        for bundle in self.bundles.values() {
            if bundle.byte_len as usize >= min_len
                && self
                    .materialize_bundle(bundle.bundle_id)
                    .map(|material| prefix.starts_with(&material))
                    .unwrap_or(false)
            {
                out.push(CatalogPrefixCandidate::Bundle {
                    bundle_id: bundle.bundle_id,
                    byte_len: bundle.byte_len,
                });
            }
        }

        out.sort_by(|left, right| {
            right
                .byte_len()
                .cmp(&left.byte_len())
                .then_with(|| match (left, right) {
                    (
                        CatalogPrefixCandidate::Bundle { .. },
                        CatalogPrefixCandidate::Block { .. },
                    ) => std::cmp::Ordering::Less,
                    (
                        CatalogPrefixCandidate::Block { .. },
                        CatalogPrefixCandidate::Bundle { .. },
                    ) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                })
        });
        out.truncate(limit);
        out
    }

    pub fn window_candidates(
        &self,
        payload: &[u8],
        min_len: usize,
        limit: usize,
    ) -> Vec<CatalogWindowCandidate> {
        let mut out = Vec::new();

        for block in self.blocks.values() {
            if block.material.len() < min_len {
                continue;
            }
            if let Some((base_start, len)) =
                longest_common_window(payload, &block.material, min_len)
            {
                out.push(CatalogWindowCandidate::Block {
                    block_id: block.block_id,
                    base_start: base_start as u32,
                    byte_len: len as u32,
                });
            }
        }

        for bundle in self.bundles.values() {
            if (bundle.byte_len as usize) < min_len {
                continue;
            }
            let Ok(material) = self.materialize_bundle(bundle.bundle_id) else {
                continue;
            };
            if let Some((base_start, len)) = longest_common_window(payload, &material, min_len) {
                out.push(CatalogWindowCandidate::Bundle {
                    bundle_id: bundle.bundle_id,
                    base_start: base_start as u32,
                    byte_len: len as u32,
                });
            }
        }

        out.sort_by(|left, right| {
            right
                .byte_len()
                .cmp(&left.byte_len())
                .then_with(|| left.base_start().cmp(&right.base_start()))
                .then_with(|| match (left, right) {
                    (
                        CatalogWindowCandidate::Bundle { .. },
                        CatalogWindowCandidate::Block { .. },
                    ) => std::cmp::Ordering::Less,
                    (
                        CatalogWindowCandidate::Block { .. },
                        CatalogWindowCandidate::Bundle { .. },
                    ) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                })
        });
        out.truncate(limit);
        out
    }

    pub fn insert_block_entry(
        &mut self,
        entry: BlockCatalogEntry,
    ) -> Result<BlockId, CatalogError> {
        entry.validate()?;
        if let Some(existing) = self.blocks.get(&entry.block_id) {
            if existing != &entry {
                return Err(CatalogError::ConflictingBlock(entry.block_id));
            }
        }
        let block_id = entry.block_id;
        self.blocks.insert(block_id, entry);
        Ok(block_id)
    }

    pub fn insert_block(
        &mut self,
        source_kind: SourceKind,
        material: Vec<u8>,
    ) -> Result<BlockId, CatalogError> {
        self.insert_block_entry(BlockCatalogEntry::from_bytes(source_kind, material)?)
    }

    pub fn insert_exact_material(
        &mut self,
        material: &ExactStateMaterial,
    ) -> Result<BlockId, CatalogError> {
        self.insert_block_entry(BlockCatalogEntry::from_bytes(
            material.source_kind,
            material.exact_bytes.clone(),
        )?)
    }

    pub fn insert_bundle_entry(
        &mut self,
        entry: BundleCatalogEntry,
    ) -> Result<BundleId, CatalogError> {
        entry.validate_with_catalog(self)?;
        if let Some(existing) = self.bundles.get(&entry.bundle_id) {
            if existing != &entry {
                return Err(CatalogError::ConflictingBundle(entry.bundle_id));
            }
        }
        let bundle_id = entry.bundle_id;
        self.bundles.insert(bundle_id, entry);
        Ok(bundle_id)
    }

    pub fn define_bundle(&mut self, members: Vec<BundleMember>) -> Result<BundleId, CatalogError> {
        let entry = BundleCatalogEntry::from_members(members, self)?;
        self.insert_bundle_entry(entry)
    }

    pub fn materialize_block_range(
        &self,
        block_id: BlockId,
        start: u32,
        len: u32,
    ) -> Result<Vec<u8>, CatalogError> {
        let block = self
            .blocks
            .get(&block_id)
            .ok_or(CatalogError::MissingBlock(block_id))?;
        let range = checked_range(block.material.len(), start, len).ok_or(
            CatalogError::InvalidBlockRange {
                block_id,
                start,
                len,
                block_len: block.material.len(),
            },
        )?;
        Ok(block.material[range].to_vec())
    }

    pub fn materialize_bundle(&self, bundle_id: BundleId) -> Result<Vec<u8>, CatalogError> {
        let bundle = self
            .bundles
            .get(&bundle_id)
            .ok_or(CatalogError::MissingBundle(bundle_id))?;
        let material = self.materialize_members(&bundle.members)?;
        if material.len() != bundle.byte_len as usize
            || content_hash(&material) != bundle.content_hash
        {
            return Err(CatalogError::BundleHashMismatch(bundle_id));
        }
        Ok(material)
    }

    pub fn apply_sync(
        &mut self,
        payload: CatalogSyncPayload,
    ) -> Result<CatalogSyncSummary, CatalogError> {
        payload.validate_basic()?;
        if payload.catalog_version < self.version {
            return Err(CatalogError::StaleCatalogVersion {
                current: self.version,
                incoming: payload.catalog_version,
            });
        }

        let mut next = self.clone();
        for block in payload.blocks.iter().cloned() {
            next.insert_block_entry(block)?;
        }
        for bundle in payload.bundles.iter().cloned() {
            next.insert_bundle_entry(bundle)?;
        }
        next.version = payload.catalog_version;

        let summary = CatalogSyncSummary {
            catalog_version: next.version,
            block_count: payload.blocks.len(),
            bundle_count: payload.bundles.len(),
            exact_block_bytes: payload
                .blocks
                .iter()
                .map(|block| block.material.len())
                .sum(),
        };
        *self = next;
        Ok(summary)
    }

    pub fn sync_payload(
        &self,
        catalog_version: BlockCatalogVersion,
        block_ids: &[BlockId],
        bundle_ids: &[BundleId],
    ) -> Result<CatalogSyncPayload, CatalogError> {
        let mut blocks = BTreeMap::<BlockId, BlockCatalogEntry>::new();
        let mut bundles = Vec::new();

        for block_id in block_ids {
            let block = self
                .blocks
                .get(block_id)
                .ok_or(CatalogError::MissingBlock(*block_id))?;
            blocks.insert(*block_id, block.clone());
        }

        for bundle_id in bundle_ids {
            let bundle = self
                .bundles
                .get(bundle_id)
                .ok_or(CatalogError::MissingBundle(*bundle_id))?;
            for member in &bundle.members {
                let block = self
                    .blocks
                    .get(&member.block_id)
                    .ok_or(CatalogError::MissingBlock(member.block_id))?;
                blocks.insert(member.block_id, block.clone());
            }
            bundles.push(bundle.clone());
        }

        Ok(CatalogSyncPayload::new(
            catalog_version,
            blocks.into_values().collect(),
            bundles,
        ))
    }

    pub fn repair_response(
        &self,
        request: &RepairRequest,
        catalog_version: BlockCatalogVersion,
    ) -> Result<RepairPayload, CatalogError> {
        request.validate()?;
        Ok(RepairPayload::response(self.sync_payload(
            catalog_version,
            &request.missing_blocks,
            &request.missing_bundles,
        )?))
    }

    fn materialize_members(&self, members: &[BundleMember]) -> Result<Vec<u8>, CatalogError> {
        let total_len = members
            .iter()
            .map(|member| member.len as usize)
            .sum::<usize>();
        let mut out = Vec::with_capacity(total_len);
        for member in members {
            out.extend_from_slice(&self.materialize_block_range(
                member.block_id,
                member.start,
                member.len,
            )?);
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CatalogError {
    #[error("catalog payload serde error: {0}")]
    Serde(String),
    #[error("invalid catalog sync payload version: {0}")]
    InvalidCatalogPayloadVersion(u8),
    #[error("invalid repair payload version: {0}")]
    InvalidRepairPayloadVersion(u8),
    #[error("catalog payload is too large")]
    PayloadTooLarge,
    #[error("catalog sync has too many blocks: {0}")]
    TooManySyncBlocks(usize),
    #[error("catalog sync has too many bundles: {0}")]
    TooManySyncBundles(usize),
    #[error("bundle has too many members: {0}")]
    TooManyBundleMembers(usize),
    #[error("block id is invalid: {0:?}")]
    InvalidBlockId(BlockId),
    #[error("bundle id is invalid: {0:?}")]
    InvalidBundleId(BundleId),
    #[error("empty bundle")]
    EmptyBundle,
    #[error("duplicate block in payload: {0:?}")]
    DuplicateBlock(BlockId),
    #[error("duplicate bundle in payload: {0:?}")]
    DuplicateBundle(BundleId),
    #[error("conflicting block already exists: {0:?}")]
    ConflictingBlock(BlockId),
    #[error("conflicting bundle already exists: {0:?}")]
    ConflictingBundle(BundleId),
    #[error("block length mismatch for {block_id:?}: expected {expected}, actual {actual}")]
    BlockLenMismatch {
        block_id: BlockId,
        expected: usize,
        actual: usize,
    },
    #[error("bundle length mismatch for {bundle_id:?}: expected {expected}, actual {actual}")]
    BundleLenMismatch {
        bundle_id: BundleId,
        expected: usize,
        actual: usize,
    },
    #[error("block hash mismatch: {0:?}")]
    BlockHashMismatch(BlockId),
    #[error("bundle hash mismatch: {0:?}")]
    BundleHashMismatch(BundleId),
    #[error("block id mismatch: expected {expected:?}, actual {actual:?}")]
    BlockIdMismatch { expected: BlockId, actual: BlockId },
    #[error("bundle id mismatch: expected {expected:?}, actual {actual:?}")]
    BundleIdMismatch {
        expected: BundleId,
        actual: BundleId,
    },
    #[error("missing block: {0:?}")]
    MissingBlock(BlockId),
    #[error("missing bundle: {0:?}")]
    MissingBundle(BundleId),
    #[error(
        "invalid block range for {block_id:?}: start={start}, len={len}, block_len={block_len}"
    )]
    InvalidBlockRange {
        block_id: BlockId,
        start: u32,
        len: u32,
        block_len: usize,
    },
    #[error("stale catalog version: current={current:?}, incoming={incoming:?}")]
    StaleCatalogVersion {
        current: BlockCatalogVersion,
        incoming: BlockCatalogVersion,
    },
    #[error("empty missing-basis repair request")]
    EmptyRepairRequest,
}

pub fn derive_catalog_block_id(source_kind: SourceKind, material: &[u8]) -> BlockId {
    derive_block_id(source_kind, &content_hash(material))
}

fn content_hash(material: &[u8]) -> [u8; 32] {
    Sha256::digest(material).into()
}

fn derive_residual_id(content_hash: &[u8; 32]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(&content_hash[..8]);
    u64::from_le_bytes(value)
}

fn derive_range_id(
    base_block_id: BlockId,
    start: u32,
    byte_len: u32,
    content_hash: &[u8; 32],
) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(base_block_id.0.to_le_bytes());
    hasher.update(start.to_le_bytes());
    hasher.update(byte_len.to_le_bytes());
    hasher.update(content_hash);
    let digest = hasher.finalize();
    let mut value = [0_u8; 8];
    value.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(value)
}

fn derive_block_id(source_kind: SourceKind, content_hash: &[u8; 32]) -> BlockId {
    let mut hasher = Sha256::new();
    hasher.update(b"pulzz:block:v1");
    hasher.update([source_kind.tag()]);
    hasher.update(content_hash);
    BlockId(nonzero_u64_from_hash(hasher.finalize().as_slice()))
}

fn derive_bundle_id(members: &[BundleMember], content_hash: &[u8; 32]) -> BundleId {
    let mut hasher = Sha256::new();
    hasher.update(b"pulzz:bundle:v1");
    for member in members {
        hasher.update(member.block_id.0.to_le_bytes());
        hasher.update(member.start.to_le_bytes());
        hasher.update(member.len.to_le_bytes());
    }
    hasher.update(content_hash);
    BundleId(nonzero_u64_from_hash(hasher.finalize().as_slice()))
}

fn nonzero_u64_from_hash(hash: &[u8]) -> u64 {
    let mut id = u64::from_le_bytes(hash[..8].try_into().unwrap());
    if id == 0 {
        id = 1;
    }
    id
}

fn checked_u32_len(len: usize) -> Result<u32, CatalogError> {
    u32::try_from(len).map_err(|_| CatalogError::PayloadTooLarge)
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

fn ensure_unique_blocks(blocks: &[BlockId]) -> Result<(), CatalogError> {
    let mut seen = BTreeSet::new();
    for block_id in blocks {
        if !seen.insert(*block_id) {
            return Err(CatalogError::DuplicateBlock(*block_id));
        }
    }
    Ok(())
}

fn ensure_unique_bundles(bundles: &[BundleId]) -> Result<(), CatalogError> {
    let mut seen = BTreeSet::new();
    for bundle_id in bundles {
        if !seen.insert(*bundle_id) {
            return Err(CatalogError::DuplicateBundle(*bundle_id));
        }
    }
    Ok(())
}

fn longest_common_window(left: &[u8], right: &[u8], min_len: usize) -> Option<(usize, usize)> {
    if left.is_empty() || right.is_empty() || min_len == 0 {
        return None;
    }

    let mut best: Option<(usize, usize)> = None;

    for left_start in 0..left.len() {
        let left_remaining = left.len() - left_start;
        if left_remaining < min_len {
            break;
        }

        let current_best_len = best.map(|(_, len)| len).unwrap_or(0);
        if left_remaining <= current_best_len {
            break;
        }

        for right_start in 0..right.len() {
            let right_remaining = right.len() - right_start;
            if right_remaining < min_len {
                break;
            }
            if right_remaining <= current_best_len {
                continue;
            }

            let len = common_prefix_len(&left[left_start..], &right[right_start..]);
            if len < min_len {
                continue;
            }

            match best {
                Some((best_start, best_len)) => {
                    if len > best_len || (len == best_len && right_start < best_start) {
                        best = Some((right_start, len));
                    }
                }
                None => best = Some((right_start, len)),
            }
        }
    }

    best
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    kernel_common_prefix_scan(left, right)
}

fn kernel_common_prefix_scan(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(a, b)| a == b)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_block(bytes: &[u8]) -> BlockCatalogEntry {
        BlockCatalogEntry::from_bytes(SourceKind::Text, bytes.to_vec()).unwrap()
    }

    #[test]
    fn valid_sync_updates_catalog_view() {
        let block = text_block(b"hello");
        let payload = CatalogSyncPayload::new(BlockCatalogVersion(1), vec![block.clone()], vec![]);
        let mut catalog = SharedBlockCatalog::default();
        let summary = catalog.apply_sync(payload).unwrap();
        assert_eq!(summary.block_count, 1);
        assert_eq!(catalog.version(), BlockCatalogVersion(1));
        assert_eq!(
            catalog
                .materialize_block_range(block.block_id, 0, block.byte_len)
                .unwrap(),
            b"hello"
        );
    }

    #[test]
    fn regressive_sync_is_rejected() {
        let mut catalog = SharedBlockCatalog::default();
        catalog
            .apply_sync(CatalogSyncPayload::new(
                BlockCatalogVersion(2),
                vec![text_block(b"hello")],
                vec![],
            ))
            .unwrap();
        let err = catalog
            .apply_sync(CatalogSyncPayload::new(
                BlockCatalogVersion(1),
                vec![text_block(b"world")],
                vec![],
            ))
            .unwrap_err();
        assert!(matches!(err, CatalogError::StaleCatalogVersion { .. }));
    }

    #[test]
    fn bundle_materialization_is_ordered_and_exact() {
        let mut catalog = SharedBlockCatalog::default();
        let hello = catalog
            .insert_block(SourceKind::Text, b"hello ".to_vec())
            .unwrap();
        let world = catalog
            .insert_block(SourceKind::Text, b"world".to_vec())
            .unwrap();
        let bundle = catalog
            .define_bundle(vec![
                BundleMember::block(hello, 0, 6),
                BundleMember::block(world, 0, 5),
            ])
            .unwrap();
        assert_eq!(catalog.materialize_bundle(bundle).unwrap(), b"hello world");
    }

    #[test]
    fn invalid_bundle_fails_validation() {
        let mut catalog = SharedBlockCatalog::default();
        let block = catalog
            .insert_block(SourceKind::Text, b"hello".to_vec())
            .unwrap();
        let err = catalog
            .define_bundle(vec![BundleMember::block(block, 0, 99)])
            .unwrap_err();
        assert!(matches!(err, CatalogError::InvalidBlockRange { .. }));
    }

    #[test]
    fn repair_response_carries_exact_requested_material() {
        let mut catalog = SharedBlockCatalog::default();
        let block = catalog
            .insert_block(SourceKind::Text, b"repair me".to_vec())
            .unwrap();
        let request = RepairRequest::missing_basis(BlockCatalogVersion(0), vec![block], Vec::new());
        let payload = catalog
            .repair_response(&request, BlockCatalogVersion(4))
            .unwrap();
        let mut restored = SharedBlockCatalog::default();
        match payload {
            RepairPayload::Response { sync, .. } => {
                restored.apply_sync(sync).unwrap();
            }
            RepairPayload::Request { .. } => panic!("expected repair response"),
        }
        assert_eq!(
            restored.materialize_block_range(block, 0, 9).unwrap(),
            b"repair me"
        );
    }

    #[test]
    fn prefix_candidates_prefer_longer_matches() {
        let mut catalog = SharedBlockCatalog::default();
        let small = catalog
            .insert_block(SourceKind::Text, b"shared ".to_vec())
            .unwrap();
        let large = catalog
            .insert_block(SourceKind::Text, b"shared prefix ".to_vec())
            .unwrap();

        let candidates = catalog.prefix_candidates(b"shared prefix tail", 4, 8);
        assert!(!candidates.is_empty());
        assert_eq!(
            candidates[0],
            CatalogPrefixCandidate::Block {
                block_id: large,
                byte_len: 14
            }
        );
        assert!(candidates.iter().any(|candidate| {
            *candidate
                == CatalogPrefixCandidate::Block {
                    block_id: small,
                    byte_len: 7,
                }
        }));
    }

    #[test]
    fn exact_material_lookup_finds_block_and_bundle() {
        let mut catalog = SharedBlockCatalog::default();
        let left = catalog
            .insert_block(SourceKind::Text, b"hello ".to_vec())
            .unwrap();
        let right = catalog
            .insert_block(SourceKind::Text, b"world".to_vec())
            .unwrap();
        let bundle = catalog
            .define_bundle(vec![
                BundleMember::block(left, 0, 6),
                BundleMember::block(right, 0, 5),
            ])
            .unwrap();

        assert_eq!(
            catalog
                .exact_block_for_material(b"hello ")
                .unwrap()
                .block_id,
            left
        );
        assert_eq!(
            catalog
                .exact_bundle_for_material(b"hello world")
                .unwrap()
                .bundle_id,
            bundle
        );
    }

    #[test]
    fn window_candidates_find_internal_match_in_block() {
        let mut catalog = SharedBlockCatalog::default();
        let block = catalog
            .insert_block(SourceKind::Text, b"123--shared-middle--456".to_vec())
            .unwrap();

        let candidates = catalog.window_candidates(b"xxshared-middleyy", 6, 8);
        assert!(!candidates.is_empty());
        assert_eq!(
            candidates[0],
            CatalogWindowCandidate::Block {
                block_id: block,
                base_start: 5,
                byte_len: 13
            }
        );
    }

    #[test]
    fn window_candidates_find_internal_match_in_bundle() {
        let mut catalog = SharedBlockCatalog::default();
        let left = catalog
            .insert_block(SourceKind::Text, b"aaa-shared".to_vec())
            .unwrap();
        let right = catalog
            .insert_block(SourceKind::Text, b"-middle-bbb".to_vec())
            .unwrap();
        let bundle = catalog
            .define_bundle(vec![
                BundleMember::block(left, 0, 10),
                BundleMember::block(right, 0, 11),
            ])
            .unwrap();

        let candidates = catalog.window_candidates(b"zzshared-middleqq", 6, 8);
        assert!(!candidates.is_empty());
        assert!(candidates.iter().any(|candidate| {
            *candidate
                == CatalogWindowCandidate::Bundle {
                    bundle_id: bundle,
                    base_start: 4,
                    byte_len: 13,
                }
        }));
    }
}

pub fn extract_catalog_assembly_candidates(
    source_kind: crate::SourceKind,
    bytes: &[u8],
    config: crate::AssemblyExtractionConfig,
) -> Vec<crate::AssemblyExtractionCandidate> {
    let mut candidates = crate::extract_contiguous_motif_candidates(source_kind, bytes, config);
    candidates.extend(crate::extract_discontinuous_field_group_candidates(
        source_kind,
        bytes,
        config,
    ));
    candidates.extend(crate::extract_delimiter_bounded_candidates(
        source_kind,
        bytes,
        config,
    ));
    candidates.extend(crate::extract_slot_bearing_candidates(
        source_kind,
        bytes,
        config,
    ));
    candidates.sort_by(|left, right| {
        right
            .agreement_score()
            .cmp(&left.agreement_score())
            .then_with(|| right.support_count().cmp(&left.support_count()))
            .then_with(|| left.ambiguity_score().cmp(&right.ambiguity_score()))
            .then_with(|| {
                left.estimated_route_wire_cost()
                    .cmp(&right.estimated_route_wire_cost())
            })
            .then_with(|| right.structural_hash.cmp(&left.structural_hash))
    });
    candidates.dedup_by(|left, right| left.structural_hash == right.structural_hash);
    candidates.truncate(16);
    candidates
}
