# Replace Specification: Integer-Only Cortical-Hippocampal Predictive Memory Transport
**Revision:** R2  
**Status:** hard replacement spec  
**Supersedes:** vector-provider regime, shallow state DSL, payload-first transport  
**Arithmetic:** integer-only, fixed-width, saturating, bitwise, SIMD-friendly, no floating point anywhere  
**Primary aim:** replace explicit catalog-first reuse with a sparse, hierarchical, predictive memory machine that remains exact on decode and cheap on CPU

---

# 0. Design position

This system is not a codec with optional state reuse.

It is a **predictive memory transport machine** with six properties:

1. **hierarchical prediction**
2. **sparse indexed assemblies**
3. **pattern separation and pattern completion**
4. **multi-timescale memory**
5. **transform abstraction**
6. **exact residual correction**

The sender does not primarily ask:

- what bytes can I reuse?
- what block is cheapest?
- what patch is smallest?

The sender asks:

- what internal assembly graph should the receiver already activate?
- what continuation should be predicted next?
- which memory stratum should explain this observation?
- what exact residual is minimally necessary to collapse remaining uncertainty?

That is the architectural pivot.

---

# 1. Hard axioms

## 1.1 Arithmetic axiom
Forbidden:
- floats
- cosine
- dense vector retrieval
- neural embedding providers
- gradient descent
- stochastic decoding
- probabilistic continuous latent state

Allowed:
- `u8/u16/u32/u64/u128`
- `i8/i16/i32/i64`
- fixed-point integers
- bitsets
- rolling hashes
- finite-state machines
- sparse codes
- context trees
- suffix structures
- count-min style counters
- exact dynamic programming where bounded
- SIMD on bytes, words, and bitsets

## 1.2 Decode axiom
Receiver reconstruction must be:
- exact
- deterministic
- bounded
- replayable
- serializable
- independently auditable

No receiver-side free search over enormous hypothesis spaces is permitted during decode.

## 1.3 Memory axiom
Memory is not one table.
Memory is partitioned into **specialized strata** with different learning rates, decay, routing privileges, and serialization rules.

## 1.4 Predictive axiom
Transport must minimize:
- wire bytes
- decoding work
- surprise mass
- synchronization risk

not merely literal payload length.

---

# 2. Biological translation into engineering modules

The architecture maps loosely onto computational roles, not biological imitation.

## 2.1 Cortical role
Slow abstraction, compositional structure, reusable schemas, nested prediction.

## 2.2 Hippocampal role
Sparse episodic indexing, pattern separation, rapid one-shot capture, pattern completion, sequence continuation.

## 2.3 Basal-ganglia-like role
Route arbitration, utility gating, confidence weighting, policy selection among competing explanation paths.

## 2.4 Thalamic-like role
Precision control and broadcast gating: decide which error channels are worth forwarding upward.

These are engineering roles only. No biological simulation is attempted.

---

# 3. Replace architecture

The replacement system is called:

# **CHPMT**
**Cortical-Hippocampal Predictive Memory Transport**

It has seven subsystems:

1. **Atom Substrate**
2. **Assembly Layer**
3. **Transform Layer** (DEMOTED — not emitted in live runtime)
4. **Episode Layer**
5. **Schema Layer**
6. **Index and Completion Layer**
7. **Controller / Precision Router**

---

# 4. Atom Substrate

## 4.1 Purpose
Exact physical substrate of reconstruction.

## 4.2 Objects
- byte atoms
- exact fragments
- exact blocks
- exact bundles
- exact ranges
- exact residual buffers

## 4.3 Properties
- immutable after definition
- compact id-based addressing
- family tagged
- versioned
- checksum protected

## 4.4 Role
All higher-level abstractions must bottom out into exact atom-substrate reconstruction.

This layer remains explicit by design.

---

# 5. Assembly Layer

This is the first real step toward something more brain-like.

## 5.1 Purpose
Represent recurring **structured assemblies**, not merely flat byte fragments.

An assembly is a sparse, reusable conjunction of lower-level elements with stable internal role structure.

## 5.2 Assembly types
- contiguous motif assembly
- discontinuous motif assembly
- role-bound field assembly
- separator-template assembly
- recurrent local phrase assembly
- alternating assembly
- mirrored assembly
- nested bracket assembly
- slot-bearing assembly

## 5.3 Assembly object
Each assembly stores:
- `assembly_id`
- `family_id`
- `arity`
- `slot_count`
- `role_signature`
- `support_count`
- `success_count`
- `failure_count`
- `salience`
- `creation_tick`
- `last_seen_tick`
- `assembly_body`
- `dependency_ids`
- `canonical_length_min`
- `canonical_length_max`

## 5.4 Key distinction
A block is a piece of material.
An assembly is a **structural conjunction**.

That distinction is mandatory.

## 5.5 Promotion rule
An assembly is promoted only if:
- it appears across multiple episodes
- its internal role structure is stable
- its average wire savings exceed threshold
- its ambiguity remains below threshold
- its decode graph is bounded
- its dependence on literal residual is not dominant

---

# 6. Transform Layer

**DEMOTED from active architecture.** Transform route emission is disabled in the live runtime. Candidate generation is retained for potential future reactivation with confirmed transform-class synchronization. The wire types (TransformDef, TransformCorrect) are preserved but no server code path emits transform routes. All transform-route plans fall through to direct-state with `FallbackReason::TransformDemoted`.

The current framework is weak here. This layer must become first-class before it can be reactivated.

## 6.1 Purpose
Store **reusable transformation families** instead of endlessly restating patches.

## 6.2 Transform families
Mandatory first generation:
- prefix insert
- suffix insert
- wrap
- bounded interior insert
- bounded delete
- bounded substitution
- repeated motif expansion
- copy-with-gap
- slot substitution
- role permutation
- delimiter-preserving rewrite
- local mirror
- bounded duplication
- strided selection
- splice from two bases
- schema slot-fill transform

## 6.3 Transform class object
Each transform class stores:
- `transform_id`
- `transform_kind`
- `parameter_schema`
- `basis_kind_mask`
- `support_count`
- `mean_residual_bytes`
- `mean_decode_steps`
- `reuse_savings`
- `stability_score`
- `failure_score`
- `promotion_level`

## 6.4 Transform instance
Each use carries:
- class id
- basis ids
- integer parameters
- residual offsets
- residual bytes
- output length contract

## 6.5 Constraint
A transform class is only allowed on-wire if its output can be verified exactly and locally.

---

# 7. Episode Layer

This is the fast memory system.

## 7.1 Purpose
Capture temporal context, recent continuations, and event-indexed local predictive structure.

## 7.2 Episode representation
An episode is a directed graph of recent assembly / transform / schema activations.

Each episode node stores:
- `episode_node_id`
- `tick`
- `context_hash`
- `active_object_id`
- `object_kind`
- `incoming_edge_ids`
- `outgoing_edge_ids`
- `novelty`
- `recency`
- `salience`
- `prediction_successes`
- `prediction_failures`

Each episode edge stores:
- `from_node`
- `to_node`
- `transition_count`
- `lag_bucket`
- `confidence`
- `branch_rank`

## 7.3 Temporal scales
- working trace
- short episodic chain
- medium episodic cluster
- replay candidate queue

## 7.4 Fast-learning behavior
Episode memory:
- learns quickly
- decays quickly
- favors recency
- is permitted to be redundant
- drives local next-step prediction

---

# 8. Schema Layer

This is the slow abstraction system.

## 8.1 Purpose
Represent stable reusable graph programs that unify many episodes.

## 8.2 Schema definition
A schema is a typed graph with:
- fixed topology
- typed slots
- admissible transforms
- optional branches
- structural constraints
- length and dependency contracts

## 8.3 Schema object
Each schema stores:
- `schema_id`
- `schema_kind`
- `node_count`
- `edge_count`
- `slot_types`
- `entry_conditions`
- `family_mask`
- `support_count`
- `cross_episode_support`
- `cross-context_support`
- `decode_cost_estimate`
- `wire_gain_estimate`
- `stability_score`
- `promotion_tick`

## 8.4 Promotion rule
Promote only if:
- multiple distinct episodes map onto same topology
- slot variability is bounded and typed
- transform reuse within topology is stable
- compression gain persists under held-out episodes
- decode cost remains bounded
- schema does not collapse into too much residual

## 8.5 Examples
A schema may model:
- record family topology
- document frame topology
- repeated message scaffold
- binary object skeleton
- protocol conversational turn structure
- nested header/body/trailer pattern
- repeated field-group arrangement

---

# 9. Index and Completion Layer

This is the sparse address and completion system.

## 9.1 Purpose
Provide content-addressable sparse indexing and completion without dense vectors.

## 9.2 Core idea
The system uses **sparse distributed integer codes**, not float embeddings.

## 9.3 Code format
Each object may expose a sparse code:
- `family_bits`
- `role_bits`
- `length_bucket_bits`
- `transition_bits`
- `delimiter_bits`
- `position_bits`
- `salience_bits`
- `schema_membership_bits`

Stored as:
- bitsets
- packed words
- sorted sparse integer lists
- compressed posting lists

## 9.4 Completion function
Given partial context:
1. derive sparse cue code
2. probe index tables
3. retrieve candidate assemblies / transforms / schemas / episodes
4. run bounded completion scoring
5. emit top-k admissible hypotheses

## 9.5 Pattern separation
New episodic entries must be separated aggressively:
- hash diversification
- family partitioning
- recency partitioning
- branch-sensitive context coding
- collision caps

## 9.6 Pattern completion
Completion is only permitted:
- under bounded confidence bands
- when dependency versions align
- when decode contract remains exact

---

# 10. Controller / Precision Router

This is the decision system.

## 10.1 Purpose
Choose which internal explanation path gets control.

## 10.2 Route families
- `DirectState`
- `ExactAtom`
- `Assembly`
- `Transform` (DEMOTED — not emitted in live runtime)
- `EpisodeCompletion`
- `SchemaExpansion`
- `Hybrid`

## 10.3 Precision concept
Precision is integer-valued confidence on a path.

Precision is influenced by:
- recent success
- support count
- residual burden
- ambiguity count
- sync confidence
- novelty
- path stability
- decode boundedness

## 10.4 Route score
Use integer-only scoring:

    score(route) =
        wire_bytes_cost
      + decode_step_cost
      + residual_cost
      + sync_risk_cost
      + ambiguity_cost
      + novelty_cost
      - support_gain
      - predictive_match_gain
      - temporal_continuation_gain
      - schema_reuse_gain

All terms are fixed-width integers.
No floats.
No probabilities on wire.

## 10.5 Winner-take-control rule
Select best admissible route.
If no route clears safety gates, force literal.

---

# 11. Inference cycle per payload

For each outbound item:

## 11.1 Phase A — Cue derivation
Compute:
- family cue
- role cue
- delimiter cue
- temporal cue
- length cue
- recent-context cue

All integer-only.

## 11.2 Phase B — Candidate generation
Generate bounded candidates from:
- atom matches
- assembly index
- transform index
- episode continuation table
- schema entry table

## 11.3 Phase C — Completion
For incomplete candidates:
- run bounded completion
- fill slots
- test admissibility
- derive exact residual if needed

## 11.4 Phase D — Competitive scoring
Compute route scores.
Select winner.

## 11.5 Phase E — Emission
Emit:
- prediction route
- dependencies
- program graph
- residual correction
- optional promotion / definition records

## 11.6 Phase F — Learning
Update:
- route statistics
- support counters
- transition counts
- salience
- decay
- replay queues
- consolidation candidates

---

# 12. Predictive transport semantics

## 12.1 On-wire principle
The transport should send **structured surprise**, not merely state references.

## 12.2 Four main wire modes

### 12.2.1 Confirm
Receiver is expected to reconstruct from already active internal model.
Only route and confirmation metadata are sent.

### 12.2.2 Correct
Receiver is expected to be close.
Send compact exact residual error plus basis.

### 12.2.3 Define-and-activate
A new memory object is introduced and immediately used.

### 12.2.4 Literal-fallback
Used when novelty or uncertainty dominates.

## 12.3 Transport objective
Prefer:
- strongest valid prediction
- smallest exact correction
- lowest divergence risk

not merely the shortest literal.

---

# 13. Program language replacement

The flat state DSL is to be discarded.

## 13.1 New reconstruction language
Use a typed DAG called **Predictive Reconstruction Graph** (`PRG`).

## 13.2 Node kinds
- `DirectState`
- `AtomRef`
- `RangeRef`
- `BundleRef`
- `AssemblyRef`
- `TransformRef`
- `EpisodeRef`
- `SchemaRef`
- `Concat`
- `Patch`
- `Repeat`
- `Select`
- `Permute`
- `SlotFill`
- `Expand`
- `Guard`
- `Branch`
- `Commit`

## 13.3 Node contract
Each node declares:
- output length
- dependency ids
- admissible input kinds
- parameter word block
- determinism class
- exactness flag

## 13.4 Graph invariants
- acyclic
- bounded depth
- bounded fanout
- explicit output length
- exact final byte material
- versioned dependency closure
- local verifiability

---

# 14. Concept formation

The system must not just store instances. It must form reusable internal concepts.

## 14.1 Concept definition
A concept is one of:
- assembly family
- transform family
- episode macro
- schema template
- role-slot pattern
- recurrent continuation class

## 14.2 Concept emergence
A concept emerges when repeated successful explanation paths share:
- stable topology
- bounded residual variation
- reusable role structure
- positive utility over literal transmission

## 14.3 Concept lifecycle
- create candidate
- accumulate support
- validate on held-out events
- promote
- compete for routing
- decay
- merge or split
- retire

---

# 15. Replay and consolidation

This is required if the architecture is to be truly multi-timescale.

## 15.1 Replay queue
Maintain prioritized replay items based on:
- surprise
- reuse potential
- partial schema overlap
- unresolved ambiguity
- temporal centrality

## 15.2 Background consolidation jobs
- merge near-equivalent assemblies
- compress repeated transform chains
- infer episode macros
- promote schemas
- prune dead concepts
- rewrite indices for locality

## 15.3 CPU rule
Replay and consolidation must be background-only.
Hot path remains bounded.

---

# 16. Sequence model substrate

Because the target is CPU-only and discrete, the sequence backbone should be exact variable-memory inference, not float latent recurrence.

## 16.1 Required discrete predictors
At least one of:
- bounded context tree
- variable-order Markov table
- probabilistic suffix structure implemented with integer counts
- finite-state continuation graph
- schema-conditioned context tree

## 16.2 Purpose
These models provide:
- cheap next-step prediction
- hierarchical context dependence
- exact finite-alphabet inference
- no float arithmetic
- direct usefulness for transport prediction

## 16.3 Integration
Episode and schema planes should be able to query the discrete predictor for:
- next object kind
- next slot type
- next transform family
- likely branch continuation
- expected delimiter / structural boundary

---

# 17. Self-organizing ontology

This is where the system becomes materially closer to your target.

## 17.1 Requirement
The system must discover not only reusable objects but also **which object classes exist**.

## 17.2 Ontology operations
- group co-activated assemblies
- infer stable role clusters
- detect motifs that predict other motifs
- split over-broad assemblies
- merge under-differentiated assemblies
- promote higher-order clusters into schemas
- assign family and subfamily ids dynamically

## 17.3 Outcome
The substrate becomes:
- self-clustering
- role-aware
- recursively structured
- less literal
- more assembly-centric

---

# 18. SIMD plan

## 18.1 SIMD is mandatory in the following kernels
- byte equality scans
- common-prefix scans
- rolling hash windows
- bitset overlap
- sparse-code Hamming calculations
- delimiter search
- patch application
- slot mask checks
- graph node materialization

## 18.2 SIMD is not the intelligence layer
SIMD accelerates local kernels.
It does not define architecture.

---

# 19. Record families

## 19.1 Mandatory new record types
- `PredictiveConfirm`
- `PredictiveCorrect`
- `AssemblyDef`
- `TransformDef`
- `SchemaDef`
- `EpisodeHint`
- `ReplayHint`
- `MemoryRetire`
- `MemoryAck`
- `Repair`
- `Resync`
- `ExactState`

## 19.2 PredictiveCorrect fields
- route family
- active plane
- basis ids
- graph bytes
- residual bytes
- output length
- dependency version vector
- confidence band
- replay hint bit

## 19.3 AssemblyDef fields
- assembly id
- role signature
- assembly graph/body
- dependency ids
- family id
- admissibility flags

## 19.4 SchemaDef fields
- schema id
- node table
- edge table
- slot table
- entry constraints
- dependency closure
- decode caps

---

# 20. Failure containment

## 20.1 Divergence sources
- stale dependencies
- over-promoted concepts
- wrong completion
- route overconfidence
- schema drift
- ontology instability

## 20.2 Hard protections
- version vectors
- exact dependency closure
- forced fallback
- resync by plane
- concept demotion on repeated failure
- ambiguity caps
- per-route kill switches

---

# 21. CPU budget discipline

The architecture must remain hard and sophisticated, but bounded.

## 21.1 Hot path budgeted operations
Allowed:
- bounded candidate fanout per plane
- bounded context depth
- bounded completion beam
- bounded graph expansion depth
- bounded transform enumeration
- bounded route comparison

## 21.2 Not allowed on hot path
- unconstrained ontology search
- global graph induction
- unbounded episode replay
- quadratic all-to-all schema matching
- huge branch completion search

## 21.3 Practical target
This should be implementable on commodity CPU because:
- arithmetic is discrete
- local kernels are SIMD-able
- prediction is variable-memory / sparse-indexed
- completion is bounded
- consolidation is deferred

---

# 22. Migration replacement order

## Phase 0
Delete vector-provider regime entirely.

## Phase 1
Keep exact substrate only as deterministic ground truth.

## Phase 2
Install sparse index and completion layer.

## Phase 3
Install assembly learning and routing.

## Phase 4
Install transform class induction and wire support.

## Phase 5
Install episodic context graph and continuation prediction.

## Phase 6
Install schema induction and PRG execution.

## Phase 7
Turn router from reuse-first into prediction-first.

## Phase 8
Enable replay, consolidation, demotion, ontology maintenance.

---

# 23. One-sentence definition

This replacement turns the framework into:

**an integer-only cortical-hippocampal predictive memory transport that uses sparse indexed assemblies, transform classes, episodic continuation, schema graphs, and exact residual correction to transmit structured prediction error rather than merely compressed explicit state.**

---

# 24. Minimal litmus test

If the system still mostly says:
- reuse block
- reuse bundle
- patch bytes

then the replacement has failed.

If the system instead says:
- activate assembly family
- complete likely episode continuation
- expand schema graph
- apply transform class
- send exact residual surprise

then the replacement is architecturally on target.