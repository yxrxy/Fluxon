use crate::dag_viz_html::{InitStepSpec, InitStepsDagSpec, StepMode};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// This is a compile-time DAG compiler:
// - YAML is the source of truth (strict validation, no fallback)
// - build.rs calls this and emits:
//   - a Rust init orchestrator (include! into the user crate)
//   - an HTML visualization (manual inspection)

pub const ATTACH_VIEWS_STEP_ID: &str = "Framework.step.0.attach_views";
pub const ATTACH_VIEWS_MODULE: &str = "Framework";

const RESOURCE_DEP_PREFIX: &str = "res:";

const TAG_MASTER: u8 = 1 << 0;
const TAG_OWNER: u8 = 1 << 1;
const TAG_EXTERNAL: u8 = 1 << 2;
const TAG_ALL: u8 = TAG_MASTER | TAG_OWNER | TAG_EXTERNAL;

// v5 introduces YAML-only variants, so tags are no longer restricted to the
// hard-coded master/owner/external set. Keep the v3/v4 bitmask path intact
// (strictly master/owner/external), while v5 uses string-set overlap.

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum InitTag {
    Master,
    Owner,
    External,
}

impl InitTag {
    fn as_str(&self) -> &'static str {
        match self {
            InitTag::Master => "master",
            InitTag::Owner => "owner",
            InitTag::External => "external",
        }
    }

    fn bit(&self) -> u8 {
        match self {
            InitTag::Master => TAG_MASTER,
            InitTag::Owner => TAG_OWNER,
            InitTag::External => TAG_EXTERNAL,
        }
    }

    fn all() -> &'static [InitTag] {
        &[InitTag::Master, InitTag::Owner, InitTag::External]
    }
}

fn parse_init_tag(s: &str) -> Result<InitTag> {
    match s {
        "master" => Ok(InitTag::Master),
        "owner" => Ok(InitTag::Owner),
        "external" => Ok(InitTag::External),
        other => bail!(
            "unknown init tag: {} (allowed: master/owner/external)",
            other
        ),
    }
}

fn tag_list_to_mask(tags: &[String], ctx: &str) -> Result<u8> {
    if tags.is_empty() {
        bail!("{} tags must not be empty", ctx);
    }
    let mut mask: u8 = 0;
    for t in tags {
        if t.trim() != t {
            bail!("{} tag must not contain surrounding whitespace: {}", ctx, t);
        }
        let bit = parse_init_tag(t.as_str())?.bit();
        if (mask & bit) != 0 {
            bail!("{} duplicate tag: {}", ctx, t);
        }
        mask |= bit;
    }
    Ok(mask)
}

fn tag_list_to_mask_allow_empty(tags: &[String], ctx: &str) -> Result<u8> {
    if tags.is_empty() {
        return Ok(0);
    }
    tag_list_to_mask(tags, ctx)
}

fn is_valid_tag_name(tag: &str) -> bool {
    // Keep tags deterministic and Rust-friendly (used in codegen and filtering).
    // Tag names are treated as enumerations, so snake_case ascii is required.
    let mut it = tag.chars();
    let Some(first) = it.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    it.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn validate_tag_list_v5(tags: &[String], ctx: &str, allow_empty: bool) -> Result<()> {
    if tags.is_empty() {
        if allow_empty {
            return Ok(());
        }
        bail!("{} tags must not be empty", ctx);
    }
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for t in tags {
        if t.trim().is_empty() {
            bail!("{} tag must not be empty", ctx);
        }
        if t.trim() != t {
            bail!("{} tag must not contain surrounding whitespace: {}", ctx, t);
        }
        if !is_valid_tag_name(t) {
            bail!(
                "{} tag must be snake_case ascii [a-z0-9_], starting with a letter: {}",
                ctx,
                t
            );
        }
        if !seen.insert(t.as_str()) {
            bail!("{} duplicate tag: {}", ctx, t);
        }
    }
    Ok(())
}

fn tags_overlap(needle: &BTreeSet<&str>, haystack: &[String]) -> bool {
    haystack.iter().any(|t| needle.contains(t.as_str()))
}

fn tags_overlap_owned(needle: &BTreeSet<String>, haystack: &[String]) -> bool {
    haystack.iter().any(|t| needle.contains(t))
}

enum DepEntry<'a> {
    Step(&'a str),
    Resource(&'a str),
}

fn parse_dep_entry(dep: &str) -> Result<DepEntry<'_>> {
    if let Some(rest) = dep.strip_prefix(RESOURCE_DEP_PREFIX) {
        if rest.is_empty() {
            bail!("invalid resource dep (empty id): {}", dep);
        }
        if rest.trim() != rest {
            bail!(
                "invalid resource dep (id must not have surrounding whitespace): {}",
                dep
            );
        }
        return Ok(DepEntry::Resource(rest));
    }
    Ok(DepEntry::Step(dep))
}

/// Compile output for a framework init-step DAG.
///
/// - `rust` is included by the user crate via `include!(concat!(env!("OUT_DIR"), ...))`.
/// - `html` is a self-contained visualization for manual validation.
pub struct CompileOutput {
    pub rust: String,
    pub html: String,
}

/// Rust codegen settings.
///
/// No defaults are provided: the caller must specify all paths/names to keep the
/// interface explicit and stable.
pub struct RustGenConfig {
    pub init_fn_name: String,
    pub framework_type_path: String,
    pub framework_args_type_path: String,
    pub result_type_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum BestEffortOnFailure {
    AllowError,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BestEffortSpec {
    pub timeout_ms: u64,
    pub on_timeout: BestEffortOnFailure,
    pub on_error: BestEffortOnFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitDagVariantYaml {
    pub id: String,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitDagResourceYaml {
    pub id: String,
    pub tags: Vec<String>,
    pub publish_tags: Vec<String>,
    pub published_by: String,
    pub doc: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "PascalCase")]
pub enum ExecSpec {
    /// Step-0 construct step: takes the module arg from `FrameworkArgs`.
    Construct {
        /// Rust path to call, e.g. `ClusterManager::construct`.
        call: String,
        /// Field name in `FrameworkArgs`, e.g. `p2p_arg`.
        arg_field: String,
    },
    /// Step-1+ init step: called with `(&Module)`.
    Call {
        /// Rust path to call, e.g. `MyModule::init2_for_init_dag`.
        call: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitDagStepYaml {
    pub id: String,
    pub module: String,
    pub mode: StepMode,
    /// Dependencies expressed at step granularity.
    ///
    /// `deps` is the single source of truth for scheduling (wait). Each entry is either:
    /// - a step id: `<Module>.step.<idx>.<name>`
    /// - a resource reference: `res:<resource_id>` (gated by the runtime resource node)
    pub deps: Vec<String>,
    /// Human-readable documentation for this step.
    ///
    /// This text is shown in the generated DAG visualization HTML.
    pub doc: String,
    pub exec: ExecSpec,
    /// Must be set to `null` for non-BestEffortWait steps.
    pub best_effort: Option<BestEffortSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitDagYaml {
    pub version: u32,
    pub title: String,
    /// Variant list (YAML-only; no implicit variants).
    ///
    /// - Required in v5.
    /// - Forbidden in v3/v4 to keep behavior explicit.
    pub variants: Option<Vec<InitDagVariantYaml>>,
    /// Resource readiness nodes (must be present; use `resources: []` for none).
    pub resources: Option<Vec<InitDagResourceYaml>>,
    /// Per-module init role tags.
    ///
    /// This field must be present even if empty (no implicit defaults).
    pub module_tags: Option<BTreeMap<String, Vec<String>>>,
    pub steps: Vec<InitDagStepYaml>,
}

pub fn compile_from_yaml_str(yaml: &str, rust_cfg: &RustGenConfig) -> Result<CompileOutput> {
    let spec: InitDagYaml = serde_yaml::from_str(yaml).context("parse init dag yaml")?;
    validate_init_dag_yaml(&spec)?;

    let viz_spec = build_viz_spec_with_attach_views_node(&spec)?;
    let html = if spec.version == 5 {
        let variants = spec.variants.as_ref().unwrap();
        let v: Vec<crate::dag_viz_html::InitDagVariantSpec> = variants
            .iter()
            .map(|vv| crate::dag_viz_html::InitDagVariantSpec {
                id: vv.id.clone(),
                tags: vv.tags.clone(),
            })
            .collect();
        crate::dag_viz_html::render_init_steps_html_variants(&viz_spec, &v)?
    } else {
        crate::dag_viz_html::render_init_steps_html_three_tags(&viz_spec)?
    };

    let rust = generate_rust(&spec, rust_cfg)?;
    Ok(CompileOutput { rust, html })
}

fn validate_init_dag_yaml(spec: &InitDagYaml) -> Result<()> {
    if spec.version != 3 && spec.version != 4 && spec.version != 5 {
        bail!("unsupported init dag yaml version: {}", spec.version);
    }
    if spec.title.trim().is_empty() {
        bail!("init dag yaml title must not be empty");
    }
    if spec.steps.is_empty() {
        bail!("init dag yaml has no steps");
    }

    let mut variant_tag_union: Option<BTreeSet<String>> = None;

    match spec.version {
        3 | 4 => {
            if spec.variants.is_some() {
                bail!(
                    "init dag yaml v{} must not contain variants (use v5)",
                    spec.version
                );
            }
        }
        5 => {
            let Some(variants) = spec.variants.as_ref() else {
                bail!("init dag yaml v5 requires top-level variants");
            };
            if variants.is_empty() {
                bail!("init dag yaml v5 variants must not be empty");
            }
            let mut tag_union: BTreeSet<String> = BTreeSet::new();
            let mut ids: BTreeSet<&str> = BTreeSet::new();
            for (i, v) in variants.iter().enumerate() {
                if v.id.trim().is_empty() {
                    bail!("init dag yaml v5 variants[{}].id must not be empty", i);
                }
                if v.id.trim() != v.id {
                    bail!(
                        "init dag yaml v5 variants[{}].id must not contain surrounding whitespace: {}",
                        i,
                        v.id
                    );
                }
                let ok = v.id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    && !v.id.chars().next().unwrap().is_ascii_digit();
                if !ok {
                    bail!(
                        "init dag yaml v5 variants[{}].id is not a valid Rust identifier fragment: {}",
                        i,
                        v.id
                    );
                }
                if !ids.insert(v.id.as_str()) {
                    bail!("init dag yaml v5 variants has duplicate id: {}", v.id);
                }

                validate_tag_list_v5(&v.tags, &format!("variants[{}].tags", i), false)?;
                for t in &v.tags {
                    tag_union.insert(t.clone());
                }
            }
            variant_tag_union = Some(tag_union);
        }
        _ => unreachable!("version validated above"),
    }

    let resources = spec.resources.as_ref().context(
        "init dag yaml must contain explicit `resources` field (use resources: [] if none)",
    )?;

    let module_tags = spec
        .module_tags
        .as_ref()
        .context("init dag yaml must contain explicit `module_tags` field")?;

    let mut ids: BTreeSet<String> = BTreeSet::new();
    let mut by_id: BTreeMap<&str, &InitDagStepYaml> = BTreeMap::new();
    let mut steps_by_module: BTreeMap<&str, Vec<&InitDagStepYaml>> = BTreeMap::new();
    let mut arg_fields: BTreeSet<String> = BTreeSet::new();

    if spec.steps.iter().any(|s| s.id == ATTACH_VIEWS_STEP_ID) {
        bail!(
            "reserved step id is not allowed in YAML: {}",
            ATTACH_VIEWS_STEP_ID
        );
    }

    let mut resource_ids: BTreeSet<String> = BTreeSet::new();
    let mut resource_published_by: BTreeMap<&str, &str> = BTreeMap::new();
    for r in resources {
        if r.id.trim().is_empty() {
            bail!("resource id must not be empty");
        }
        if !is_valid_resource_id(&r.id) {
            bail!(
                "resource id must be snake_case ascii [a-z0-9_], starting with a letter: {}",
                r.id
            );
        }
        if !resource_ids.insert(r.id.clone()) {
            bail!("duplicate resource id: {}", r.id);
        }
        match spec.version {
            3 | 4 => {
                let _ = tag_list_to_mask(&r.tags, &format!("resource {}.tags", r.id))?;
                let _ = tag_list_to_mask_allow_empty(
                    &r.publish_tags,
                    &format!("resource {}.publish_tags", r.id),
                )?;
            }
            5 => {
                validate_tag_list_v5(&r.tags, &format!("resource {}.tags", r.id), false)?;
                validate_tag_list_v5(
                    &r.publish_tags,
                    &format!("resource {}.publish_tags", r.id),
                    true,
                )?;
                let tag_union = variant_tag_union.as_ref().unwrap();
                let mut all: Vec<String> = Vec::new();
                all.extend(r.tags.iter().cloned());
                all.extend(r.publish_tags.iter().cloned());
                if !tags_overlap_owned(tag_union, &all) {
                    bail!(
                        "resource {} tags must overlap at least one variants.tags (resource would be unreachable in all variants)",
                        r.id
                    );
                }
            }
            _ => unreachable!("version validated above"),
        }
        if r.published_by.trim().is_empty() {
            bail!("resource {} published_by must not be empty", r.id);
        }
        if r.doc.trim().is_empty() {
            bail!("resource {} doc must not be empty", r.id);
        }
        resource_published_by.insert(r.id.as_str(), r.published_by.as_str());
    }

    for s in &spec.steps {
        if s.id.trim().is_empty() {
            bail!("init step id must not be empty");
        }
        if !ids.insert(s.id.clone()) {
            bail!("duplicate init step id: {}", s.id);
        }
        if s.module.trim().is_empty() {
            bail!("init step {} has empty module", s.id);
        }
        if s.doc.trim().is_empty() {
            bail!("init step {} has empty doc", s.id);
        }

        let (id_module, idx, _name) = parse_step_id(&s.id)?;
        if id_module != s.module {
            bail!(
                "init step id module mismatch: id={} implies module={}, but yaml module={} ",
                s.id,
                id_module,
                s.module
            );
        }

        match s.mode {
            StepMode::Blocking | StepMode::AsyncSpawn => {
                if s.best_effort.is_some() {
                    bail!(
                        "init step {}: best_effort must be null unless mode is BestEffortWait",
                        s.id
                    );
                }
            }
            StepMode::BestEffortWait => {
                if s.best_effort.is_none() {
                    bail!(
                        "init step {}: best_effort must be set for BestEffortWait",
                        s.id
                    );
                }
            }
        }

        match &s.exec {
            ExecSpec::Construct { call, arg_field } => {
                if idx != 0 {
                    bail!(
                        "init step {}: exec.kind=Construct is only allowed for step index 0",
                        s.id
                    );
                }
                if s.mode != StepMode::Blocking {
                    bail!("init step {}: step-0 Construct must be mode=Blocking", s.id);
                }
                if call.trim().is_empty() {
                    bail!("init step {} has empty exec.call", s.id);
                }
                if arg_field.trim().is_empty() {
                    bail!("init step {} has empty exec.arg_field", s.id);
                }
                if !is_valid_rust_ident(arg_field) {
                    bail!(
                        "init step {}: exec.arg_field must be a valid Rust identifier: {}",
                        s.id,
                        arg_field
                    );
                }
                if !arg_fields.insert(arg_field.clone()) {
                    bail!("duplicate exec.arg_field: {}", arg_field);
                }
            }
            ExecSpec::Call { call } => {
                if idx == 0 {
                    bail!("init step {}: step index 0 must be Construct", s.id);
                }
                if call.trim().is_empty() {
                    bail!("init step {} has empty exec.call", s.id);
                }
            }
        }

        if s.deps.iter().any(|d| d.trim().is_empty()) {
            bail!("init step {} has empty deps entry", s.id);
        }

        steps_by_module
            .entry(s.module.as_str())
            .or_default()
            .push(s);
        by_id.insert(s.id.as_str(), s);
    }

    // module_tags must match the set of modules in `steps` (no implicit defaults).
    if module_tags.is_empty() {
        bail!("init dag yaml module_tags must not be empty");
    }
    if module_tags.contains_key(ATTACH_VIEWS_MODULE) {
        bail!(
            "module_tags must not contain reserved module name: {}",
            ATTACH_VIEWS_MODULE
        );
    }
    for m in steps_by_module.keys() {
        if !module_tags.contains_key(*m) {
            bail!("module_tags missing module: {}", m);
        }
    }
    for m in module_tags.keys() {
        if !steps_by_module.contains_key(m.as_str()) {
            bail!("module_tags contains unknown module (not in steps): {}", m);
        }
    }

    let mut module_mask_by_module: BTreeMap<&str, u8> = BTreeMap::new();
    if spec.version == 3 || spec.version == 4 {
        for (m, tags) in module_tags.iter() {
            let mask = tag_list_to_mask(tags, &format!("module_tags.{}", m))?;
            module_mask_by_module.insert(m.as_str(), mask);
        }
    } else if spec.version == 5 {
        for (m, tags) in module_tags.iter() {
            validate_tag_list_v5(tags, &format!("module_tags.{}", m), false)?;
            if !tags_overlap_owned(variant_tag_union.as_ref().unwrap(), tags) {
                bail!(
                    "module_tags.{} must overlap at least one variants.tags (module would be unreachable in all variants)",
                    m
                );
            }
        }
    }

    // deps entries must reference existing steps or existing resources.
    for s in &spec.steps {
        for d in &s.deps {
            match parse_dep_entry(d)? {
                DepEntry::Step(step_id) => {
                    if !ids.contains(step_id) {
                        bail!("init step {} deps unknown step id: {}", s.id, step_id);
                    }
                }
                DepEntry::Resource(resource_id) => {
                    if !resource_ids.contains(resource_id) {
                        bail!(
                            "init step {} deps unknown resource id: {}",
                            s.id,
                            resource_id
                        );
                    }
                }
            }
        }
    }

    // resources.published_by must reference existing steps, and resource ids must not collide with step ids.
    for r in resources {
        if ids.contains(&r.id) {
            bail!("resource id must not collide with a step id: {}", r.id);
        }
        if r.id == ATTACH_VIEWS_STEP_ID {
            bail!(
                "resource id must not use reserved attach_views id: {}",
                r.id
            );
        }
        if !ids.contains(&r.published_by) {
            bail!(
                "resource {} published_by unknown step id: {}",
                r.id,
                r.published_by
            );
        }
        let (pm, pub_idx, _pn) = parse_step_id(&r.published_by)?;
        if pub_idx == 0 {
            bail!(
                "resource {} published_by must be a postview step (idx>0): {}",
                r.id,
                r.published_by
            );
        }

        match spec.version {
            3 | 4 => {
                let res_mask = tag_list_to_mask(&r.tags, &format!("resource {}.tags", r.id))?;
                let publish_mask = tag_list_to_mask_allow_empty(
                    &r.publish_tags,
                    &format!("resource {}.publish_tags", r.id),
                )?;
                let pub_mask = *module_mask_by_module.get(pm.as_str()).unwrap_or_else(|| {
                    panic!("publisher module must exist in module_mask_by_module")
                });
                if (res_mask & !pub_mask) != 0 {
                    bail!(
                        "resource {} tags must be a subset of publisher module tags: resource_tags={:?}, publisher_module={}, publisher_tags={:?}",
                        r.id,
                        r.tags,
                        pm,
                        module_tags.get(&pm).unwrap()
                    );
                }

                if (publish_mask & !res_mask) != 0 {
                    bail!(
                        "resource {} publish_tags must be a subset of resource tags: publish_tags={:?}, tags={:?}",
                        r.id,
                        r.publish_tags,
                        r.tags
                    );
                }
            }
            5 => {
                // v5 semantics:
                // - `tags` are the waiter tags
                // - `publish_tags` are the publisher tags
                // The two sets are independent; variant-local closure validation will ensure
                // the publisher step exists whenever this resource node is active.
                let pub_tags = module_tags.get(&pm).unwrap();
                let pub_set: BTreeSet<&str> = pub_tags.iter().map(|s| s.as_str()).collect();
                for t in r.tags.iter().chain(r.publish_tags.iter()) {
                    if !pub_set.contains(t.as_str()) {
                        bail!(
                            "resource {} tags must be a subset of publisher module tags: waiter_tags={:?}, publish_tags={:?}, publisher_module={}, publisher_tags={:?}",
                            r.id,
                            r.tags,
                            r.publish_tags,
                            pm,
                            pub_tags
                        );
                    }
                }
            }
            _ => unreachable!("version validated above"),
        }
    }

    // Per-module step indices must be contiguous 0..n-1.
    for (module, steps) in &mut steps_by_module {
        let mut indices: Vec<u32> = steps
            .iter()
            .map(|s| parse_step_id(&s.id).map(|(_, idx, _)| idx))
            .collect::<Result<Vec<_>>>()?;
        indices.sort();
        for (expected, got) in (0u32..).zip(indices.iter().copied()) {
            if expected != got {
                bail!(
                    "module {} step indices must be contiguous starting from 0; expected {}, got {}",
                    module,
                    expected,
                    got
                );
            }
            if expected as usize + 1 == indices.len() {
                break;
            }
        }
    }

    // Construct steps (idx=0) must not depend on postview steps (idx>0).
    // The runtime view is attached at the barrier step, and postview steps depend on it.
    for s in &spec.steps {
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        if idx != 0 {
            continue;
        }
        if s.deps
            .iter()
            .any(|d| matches!(parse_dep_entry(d), Ok(DepEntry::Resource(_))))
        {
            bail!("construct step {} must not depend on resources", s.id);
        }
        let mut expanded: Vec<&str> = Vec::new();
        for d in &s.deps {
            match parse_dep_entry(d)? {
                DepEntry::Step(step_id) => expanded.push(step_id),
                DepEntry::Resource(resource_id) => expanded.push(
                    resource_published_by
                        .get(resource_id)
                        .expect("resource must exist"),
                ),
            }
        }
        expanded.sort();
        expanded.dedup();
        for d in expanded {
            let dep = by_id.get(d).unwrap();
            let (_dm, didx, _dname) = parse_step_id(&dep.id)?;
            if didx != 0 {
                bail!(
                    "construct step {} must not depend on postview step {}",
                    s.id,
                    dep.id
                );
            }
        }
    }

    // Enforce explicit deps not referencing steps within the same module.
    // Intra-module ordering is implicit via step indices.
    for s in &spec.steps {
        let (m, _idx, _name) = parse_step_id(&s.id)?;
        for d in &s.deps {
            let DepEntry::Step(step_id) = parse_dep_entry(d)? else {
                // Resource deps are allowed for intra-module semantic documentation.
                continue;
            };
            let dep = by_id.get(step_id).unwrap();
            let (dm, _didx, _dname) = parse_step_id(&dep.id)?;
            if dm == m {
                bail!(
                    "init step {}: deps must not reference steps in the same module (redundant; order is implicit): {}",
                    s.id,
                    step_id
                );
            }
        }
    }

    // DAG must be acyclic when combining:
    // - explicit deps
    // - implicit intra-module order edges
    // - implicit attach_views barrier edges
    assert_acyclic_with_attach_views(spec)?;

    // Validate that tag/variant-specific DAGs are closed under dependencies.
    // This keeps behavior strict: no silent skipping at runtime.
    if spec.version == 5 {
        let variants = spec.variants.as_ref().unwrap();
        for v in variants {
            let tag_set: BTreeSet<&str> = v.tags.iter().map(|s| s.as_str()).collect();
            let (nodes, deps_by_id) =
                build_runtime_node_deps_with_attach_views_v5_for_variant(spec, &tag_set)?;
            for (id, deps) in deps_by_id.iter() {
                if !nodes.contains(id) {
                    bail!("internal error: deps map contains unknown node: {}", id);
                }
                for d in deps {
                    if !nodes.contains(d) {
                        bail!(
                            "variant {}: active node {} depends on inactive node {}",
                            v.id,
                            id,
                            d
                        );
                    }
                }
            }
        }
    } else if spec.version == 4 {
        for tag in InitTag::all() {
            let (nodes, deps_by_id) =
                build_runtime_node_deps_with_attach_views_v4_for_tag(spec, tag.bit())?;
            for (id, deps) in deps_by_id.iter() {
                if !nodes.contains(id) {
                    bail!("internal error: deps map contains unknown node: {}", id);
                }
                for d in deps {
                    if !nodes.contains(d) {
                        bail!(
                            "tag {}: active node {} depends on inactive node {}",
                            tag.as_str(),
                            id,
                            d
                        );
                    }
                }
            }
        }
    } else {
        let deps_by_id = build_runtime_node_deps_with_attach_views(spec)?;
        let mut node_mask_by_id: BTreeMap<String, u8> = BTreeMap::new();

        // v3: step.0 and attach_views are always active for all tags.
        for s in &spec.steps {
            let (_m, idx, _name) = parse_step_id(&s.id)?;
            let module_mask = *module_mask_by_module
                .get(s.module.as_str())
                .unwrap_or_else(|| panic!("module mask must exist"));
            let mask = if idx == 0 { TAG_ALL } else { module_mask };
            node_mask_by_id.insert(s.id.clone(), mask);
        }
        node_mask_by_id.insert(ATTACH_VIEWS_STEP_ID.to_string(), TAG_ALL);

        // Resources.
        for r in resources {
            let mask = tag_list_to_mask(&r.tags, &format!("resource {}.tags", r.id))?;
            node_mask_by_id.insert(format!("{}{}", RESOURCE_DEP_PREFIX, r.id), mask);
        }

        for (id, _deps) in deps_by_id.iter() {
            if !node_mask_by_id.contains_key(id) {
                bail!("internal error: node has deps but missing tag mask: {}", id);
            }
        }

        for tag in InitTag::all() {
            let bit = tag.bit();
            let mut active: BTreeSet<String> = BTreeSet::new();
            for (id, mask) in node_mask_by_id.iter() {
                if (mask & bit) != 0 {
                    active.insert(id.clone());
                }
            }
            for id in active.iter() {
                let deps = deps_by_id
                    .get(id)
                    .unwrap_or_else(|| panic!("deps map must contain node"));
                for d in deps {
                    if !active.contains(d) {
                        bail!(
                            "tag {}: active node {} depends on inactive node {}",
                            tag.as_str(),
                            id,
                            d
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn assert_acyclic_with_attach_views(spec: &InitDagYaml) -> Result<()> {
    if spec.version == 4 {
        for t in InitTag::all() {
            let tag_bit = t.bit();
            let (nodes, deps) =
                build_runtime_node_deps_with_attach_views_v4_for_tag(spec, tag_bit)?;
            assert_acyclic_nodes_with_deps(&nodes, &deps)
                .with_context(|| format!("cycle detected for tag {}", t.as_str()))?;
        }
        return Ok(());
    }

    if spec.version == 5 {
        let variants = spec.variants.as_ref().unwrap();
        for v in variants {
            let tag_set: BTreeSet<&str> = v.tags.iter().map(|s| s.as_str()).collect();
            let (nodes, deps) =
                build_runtime_node_deps_with_attach_views_v5_for_variant(spec, &tag_set)?;
            assert_acyclic_nodes_with_deps(&nodes, &deps)
                .with_context(|| format!("cycle detected for variant {}", v.id))?;
        }
        return Ok(());
    }

    let resources = spec.resources.as_ref().unwrap();
    let module_tags = spec.module_tags.as_ref().unwrap();

    let mut module_mask_by_module: BTreeMap<&str, u8> = BTreeMap::new();
    for (m, tags) in module_tags {
        let mask = tag_list_to_mask(tags, &format!("module_tags.{}", m))?;
        module_mask_by_module.insert(m.as_str(), mask);
    }

    let mut resource_mask_by_id: BTreeMap<&str, u8> = BTreeMap::new();
    let mut resource_publish_mask_by_id: BTreeMap<&str, u8> = BTreeMap::new();
    for r in resources {
        let mask = tag_list_to_mask(&r.tags, &format!("resource {}.tags", r.id))?;
        resource_mask_by_id.insert(r.id.as_str(), mask);
        let pmask = tag_list_to_mask_allow_empty(
            &r.publish_tags,
            &format!("resource {}.publish_tags", r.id),
        )?;
        resource_publish_mask_by_id.insert(r.id.as_str(), pmask);
    }

    // Node ids are either:
    // - step ids
    // - the compiler-inserted attach_views step id
    // - resource node ids: `res:<resource_id>`
    let mut nodes: BTreeSet<String> = BTreeSet::new();
    for s in &spec.steps {
        nodes.insert(s.id.clone());
    }
    nodes.insert(ATTACH_VIEWS_STEP_ID.to_string());
    for r in resources {
        nodes.insert(format!("{}{}", RESOURCE_DEP_PREFIX, r.id));
    }

    let mut indeg: BTreeMap<String, usize> = BTreeMap::new();
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for id in &nodes {
        indeg.insert(id.clone(), 0);
        out.insert(id.clone(), Vec::new());
    }

    // Explicit deps.
    for s in &spec.steps {
        for d in &s.deps {
            match parse_dep_entry(d)? {
                DepEntry::Step(step_id) => {
                    out.get_mut(step_id).unwrap().push(s.id.clone());
                    *indeg.get_mut(&s.id).unwrap() += 1;
                }
                DepEntry::Resource(resource_id) => {
                    let res_node = format!("{}{}", RESOURCE_DEP_PREFIX, resource_id);
                    out.get_mut(&res_node).unwrap().push(s.id.clone());
                    *indeg.get_mut(&s.id).unwrap() += 1;
                }
            }
        }
    }

    // Implicit intra-module order edges.
    let mut by_module: BTreeMap<&str, Vec<&InitDagStepYaml>> = BTreeMap::new();
    for s in &spec.steps {
        by_module.entry(s.module.as_str()).or_default().push(s);
    }
    for steps in by_module.values_mut() {
        steps.sort_by(|a, b| {
            let (_, ia, _) = parse_step_id(&a.id).unwrap();
            let (_, ib, _) = parse_step_id(&b.id).unwrap();
            ia.cmp(&ib).then_with(|| a.id.cmp(&b.id))
        });
        for w in steps.windows(2) {
            out.get_mut(&w[0].id).unwrap().push(w[1].id.clone());
            *indeg.get_mut(&w[1].id).unwrap() += 1;
        }
    }

    // attach_views barrier edges.
    for s in &spec.steps {
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        if idx == 0 {
            // construct -> attach_views
            out.get_mut(&s.id)
                .unwrap()
                .push(ATTACH_VIEWS_STEP_ID.to_string());
            *indeg.get_mut(ATTACH_VIEWS_STEP_ID).unwrap() += 1;
        } else if idx == 1 {
            // attach_views -> first postview step (others are ordered by intra-module seq edges)
            out.get_mut(ATTACH_VIEWS_STEP_ID)
                .unwrap()
                .push(s.id.clone());
            *indeg.get_mut(&s.id).unwrap() += 1;
        }
    }

    // Resource publish edges: publisher_step -> resource_node.
    for r in resources {
        let res_node = format!("{}{}", RESOURCE_DEP_PREFIX, r.id);
        out.get_mut(&r.published_by).unwrap().push(res_node.clone());
        *indeg.get_mut(&res_node).unwrap() += 1;
    }

    let mut q: VecDeque<String> = VecDeque::new();
    for (id, d) in indeg.iter() {
        if *d == 0 {
            q.push_back(id.clone());
        }
    }
    let mut visited = 0usize;
    while let Some(u) = q.pop_front() {
        visited += 1;
        for v in out.get(&u).unwrap().iter() {
            let dv = indeg.get_mut(v).unwrap();
            *dv -= 1;
            if *dv == 0 {
                q.push_back(v.clone());
            }
        }
    }
    if visited != nodes.len() {
        let stuck = indeg.iter().find(|(_k, v)| **v > 0).map(|(k, _)| k.clone());
        bail!("cycle detected in init dag (stuck_node={:?})", stuck);
    }
    Ok(())
}

fn assert_acyclic_nodes_with_deps(
    nodes: &BTreeSet<String>,
    deps: &BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let mut indeg: BTreeMap<&str, usize> = BTreeMap::new();
    let mut out: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for id in nodes {
        indeg.insert(id.as_str(), 0);
        out.insert(id.as_str(), Vec::new());
    }
    for (id, ds) in deps {
        if !nodes.contains(id) {
            continue;
        }
        for d in ds {
            if !nodes.contains(d) {
                continue;
            }
            *indeg.get_mut(id.as_str()).unwrap() += 1;
            out.get_mut(d.as_str()).unwrap().push(id.as_str());
        }
    }
    let mut q: VecDeque<&str> = VecDeque::new();
    for (id, d) in indeg.iter() {
        if *d == 0 {
            q.push_back(*id);
        }
    }
    let mut visited = 0usize;
    while let Some(u) = q.pop_front() {
        visited += 1;
        for v in out.get(u).unwrap().iter() {
            let dv = indeg.get_mut(v).unwrap();
            *dv -= 1;
            if *dv == 0 {
                q.push_back(*v);
            }
        }
    }
    if visited != nodes.len() {
        bail!("cycle detected in init dag");
    }
    Ok(())
}

fn build_viz_spec_with_attach_views_node(spec: &InitDagYaml) -> Result<InitStepsDagSpec> {
    let resources = spec.resources.as_ref().unwrap();
    let module_tags = spec.module_tags.as_ref().unwrap();

    let mut viz_resources: Vec<crate::dag_viz_html::InitResourceSpec> = Vec::new();
    for r in resources {
        viz_resources.push(crate::dag_viz_html::InitResourceSpec {
            id: r.id.clone(),
            tags: r.tags.clone(),
            publish_tags: r.publish_tags.clone(),
            hook_call: format!(
                "InitResourceHooks::publish_{}",
                resource_id_to_rust_snake_ident(&r.id)
            ),
            published_by: r.published_by.clone(),
            doc: r.doc.clone(),
        });
    }
    viz_resources.sort_by(|a, b| a.id.cmp(&b.id));

    let mut steps: Vec<InitStepSpec> = Vec::with_capacity(spec.steps.len() + 1);
    for s in &spec.steps {
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        let mut deps: Vec<String> = Vec::new();
        let mut waits: Vec<String> = Vec::new();
        for d in &s.deps {
            match parse_dep_entry(d)? {
                DepEntry::Step(step_id) => deps.push(step_id.to_string()),
                DepEntry::Resource(resource_id) => waits.push(resource_id.to_string()),
            }
        }
        deps.sort();
        deps.dedup();
        waits.sort();
        waits.dedup();
        let exec_call = match &s.exec {
            ExecSpec::Construct { call, .. } => call.clone(),
            ExecSpec::Call { call } => call.clone(),
        };
        steps.push(InitStepSpec {
            id: s.id.clone(),
            module: s.module.clone(),
            tags: if idx == 0 {
                if spec.version == 3 {
                    vec![
                        "master".to_string(),
                        "owner".to_string(),
                        "external".to_string(),
                    ]
                } else {
                    module_tags.get(&s.module).unwrap().clone()
                }
            } else {
                module_tags.get(&s.module).unwrap().clone()
            },
            order: idx,
            mode: s.mode.clone(),
            exec_call,
            deps,
            waits,
            doc: s.doc.clone(),
        });
    }

    // attach_views is a compiler-inserted barrier.
    // It is a hard runtime gating point for PostView availability, but is not a business dependency.
    // We show the node itself, while hiding its scheduling edges to keep the DAG clean.
    steps.push(InitStepSpec {
        id: ATTACH_VIEWS_STEP_ID.to_string(),
        module: ATTACH_VIEWS_MODULE.to_string(),
        tags: if spec.version == 5 {
            let mut all: BTreeSet<String> = BTreeSet::new();
            for v in spec.variants.as_ref().unwrap() {
                for t in &v.tags {
                    all.insert(t.clone());
                }
            }
            all.into_iter().collect()
        } else {
            vec!["master".to_string(), "owner".to_string(), "external".to_string()]
        },
        order: 0,
        mode: StepMode::Blocking,
        exec_call: "Framework::init_attach_views".to_string(),
        deps: Vec::new(),
        waits: Vec::new(),
        doc: "- 绑定: PostView(运行时View) -> 全部模块\n- 门闩: step.0 构造已完成\n- 注意: 本节点的调度约束不在图中展示"
            .to_string(),
    });

    Ok(InitStepsDagSpec {
        title: spec.title.clone(),
        steps,
        resources: viz_resources,
    })
}

fn generate_rust(spec: &InitDagYaml, cfg: &RustGenConfig) -> Result<String> {
    if cfg.init_fn_name.trim().is_empty() {
        bail!("RustGenConfig.init_fn_name must not be empty");
    }
    if cfg.framework_type_path.trim().is_empty() {
        bail!("RustGenConfig.framework_type_path must not be empty");
    }
    if cfg.framework_args_type_path.trim().is_empty() {
        bail!("RustGenConfig.framework_args_type_path must not be empty");
    }
    if cfg.result_type_path.trim().is_empty() {
        bail!("RustGenConfig.result_type_path must not be empty");
    }

    if spec.version == 5 {
        return generate_rust_v5(spec, cfg);
    }

    let resources = spec.resources.as_ref().unwrap();
    let module_tags = spec.module_tags.as_ref().unwrap();

    // Collect modules and their FrameworkArgs field names for step-0 Construct.
    let mut all_modules: BTreeSet<&str> = BTreeSet::new();
    let mut arg_field_by_module: BTreeMap<&str, String> = BTreeMap::new();
    for s in &spec.steps {
        all_modules.insert(s.module.as_str());
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        if idx != 0 {
            continue;
        }
        match &s.exec {
            ExecSpec::Construct { arg_field, .. } => {
                arg_field_by_module.insert(s.module.as_str(), arg_field.clone());
            }
            _ => unreachable!("step-0 must be Construct (validated earlier)"),
        }
    }

    let mut step_ids: Vec<&str> = spec.steps.iter().map(|s| s.id.as_str()).collect();
    step_ids.push(ATTACH_VIEWS_STEP_ID);
    step_ids.sort();

    let mut variant_by_id: BTreeMap<&str, String> = BTreeMap::new();
    for id in &step_ids {
        variant_by_id.insert(*id, step_id_to_rust_ident(id));
    }

    let mut resource_ids: Vec<&str> = resources.iter().map(|r| r.id.as_str()).collect();
    resource_ids.sort();
    let mut resource_variant_by_id: BTreeMap<&str, String> = BTreeMap::new();
    for id in &resource_ids {
        resource_variant_by_id.insert(*id, step_id_to_rust_ident(id));
    }

    let mut module_mask_by_module: BTreeMap<&str, u8> = BTreeMap::new();
    for (m, tags) in module_tags {
        let mask = tag_list_to_mask(tags, &format!("module_tags.{}", m))?;
        module_mask_by_module.insert(m.as_str(), mask);
    }

    let mut resource_mask_by_id: BTreeMap<&str, u8> = BTreeMap::new();
    let mut resource_publish_mask_by_id: BTreeMap<&str, u8> = BTreeMap::new();
    let mut resource_wait_mask_by_id: BTreeMap<&str, u8> = BTreeMap::new();
    for r in resources {
        let mask = tag_list_to_mask(&r.tags, &format!("resource {}.tags", r.id))?;
        resource_mask_by_id.insert(r.id.as_str(), mask);

        let pmask = tag_list_to_mask_allow_empty(
            &r.publish_tags,
            &format!("resource {}.publish_tags", r.id),
        )?;
        resource_publish_mask_by_id.insert(r.id.as_str(), pmask);
        resource_wait_mask_by_id.insert(r.id.as_str(), 0);
    }

    // Derive per-resource wait masks from the DAG edges:
    // a tag needs to execute wait_<resource>() only if some step in that tag depends on it.
    for s in &spec.steps {
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        let module_mask = *module_mask_by_module
            .get(s.module.as_str())
            .unwrap_or_else(|| panic!("module mask must exist: {}", s.module));
        let step_mask = if idx == 0 {
            if spec.version == 3 {
                TAG_ALL
            } else {
                module_mask
            }
        } else {
            module_mask
        };
        for d in &s.deps {
            let DepEntry::Resource(resource_id) = parse_dep_entry(d)? else {
                continue;
            };
            let e = resource_wait_mask_by_id
                .get_mut(resource_id)
                .unwrap_or_else(|| panic!("resource_wait_mask_by_id missing: {}", resource_id));
            *e |= step_mask;
        }
    }

    let deps_by_id_v3 = if spec.version == 3 {
        Some(build_runtime_node_deps_with_attach_views(spec)?)
    } else {
        None
    };

    let v4_master = if spec.version == 4 {
        Some(build_runtime_node_deps_with_attach_views_v4_for_tag(
            spec, TAG_MASTER,
        )?)
    } else {
        None
    };
    let v4_owner = if spec.version == 4 {
        Some(build_runtime_node_deps_with_attach_views_v4_for_tag(
            spec, TAG_OWNER,
        )?)
    } else {
        None
    };
    let v4_external = if spec.version == 4 {
        Some(build_runtime_node_deps_with_attach_views_v4_for_tag(
            spec,
            TAG_EXTERNAL,
        )?)
    } else {
        None
    };

    let mut out = String::new();
    out.push_str("// @generated by fluxon_util::init_dag_compiler. DO NOT EDIT.\n");
    out.push_str(&format!("// title: {}\n\n", spec.title.replace('\n', " ")));
    out.push_str(&format!(
        "pub use framework_init_generated::{{{}, InitResourceHooks, ResourceId}};\n\n",
        cfg.init_fn_name
    ));
    out.push_str("mod framework_init_generated {\n");

    if !all_modules.is_empty() {
        out.push_str("use super::{");
        let mut first = true;
        for m in &all_modules {
            if !first {
                out.push_str(", ");
            }
            first = false;
            out.push_str(m);
        }
        out.push_str("};\n");
    }

    out.push_str("use std::collections::{BTreeMap, BTreeSet};\n");
    out.push_str("use std::sync::{Arc, Mutex};\n");
    out.push_str("use async_trait::async_trait;\n");
    out.push_str("use anyhow::Context;\n");
    out.push_str("use tracing::{info, warn};\n\n");

    out.push_str(&format!("type FrameworkT = {};\n", cfg.framework_type_path));
    out.push_str(&format!(
        "type FrameworkArgsT = {};\n\n",
        cfg.framework_args_type_path
    ));

    // Role tag bits used for per-node scheduling.
    out.push_str("const TAG_MASTER: u8 = 1 << 0;\n");
    out.push_str("const TAG_OWNER: u8 = 1 << 1;\n");
    out.push_str("const TAG_EXTERNAL: u8 = 1 << 2;\n");
    out.push_str("const TAG_ALL: u8 = TAG_MASTER | TAG_OWNER | TAG_EXTERNAL;\n\n");

    // Determine init role (master/owner/external) from ClusterManagerNewArg.metadata.
    // No fallback: exactly one of {master, client, external_client} must be true.
    out.push_str(
        "fn detect_run_tag(args: &FrameworkArgsT) -> anyhow::Result<(u8, &'static str)> {\n",
    );
    out.push_str("    let md = &args.cluster_manager_arg.metadata;\n");
    out.push_str(
        "    let is_master = md.get(\"master\").map(|v| v == \"true\").unwrap_or(false);\n",
    );
    out.push_str(
        "    let is_owner = md.get(\"client\").map(|v| v == \"true\").unwrap_or(false);\n",
    );
    out.push_str("    let is_external = md.get(\"external_client\").map(|v| v == \"true\").unwrap_or(false);\n");
    out.push_str("    let count = (is_master as u8) + (is_owner as u8) + (is_external as u8);\n");
    out.push_str("    if count != 1 {\n");
    out.push_str("        return Err(anyhow::anyhow!(\"invalid role metadata for init: master={} owner(client)={} external={} (expected exactly one true)\", is_master, is_owner, is_external));\n");
    out.push_str("    }\n");
    out.push_str("    if is_master { return Ok((TAG_MASTER, \"master\")); }\n");
    out.push_str("    if is_owner { return Ok((TAG_OWNER, \"owner\")); }\n");
    out.push_str("    Ok((TAG_EXTERNAL, \"external\"))\n");
    out.push_str("}\n\n");

    out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]\n");
    out.push_str("enum StepId {\n");
    for id in &step_ids {
        out.push_str(&format!("    {},\n", variant_by_id.get(id).unwrap()));
    }
    out.push_str("}\n\n");

    out.push_str("impl StepId {\n");
    out.push_str("    fn as_str(&self) -> &'static str {\n");
    out.push_str("        match self {\n");
    for id in &step_ids {
        out.push_str(&format!(
            "            StepId::{} => {:?},\n",
            variant_by_id.get(id).unwrap(),
            id
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out.push_str(&format!(
        "const RESOURCE_COUNT: usize = {};\n\n",
        resource_ids.len()
    ));

    out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]\n");
    out.push_str("pub enum ResourceId {\n");
    for id in &resource_ids {
        out.push_str(&format!(
            "    {},\n",
            resource_variant_by_id.get(id).unwrap()
        ));
    }
    out.push_str("}\n\n");

    out.push_str("impl ResourceId {\n");
    out.push_str("    pub fn idx(&self) -> usize {\n");
    out.push_str("        match *self {\n");
    for (i, id) in resource_ids.iter().enumerate() {
        out.push_str(&format!(
            "            ResourceId::{} => {},\n",
            resource_variant_by_id.get(id).unwrap(),
            i
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n\n");

    out.push_str("    pub fn as_str(&self) -> &'static str {\n");
    out.push_str("        match *self {\n");
    for id in &resource_ids {
        out.push_str(&format!(
            "            ResourceId::{} => {:?},\n",
            resource_variant_by_id.get(id).unwrap(),
            id
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]\n");
    out.push_str("enum NodeId { Step(StepId), Resource(ResourceId) }\n\n");

    out.push_str("impl NodeId {\n");
    out.push_str("    fn as_str(&self) -> &'static str {\n");
    out.push_str("        match self {\n");
    out.push_str("            NodeId::Step(s) => s.as_str(),\n");
    out.push_str("            NodeId::Resource(r) => match *r {\n");
    for id in &resource_ids {
        let node_id = format!("{}{}", RESOURCE_DEP_PREFIX, id);
        out.push_str(&format!(
            "                ResourceId::{} => {:?},\n",
            resource_variant_by_id.get(id).unwrap(),
            node_id
        ));
    }
    out.push_str("            },\n");
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out.push_str("#[async_trait]\n");
    out.push_str("pub trait InitResourceHooks: Send + Sync {\n");
    for id in &resource_ids {
        let method = resource_id_to_rust_snake_ident(id);
        out.push_str(&format!(
            "    async fn publish_{}(fw: &FrameworkT) -> anyhow::Result<()>;\n",
            method
        ));
        out.push_str(&format!(
            "    async fn wait_{}(fw: &FrameworkT) -> anyhow::Result<()>;\n",
            method
        ));
    }
    out.push_str("}\n\n");

    out.push_str("#[derive(Clone, Copy, Debug)]\n");
    out.push_str("enum Mode { Blocking, AsyncSpawn, BestEffortWait }\n\n");

    out.push_str("#[derive(Clone, Copy, Debug)]\n");
    out.push_str("enum BestEffortOnFailure { AllowError, Error }\n\n");

    out.push_str("#[derive(Clone, Copy, Debug)]\n");
    out.push_str("struct BestEffortCfg { timeout_ms: u64, on_timeout: BestEffortOnFailure, on_error: BestEffortOnFailure }\n\n");

    out.push_str("#[derive(Clone, Copy, Debug)]\n");
    out.push_str("struct NodeMeta { id: NodeId, tag_mask: u8, mode: Mode, best_effort: Option<BestEffortCfg>, deps: &'static [NodeId] }\n\n");

    // ArgsStore for Construct steps.
    out.push_str("struct ArgsStore {\n");
    for m in &all_modules {
        let module_snake = to_snake(m);
        out.push_str(&format!(
            "    {}: Mutex<Option<<{} as fluxon_framework::LogicalModule>::NewArg>>,\n",
            module_snake, m
        ));
    }
    out.push_str("}\n\n");
    out.push_str("impl ArgsStore {\n");
    out.push_str("    fn new(args: FrameworkArgsT) -> Self {\n");
    out.push_str("        Self {\n");
    for m in &all_modules {
        let module_snake = to_snake(m);
        let arg_field = arg_field_by_module.get(m).unwrap();
        out.push_str(&format!(
            "            {}: Mutex::new(Some(args.{})),\n",
            module_snake, arg_field
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n\n");
    for m in &all_modules {
        let module_snake = to_snake(m);
        out.push_str(&format!(
            "    fn take_{}(&self) -> <{} as fluxon_framework::LogicalModule>::NewArg {{\n",
            module_snake, m
        ));
        out.push_str(&format!(
            "        self.{}.lock().unwrap().take().expect(\"{} arg already taken\")\n",
            module_snake, module_snake
        ));
        out.push_str("    }\n\n");
    }
    out.push_str("}\n\n");

    // Node meta arrays (steps + resources).
    if spec.version == 4 {
        out.push_str("const NODES_MASTER: &[NodeMeta] = &[\n");
    } else {
        out.push_str("const NODES: &[NodeMeta] = &[\n");
    }

    let node_expr = |node_id: &str| -> String {
        if let Some(resource_id) = node_id.strip_prefix(RESOURCE_DEP_PREFIX) {
            format!(
                "NodeId::Resource(ResourceId::{})",
                resource_variant_by_id.get(resource_id).unwrap()
            )
        } else {
            format!(
                "NodeId::Step(StepId::{})",
                variant_by_id.get(node_id).unwrap()
            )
        }
    };

    let emit_nodes = |out: &mut String,
                      nodes: &BTreeSet<String>,
                      deps_by_id: &BTreeMap<String, Vec<String>>,
                      tag_mask: u8|
     -> Result<()> {
        for id in &step_ids {
            if !nodes.contains(*id) {
                continue;
            }
            let (mode, be, deps) = if *id == ATTACH_VIEWS_STEP_ID {
                (
                    "Mode::Blocking".to_string(),
                    "None".to_string(),
                    deps_by_id
                        .get(ATTACH_VIEWS_STEP_ID)
                        .cloned()
                        .unwrap_or_default(),
                )
            } else {
                let s = spec.steps.iter().find(|s| s.id == *id).unwrap();
                let deps = deps_by_id.get(&s.id).cloned().unwrap_or_default();
                let be = if s.mode == StepMode::BestEffortWait {
                    let be = s.best_effort.as_ref().unwrap();
                    format!(
                        "Some(BestEffortCfg {{ timeout_ms: {}, on_timeout: BestEffortOnFailure::{}, on_error: BestEffortOnFailure::{} }})",
                        be.timeout_ms,
                        best_effort_to_rust(&be.on_timeout),
                        best_effort_to_rust(&be.on_error)
                    )
                } else {
                    "None".to_string()
                };
                (format!("Mode::{}", mode_to_rust(&s.mode)), be, deps)
            };

            out.push_str(&format!(
                "    NodeMeta {{ id: NodeId::Step(StepId::{}), tag_mask: {}, mode: {}, best_effort: {}, deps: &[{}] }},\n",
                variant_by_id.get(id).unwrap(),
                tag_mask,
                mode,
                be,
                deps.iter().map(|d| node_expr(d)).collect::<Vec<_>>().join(", ")
            ));
        }

        for id in &resource_ids {
            let node_id = format!("{}{}", RESOURCE_DEP_PREFIX, id);
            if !nodes.contains(&node_id) {
                continue;
            }
            let deps = deps_by_id.get(&node_id).cloned().unwrap_or_default();
            out.push_str(&format!(
                "    NodeMeta {{ id: NodeId::Resource(ResourceId::{}), tag_mask: {}, mode: Mode::Blocking, best_effort: None, deps: &[{}] }},\n",
                resource_variant_by_id.get(id).unwrap(),
                tag_mask,
                deps.iter().map(|d| node_expr(d)).collect::<Vec<_>>().join(", ")
            ));
        }
        Ok(())
    };

    if spec.version == 4 {
        let (nodes_m, deps_m) = v4_master.as_ref().unwrap();
        emit_nodes(&mut out, nodes_m, deps_m, TAG_ALL)?;
        out.push_str("];\n\n");

        out.push_str("const NODES_OWNER: &[NodeMeta] = &[\n");
        let (nodes_o, deps_o) = v4_owner.as_ref().unwrap();
        emit_nodes(&mut out, nodes_o, deps_o, TAG_ALL)?;
        out.push_str("];\n\n");

        out.push_str("const NODES_EXTERNAL: &[NodeMeta] = &[\n");
        let (nodes_e, deps_e) = v4_external.as_ref().unwrap();
        emit_nodes(&mut out, nodes_e, deps_e, TAG_ALL)?;
        out.push_str("];\n\n");
    } else {
        let deps_by_id = deps_by_id_v3.as_ref().unwrap();
        // v3 keeps a single nodes array with per-node tag masks.
        // Steps.
        for id in &step_ids {
            let (tag_mask, mode, be, deps) = if *id == ATTACH_VIEWS_STEP_ID {
                (
                    TAG_ALL,
                    "Mode::Blocking".to_string(),
                    "None".to_string(),
                    deps_by_id
                        .get(ATTACH_VIEWS_STEP_ID)
                        .cloned()
                        .unwrap_or_default(),
                )
            } else {
                let s = spec.steps.iter().find(|s| s.id == *id).unwrap();
                let (_m, idx, _name) = parse_step_id(&s.id)?;
                let tag_mask = if idx == 0 {
                    TAG_ALL
                } else {
                    *module_mask_by_module.get(s.module.as_str()).unwrap()
                };

                let deps = deps_by_id.get(&s.id).cloned().unwrap_or_default();
                let be = if s.mode == StepMode::BestEffortWait {
                    let be = s.best_effort.as_ref().unwrap();
                    format!(
                        "Some(BestEffortCfg {{ timeout_ms: {}, on_timeout: BestEffortOnFailure::{}, on_error: BestEffortOnFailure::{} }})",
                        be.timeout_ms,
                        best_effort_to_rust(&be.on_timeout),
                        best_effort_to_rust(&be.on_error)
                    )
                } else {
                    "None".to_string()
                };
                (
                    tag_mask,
                    format!("Mode::{}", mode_to_rust(&s.mode)),
                    be,
                    deps,
                )
            };

            out.push_str(&format!(
                "    NodeMeta {{ id: NodeId::Step(StepId::{}), tag_mask: {}, mode: {}, best_effort: {}, deps: &[{}] }},\n",
                variant_by_id.get(id).unwrap(),
                tag_mask,
                mode,
                be,
                deps.iter().map(|d| node_expr(d)).collect::<Vec<_>>().join(", ")
            ));
        }

        // Resources.
        for id in &resource_ids {
            let tag_mask = *resource_mask_by_id.get(*id).unwrap();
            let node_id = format!("{}{}", RESOURCE_DEP_PREFIX, id);
            let deps = deps_by_id.get(&node_id).cloned().unwrap_or_default();
            out.push_str(&format!(
                "    NodeMeta {{ id: NodeId::Resource(ResourceId::{}), tag_mask: {}, mode: Mode::Blocking, best_effort: None, deps: &[{}] }},\n",
                resource_variant_by_id.get(id).unwrap(),
                tag_mask,
                deps.iter().map(|d| node_expr(d)).collect::<Vec<_>>().join(", ")
            ));
        }
        out.push_str("];\n\n");
    }

    out.push_str(&format!(
        "pub async fn {}(fw: &FrameworkT, args: FrameworkArgsT) -> {} {{\n",
        cfg.init_fn_name, cfg.result_type_path
    ));
    out.push_str("    fw.init_set_resource_registry(fluxon_framework::ResourceRegistry::new(RESOURCE_COUNT));\n");
    out.push_str("    let (run_tag_bit, run_tag_name) = detect_run_tag(&args)?;\n");
    out.push_str("    let args = ArgsStore::new(args);\n");
    out.push_str("    info!(tag = run_tag_name, \"init-dag: begin\");\n");
    if spec.version == 4 {
        out.push_str("    let nodes: &'static [NodeMeta] = match run_tag_bit {\n");
        out.push_str("        TAG_MASTER => NODES_MASTER,\n");
        out.push_str("        TAG_OWNER => NODES_OWNER,\n");
        out.push_str("        TAG_EXTERNAL => NODES_EXTERNAL,\n");
        out.push_str("        _ => unreachable!(\"run_tag_bit must be exactly one bit\"),\n");
        out.push_str("    };\n");
        out.push_str(
            "    run_dag(fw, &args, run_tag_bit, nodes).await.context(\"run init dag\")?;\n",
        );
    } else {
        out.push_str(
            "    run_dag(fw, &args, run_tag_bit, NODES).await.context(\"run init dag\")?;\n",
        );
    }
    out.push_str("    info!(tag = run_tag_name, \"init-dag: initialized\");\n");
    out.push_str("    Ok(())\n");
    out.push_str("}\n\n");

    out.push_str("async fn run_dag(fw: &FrameworkT, args: &ArgsStore, run_tag_bit: u8, nodes: &'static [NodeMeta]) -> anyhow::Result<()> {\n");
    out.push_str("    let mut active: BTreeSet<NodeId> = BTreeSet::new();\n");
    out.push_str(
        "    for n in nodes { if (n.tag_mask & run_tag_bit) != 0 { active.insert(n.id); } }\n",
    );

    out.push_str("    let mut indeg: BTreeMap<NodeId, usize> = BTreeMap::new();\n");
    out.push_str("    let mut out_edges: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();\n");
    out.push_str("    let mut meta_by_id: BTreeMap<NodeId, NodeMeta> = BTreeMap::new();\n");
    out.push_str("    for n in nodes {\n");
    out.push_str("        if !active.contains(&n.id) { continue; }\n");
    out.push_str("        indeg.insert(n.id, 0);\n");
    out.push_str("        out_edges.insert(n.id, Vec::new());\n");
    out.push_str("        meta_by_id.insert(n.id, *n);\n");
    out.push_str("    }\n");
    out.push_str("    for n in nodes {\n");
    out.push_str("        if !active.contains(&n.id) { continue; }\n");
    out.push_str("        for d in n.deps {\n");
    out.push_str("            if !active.contains(d) {\n");
    out.push_str("                return Err(anyhow::anyhow!(\"init dag invalid for this role: node {} depends on inactive {}\", n.id.as_str(), d.as_str()));\n");
    out.push_str("            }\n");
    out.push_str("            *indeg.get_mut(&n.id).unwrap() += 1;\n");
    out.push_str("            out_edges.get_mut(d).unwrap().push(n.id);\n");
    out.push_str("        }\n");
    out.push_str("    }\n");

    out.push_str("    let mut ready: BTreeSet<NodeId> = BTreeSet::new();\n");
    out.push_str("    for (id, d) in &indeg { if *d == 0 { ready.insert(*id); } }\n");
    out.push_str(
        "    let mut running = ::fluxon_util::scoped_future_set::ScopedFutureSet::new();\n",
    );
    out.push_str("    let mut completed = 0usize;\n");
    out.push_str("    while completed < active.len() {\n");
    out.push_str("        while let Some(id) = ready.pop_first() {\n");
    out.push_str("            let meta = *meta_by_id.get(&id).unwrap();\n");
    out.push_str("            running.push(async move {\n");
    out.push_str("                let r = run_node(fw, args, run_tag_bit, meta).await;\n");
    out.push_str("                (id, r)\n");
    out.push_str("            });\n");
    out.push_str("        }\n");
    out.push_str("        let Some((id, r)) = running.next().await else {\n");
    out.push_str("            return Err(anyhow::anyhow!(\"init dag runner stalled: no ready nodes and no running tasks\"));\n");
    out.push_str("        };\n");
    out.push_str("        r.with_context(|| format!(\"init node failed: {}\", id.as_str()))?;\n");
    out.push_str("        completed += 1;\n");
    out.push_str("        for nxt in out_edges.get(&id).unwrap().iter().copied() {\n");
    out.push_str("            let d = indeg.get_mut(&nxt).unwrap();\n");
    out.push_str("            *d -= 1;\n");
    out.push_str("            if *d == 0 { ready.insert(nxt); }\n");
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("    Ok(())\n");
    out.push_str("}\n\n");

    out.push_str("async fn run_node(fw: &FrameworkT, args: &ArgsStore, run_tag_bit: u8, meta: NodeMeta) -> anyhow::Result<()> {\n");
    out.push_str("    match meta.id {\n");
    out.push_str("        NodeId::Step(step_id) => run_step(fw, args, run_tag_bit, step_id, meta.mode, meta.best_effort).await,\n");
    out.push_str("        NodeId::Resource(resource_id) => run_resource(fw, run_tag_bit, resource_id).await,\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out.push_str("async fn run_step(\n");
    out.push_str("    fw: &FrameworkT,\n");
    out.push_str("    args: &ArgsStore,\n");
    out.push_str("    run_tag_bit: u8,\n");
    out.push_str("    id: StepId,\n");
    out.push_str("    mode: Mode,\n");
    out.push_str("    best_effort: Option<BestEffortCfg>,\n");
    out.push_str(") -> anyhow::Result<()> {\n");
    out.push_str("    match mode {\n");
    out.push_str(
        "        Mode::Blocking => run_step_inner(fw, Some(args), run_tag_bit, id).await,\n",
    );
    out.push_str("        Mode::AsyncSpawn => {\n");
    out.push_str("            use fluxon_framework_compiled::spawn::ViewSpawnExt;\n");
    out.push_str("            let fw2 = fw.clone();\n");
    out.push_str("            let step = id;\n");
    out.push_str("            let name = format!(\"init_async:{}\", step.as_str());\n");
    out.push_str("            let boxed: ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ()> + Send>> = Box::pin(async move {\n");
    out.push_str("                let r = run_step_inner(&fw2, None, run_tag_bit, step).await;\n");
    out.push_str("                if let Err(e) = r {\n");
    out.push_str("                    warn!(step = step.as_str(), err = %e, \"async-spawn init step failed\");\n");
    out.push_str("                }\n");
    out.push_str("            });\n");
    out.push_str("            let handle = ViewSpawnExt::spawn_boxed(fw, boxed);\n");
    out.push_str("            ViewSpawnExt::push_join_handle(fw, name, handle);\n");
    out.push_str("            Ok(())\n");
    out.push_str("        }\n");
    out.push_str("        Mode::BestEffortWait => {\n");
    out.push_str("            let be = best_effort.expect(\"best_effort cfg must exist\");\n");
    out.push_str("            let fut = run_step_inner(fw, Some(args), run_tag_bit, id);\n");
    out.push_str("            match ::tokio::time::timeout(::std::time::Duration::from_millis(be.timeout_ms), fut).await {\n");
    out.push_str("                Ok(Ok(())) => Ok(()),\n");
    out.push_str("                Ok(Err(e)) => match be.on_error {\n");
    out.push_str("                    BestEffortOnFailure::AllowError => { warn!(step = id.as_str(), err = %e, \"best-effort init step failed; continuing\"); Ok(()) }\n");
    out.push_str("                    BestEffortOnFailure::Error => Err(e),\n");
    out.push_str("                },\n");
    out.push_str("                Err(_elapsed) => match be.on_timeout {\n");
    out.push_str("                    BestEffortOnFailure::AllowError => { warn!(step = id.as_str(), timeout_ms = be.timeout_ms, \"best-effort init step timed out; continuing\"); Ok(()) }\n");
    out.push_str("                    BestEffortOnFailure::Error => Err(anyhow::anyhow!(\"best-effort init step timed out (timeout_ms={})\", be.timeout_ms)),\n");
    out.push_str("                },\n");
    out.push_str("            }\n");
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    out.push_str("async fn run_resource(fw: &FrameworkT, run_tag_bit: u8, id: ResourceId) -> anyhow::Result<()> {\n");
    if resource_ids.is_empty() {
        out.push_str("    match id {}\n");
        out.push_str("}\n\n");
    } else {
        out.push_str("    match id {\n");
        for rid in &resource_ids {
            let var = resource_variant_by_id.get(rid).unwrap();
            let method = resource_id_to_rust_snake_ident(rid);
            let publish_mask = resource_publish_mask_by_id
                .get(rid)
                .unwrap_or_else(|| panic!("resource publish mask must exist: {}", rid));
            let wait_mask = resource_wait_mask_by_id
                .get(rid)
                .unwrap_or_else(|| panic!("resource wait mask must exist: {}", rid));
            out.push_str(&format!("        ResourceId::{} => {{\n", var));
            out.push_str(&format!(
                "            info!(\"init-dag: resource {}\");\n",
                rid
            ));
            out.push_str(&format!(
                "            if (run_tag_bit & {}) != 0 {{\n",
                publish_mask
            ));
            out.push_str(&format!(
                "                <FrameworkT as InitResourceHooks>::publish_{}(fw).await?;\n",
                method
            ));
            out.push_str("            }\n");
            out.push_str(&format!(
                "            if (run_tag_bit & {}) != 0 {{\n",
                wait_mask
            ));
            out.push_str(&format!(
                "                <FrameworkT as InitResourceHooks>::wait_{}(fw).await?;\n",
                method
            ));
            out.push_str("            }\n");
            out.push_str(&format!(
                "            fluxon_framework::framework::ResourceRegistryAccessTrait::resource_registry(fw).mark_ready(ResourceId::{}.idx());\n",
                var
            ));
            out.push_str("            Ok(())\n");
            out.push_str("        }\n");
        }
        out.push_str("    }\n");
        out.push_str("}\n\n");
    }

    out.push_str("async fn run_step_inner(fw: &FrameworkT, args: Option<&ArgsStore>, run_tag_bit: u8, id: StepId) -> anyhow::Result<()> {\n");
    out.push_str("    match id {\n");
    out.push_str(&format!(
        "        StepId::{} => {{\n",
        variant_by_id.get(ATTACH_VIEWS_STEP_ID).unwrap()
    ));
    out.push_str("            info!(\"init-dag: attach views\");\n");
    if spec.version == 4 {
        // In v4, modules can be absent for a role. We attach views only for modules
        // that are part of the selected role DAG.
        let mut view_method_by_module: BTreeMap<&str, String> = BTreeMap::new();
        for m in &all_modules {
            let m = *m;
            let arg_field = arg_field_by_module.get(m).ok_or_else(|| {
                anyhow::anyhow!("missing step-0 exec.arg_field for module: {}", m)
            })?;
            let base = arg_field.strip_suffix("_arg").ok_or_else(|| {
                anyhow::anyhow!(
                    "v4 requires step-0 exec.arg_field to end with _arg (module={}, arg_field={})",
                    m,
                    arg_field
                )
            })?;
            view_method_by_module.insert(m, format!("{}_view", base));
        }

        let mut mods_master: Vec<&str> = Vec::new();
        let mut mods_owner: Vec<&str> = Vec::new();
        let mut mods_external: Vec<&str> = Vec::new();
        for m in &all_modules {
            let m = *m;
            let mm = *module_mask_by_module
                .get(m)
                .unwrap_or_else(|| panic!("module mask must exist"));
            if (mm & TAG_MASTER) != 0 {
                mods_master.push(m);
            }
            if (mm & TAG_OWNER) != 0 {
                mods_owner.push(m);
            }
            if (mm & TAG_EXTERNAL) != 0 {
                mods_external.push(m);
            }
        }

        out.push_str("            fw.init_mark_views_ready();\n");
        out.push_str("            match run_tag_bit {\n");

        let emit_attach_block = |out: &mut String, modules: &[&str]| {
            for m in modules {
                let module_snake = to_snake(m);
                let view_method = view_method_by_module.get(*m).unwrap();
                out.push_str(&format!(
                    "                    let m = fw.init_get_{}();\n",
                    module_snake
                ));
                out.push_str(&format!(
                    "                    fluxon_framework::LogicalModule::attach_view(m.as_ref(), fw.{}());\n",
                    view_method
                ));
            }
        };

        out.push_str("                TAG_MASTER => {\n");
        emit_attach_block(&mut out, &mods_master);
        out.push_str("                }\n");
        out.push_str("                TAG_OWNER => {\n");
        emit_attach_block(&mut out, &mods_owner);
        out.push_str("                }\n");
        out.push_str("                TAG_EXTERNAL => {\n");
        emit_attach_block(&mut out, &mods_external);
        out.push_str("                }\n");
        out.push_str(
            "                _ => unreachable!(\"run_tag_bit must be exactly one bit\"),\n",
        );
        out.push_str("            }\n");
        out.push_str("            Ok(())\n");
    } else {
        out.push_str("            fw.init_attach_views();\n");
        out.push_str("            Ok(())\n");
    }
    out.push_str("        }\n");

    for s in &spec.steps {
        let var = variant_by_id.get(s.id.as_str()).unwrap();
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        out.push_str(&format!("        StepId::{} => {{\n", var));
        out.push_str(&format!("            info!(\"init-dag: {}\");\n", s.id));
        let module_snake = to_snake(s.module.as_str());
        match &s.exec {
            ExecSpec::Construct { call, .. } => {
                out.push_str(
                    "            let args = args.expect(\"construct step requires args store\");\n",
                );
                out.push_str(&format!(
                    "            let arg = args.take_{}();\n",
                    module_snake
                ));
                out.push_str(&format!("            let built = {}(arg).await;\n", call));
                out.push_str(
                    "            let module = built.map_err(|e| anyhow::anyhow!(\"{}\", e))?;\n",
                );
                out.push_str(&format!(
                    "            fw.init_set_{}(Arc::new(module));\n",
                    module_snake
                ));
                out.push_str("            Ok(())\n");
            }
            ExecSpec::Call { call } => {
                if idx == 0 {
                    unreachable!("step-0 cannot be Call (validated earlier)");
                }
                out.push_str(&format!(
                    "            let m = fw.init_get_{}();\n",
                    module_snake
                ));
                out.push_str(&format!(
                    "            ({})(m.as_ref()).await.map_err(|e| anyhow::anyhow!(\"{{}}\", e))?;\n",
                    call
                ));
                out.push_str("            Ok(())\n");
            }
        }
        out.push_str("        }\n");
    }
    out.push_str("    }\n");
    out.push_str("}\n");
    out.push_str("}\n");

    Ok(out)
}

fn generate_rust_v5(spec: &InitDagYaml, cfg: &RustGenConfig) -> Result<String> {
    assert!(
        spec.version == 5,
        "generate_rust_v5 requires spec.version == 5"
    );
    let resources = spec.resources.as_ref().unwrap();
    let _module_tags = spec.module_tags.as_ref().unwrap();
    let variants = spec.variants.as_ref().unwrap();

    // Collect modules and their FrameworkArgs field names for step-0 Construct.
    let mut all_modules: BTreeSet<&str> = BTreeSet::new();
    let mut arg_field_by_module: BTreeMap<&str, String> = BTreeMap::new();
    for s in &spec.steps {
        all_modules.insert(s.module.as_str());
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        if idx != 0 {
            continue;
        }
        match &s.exec {
            ExecSpec::Construct { arg_field, .. } => {
                arg_field_by_module.insert(s.module.as_str(), arg_field.clone());
            }
            _ => unreachable!("step-0 must be Construct (validated earlier)"),
        }
    }

    let mut resource_ids: Vec<&str> = resources.iter().map(|r| r.id.as_str()).collect();
    resource_ids.sort();
    let mut resource_variant_by_id: BTreeMap<&str, String> = BTreeMap::new();
    for id in &resource_ids {
        resource_variant_by_id.insert(*id, step_id_to_rust_ident(id));
    }

    let mut out = String::new();
    out.push_str("// @generated by fluxon_util::init_dag_compiler. DO NOT EDIT.\n");
    out.push_str(&format!("// title: {}\n\n", spec.title));

    // Public re-exports from the nested generated module.
    out.push_str("pub use framework_init_generated::{InitResourceHooks, ResourceId};\n");
    for v in variants {
        let fn_name = format!("{}_{}", cfg.init_fn_name, v.id);
        let args_name = format!("InitArgs{}", variant_id_to_rust_pascal_ident(&v.id));
        out.push_str(&format!(
            "pub use framework_init_generated::{{{}, {}}};\n",
            fn_name, args_name
        ));
    }
    out.push_str("\nmod framework_init_generated {\n");

    // Bring all module types into scope for associated NewArg types.
    out.push_str("use super::{");
    let mut module_list: Vec<&str> = all_modules.into_iter().collect();
    module_list.sort();
    for (i, m) in module_list.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(m);
    }
    out.push_str("};\n");

    out.push_str("use std::collections::{BTreeMap, BTreeSet};\n");
    out.push_str("use std::sync::{Arc, Mutex};\n");
    out.push_str("use async_trait::async_trait;\n");
    out.push_str("use anyhow::Context;\n");
    out.push_str("use tracing::{info, warn};\n\n");

    out.push_str(&format!("type FrameworkT = {};\n", cfg.framework_type_path));
    out.push_str(&format!("type InitResultT = {};\n\n", cfg.result_type_path));

    out.push_str(&format!(
        "const RESOURCE_COUNT: usize = {};\n\n",
        resources.len()
    ));

    // ResourceId enum is shared across variants.
    out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]\n");
    out.push_str("pub enum ResourceId {\n");
    for id in &resource_ids {
        out.push_str(&format!(
            "    {},\n",
            resource_variant_by_id.get(id).unwrap()
        ));
    }
    out.push_str("}\n\n");
    out.push_str("impl ResourceId {\n");
    out.push_str("    fn idx(&self) -> usize {\n");
    out.push_str("        match self {\n");
    for (idx, id) in resource_ids.iter().enumerate() {
        out.push_str(&format!(
            "            ResourceId::{} => {},\n",
            resource_variant_by_id.get(id).unwrap(),
            idx
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n\n");
    out.push_str("    fn as_str(&self) -> &'static str {\n");
    out.push_str("        match self {\n");
    for id in &resource_ids {
        out.push_str(&format!(
            "            ResourceId::{} => \"{}\",\n",
            resource_variant_by_id.get(id).unwrap(),
            id
        ));
    }
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    // Resource hooks are implemented by the user framework.
    out.push_str("#[async_trait]\n");
    out.push_str("pub trait InitResourceHooks: Send + Sync {\n");
    for id in &resource_ids {
        let snake = resource_id_to_rust_snake_ident(id);
        out.push_str(&format!(
            "    async fn publish_{}(fw: &FrameworkT) -> anyhow::Result<()>;\n",
            snake
        ));
        out.push_str(&format!(
            "    async fn wait_{}(fw: &FrameworkT) -> anyhow::Result<()>;\n",
            snake
        ));
    }
    out.push_str("}\n\n");

    out.push_str("#[derive(Clone, Copy, Debug)]\n");
    out.push_str("enum Mode { Blocking, AsyncSpawn, BestEffortWait }\n\n");
    out.push_str("#[derive(Clone, Copy, Debug)]\n");
    out.push_str("enum BestEffortOnFailure { AllowError, Error }\n\n");
    out.push_str("#[derive(Clone, Copy, Debug)]\n");
    out.push_str(
        "struct BestEffortCfg { timeout_ms: u64, on_timeout: BestEffortOnFailure, on_error: BestEffortOnFailure }\n\n",
    );

    // Generate variant-specific args types + runners.
    for v in variants {
        let variant_id = v.id.as_str();
        let variant_tags: BTreeSet<&str> = v.tags.iter().map(|s| s.as_str()).collect();

        // Collect active nodes + deps for this variant.
        let (nodes, deps_by_id) =
            build_runtime_node_deps_with_attach_views_v5_for_variant(spec, &variant_tags)?;

        // Determine which modules are constructed (step.0) in this variant.
        let mut construct_modules: Vec<&str> = Vec::new();
        for s in &spec.steps {
            if !nodes.contains(&s.id) {
                continue;
            }
            let (_m, idx, _name) = parse_step_id(&s.id)?;
            if idx == 0 {
                construct_modules.push(s.module.as_str());
            }
        }
        construct_modules.sort();
        construct_modules.dedup();

        let args_name = format!("InitArgs{}", variant_id_to_rust_pascal_ident(variant_id));
        out.push_str("#[derive(Debug)]\n");
        out.push_str(&format!("pub struct {} {{\n", args_name));
        for m in &construct_modules {
            let arg_field = arg_field_by_module
                .get(m)
                .unwrap_or_else(|| panic!("arg_field must exist for module step0"));
            out.push_str(&format!(
                "    pub {}: <{} as fluxon_framework::LogicalModule>::NewArg,\n",
                arg_field, m
            ));
        }
        out.push_str("}\n\n");

        // Build StepId enum for this variant.
        let mut step_ids: Vec<&str> = Vec::new();
        for id in nodes.iter() {
            if id == ATTACH_VIEWS_STEP_ID {
                step_ids.push(ATTACH_VIEWS_STEP_ID);
                continue;
            }
            if id.starts_with(RESOURCE_DEP_PREFIX) {
                continue;
            }
            step_ids.push(id.as_str());
        }
        step_ids.sort();
        let mut step_variant_by_id: BTreeMap<&str, String> = BTreeMap::new();
        for id in &step_ids {
            step_variant_by_id.insert(*id, step_id_to_rust_ident(id));
        }

        // Variant module.
        let mod_name = format!("variant_{}", variant_id);
        out.push_str(&format!("mod {} {{\n", mod_name));
        out.push_str("use super::*;\n\n");

        out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]\n");
        out.push_str("enum StepId {\n");
        for id in &step_ids {
            out.push_str(&format!("    {},\n", step_variant_by_id.get(id).unwrap()));
        }
        out.push_str("}\n\n");
        out.push_str("impl StepId {\n");
        out.push_str("    fn as_str(&self) -> &'static str {\n");
        out.push_str("        match self {\n");
        for id in &step_ids {
            out.push_str(&format!(
                "            StepId::{} => \"{}\",\n",
                step_variant_by_id.get(id).unwrap(),
                id
            ));
        }
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]\n");
        out.push_str("enum NodeId { Step(StepId), Resource(ResourceId) }\n\n");
        out.push_str("impl NodeId {\n");
        out.push_str("    fn as_str(&self) -> &'static str {\n");
        out.push_str("        match self {\n");
        out.push_str("            NodeId::Step(s) => s.as_str(),\n");
        out.push_str("            NodeId::Resource(r) => match *r {\n");
        for id in &resource_ids {
            out.push_str(&format!(
                "                ResourceId::{} => \"res:{}\",\n",
                resource_variant_by_id.get(id).unwrap(),
                id
            ));
        }
        out.push_str("            },\n");
        out.push_str("        }\n");
        out.push_str("    }\n");
        out.push_str("}\n\n");

        out.push_str("#[derive(Clone, Copy, Debug)]\n");
        out.push_str(
            "struct NodeMeta { id: NodeId, mode: Mode, best_effort: Option<BestEffortCfg>, deps: &'static [NodeId] }\n\n",
        );

        // ArgsStore for this variant.
        out.push_str("struct ArgsStore {\n");
        for m in &construct_modules {
            let field = to_snake(m);
            out.push_str(&format!(
                "    {}: Mutex<Option<<{} as fluxon_framework::LogicalModule>::NewArg>>,
",
                field, m
            ));
        }
        out.push_str("}\n\n");
        out.push_str("impl ArgsStore {\n");
        out.push_str(&format!(
            "    fn new(args: super::{}) -> Self {{\n",
            args_name
        ));
        out.push_str("        Self {\n");
        for m in &construct_modules {
            let field = to_snake(m);
            let arg_field = arg_field_by_module.get(m).unwrap();
            out.push_str(&format!(
                "            {}: Mutex::new(Some(args.{})),\n",
                field, arg_field
            ));
        }
        out.push_str("        }\n");
        out.push_str("    }\n\n");
        for m in &construct_modules {
            let field = to_snake(m);
            out.push_str(&format!(
                "    fn take_{}(&self) -> <{} as fluxon_framework::LogicalModule>::NewArg {{\n",
                field, m
            ));
            out.push_str(&format!(
                "        self.{}.lock().unwrap().take().expect(\"{} arg already taken\")\n",
                field, field
            ));
            out.push_str("    }\n\n");
        }
        out.push_str("}\n\n");

        // NODES array.
        let mut node_ids: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
        node_ids.sort();
        out.push_str("const NODES: &[NodeMeta] = &[\n");
        for id in &node_ids {
            let deps = deps_by_id
                .get(*id)
                .unwrap_or_else(|| panic!("deps missing node"));
            let (mode, best_effort) = if *id == ATTACH_VIEWS_STEP_ID {
                (StepMode::Blocking, None)
            } else if let Some(rest) = id.strip_prefix(RESOURCE_DEP_PREFIX) {
                let _ = rest;
                (StepMode::Blocking, None)
            } else {
                let step = spec
                    .steps
                    .iter()
                    .find(|s| s.id == *id)
                    .unwrap_or_else(|| panic!("step not found: {}", id));
                (step.mode.clone(), step.best_effort.clone())
            };

            let id_expr = if *id == ATTACH_VIEWS_STEP_ID {
                "NodeId::Step(StepId::FrameworkStep0AttachViews)".to_string()
            } else if let Some(rest) = id.strip_prefix(RESOURCE_DEP_PREFIX) {
                let rv = resource_variant_by_id.get(rest).unwrap();
                format!("NodeId::Resource(ResourceId::{})", rv)
            } else {
                let sv = step_variant_by_id.get(*id).unwrap();
                format!("NodeId::Step(StepId::{})", sv)
            };

            let deps_expr = if deps.is_empty() {
                "&[]".to_string()
            } else {
                let mut parts: Vec<String> = Vec::new();
                for d in deps {
                    let expr = if d == ATTACH_VIEWS_STEP_ID {
                        "NodeId::Step(StepId::FrameworkStep0AttachViews)".to_string()
                    } else if let Some(rest) = d.strip_prefix(RESOURCE_DEP_PREFIX) {
                        let rv = resource_variant_by_id.get(rest).unwrap();
                        format!("NodeId::Resource(ResourceId::{})", rv)
                    } else {
                        let sv = step_variant_by_id.get(d.as_str()).unwrap();
                        format!("NodeId::Step(StepId::{})", sv)
                    };
                    parts.push(expr);
                }
                format!("&[{}]", parts.join(", "))
            };

            let be_expr = if mode != StepMode::BestEffortWait {
                "None".to_string()
            } else {
                let be = best_effort.expect("best_effort must exist");
                format!(
                    "Some(BestEffortCfg {{ timeout_ms: {}, on_timeout: BestEffortOnFailure::{}, on_error: BestEffortOnFailure::{} }})",
                    be.timeout_ms,
                    best_effort_to_rust(&be.on_timeout),
                    best_effort_to_rust(&be.on_error)
                )
            };

            out.push_str(&format!(
                "    NodeMeta {{ id: {}, mode: Mode::{}, best_effort: {}, deps: {} }},\n",
                id_expr,
                mode_to_rust(&mode),
                be_expr,
                deps_expr
            ));
        }
        out.push_str("];\n\n");

        // Public init entry for this variant.
        let init_fn_name = format!("{}_{}", cfg.init_fn_name, variant_id);
        out.push_str(&format!(
            "pub async fn {}(fw: &FrameworkT, args: super::{}) -> InitResultT {{\n",
            init_fn_name, args_name
        ));
        out.push_str(
            "    fw.init_set_resource_registry(fluxon_framework::ResourceRegistry::new(RESOURCE_COUNT));\n",
        );
        out.push_str("    let args = ArgsStore::new(args);\n");
        out.push_str(&format!(
            "    info!(variant = \"{}\", \"init-dag: begin\");\n",
            variant_id
        ));
        out.push_str("    run_dag(fw, &args).await.context(\"run init dag\")?;\n");
        out.push_str(&format!(
            "    info!(variant = \"{}\", \"init-dag: initialized\");\n",
            variant_id
        ));
        out.push_str("    Ok(())\n");
        out.push_str("}\n\n");

        // Runner.
        out.push_str(
            "async fn run_dag(fw: &FrameworkT, args: &ArgsStore) -> anyhow::Result<()> {\n",
        );
        out.push_str(
            "    let mut indeg: BTreeMap<NodeId, usize> = BTreeMap::new();\n    let mut out_edges: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();\n    let mut meta_by_id: BTreeMap<NodeId, NodeMeta> = BTreeMap::new();\n",
        );
        out.push_str(
            "    for n in NODES { indeg.insert(n.id, 0); out_edges.insert(n.id, Vec::new()); meta_by_id.insert(n.id, *n); }\n",
        );
        out.push_str(
            "    for n in NODES { for d in n.deps { *indeg.get_mut(&n.id).unwrap() += 1; out_edges.get_mut(d).unwrap().push(n.id); } }\n",
        );
        out.push_str(
            "    let mut ready: BTreeSet<NodeId> = BTreeSet::new();\n    for (id, d) in &indeg { if *d == 0 { ready.insert(*id); } }\n",
        );
        out.push_str(
            "    let mut running = ::fluxon_util::scoped_future_set::ScopedFutureSet::new();\n    let mut completed = 0usize;\n",
        );
        out.push_str(
            "    while completed < NODES.len() {\n        while let Some(id) = ready.pop_first() {\n            let meta = *meta_by_id.get(&id).unwrap();\n            running.push(async move { let r = run_node(fw, args, meta).await; (id, r) });\n        }\n        let Some((id, r)) = running.next().await else { return Err(anyhow::anyhow!(\"init dag runner stalled: no ready nodes and no running tasks\")); };\n        r.with_context(|| format!(\"init node failed: {}\", id.as_str()))?;\n        completed += 1;\n        for nxt in out_edges.get(&id).unwrap().iter().copied() {\n            let d = indeg.get_mut(&nxt).unwrap();\n            *d -= 1;\n            if *d == 0 { ready.insert(nxt); }\n        }\n    }\n    Ok(())\n}\n\n",
        );

        out.push_str(
            "async fn run_node(fw: &FrameworkT, args: &ArgsStore, meta: NodeMeta) -> anyhow::Result<()> {\n    match meta.id {\n        NodeId::Step(step_id) => run_step(fw, args, step_id, meta.mode, meta.best_effort).await,\n        NodeId::Resource(resource_id) => run_resource(fw, resource_id).await,\n    }\n}\n\n",
        );

        out.push_str(
            "async fn run_step(fw: &FrameworkT, args: &ArgsStore, id: StepId, mode: Mode, best_effort: Option<BestEffortCfg>) -> anyhow::Result<()> {\n    match mode {\n        Mode::Blocking => run_step_inner(fw, Some(args), id).await,\n        Mode::AsyncSpawn => {\n            use fluxon_framework_compiled::spawn::ViewSpawnExt;\n            let fw2 = fw.clone();\n            let step = id;\n            let name = format!(\"init_async:{}\", step.as_str());\n            let boxed: ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ()> + Send>> = Box::pin(async move {\n                let r = run_step_inner(&fw2, None, step).await;\n                if let Err(e) = r { warn!(step = step.as_str(), err = %e, \"async-spawn init step failed\"); }\n            });\n            let handle = ViewSpawnExt::spawn_boxed(fw, boxed);\n            ViewSpawnExt::push_join_handle(fw, name, handle);\n            Ok(())\n        }\n        Mode::BestEffortWait => {\n            let be = best_effort.expect(\"best_effort cfg must exist\");\n            let fut = run_step_inner(fw, Some(args), id);\n            match ::tokio::time::timeout(::std::time::Duration::from_millis(be.timeout_ms), fut).await {\n                Ok(Ok(())) => Ok(()),\n                Ok(Err(e)) => match be.on_error {\n                    BestEffortOnFailure::AllowError => { warn!(step = id.as_str(), err = %e, \"best-effort init step failed; continuing\"); Ok(()) }\n                    BestEffortOnFailure::Error => Err(e),\n                },\n                Err(_elapsed) => match be.on_timeout {\n                    BestEffortOnFailure::AllowError => { warn!(step = id.as_str(), timeout_ms = be.timeout_ms, \"best-effort init step timed out; continuing\"); Ok(()) }\n                    BestEffortOnFailure::Error => Err(anyhow::anyhow!(\"best-effort init step timed out (timeout_ms={})\", be.timeout_ms)),\n                },\n            }\n        }\n    }\n}\n\n",
        );

        // Resource runner.
        out.push_str(
            "async fn run_resource(fw: &FrameworkT, id: ResourceId) -> anyhow::Result<()> {\n",
        );
        out.push_str("    match id {\n");
        for id in &resource_ids {
            let r = resources
                .iter()
                .find(|rr| rr.id.as_str() == *id)
                .unwrap_or_else(|| panic!("resource missing: {}", id));
            let rid = resource_variant_by_id.get(*id).unwrap();
            let active_in_variant = tags_overlap(&variant_tags, &r.tags)
                || r.publish_tags
                    .iter()
                    .any(|t| variant_tags.contains(t.as_str()));
            if !active_in_variant {
                out.push_str(&format!(
                    "        ResourceId::{} => unreachable!(\"resource {} is not active in variant {}\"),\n",
                    rid, r.id, variant_id
                ));
                continue;
            }
            let snake = resource_id_to_rust_snake_ident(&r.id);
            let do_publish = r
                .publish_tags
                .iter()
                .any(|t| variant_tags.contains(t.as_str()));
            let do_wait = tags_overlap(&variant_tags, &r.tags);
            out.push_str(&format!("        ResourceId::{} => {{\n", rid));
            out.push_str(&format!(
                "            info!(\"init-dag: resource {}\");\n",
                r.id
            ));
            if do_publish {
                out.push_str(&format!(
                    "            <FrameworkT as InitResourceHooks>::publish_{}(fw).await?;\n",
                    snake
                ));
            }
            if do_wait {
                out.push_str(&format!(
                    "            <FrameworkT as InitResourceHooks>::wait_{}(fw).await?;\n",
                    snake
                ));
            }
            out.push_str(&format!(
                "            fluxon_framework::framework::ResourceRegistryAccessTrait::resource_registry(fw).mark_ready(ResourceId::{}.idx());\n",
                rid
            ));
            out.push_str("            Ok(())\n");
            out.push_str("        }\n");
        }
        out.push_str("    }\n");
        out.push_str("}\n\n");

        // Step runner (match only active steps).
        out.push_str("async fn run_step_inner(fw: &FrameworkT, args: Option<&ArgsStore>, id: StepId) -> anyhow::Result<()> {\n");
        out.push_str("    match id {\n");

        // attach_views barrier.
        if nodes.contains(ATTACH_VIEWS_STEP_ID) {
            out.push_str("        StepId::FrameworkStep0AttachViews => {\n");
            out.push_str("            info!(\"init-dag: attach views\");\n");
            out.push_str("            fw.init_mark_views_ready();\n");
            // Attach views for constructed modules only.
            for m in &construct_modules {
                let module_snake = to_snake(m);
                let arg_field = arg_field_by_module.get(m).unwrap();
                let base_view = match arg_field.strip_suffix("_arg") {
                    Some(base) => format!("{}_view", base),
                    None => bail!(
                        "v5 requires step-0 exec.arg_field to end with _arg (module={}, arg_field={})",
                        m,
                        arg_field
                    ),
                };
                out.push_str(&format!(
                    "            let m = fw.init_get_{}();\n",
                    module_snake
                ));
                out.push_str(&format!(
                    "            fluxon_framework::LogicalModule::attach_view(m.as_ref(), fw.{}());\n",
                    base_view
                ));
            }
            out.push_str("            Ok(())\n");
            out.push_str("        }\n");
        }

        for s in &spec.steps {
            if !nodes.contains(&s.id) {
                continue;
            }
            let var = step_variant_by_id.get(s.id.as_str()).unwrap();
            let (_m, idx, _name) = parse_step_id(&s.id)?;
            out.push_str(&format!("        StepId::{} => {{\n", var));
            out.push_str(&format!("            info!(\"init-dag: {}\");\n", s.id));
            let module_snake = to_snake(s.module.as_str());
            match &s.exec {
                ExecSpec::Construct { call, .. } => {
                    out.push_str(
                        "            let args = args.expect(\"construct step requires args store\");\n",
                    );
                    out.push_str(&format!(
                        "            let arg = args.take_{}();\n",
                        module_snake
                    ));
                    out.push_str(&format!("            let built = {}(arg).await;\n", call));
                    out.push_str("            let module = built.map_err(|e| anyhow::anyhow!(\"{}\", e))?;\n");
                    out.push_str(&format!(
                        "            fw.init_set_{}(Arc::new(module));\n",
                        module_snake
                    ));
                    out.push_str("            Ok(())\n");
                }
                ExecSpec::Call { call } => {
                    if idx == 0 {
                        unreachable!("step-0 cannot be Call (validated earlier)");
                    }
                    out.push_str(&format!(
                        "            let m = fw.init_get_{}();\n",
                        module_snake
                    ));
                    out.push_str(&format!(
                        "            ({})(m.as_ref()).await.map_err(|e| anyhow::anyhow!(\"{{}}\", e))?;\n",
                        call
                    ));
                    out.push_str("            Ok(())\n");
                }
            }
            out.push_str("        }\n");
        }

        out.push_str("    }\n");
        out.push_str("}\n");

        // Close variant module.
        out.push_str("}\n\n");
        out.push_str(&format!("pub use {}::{};\n\n", mod_name, init_fn_name));
    }

    out.push_str("}\n");
    Ok(out)
}

fn variant_id_to_rust_pascal_ident(id: &str) -> String {
    // v5 validation keeps id ASCII and identifier-friendly.
    let mut out = String::new();
    for part in id.split('_') {
        if part.is_empty() {
            continue;
        }
        let mut it = part.chars();
        let first = it.next().unwrap();
        out.push(first.to_ascii_uppercase());
        for c in it {
            out.push(c);
        }
    }
    out
}

fn build_runtime_node_deps_with_attach_views(
    spec: &InitDagYaml,
) -> Result<BTreeMap<String, Vec<String>>> {
    let resources = spec.resources.as_ref().unwrap();

    let mut deps: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let mut nodes: BTreeSet<String> = BTreeSet::new();
    for s in &spec.steps {
        nodes.insert(s.id.clone());
    }
    nodes.insert(ATTACH_VIEWS_STEP_ID.to_string());
    for r in resources {
        nodes.insert(format!("{}{}", RESOURCE_DEP_PREFIX, r.id));
    }

    // Explicit deps.
    for s in &spec.steps {
        for d in &s.deps {
            match parse_dep_entry(d)? {
                DepEntry::Step(step_id) => {
                    deps.entry(s.id.clone())
                        .or_default()
                        .push(step_id.to_string());
                }
                DepEntry::Resource(resource_id) => {
                    deps.entry(s.id.clone())
                        .or_default()
                        .push(format!("{}{}", RESOURCE_DEP_PREFIX, resource_id));
                }
            }
        }
    }

    // Implicit intra-module ordering deps.
    let mut by_module: BTreeMap<&str, Vec<&InitDagStepYaml>> = BTreeMap::new();
    for s in &spec.steps {
        by_module.entry(s.module.as_str()).or_default().push(s);
    }
    for steps in by_module.values_mut() {
        steps.sort_by(|a, b| {
            let (_, ia, _) = parse_step_id(&a.id).unwrap();
            let (_, ib, _) = parse_step_id(&b.id).unwrap();
            ia.cmp(&ib).then_with(|| a.id.cmp(&b.id))
        });
        for w in steps.windows(2) {
            deps.entry(w[1].id.clone())
                .or_default()
                .push(w[0].id.clone());
        }
    }

    // attach_views barrier deps.
    let mut attach_deps: Vec<String> = Vec::new();
    for s in &spec.steps {
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        if idx == 0 {
            attach_deps.push(s.id.clone());
        } else if idx == 1 {
            deps.entry(s.id.clone())
                .or_default()
                .push(ATTACH_VIEWS_STEP_ID.to_string());
        }
    }
    deps.insert(ATTACH_VIEWS_STEP_ID.to_string(), attach_deps);

    // Resource publish deps: resource_node depends on its publisher step.
    for r in resources {
        deps.entry(format!("{}{}", RESOURCE_DEP_PREFIX, r.id))
            .or_default()
            .push(r.published_by.clone());
    }

    // Normalize.
    for id in nodes.iter() {
        deps.entry(id.clone()).or_default();
    }

    for v in deps.values_mut() {
        v.sort();
        v.dedup();
        v.retain(|id| nodes.contains(id.as_str()));
    }
    Ok(deps)
}

fn build_runtime_node_deps_with_attach_views_v4_for_tag(
    spec: &InitDagYaml,
    tag_bit: u8,
) -> Result<(BTreeSet<String>, BTreeMap<String, Vec<String>>)> {
    assert!(spec.version == 4, "v4 builder requires spec.version == 4");
    let resources = spec.resources.as_ref().unwrap();
    let module_tags = spec.module_tags.as_ref().unwrap();

    let mut module_mask_by_module: BTreeMap<&str, u8> = BTreeMap::new();
    for (m, tags) in module_tags {
        let mask = tag_list_to_mask(tags, &format!("module_tags.{}", m))?;
        module_mask_by_module.insert(m.as_str(), mask);
    }

    let mut resource_mask_by_id: BTreeMap<&str, u8> = BTreeMap::new();
    for r in resources {
        let mask = tag_list_to_mask(&r.tags, &format!("resource {}.tags", r.id))?;
        resource_mask_by_id.insert(r.id.as_str(), mask);
    }

    let mut nodes: BTreeSet<String> = BTreeSet::new();
    for s in &spec.steps {
        let module_mask = *module_mask_by_module
            .get(s.module.as_str())
            .unwrap_or_else(|| panic!("module mask must exist"));
        if is_tag_active(module_mask, tag_bit) {
            nodes.insert(s.id.clone());
        }
    }
    nodes.insert(ATTACH_VIEWS_STEP_ID.to_string());
    for r in resources {
        let mask = *resource_mask_by_id
            .get(r.id.as_str())
            .unwrap_or_else(|| panic!("resource mask must exist"));
        if is_tag_active(mask, tag_bit) {
            nodes.insert(format!("{}{}", RESOURCE_DEP_PREFIX, r.id));
        }
    }

    let mut deps: BTreeMap<String, Vec<String>> = BTreeMap::new();

    // Explicit deps.
    for s in &spec.steps {
        if !nodes.contains(&s.id) {
            continue;
        }
        for d in &s.deps {
            match parse_dep_entry(d)? {
                DepEntry::Step(step_id) => {
                    if !nodes.contains(step_id) {
                        bail!(
                            "v4 init dag invalid for tag: step {} depends on inactive step {}",
                            s.id,
                            step_id
                        );
                    }
                    deps.entry(s.id.clone())
                        .or_default()
                        .push(step_id.to_string());
                }
                DepEntry::Resource(resource_id) => {
                    let res_node = format!("{}{}", RESOURCE_DEP_PREFIX, resource_id);
                    if !nodes.contains(&res_node) {
                        bail!(
                            "v4 init dag invalid for tag: step {} depends on inactive resource {}",
                            s.id,
                            resource_id
                        );
                    }
                    deps.entry(s.id.clone()).or_default().push(res_node);
                }
            }
        }
    }

    // Implicit intra-module ordering deps (only among active steps).
    let mut by_module: BTreeMap<&str, Vec<&InitDagStepYaml>> = BTreeMap::new();
    for s in &spec.steps {
        if !nodes.contains(&s.id) {
            continue;
        }
        by_module.entry(s.module.as_str()).or_default().push(s);
    }
    for steps in by_module.values_mut() {
        steps.sort_by(|a, b| {
            let (_, ia, _) = parse_step_id(&a.id).unwrap();
            let (_, ib, _) = parse_step_id(&b.id).unwrap();
            ia.cmp(&ib).then_with(|| a.id.cmp(&b.id))
        });
        for w in steps.windows(2) {
            deps.entry(w[1].id.clone())
                .or_default()
                .push(w[0].id.clone());
        }
    }

    // attach_views barrier deps (tag-local).
    let mut attach_deps: Vec<String> = Vec::new();
    for s in &spec.steps {
        if !nodes.contains(&s.id) {
            continue;
        }
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        if idx == 0 {
            attach_deps.push(s.id.clone());
        } else if idx == 1 {
            deps.entry(s.id.clone())
                .or_default()
                .push(ATTACH_VIEWS_STEP_ID.to_string());
        }
    }
    deps.insert(ATTACH_VIEWS_STEP_ID.to_string(), attach_deps);

    // Resource publish deps: resource_node depends on its publisher step.
    for r in resources {
        let res_node = format!("{}{}", RESOURCE_DEP_PREFIX, r.id);
        if !nodes.contains(&res_node) {
            continue;
        }
        if !nodes.contains(&r.published_by) {
            bail!(
                "v4 init dag invalid for tag: resource {} depends on inactive publisher step {}",
                r.id,
                r.published_by
            );
        }
        deps.entry(res_node)
            .or_default()
            .push(r.published_by.clone());
    }

    // Normalize.
    for id in nodes.iter() {
        deps.entry(id.clone()).or_default();
    }
    for v in deps.values_mut() {
        v.sort();
        v.dedup();
        v.retain(|id| nodes.contains(id.as_str()));
    }

    Ok((nodes, deps))
}

fn build_runtime_node_deps_with_attach_views_v5_for_variant(
    spec: &InitDagYaml,
    variant_tags: &BTreeSet<&str>,
) -> Result<(BTreeSet<String>, BTreeMap<String, Vec<String>>)> {
    assert!(spec.version == 5, "v5 builder requires spec.version == 5");
    let resources = spec.resources.as_ref().unwrap();
    let module_tags = spec.module_tags.as_ref().unwrap();

    let mut nodes: BTreeSet<String> = BTreeSet::new();
    for s in &spec.steps {
        let tags = module_tags
            .get(s.module.as_str())
            .unwrap_or_else(|| panic!("module_tags must contain module: {}", s.module));
        if tags_overlap(variant_tags, tags) {
            nodes.insert(s.id.clone());
        }
    }

    nodes.insert(ATTACH_VIEWS_STEP_ID.to_string());

    for r in resources {
        let active = tags_overlap(variant_tags, &r.tags)
            || r.publish_tags
                .iter()
                .any(|t| variant_tags.contains(t.as_str()));
        if active {
            nodes.insert(format!("{}{}", RESOURCE_DEP_PREFIX, r.id));
        }
    }

    let mut deps: BTreeMap<String, Vec<String>> = BTreeMap::new();

    // Explicit deps.
    for s in &spec.steps {
        if !nodes.contains(&s.id) {
            continue;
        }
        for d in &s.deps {
            match parse_dep_entry(d)? {
                DepEntry::Step(step_id) => {
                    if !nodes.contains(step_id) {
                        bail!(
                            "v5 init dag invalid for variant: step {} depends on inactive step {}",
                            s.id,
                            step_id
                        );
                    }
                    deps.entry(s.id.clone())
                        .or_default()
                        .push(step_id.to_string());
                }
                DepEntry::Resource(resource_id) => {
                    let res_node = format!("{}{}", RESOURCE_DEP_PREFIX, resource_id);
                    if !nodes.contains(&res_node) {
                        bail!(
                            "v5 init dag invalid for variant: step {} depends on inactive resource {}",
                            s.id,
                            resource_id
                        );
                    }
                    deps.entry(s.id.clone()).or_default().push(res_node);
                }
            }
        }
    }

    // Implicit intra-module ordering deps (only among active steps).
    let mut by_module: BTreeMap<&str, Vec<&InitDagStepYaml>> = BTreeMap::new();
    for s in &spec.steps {
        if !nodes.contains(&s.id) {
            continue;
        }
        by_module.entry(s.module.as_str()).or_default().push(s);
    }
    for steps in by_module.values_mut() {
        steps.sort_by(|a, b| {
            let (_, ia, _) = parse_step_id(&a.id).unwrap();
            let (_, ib, _) = parse_step_id(&b.id).unwrap();
            ia.cmp(&ib).then_with(|| a.id.cmp(&b.id))
        });
        for w in steps.windows(2) {
            deps.entry(w[1].id.clone())
                .or_default()
                .push(w[0].id.clone());
        }
    }

    // attach_views barrier deps (variant-local).
    let mut attach_deps: Vec<String> = Vec::new();
    for s in &spec.steps {
        if !nodes.contains(&s.id) {
            continue;
        }
        let (_m, idx, _name) = parse_step_id(&s.id)?;
        if idx == 0 {
            attach_deps.push(s.id.clone());
        } else if idx == 1 {
            deps.entry(s.id.clone())
                .or_default()
                .push(ATTACH_VIEWS_STEP_ID.to_string());
        }
    }
    deps.insert(ATTACH_VIEWS_STEP_ID.to_string(), attach_deps);

    // Resource publish deps: resource_node depends on its publisher step.
    for r in resources {
        let res_node = format!("{}{}", RESOURCE_DEP_PREFIX, r.id);
        if !nodes.contains(&res_node) {
            continue;
        }
        if !nodes.contains(&r.published_by) {
            bail!(
                "v5 init dag invalid for variant: resource {} depends on inactive publisher step {}",
                r.id,
                r.published_by
            );
        }
        deps.entry(res_node)
            .or_default()
            .push(r.published_by.clone());
    }

    // Normalize.
    for id in nodes.iter() {
        deps.entry(id.clone()).or_default();
    }
    for v in deps.values_mut() {
        v.sort();
        v.dedup();
        v.retain(|id| nodes.contains(id.as_str()));
    }

    Ok((nodes, deps))
}

fn mode_to_rust(m: &StepMode) -> &'static str {
    match m {
        StepMode::Blocking => "Blocking",
        StepMode::AsyncSpawn => "AsyncSpawn",
        StepMode::BestEffortWait => "BestEffortWait",
    }
}

fn best_effort_to_rust(m: &BestEffortOnFailure) -> &'static str {
    match m {
        BestEffortOnFailure::AllowError => "AllowError",
        BestEffortOnFailure::Error => "Error",
    }
}

fn is_tag_active(mask: u8, tag_bit: u8) -> bool {
    (mask & tag_bit) != 0
}

fn parse_step_id(step_id: &str) -> Result<(String, u32, String)> {
    // Format: <Module>.step.<idx>.<name>
    let Some((module, rest)) = step_id.split_once(".step.") else {
        bail!("invalid init step id (missing .step.): {}", step_id);
    };
    let mut it = rest.splitn(2, '.');
    let idx_s = it
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid init step id (missing idx): {}", step_id))?;
    let name = it
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid init step id (missing name): {}", step_id))?;
    let idx: u32 = idx_s
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid init step id idx: {} ({}): {}", idx_s, step_id, e))?;
    if module.trim().is_empty() {
        bail!("invalid init step id module empty: {}", step_id);
    }
    if name.trim().is_empty() {
        bail!("invalid init step id name empty: {}", step_id);
    }
    Ok((module.to_string(), idx, name.to_string()))
}

fn step_id_to_rust_ident(step_id: &str) -> String {
    let mut out = String::new();
    for part in step_id
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|p| !p.is_empty())
    {
        let mut chars = part.chars();
        if let Some(c0) = chars.next() {
            out.push(c0.to_ascii_uppercase());
            for c in chars {
                out.push(c.to_ascii_lowercase());
            }
        }
    }
    if out.is_empty() {
        "Step".to_string()
    } else {
        out
    }
}
fn resource_id_to_rust_snake_ident(resource_id: &str) -> String {
    let mut out = String::new();
    let mut prev_underscore = false;
    for c in resource_id.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            prev_underscore = false;
        } else if !prev_underscore {
            out.push('_');
            prev_underscore = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        out = "resource".to_string();
    }
    if out.chars().next().unwrap().is_ascii_digit() {
        out = format!("r_{}", out);
    }
    out
}

fn is_valid_rust_ident(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(c0) = chars.next() else {
        return false;
    };
    if !(c0 == '_' || c0.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn is_valid_resource_id(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(c0) = chars.next() else {
        return false;
    };
    if !c0.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c == '_' || c.is_ascii_lowercase() || c.is_ascii_digit())
}

fn to_snake(ty: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = ty.chars().collect();
    for i in 0..chars.len() {
        let ch = chars[i];
        if ch.is_ascii_uppercase() {
            if i != 0 {
                let prev = chars[i - 1];
                if prev.is_ascii_lowercase() || prev.is_ascii_digit() {
                    out.push('_');
                }
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_minimal_html_and_rust() {
        let yaml = r#"
version: 3
title: demo
resources: []
module_tags:
  A: [master, owner, external]
steps:
  - id: A.step.0.construct
    module: A
    mode: Blocking
    deps: []
    doc: |
      - demo
    exec:
      kind: Construct
      call: "A::construct"
      arg_field: "a_arg"
    best_effort: null

  - id: A.step.1.init2
    module: A
    mode: Blocking
    deps: []
    doc: |
      - demo
    exec:
      kind: Call
      call: "A::init2_for_init_dag"
    best_effort: null
"#;

        let cfg = RustGenConfig {
            init_fn_name: "init_framework".to_string(),
            framework_type_path: "crate::Framework".to_string(),
            framework_args_type_path: "crate::FrameworkArgs".to_string(),
            result_type_path: "anyhow::Result<()>".to_string(),
        };

        let out = compile_from_yaml_str(yaml, &cfg).unwrap();
        assert!(out.rust.contains("init_framework"));
        assert!(out.html.contains("demo"));
        assert!(out.rust.contains("attach views"));
    }
}
