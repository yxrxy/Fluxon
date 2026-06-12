use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceKind {
    /// Ready exactly once and never changes.
    InitOnce,
    /// A watch-style resource: once the watch is established, values may change at runtime.
    WatchReady,
    /// A one-time barrier that is satisfied at init time (e.g. "first owner observed").
    Barrier,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StepMode {
    /// The step is part of the init critical path.
    Blocking,
    /// The step starts background work and returns without waiting for completion.
    AsyncSpawn,
    /// The step is a best-effort wait (e.g. timeout and continue).
    BestEffortWait,
}

/// A resource node in the init DAG.
///
/// Notes:
/// - `id` is the stable identifier shown in the graph.
/// - `kind` controls node styling.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceSpec {
    pub id: String,
    pub kind: ResourceKind,
}

/// A DAG step that consumes resources and produces resources.
///
/// In the resource-only visualization, steps are attached to edges as labels.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StepSpec {
    pub id: String,
    pub mode: StepMode,
    pub requires: Vec<String>,
    pub provides: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagSpec {
    pub title: String,
    pub resources: Vec<ResourceSpec>,
    pub steps: Vec<StepSpec>,
}

/// A module init step (the smallest DAG node granularity).
///
/// - Steps with the same `module` are executed in ascending `order`.
/// - `deps` are dependencies on other step ids.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitStepSpec {
    pub id: String,
    pub module: String,
    pub tags: Vec<String>,
    pub order: u32,
    pub mode: StepMode,
    /// Rust call path for this step (for visualization/debugging).
    pub exec_call: String,
    pub deps: Vec<String>,
    /// Resource prerequisites for this step.
    pub waits: Vec<String>,
    /// Human-readable documentation for this init step.
    ///
    /// This text is rendered in the HTML visualization (e.g. as a tooltip) to explain:
    /// - what the step does
    /// - why it requires its declared dependencies
    pub doc: String,
}

/// A resource readiness node in the init DAG visualization.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitResourceSpec {
    pub id: String,
    pub tags: Vec<String>,
    /// Tags for which the publisher-side hook is executed.
    ///
    /// This is used by the visualization to hide publisher edges in non-publisher tag views.
    pub publish_tags: Vec<String>,
    /// Rust symbol for the publish hook (implemented by the crate via generated InitResourceHooks).
    pub hook_call: String,
    /// Publisher step id.
    pub published_by: String,
    /// Human-readable documentation for this resource readiness point.
    pub doc: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitStepsDagSpec {
    pub title: String,
    pub steps: Vec<InitStepSpec>,
    pub resources: Vec<InitResourceSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitDagVariantSpec {
    pub id: String,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ResourceNode {
    id: String,
    kind: ResourceKind,
    provided_by_steps: Vec<String>,
    required_by_steps: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ResourceEdge {
    from: String,
    to: String,
    step_labels: Vec<String>,
    dash: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct RenderData {
    title: String,
    nodes: Vec<ResourceNode>,
    edges: Vec<ResourceEdge>,
}

#[derive(Clone, Debug, Serialize)]
struct InitStepNode {
    id: String,
    kind: String,
    tags: Vec<String>,
    publish_tags: Vec<String>,
    published_by: Option<String>,
    module: String,
    order: u32,
    mode: StepMode,
    exec_call: Option<String>,
    doc: String,
}

#[derive(Clone, Debug, Serialize)]
struct InitStepEdge {
    from: String,
    to: String,
    dash: Option<String>,
    kind: String,
}

#[derive(Clone, Debug, Serialize)]
struct InitStepsRenderData {
    title: String,
    nodes: Vec<InitStepNode>,
    edges: Vec<InitStepEdge>,
}

pub fn render_resources_html(spec: &DagSpec) -> Result<String> {
    validate_spec(spec)?;
    let data = build_render_data(spec)?;

    let mut json = serde_json::to_string(&data).context("serialize dag render data")?;
    // Prevent accidental script tag termination. This keeps the HTML self-contained
    // and safe even if ids contain '<' (should not happen in normal usage).
    json = json.replace('<', "\\u003c");

    let template = r##"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>__DAG_TITLE__</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 16px; }
    .meta { color: #555; font-size: 12px; margin-bottom: 12px; }
    #canvas { border: 1px solid #ddd; height: 82vh; overflow: auto; background: #fff; }
    svg { background: #fff; }
    .node { cursor: move; }
    .node rect { stroke-width: 1; rx: 6; ry: 6; }
    .node .node-title { font-size: 12px; font-weight: 600; dominant-baseline: hanging; }
    .node .node-body { font-size: 11px; fill: #222; dominant-baseline: hanging; }
    .edge { stroke: #555; stroke-width: 1; fill: none; }
    .edge-label { font-size: 10px; fill: #333; }
  </style>
</head>
<body>
  <h2>__DAG_TITLE__</h2>
  <div class="meta">Resources are nodes. Edges are derived from step requires/provides; edge labels are step ids.</div>
  <div style="display:flex; gap:8px; align-items:center; margin-bottom: 8px;">
    <button id="reset-layout" type="button">Reset layout</button>
    <span class="meta">Drag nodes to adjust layout. Positions are saved in localStorage.</span>
  </div>
  <div id="tag-filter" style="display:flex; gap:12px; align-items:center; margin-bottom: 8px;">
    <span class="meta">Tag view:</span>
    <label class="meta"><input type="checkbox" id="tag-master" checked> master</label>
    <label class="meta"><input type="checkbox" id="tag-owner" checked> owner</label>
    <label class="meta"><input type="checkbox" id="tag-external" checked> external</label>
  </div>
  <div id="tag-filter" style="display:flex; gap:12px; align-items:center; margin-bottom: 8px;">
    <span class="meta">Tag view:</span>
    <select id="tag-view" class="meta">
      <option value="all">all</option>
      <option value="master">master</option>
      <option value="owner">owner</option>
      <option value="external">external</option>
    </select>
  </div>
  <script type="application/json" id="dag-data">__DAG_JSON__</script>
  <div id="canvas">
    <svg id="dag" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="arrow" markerWidth="10" markerHeight="10" refX="10" refY="3" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,3 L0,6 Z" fill="#555"></path>
        </marker>
      </defs>
    </svg>
  </div>
  <script>
  (function () {
    const raw = document.getElementById('dag-data').textContent;
    let data = JSON.parse(raw);

    const TAGS = ['master','owner','external'];
    const tagKey = 'fluxon_dagviz_tagview';

    function loadTagView() {
      const s = localStorage.getItem(tagKey);
      if (!s) return 'all';
      if (s === 'all') return 'all';
      if (TAGS.includes(s)) return s;
      localStorage.removeItem(tagKey);
      return 'all';
    }

    function saveTagView(v) {
      localStorage.setItem(tagKey, v);
    }

    function applyTagView() {
      const selected = loadTagView();
      if (selected === 'all') return selected;
      const keep = (n) => Array.isArray(n.tags) && n.tags.includes(selected);
      const keptNodes = data.nodes.filter(keep);
      const kept = new Set(keptNodes.map(n => n.id));
      const byId = new Map(keptNodes.map(n => [n.id, n]));
      const keptEdges = data.edges.filter(e => {
        if (!kept.has(e.from) || !kept.has(e.to)) return false;
        if (e.kind === 'pub') {
          const toNode = byId.get(e.to);
          return toNode && Array.isArray(toNode.publish_tags) && toNode.publish_tags.includes(selected);
        }
        return true;
      });
      data = { title: data.title, nodes: keptNodes, edges: keptEdges };
      return selected;
    }

    const selectedTagView = applyTagView();

    // Layout constants: keep them explicit and stable for deterministic output.
    const MARGIN_X = 24;
    const MARGIN_Y = 24;
    // Vertical (top-down) layout.
    const LAYER_Y = 120;
    const COL_X = 260;
    const NODE_W = 240;
    const NODE_H = 36;
    // Fixed curvature (stable).
    const EDGE_CURVE_C = 120;

    const svg = document.getElementById('dag');
    const resetBtn = document.getElementById('reset-layout');

    const tagView = document.getElementById('tag-view');

    function syncTagUi(selected) {
      tagView.value = selected;
    }

    syncTagUi(selectedTagView);

    tagView.addEventListener('change', function() {
      const v = tagView.value;
      if (v !== 'all' && !TAGS.includes(v)) {
        return;
      }
      saveTagView(v);
      location.reload();
    });

    function fnv1a(str) {
      // 32-bit FNV-1a hash for stable localStorage keys.
      let h = 0x811c9dc5;
      for (let i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        // h *= 16777619 (with 32-bit overflow)
        h = (h + (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24)) >>> 0;
      }
      return ('00000000' + h.toString(16)).slice(-8);
    }

    const storageKey = 'fluxon_dagviz_layout:' + fnv1a(raw) + ':tag=' + selectedTagView;
    const uiKey = storageKey + ':ui';

    function loadUiState() {
      const s = localStorage.getItem(uiKey);
      if (!s) return {focusId: null, muteEdgeNodes: new Set()};
      try {
        const obj = JSON.parse(s);
        const focusId = (obj && typeof obj.focusId === 'string') ? obj.focusId : null;
        const mute = new Set(Array.isArray(obj && obj.muteEdgeNodes) ? obj.muteEdgeNodes.filter(x => typeof x === 'string') : []);
        return {focusId, muteEdgeNodes: mute};
      } catch (_) {
        localStorage.removeItem(uiKey);
        return {focusId: null, muteEdgeNodes: new Set()};
      }
    }

    function saveUiState(ui) {
      const out = {focusId: ui.focusId, muteEdgeNodes: Array.from(ui.muteEdgeNodes.values())};
      localStorage.setItem(uiKey, JSON.stringify(out));
    }

    const ui = loadUiState();

    function loadUiState() {
      const s = localStorage.getItem(uiKey);
      if (!s) return {focusId: null, muteEdgeNodes: new Set()};
      try {
        const obj = JSON.parse(s);
        const focusId = (obj && typeof obj.focusId === 'string') ? obj.focusId : null;
        const mute = new Set(Array.isArray(obj && obj.muteEdgeNodes) ? obj.muteEdgeNodes.filter(x => typeof x === 'string') : []);
        return {focusId, muteEdgeNodes: mute};
      } catch (_) {
        localStorage.removeItem(uiKey);
        return {focusId: null, muteEdgeNodes: new Set()};
      }
    }

    function saveUiState(ui) {
      const out = {focusId: ui.focusId, muteEdgeNodes: Array.from(ui.muteEdgeNodes.values())};
      localStorage.setItem(uiKey, JSON.stringify(out));
    }

    function loadUiState() {
      const s = localStorage.getItem(uiKey);
      if (!s) return {focusId: null, muteEdgeNodes: new Set()};
      try {
        const obj = JSON.parse(s);
        const focusId = (obj && typeof obj.focusId === 'string') ? obj.focusId : null;
        const mute = new Set(Array.isArray(obj && obj.muteEdgeNodes) ? obj.muteEdgeNodes.filter(x => typeof x === 'string') : []);
        return {focusId, muteEdgeNodes: mute};
      } catch (_) {
        localStorage.removeItem(uiKey);
        return {focusId: null, muteEdgeNodes: new Set()};
      }
    }

    function saveUiState(ui) {
      const out = {focusId: ui.focusId, muteEdgeNodes: Array.from(ui.muteEdgeNodes.values())};
      localStorage.setItem(uiKey, JSON.stringify(out));
    }

    const ui = loadUiState();

    function loadLayout() {
      const s = localStorage.getItem(storageKey);
      if (!s) return null;
      try {
        const obj = JSON.parse(s);
        if (!obj || typeof obj !== 'object') throw new Error('layout is not an object');
        return obj;
      } catch (_) {
        // If stored data is corrupted, drop it and fall back to the deterministic layout.
        localStorage.removeItem(storageKey);
        return null;
      }
    }

    function saveLayout(pos) {
      const out = {};
      for (const [id, p] of pos.entries()) out[id] = {x: p.x, y: p.y};
      localStorage.setItem(storageKey, JSON.stringify(out));
    }

    function loadUiState() {
      const s = localStorage.getItem(uiKey);
      if (!s) return {focusId: null, muteEdgeNodes: new Set()};
      try {
        const obj = JSON.parse(s);
        const focusId = (obj && typeof obj.focusId === 'string') ? obj.focusId : null;
        const mute = new Set(Array.isArray(obj && obj.muteEdgeNodes) ? obj.muteEdgeNodes.filter(x => typeof x === 'string') : []);
        return {focusId, muteEdgeNodes: mute};
      } catch (_) {
        localStorage.removeItem(uiKey);
        return {focusId: null, muteEdgeNodes: new Set()};
      }
    }

    function saveUiState(ui) {
      const out = {focusId: ui.focusId, muteEdgeNodes: Array.from(ui.muteEdgeNodes.values())};
      localStorage.setItem(uiKey, JSON.stringify(out));
    }

    function clientToSvg(clientX, clientY) {
      const pt = svg.createSVGPoint();
      pt.x = clientX;
      pt.y = clientY;
      const ctm = svg.getScreenCTM();
      if (!ctm) throw new Error('svg has no screen CTM');
      return pt.matrixTransform(ctm.inverse());
    }

    const nodes = new Map();
    for (const n of data.nodes) nodes.set(n.id, n);

    // Build adjacency for topo + level computation.
    const out = new Map();
    const inb = new Map();
    const indeg = new Map();
    for (const id of nodes.keys()) { out.set(id, []); inb.set(id, []); indeg.set(id, 0); }
    for (const e of data.edges) {
      out.get(e.from).push(e.to);
      inb.get(e.to).push(e.from);
      indeg.set(e.to, indeg.get(e.to) + 1);
    }

    // Deterministic Kahn topo: always pick smallest id when multiple are ready.
    const ready = [];
    for (const [id, d] of indeg.entries()) if (d === 0) ready.push(id);
    ready.sort();
    const topo = [];
    while (ready.length > 0) {
      const id = ready.shift();
      topo.push(id);
      for (const v of out.get(id)) {
        const nd = indeg.get(v) - 1;
        indeg.set(v, nd);
        if (nd === 0) { ready.push(v); ready.sort(); }
      }
    }
    if (topo.length !== nodes.size) {
      throw new Error('resource graph has a cycle (topo sort incomplete)');
    }

    // Level = longest path from any source (deterministic given topo order).
    const level = new Map();
    for (const id of topo) level.set(id, 0);
    for (const u of topo) {
      const lu = level.get(u);
      for (const v of out.get(u)) {
        const lv = level.get(v);
        if (lv < lu + 1) level.set(v, lu + 1);
      }
    }

    // Group by level and assign deterministic default positions.
    const layers = new Map();
    for (const id of topo) {
      const l = level.get(id);
      if (!layers.has(l)) layers.set(l, []);
      layers.get(l).push(id);
    }
    for (const ids of layers.values()) ids.sort();

    const defaultPos = new Map();
    const layerKeys = Array.from(layers.keys()).sort((a,b)=>a-b);
    for (const l of layerKeys) {
      const ids = layers.get(l);
      for (let i = 0; i < ids.length; i++) {
        const x = MARGIN_X + i * COL_X;
        const y = MARGIN_Y + l * LAYER_Y;
        defaultPos.set(ids[i], {x, y});
      }
    }

    const pos = new Map();
    for (const [id, p] of defaultPos.entries()) pos.set(id, {x: p.x, y: p.y});

    // Apply saved layout if available.
    const saved = loadLayout();
    if (saved) {
      for (const id of Object.keys(saved)) {
        if (!pos.has(id)) continue;
        const p = saved[id];
        if (!p || typeof p.x !== 'number' || typeof p.y !== 'number') continue;
        pos.set(id, {x: p.x, y: p.y});
      }
    }

    const ui = loadUiState();

    const ui = loadUiState();

    const ui = loadUiState();

    const ui = loadUiState();

    function resizeSvgToFit(visibleSet) {
      // Use viewBox so negative coordinates (from manual drag) never clip nodes.
      // If a focus chain is active, we fit the viewport to the focused subgraph.
      let minX = Infinity;
      let minY = Infinity;
      let maxX = -Infinity;
      let maxY = -Infinity;

      const ids = visibleSet ? Array.from(visibleSet) : Array.from(pos.keys());
      for (const id of ids) {
        const p = pos.get(id);
        if (!p) continue;
        if (p.x < minX) minX = p.x;
        if (p.y < minY) minY = p.y;
        if (p.x > maxX) maxX = p.x;
        if (p.y > maxY) maxY = p.y;
      }

      if (!isFinite(minX) || !isFinite(minY) || !isFinite(maxX) || !isFinite(maxY)) {
        minX = 0; minY = 0; maxX = 0; maxY = 0;
      }

      const vbX = minX - MARGIN_X;
      const vbY = minY - MARGIN_Y;
      const vbW = (maxX - minX) + NODE_W + MARGIN_X * 2;
      const vbH = (maxY - minY) + NODE_H + MARGIN_Y * 2;
      svg.setAttribute('viewBox', `${vbX} ${vbY} ${vbW} ${vbH}`);
      svg.setAttribute('width', String(vbW));
      svg.setAttribute('height', String(vbH));
    }

    function kindFill(kind) {
      if (kind === 'InitOnce') return '#e8f1ff';
      if (kind === 'WatchReady') return '#eaf9ee';
      if (kind === 'Barrier') return '#fff7e6';
      throw new Error('unknown ResourceKind: ' + kind);
    }

    function edgeGeom(fromId, toId) {
      const a = pos.get(fromId);
      const b = pos.get(toId);
      if (!a || !b) throw new Error('missing node position for edge: ' + fromId + ' -> ' + toId);
      const x1 = a.x + NODE_W / 2;
      const y1 = a.y + NODE_H;
      const x2 = b.x + NODE_W / 2;
      const y2 = b.y;
      const d = `M ${x1} ${y1} C ${x1} ${y1 + EDGE_CURVE_C}, ${x2} ${y2 - EDGE_CURVE_C}, ${x2} ${y2}`;
      return {d, midX: (x1 + x2) / 2, midY: (y1 + y2) / 2};
    }

    // Draw edges first (so nodes are on top).
    const edgeItems = [];
    for (const e of data.edges) {
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('class', 'edge');
      path.setAttribute('marker-end', 'url(#arrow)');
      if (e.dash) {
        path.setAttribute('stroke-dasharray', e.dash);
      }
      svg.appendChild(path);

      const label = e.step_labels.join('\\n');
      const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      text.setAttribute('class', 'edge-label');
      const lines = label.split('\\n');
      for (let i = 0; i < lines.length; i++) {
        const tspan = document.createElementNS('http://www.w3.org/2000/svg', 'tspan');
        tspan.setAttribute('dy', i === 0 ? '0' : '12');
        tspan.textContent = lines[i];
        text.appendChild(tspan);
      }
      svg.appendChild(text);

      edgeItems.push({from: e.from, to: e.to, path, text});
    }

    const nodeItems = new Map();
    const focusIcons = new Map();
    const muteIcons = new Map();
    for (const id of topo) {
      const n = nodes.get(id);
      const p = pos.get(id);
      const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      g.setAttribute('class', 'node');
      g.setAttribute('data-id', id);
      g.setAttribute('transform', `translate(${p.x},${p.y})`);

      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('x', 0);
      rect.setAttribute('y', 0);
      rect.setAttribute('width', NODE_W);
      rect.setAttribute('height', NODE_H);
      rect.setAttribute('fill', kindFill(n.kind));
      g.appendChild(rect);

      const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      text.setAttribute('x', 8);
      text.setAttribute('y', NODE_H / 2);
      text.textContent = id;
      g.appendChild(text);

      const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
      const mode = n.mode ? ('mode: ' + n.mode + '\n') : '';
      const module = n.module ? ('module: ' + n.module + '\n') : '';
      const call = n.exec_call ? ('exec: ' + n.exec_call + '\n') : '';
      title.textContent = mode + module + call + '\n' + (n.doc || '');
      g.appendChild(title);

      svg.appendChild(g);
      nodeItems.set(id, g);
    }

    // UI: event delegation for node icons.
    // - focus icon: keep only the transitive upstream+downstream chain
    // - mute icon: hide all incident edges for this node
    svg.addEventListener('click', function (ev) {
      let el = ev.target;
      while (el && el !== svg) {
        if (el.classList && el.classList.contains('icon-hit')) break;
        el = el.parentNode;
      }
      if (!el || el === svg) return;

      const kind = el.getAttribute('data-kind');
      const nodeId = el.parentNode && el.parentNode.getAttribute ? el.parentNode.getAttribute('data-id') : null;
      if (!nodeId || !kind) return;

      if (kind === 'focus') {
        ui.focusId = (ui.focusId === nodeId) ? null : nodeId;
      } else if (kind === 'mute') {
        if (ui.muteEdgeNodes.has(nodeId)) ui.muteEdgeNodes.delete(nodeId);
        else ui.muteEdgeNodes.add(nodeId);
      } else {
        return;
      }

      saveUiState(ui);
      applyVisibility();
    });

    function updateAllEdges() {
      for (const it of edgeItems) {
        const r = edgeGeom(it.from, it.to);
        it.path.setAttribute('d', r.d);
        it.text.setAttribute('x', r.midX + 4);
        it.text.setAttribute('y', r.midY - 2);
        const tspans = it.text.querySelectorAll('tspan');
        for (const t of tspans) t.setAttribute('x', r.midX + 4);
      }
    }

    function updateNode(id) {
      const p = pos.get(id);
      const g = nodeItems.get(id);
      if (!p || !g) return;
      g.setAttribute('transform', `translate(${p.x},${p.y})`);
    }

    // Initial layout.
    updateAllEdges();
    applyVisibility();
    resizeSvgToFit(computeFocusSet(ui.focusId));

    // Drag support.
    let drag = null;
    function onPointerDown(ev) {
      const id = ev.currentTarget.getAttribute('data-id');
      if (!id) return;
      // Ignore drag start when clicking the node's small UI icons.
      let t = ev.target;
      while (t && t !== ev.currentTarget) {
        if (t.classList && t.classList.contains('icon-hit')) return;
        t = t.parentNode;
      }
      const p0 = pos.get(id);
      if (!p0) return;
      const s = clientToSvg(ev.clientX, ev.clientY);
      drag = {id, startX: s.x, startY: s.y, origX: p0.x, origY: p0.y, pointerId: ev.pointerId};
      ev.currentTarget.setPointerCapture(ev.pointerId);
    }
    function onPointerMove(ev) {
      if (!drag) return;
      if (ev.pointerId !== drag.pointerId) return;
      const s = clientToSvg(ev.clientX, ev.clientY);
      const nx = drag.origX + (s.x - drag.startX);
      const ny = drag.origY + (s.y - drag.startY);
      pos.set(drag.id, {x: nx, y: ny});
      updateNode(drag.id);
      updateAllEdges();
      resizeSvgToFit();
    }
    function onPointerUp(ev) {
      if (!drag) return;
      if (ev.pointerId !== drag.pointerId) return;
      saveLayout(pos);
      drag = null;
    }

    for (const g of nodeItems.values()) {
      g.addEventListener('pointerdown', onPointerDown);
      g.addEventListener('pointermove', onPointerMove);
      g.addEventListener('pointerup', onPointerUp);
      g.addEventListener('pointercancel', onPointerUp);
    }

    function onIconClick(ev) {
      ev.stopPropagation();
      ev.preventDefault();
      const nodeId = ev.currentTarget.parentNode.getAttribute('data-id');
      const kind = ev.currentTarget.getAttribute('data-kind');
      if (!nodeId || !kind) return;
      if (kind === 'focus') {
        ui.focusId = (ui.focusId === nodeId) ? null : nodeId;
      } else if (kind === 'mute') {
        if (ui.muteEdgeNodes.has(nodeId)) ui.muteEdgeNodes.delete(nodeId);
        else ui.muteEdgeNodes.add(nodeId);
      } else {
        return;
      }
      saveUiState(ui);
      applyVisibility();
    }

    for (const ic of focusIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }
    for (const ic of muteIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }

    function onIconClick(ev) {
      ev.stopPropagation();
      ev.preventDefault();
      const nodeId = ev.currentTarget.parentNode.getAttribute('data-id');
      const kind = ev.currentTarget.getAttribute('data-kind');
      if (!nodeId || !kind) return;
      if (kind === 'focus') {
        ui.focusId = (ui.focusId === nodeId) ? null : nodeId;
      } else if (kind === 'mute') {
        if (ui.muteEdgeNodes.has(nodeId)) ui.muteEdgeNodes.delete(nodeId);
        else ui.muteEdgeNodes.add(nodeId);
      } else {
        return;
      }
      saveUiState(ui);
      applyVisibility();
    }

    for (const ic of focusIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }
    for (const ic of muteIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }

    function onIconClick(ev) {
      ev.stopPropagation();
      ev.preventDefault();
      const nodeId = ev.currentTarget.parentNode.getAttribute('data-id');
      const kind = ev.currentTarget.getAttribute('data-kind');
      if (!nodeId || !kind) return;
      if (kind === 'focus') {
        ui.focusId = (ui.focusId === nodeId) ? null : nodeId;
      } else if (kind === 'mute') {
        if (ui.muteEdgeNodes.has(nodeId)) ui.muteEdgeNodes.delete(nodeId);
        else ui.muteEdgeNodes.add(nodeId);
      } else {
        return;
      }
      saveUiState(ui);
      applyVisibility();
    }

    for (const ic of focusIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }
    for (const ic of muteIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }

    resetBtn.addEventListener('click', function () {
      localStorage.removeItem(storageKey);
      localStorage.removeItem(uiKey);
      ui.focusId = null;
      ui.muteEdgeNodes = new Set();
      selected.clear();
      syncSelectionStyles();
      for (const [id, p] of defaultPos.entries()) pos.set(id, {x: p.x, y: p.y});
      for (const id of topo) updateNode(id);
      updateAllEdges();
      applyVisibility();
      resizeSvgToFit();
    });
  })();
  </script>
</body>
</html>
"##;

    let escaped_title = html_escape_text(&data.title);
    let html = template
        .replace("__DAG_TITLE__", &escaped_title)
        .replace("__DAG_JSON__", &json);

    Ok(html)
}

pub fn write_resources_html_file(path: &std::path::Path, spec: &DagSpec) -> Result<()> {
    let html = render_resources_html(spec)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir: {}", parent.display()))?;
    }
    std::fs::write(path, html).with_context(|| format!("write: {}", path.display()))?;
    Ok(())
}

pub fn render_init_steps_html_legacy(spec: &InitStepsDagSpec) -> Result<String> {
    validate_init_steps_spec(spec)?;
    let data = build_init_steps_render_data(spec)?;

    let mut json = serde_json::to_string(&data).context("serialize init steps render data")?;
    json = json.replace('<', "\\u003c");

    let template = r##"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>__DAG_TITLE__</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 16px; }
    .meta { color: #555; font-size: 12px; margin-bottom: 12px; }
    #canvas { border: 1px solid #ddd; height: 82vh; overflow: auto; background: #fff; }
    svg { background: #fff; }
    .node { cursor: move; }
    .node rect { stroke: #333; stroke-width: 1; rx: 6; ry: 6; }
    .node text { font-size: 12px; dominant-baseline: middle; }
    .edge { stroke: #555; stroke-width: 1; fill: none; }
    .edge-intra { stroke: #999; }
    .edge-label { font-size: 10px; fill: #333; }
  </style>
</head>
<body>
  <h2>__DAG_TITLE__</h2>
  <div class="meta">Nodes are module init steps. Intra-module order edges are auto-generated.</div>
  <div style="display:flex; gap:8px; align-items:center; margin-bottom: 8px;">
    <button id="reset-layout" type="button">Reset layout</button>
    <span class="meta">Drag nodes to adjust layout. Positions are saved in localStorage.</span>
  </div>
  <div id="tag-filter" style="display:flex; gap:12px; align-items:center; margin-bottom: 8px;">
    <span class="meta">Tag view:</span>
    <select id="tag-view" class="meta">
      <option value="all">all</option>
      <option value="master">master</option>
      <option value="owner">owner</option>
      <option value="external">external</option>
    </select>
  </div>
  <script type="application/json" id="dag-data">__DAG_JSON__</script>
  <div id="canvas">
    <svg id="dag" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="arrow" markerWidth="10" markerHeight="10" refX="10" refY="3" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,3 L0,6 Z" fill="#555"></path>
        </marker>
      </defs>
    </svg>
  </div>
  <script>
  (function () {
    const raw = document.getElementById('dag-data').textContent;
    let data = JSON.parse(raw);
    const TAGS = ['master','owner','external'];
    const tagKey = 'fluxon_dagviz_tagview';

    function loadTagView() {
      const s = localStorage.getItem(tagKey);
      if (!s) return 'all';
      if (s === 'all') return 'all';
      if (TAGS.includes(s)) return s;
      localStorage.removeItem(tagKey);
      return 'all';
    }

    function saveTagView(v) {
      localStorage.setItem(tagKey, v);
    }

    function applyTagView() {
      const selected = loadTagView();
      if (selected === 'all') return selected;
      const keep = (n) => Array.isArray(n.tags) && n.tags.includes(selected);
      const keptNodes = data.nodes.filter(keep);
      const kept = new Set(keptNodes.map(n => n.id));
      const byId = new Map(keptNodes.map(n => [n.id, n]));
      const keptEdges = data.edges.filter(e => {
        if (!kept.has(e.from) || !kept.has(e.to)) return false;
        if (e.kind === 'pub') {
          const toNode = byId.get(e.to);
          return toNode && Array.isArray(toNode.publish_tags) && toNode.publish_tags.includes(selected);
        }
        return true;
      });
      data = { title: data.title, nodes: keptNodes, edges: keptEdges };
      return selected;
    }

    const selectedTagView = applyTagView();

    const MARGIN_X = 24;
    const MARGIN_Y = 24;
    const LAYER_Y = 120;
    const COL_X = 360;
    const NODE_W = 340;
    const NODE_H = 44;
    const EDGE_CURVE_C = 120;

    const svg = document.getElementById('dag');
    const resetBtn = document.getElementById('reset-layout');

    const tagView = document.getElementById('tag-view');

    function syncTagUi(selected) {
      tagView.value = selected;
    }

    syncTagUi(selectedTagView);

    tagView.addEventListener('change', function() {
      const v = tagView.value;
      if (v !== 'all' && !TAGS.includes(v)) {
        return;
      }
      saveTagView(v);
      location.reload();
    });

    function fnv1a(str) {
      let h = 0x811c9dc5;
      for (let i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        h = (h + (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24)) >>> 0;
      }
      return ('00000000' + h.toString(16)).slice(-8);
    }

    const storageKey = 'fluxon_dagviz_layout:' + fnv1a(raw) + ':tag=' + selectedTagView;

    function loadLayout() {
      const s = localStorage.getItem(storageKey);
      if (!s) return null;
      try {
        const obj = JSON.parse(s);
        if (!obj || typeof obj !== 'object') throw new Error('layout is not an object');
        return obj;
      } catch (_) {
        localStorage.removeItem(storageKey);
        return null;
      }
    }

    function saveLayout(pos) {
      const out = {};
      for (const [id, p] of pos.entries()) out[id] = {x: p.x, y: p.y};
      localStorage.setItem(storageKey, JSON.stringify(out));
    }

    function clientToSvg(clientX, clientY) {
      const pt = svg.createSVGPoint();
      pt.x = clientX;
      pt.y = clientY;
      const ctm = svg.getScreenCTM();
      if (!ctm) throw new Error('svg has no screen CTM');
      return pt.matrixTransform(ctm.inverse());
    }

    const nodes = new Map();
    for (const n of data.nodes) nodes.set(n.id, n);

    const out = new Map();
    const inb = new Map();
    const indeg = new Map();
    for (const id of nodes.keys()) { out.set(id, []); inb.set(id, []); indeg.set(id, 0); }
    for (const e of data.edges) {
      out.get(e.from).push(e.to);
      inb.get(e.to).push(e.from);
      indeg.set(e.to, indeg.get(e.to) + 1);
    }

    const ready = [];
    for (const [id, d] of indeg.entries()) if (d === 0) ready.push(id);
    ready.sort();
    const topo = [];
    while (ready.length > 0) {
      const id = ready.shift();
      topo.push(id);
      for (const v of out.get(id)) {
        const nd = indeg.get(v) - 1;
        indeg.set(v, nd);
        if (nd === 0) { ready.push(v); ready.sort(); }
      }
    }
    if (topo.length !== nodes.size) throw new Error('step graph has a cycle');

    const level = new Map();
    for (const id of topo) level.set(id, 0);
    for (const u of topo) {
      const lu = level.get(u);
      for (const v of out.get(u)) {
        const lv = level.get(v);
        if (lv < lu + 1) level.set(v, lu + 1);
      }
    }

    const layers = new Map();
    for (const id of topo) {
      const l = level.get(id);
      if (!layers.has(l)) layers.set(l, []);
      layers.get(l).push(id);
    }
    for (const ids of layers.values()) ids.sort();

    const defaultPos = new Map();
    const layerKeys = Array.from(layers.keys()).sort((a,b)=>a-b);
    for (const l of layerKeys) {
      const ids = layers.get(l);
      for (let i = 0; i < ids.length; i++) {
        const x = MARGIN_X + i * COL_X;
        const y = MARGIN_Y + l * LAYER_Y;
        defaultPos.set(ids[i], {x, y});
      }
    }

    const pos = new Map();
    for (const [id, p] of defaultPos.entries()) pos.set(id, {x: p.x, y: p.y});
    const saved = loadLayout();
    if (saved) {
      for (const id of Object.keys(saved)) {
        if (!pos.has(id)) continue;
        const p = saved[id];
        if (!p || typeof p.x !== 'number' || typeof p.y !== 'number') continue;
        pos.set(id, {x: p.x, y: p.y});
      }
    }

    const ui = loadUiState();

    function resizeSvgToFit(visibleSet) {
      // Use viewBox so negative coordinates (from manual drag) never clip nodes.
      // If a focus chain is active, we fit the viewport to the focused subgraph.
      let minX = Infinity;
      let minY = Infinity;
      let maxX = -Infinity;
      let maxY = -Infinity;

      const ids = visibleSet ? Array.from(visibleSet) : Array.from(pos.keys());
      for (const id of ids) {
        const p = pos.get(id);
        if (!p) continue;
        if (p.x < minX) minX = p.x;
        if (p.y < minY) minY = p.y;
        if (p.x > maxX) maxX = p.x;
        if (p.y > maxY) maxY = p.y;
      }

      if (!isFinite(minX) || !isFinite(minY) || !isFinite(maxX) || !isFinite(maxY)) {
        minX = 0; minY = 0; maxX = 0; maxY = 0;
      }

      const vbX = minX - MARGIN_X;
      const vbY = minY - MARGIN_Y;
      const vbW = (maxX - minX) + NODE_W + MARGIN_X * 2;
      const vbH = (maxY - minY) + NODE_H + MARGIN_Y * 2;
      svg.setAttribute('viewBox', `${vbX} ${vbY} ${vbW} ${vbH}`);
      svg.setAttribute('width', String(vbW));
      svg.setAttribute('height', String(vbH));
    }

    function nodeFill(mode) {
      if (mode === 'Blocking') return '#e8f1ff';
      if (mode === 'AsyncSpawn') return '#eaf9ee';
      if (mode === 'BestEffortWait') return '#fff7e6';
      return '#f5f5f5';
    }

    function phaseStroke(phase) {
      if (phase === 'PreView') return '#1e6bd6';
      if (phase === 'PostView') return '#333';
      if (phase === 'ViewBarrier') return '#8a8a8a';
      return '#333';
    }

    function edgeGeom(fromId, toId) {
      const a = pos.get(fromId);
      const b = pos.get(toId);
      if (!a || !b) throw new Error('missing node position for edge: ' + fromId + ' -> ' + toId);
      const x1 = a.x + NODE_W / 2;
      const y1 = a.y + NODE_H;
      const x2 = b.x + NODE_W / 2;
      const y2 = b.y;
      const d = `M ${x1} ${y1} C ${x1} ${y1 + EDGE_CURVE_C}, ${x2} ${y2 - EDGE_CURVE_C}, ${x2} ${y2}`;
      return {d};
    }

    const edgeItems = [];
    for (const e of data.edges) {
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('class', 'edge');
      path.setAttribute('marker-end', 'url(#arrow)');
      if (e.kind === 'intra') path.classList.add('edge-intra');
      if (e.dash) path.setAttribute('stroke-dasharray', e.dash);
      svg.appendChild(path);
      edgeItems.push({from: e.from, to: e.to, path});
    }

    const nodeItems = new Map();
    const focusIcons = new Map();
    const muteIcons = new Map();
    for (const id of topo) {
      const n = nodes.get(id);
      const p = pos.get(id);
      const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      g.setAttribute('class', 'node');
      g.setAttribute('data-id', id);
      g.setAttribute('transform', `translate(${p.x},${p.y})`);

      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('x', 0);
      rect.setAttribute('y', 0);
      rect.setAttribute('width', NODE_W);
      rect.setAttribute('height', NODE_H);
      rect.setAttribute('fill', nodeFill(n.mode));
      rect.style.stroke = phaseStroke(n.phase);
      g.appendChild(rect);

      const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      text.setAttribute('x', 8);
      text.setAttribute('y', NODE_H / 2);
      text.textContent = id;
      g.appendChild(text);

      const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
      const mode = n.mode ? ('mode: ' + n.mode + '\n') : '';
      const module = n.module ? ('module: ' + n.module + '\n') : '';
      const call = n.exec_call ? ('exec: ' + n.exec_call + '\n') : '';
      title.textContent = mode + module + call + '\n' + (n.doc || '');
      g.appendChild(title);

      svg.appendChild(g);
      nodeItems.set(id, g);
    }

    function updateAllEdges() {
      for (const it of edgeItems) {
        const r = edgeGeom(it.from, it.to);
        it.path.setAttribute('d', r.d);
      }
    }

    function updateNode(id) {
      const p = pos.get(id);
      const g = nodeItems.get(id);
      if (!p || !g) return;
      g.setAttribute('transform', `translate(${p.x},${p.y})`);
    }

    updateAllEdges();
    applyVisibility();
    resizeSvgToFit(computeFocusSet(ui.focusId));

    let drag = null;
    function onPointerDown(ev) {
      const id = ev.currentTarget.getAttribute('data-id');
      if (!id) return;
      const p0 = pos.get(id);
      if (!p0) return;
      const s = clientToSvg(ev.clientX, ev.clientY);
      drag = {id, startX: s.x, startY: s.y, origX: p0.x, origY: p0.y, pointerId: ev.pointerId};
      ev.currentTarget.setPointerCapture(ev.pointerId);
    }
    function onPointerMove(ev) {
      if (!drag) return;
      if (ev.pointerId !== drag.pointerId) return;
      const s = clientToSvg(ev.clientX, ev.clientY);
      const nx = drag.origX + (s.x - drag.startX);
      const ny = drag.origY + (s.y - drag.startY);
      pos.set(drag.id, {x: nx, y: ny});
      updateNode(drag.id);
      updateAllEdges();
      resizeSvgToFit();
    }
    function onPointerUp(ev) {
      if (!drag) return;
      if (ev.pointerId !== drag.pointerId) return;
      saveLayout(pos);
      drag = null;
    }

    for (const g of nodeItems.values()) {
      g.addEventListener('pointerdown', onPointerDown);
      g.addEventListener('pointermove', onPointerMove);
      g.addEventListener('pointerup', onPointerUp);
      g.addEventListener('pointercancel', onPointerUp);
    }

    function onIconClick(ev) {
      ev.stopPropagation();
      ev.preventDefault();
      const nodeId = ev.currentTarget.parentNode.getAttribute('data-id');
      const kind = ev.currentTarget.getAttribute('data-kind');
      if (!nodeId || !kind) return;
      if (kind === 'focus') {
        ui.focusId = (ui.focusId === nodeId) ? null : nodeId;
      } else if (kind === 'mute') {
        if (ui.muteEdgeNodes.has(nodeId)) ui.muteEdgeNodes.delete(nodeId);
        else ui.muteEdgeNodes.add(nodeId);
      } else {
        return;
      }
      saveUiState(ui);
      applyVisibility();
    }

    for (const ic of focusIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }
    for (const ic of muteIcons.values()) {
      ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
      ic.addEventListener('click', onIconClick);
    }

    resetBtn.addEventListener('click', function () {
      localStorage.removeItem(storageKey);
      localStorage.removeItem(uiKey);
      ui.focusId = null;
      ui.muteEdgeNodes = new Set();
      for (const [id, p] of defaultPos.entries()) pos.set(id, {x: p.x, y: p.y});
      for (const id of topo) updateNode(id);
      updateAllEdges();
      applyVisibility();
      resizeSvgToFit();
    });
  })();
  </script>
</body>
</html>
"##;

    let escaped_title = html_escape_text(&data.title);
    Ok(template
        .replace("__DAG_TITLE__", &escaped_title)
        .replace("__DAG_JSON__", &json))
}

pub fn write_init_steps_html_file_legacy(
    path: &std::path::Path,
    spec: &InitStepsDagSpec,
) -> Result<()> {
    let html = render_init_steps_html_legacy(spec)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir: {}", parent.display()))?;
    }
    std::fs::write(path, html).with_context(|| format!("write: {}", path.display()))?;
    Ok(())
}

pub fn render_init_steps_html(spec: &InitStepsDagSpec) -> Result<String> {
    validate_init_steps_spec(spec)?;
    let data = build_init_steps_render_data(spec)?;

    let mut json = serde_json::to_string(&data).context("serialize init steps render data")?;
    json = json.replace('<', "\\u003c");

    // A self-contained HTML that draws step-nodes with deterministic layout,
    // supports drag+persist, and differentiates async/best-effort steps via edge style.
    let template = r##"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>__DAG_TITLE__</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 16px; }
    .meta { color: #555; font-size: 12px; margin-bottom: 12px; }
    #canvas { border: 1px solid #ddd; height: 82vh; overflow: auto; background: #fff; }
    svg { background: #fff; }
    .node { cursor: move; }
    .node rect { stroke: #333; stroke-width: 1; rx: 6; ry: 6; }
    .node text { font-size: 12px; dominant-baseline: middle; }
    .edge { stroke: #555; stroke-width: 1; fill: none; }
    .edge.seq { stroke: #999; stroke-opacity: 0.45; }
    .node .icon-hit { cursor: pointer; }
    .node .icon-hit rect { fill: #fff; stroke: #666; stroke-width: 1; rx: 3; ry: 3; }
    .node .icon-hit.active rect { fill: #222; stroke: #222; }
    .node .icon-hit path { stroke: #222; stroke-width: 1.5; fill: none; stroke-linecap: round; stroke-linejoin: round; }
    .node .icon-hit.active path { stroke: #fff; }
    .node .icon-hit line { stroke: #222; stroke-width: 1.5; stroke-linecap: round; }
    .node .icon-hit.active line { stroke: #fff; }
  </style>
</head>
<body>
  <h2>__DAG_TITLE__</h2>
  <div class="meta">Nodes are init steps + resource readiness points. Edges: step deps (solid), module sequence (faded), publish resource, wait resource (dash by subscriber mode). Node icons: focus (magnifier) shows the full upstream+downstream chain; mute (broken link) hides all incident edges.</div>
  <div style="display:flex; gap:8px; align-items:center; margin-bottom: 8px;">
    <button id="reset-layout" type="button">Reset layout</button>
    <span class="meta">Drag nodes to adjust layout. Positions are saved in localStorage.</span>
  </div>
  <div id="tag-filter" style="display:flex; gap:12px; align-items:center; margin-bottom: 8px;">
    <span class="meta">Tag view:</span>
    <select id="tag-view" class="meta">
      <option value="all">all</option>
      <option value="master">master</option>
      <option value="owner">owner</option>
      <option value="external">external</option>
    </select>
  </div>
  <script type="application/json" id="dag-data">__DAG_JSON__</script>
  <div id="canvas">
    <svg id="dag" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="arrow" markerWidth="10" markerHeight="10" refX="10" refY="3" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,3 L0,6 Z" fill="#555"></path>
        </marker>
      </defs>
    </svg>
  </div>
  <script>
  (function () {
    const raw = document.getElementById('dag-data').textContent;
    let data = JSON.parse(raw);
    const TAGS = ['master','owner','external'];
    const tagKey = 'fluxon_dagviz_tagview';

    function loadTagView() {
      const s = localStorage.getItem(tagKey);
      if (!s) return 'all';
      if (s === 'all') return 'all';
      if (TAGS.includes(s)) return s;
      localStorage.removeItem(tagKey);
      return 'all';
    }

    function saveTagView(v) {
      localStorage.setItem(tagKey, v);
    }

    function applyTagView() {
      const selected = loadTagView();
      if (selected === 'all') return selected;
      const keep = (n) => Array.isArray(n.tags) && n.tags.includes(selected);
      const keptNodes = data.nodes.filter(keep);
      const kept = new Set(keptNodes.map(n => n.id));
      const byId = new Map(keptNodes.map(n => [n.id, n]));
      const keptEdges = data.edges.filter(e => {
        if (!kept.has(e.from) || !kept.has(e.to)) return false;
        if (e.kind === 'pub') {
          const toNode = byId.get(e.to);
          return toNode && Array.isArray(toNode.publish_tags) && toNode.publish_tags.includes(selected);
        }
        return true;
      });
      data = { title: data.title, nodes: keptNodes, edges: keptEdges };
      return selected;
    }

    const selectedTagView = applyTagView();

    const MARGIN_X = 24;
    const MARGIN_Y = 24;
    const LAYER_Y = 110;
    const COL_X = 560;
    const NODE_W = 520;
    const NODE_H = 92;
    const EDGE_CURVE_C = 120;

    const svg = document.getElementById('dag');
    const resetBtn = document.getElementById('reset-layout');

    const tagView = document.getElementById('tag-view');

    function syncTagUi(selected) {
      tagView.value = selected;
    }

    syncTagUi(selectedTagView);

    tagView.addEventListener('change', function() {
      const v = tagView.value;
      if (v !== 'all' && !TAGS.includes(v)) {
        return;
      }
      saveTagView(v);
      location.reload();
    });

    function fnv1a(str) {
      let h = 0x811c9dc5;
      for (let i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        h = (h + (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24)) >>> 0;
      }
      return ('00000000' + h.toString(16)).slice(-8);
    }

    const storageKey = 'fluxon_dagviz_layout:' + fnv1a(raw) + ':tag=' + selectedTagView;
    const uiKey = storageKey + ':ui';

    function loadUiState() {
      const s = localStorage.getItem(uiKey);
      if (!s) return {focusId: null, muteEdgeNodes: new Set()};
      try {
        const obj = JSON.parse(s);
        const focusId = (obj && typeof obj.focusId === 'string') ? obj.focusId : null;
        const mute = new Set(Array.isArray(obj && obj.muteEdgeNodes) ? obj.muteEdgeNodes.filter(x => typeof x === 'string') : []);
        return {focusId, muteEdgeNodes: mute};
      } catch (_) {
        localStorage.removeItem(uiKey);
        return {focusId: null, muteEdgeNodes: new Set()};
      }
    }

    function saveUiState(ui) {
      const out = {focusId: ui.focusId, muteEdgeNodes: Array.from(ui.muteEdgeNodes.values())};
      localStorage.setItem(uiKey, JSON.stringify(out));
    }

    const ui = loadUiState();

    function loadLayout() {
      const s = localStorage.getItem(storageKey);
      if (!s) return null;
      try {
        const obj = JSON.parse(s);
        if (!obj || typeof obj !== 'object') throw new Error('layout is not an object');
        return obj;
      } catch (_) {
        localStorage.removeItem(storageKey);
        return null;
      }
    }

    function saveLayout(pos) {
      const out = {};
      for (const [id, p] of pos.entries()) out[id] = {x: p.x, y: p.y};
      localStorage.setItem(storageKey, JSON.stringify(out));
    }

    function clientToSvg(clientX, clientY) {
      const pt = svg.createSVGPoint();
      pt.x = clientX;
      pt.y = clientY;
      const ctm = svg.getScreenCTM();
      if (!ctm) throw new Error('svg has no screen CTM');
      return pt.matrixTransform(ctm.inverse());
    }

    function modeDash(mode) {
      if (mode === 'Blocking') return null;
      if (mode === 'AsyncSpawn') return '6,4';
      if (mode === 'BestEffortWait') return '2,4';
      throw new Error('unknown StepMode: ' + mode);
    }

    function moduleColor(module) {
      // Deterministic pastel color derived from module name.
      let h = 0;
      for (let i = 0; i < module.length; i++) h = (h * 131 + module.charCodeAt(i)) >>> 0;
      const hue = h % 360;
      return `hsl(${hue}, 60%, 92%)`;
    }

    const nodes = new Map();
    for (const n of data.nodes) nodes.set(n.id, n);

    const out = new Map();
    const inb = new Map();
    const indeg = new Map();
    for (const id of nodes.keys()) { out.set(id, []); inb.set(id, []); indeg.set(id, 0); }
    for (const e of data.edges) {
      out.get(e.from).push(e.to);
      inb.get(e.to).push(e.from);
      indeg.set(e.to, indeg.get(e.to) + 1);
    }

    const ready = [];
    for (const [id, d] of indeg.entries()) if (d === 0) ready.push(id);
    ready.sort();
    const topo = [];
    while (ready.length > 0) {
      const id = ready.shift();
      topo.push(id);
      for (const v of out.get(id)) {
        const nd = indeg.get(v) - 1;
        indeg.set(v, nd);
        if (nd === 0) { ready.push(v); ready.sort(); }
      }
    }
    if (topo.length !== nodes.size) throw new Error('step graph has a cycle (topo sort incomplete)');

    const level = new Map();
    for (const id of topo) level.set(id, 0);
    for (const u of topo) {
      const lu = level.get(u);
      for (const v of out.get(u)) {
        const lv = level.get(v);
        if (lv < lu + 1) level.set(v, lu + 1);
      }
    }

    const layers = new Map();
    for (const id of topo) {
      const l = level.get(id);
      if (!layers.has(l)) layers.set(l, []);
      layers.get(l).push(id);
    }
    for (const ids of layers.values()) ids.sort();

    const defaultPos = new Map();
    const layerKeys = Array.from(layers.keys()).sort((a,b)=>a-b);
    for (const l of layerKeys) {
      const ids = layers.get(l);
      for (let i = 0; i < ids.length; i++) {
        defaultPos.set(ids[i], {x: MARGIN_X + i * COL_X, y: MARGIN_Y + l * LAYER_Y});
      }
    }

    const pos = new Map();
    for (const [id, p] of defaultPos.entries()) pos.set(id, {x: p.x, y: p.y});
    const saved = loadLayout();
    if (saved) {
      for (const id of Object.keys(saved)) {
        if (!pos.has(id)) continue;
        const p = saved[id];
        if (!p || typeof p.x !== 'number' || typeof p.y !== 'number') continue;
        pos.set(id, {x: p.x, y: p.y});
      }
    }

    function resizeSvgToFit(visibleSet) {
      let first = true;
      let minX = 0;
      let minY = 0;
      let maxX = 0;
      let maxY = 0;

      for (const [id, p] of pos.entries()) {
        if (visibleSet && !visibleSet.has(id)) continue;
        const x0 = p.x;
        const y0 = p.y;
        const x1 = p.x + NODE_W;
        const y1 = p.y + NODE_H;
        if (first) {
          minX = x0;
          minY = y0;
          maxX = x1;
          maxY = y1;
          first = false;
        } else {
          if (x0 < minX) minX = x0;
          if (y0 < minY) minY = y0;
          if (x1 > maxX) maxX = x1;
          if (y1 > maxY) maxY = y1;
        }
      }

      if (first) throw new Error('resizeSvgToFit: no visible nodes');

      minX -= MARGIN_X;
      minY -= MARGIN_Y;
      maxX += MARGIN_X;
      maxY += MARGIN_Y;

      const w = maxX - minX;
      const h = maxY - minY;
      svg.setAttribute('width', String(w));
      svg.setAttribute('height', String(h));
      svg.setAttribute('viewBox', `${minX} ${minY} ${w} ${h}`);
    }

    function edgeGeom(fromId, toId) {
      const a = pos.get(fromId);
      const b = pos.get(toId);
      if (!a || !b) throw new Error('missing node position for edge: ' + fromId + ' -> ' + toId);
      const x1 = a.x + NODE_W / 2;
      const y1 = a.y + NODE_H;
      const x2 = b.x + NODE_W / 2;
      const y2 = b.y;
      const d = `M ${x1} ${y1} C ${x1} ${y1 + EDGE_CURVE_C}, ${x2} ${y2 - EDGE_CURVE_C}, ${x2} ${y2}`;
      return {d};
    }

    const edgeItems = [];
    for (const e of data.edges) {
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      path.setAttribute('class', 'edge' + (e.kind === 'seq' ? ' seq' : ''));
      path.setAttribute('marker-end', 'url(#arrow)');
      if (e.dash) path.setAttribute('stroke-dasharray', e.dash);
      const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
      const mode = n.mode ? ('mode: ' + n.mode + '\n') : '';
      const module = n.module ? ('module: ' + n.module + '\n') : '';
      const call = n.exec_call ? ('exec: ' + n.exec_call + '\n') : '';
      title.textContent = mode + module + call + '\n' + (n.doc || '');
      g.appendChild(title);

      svg.appendChild(g);
      nodeItems.set(id, g);
    }

    // UI: event delegation for node icons.
    // - focus icon: keep only the transitive upstream+downstream chain
    // - mute icon: hide all incident edges for this node
    svg.addEventListener('click', function (ev) {
      let el = ev.target;
      while (el && el !== svg) {
        if (el.classList && el.classList.contains('icon-hit')) break;
        el = el.parentNode;
      }
      if (!el || el === svg) return;

      const kind = el.getAttribute('data-kind');
      const nodeId = el.parentNode && el.parentNode.getAttribute ? el.parentNode.getAttribute('data-id') : null;
      if (!nodeId || !kind) return;

      if (kind === 'focus') {
        ui.focusId = (ui.focusId === nodeId) ? null : nodeId;
      } else if (kind === 'mute') {
        if (ui.muteEdgeNodes.has(nodeId)) ui.muteEdgeNodes.delete(nodeId);
        else ui.muteEdgeNodes.add(nodeId);
      } else {
        return;
      }

      saveUiState(ui);
      applyVisibility();
    });

    function computeFocusSet(focusId) {
      if (!focusId || !nodes.has(focusId)) return null;

      // Respect muted edges: focus traversal only follows currently visible edges.
      const isMuted = (from, to) => ui.muteEdgeNodes.has(from) || ui.muteEdgeNodes.has(to);

      const vis = new Set();
      const q = [focusId];
      vis.add(focusId);
      while (q.length) {
        const u = q.pop();
        for (const v of out.get(u) || []) {
          if (isMuted(u, v)) continue;
          if (!vis.has(v)) { vis.add(v); q.push(v); }
        }
        for (const v of inb.get(u) || []) {
          if (isMuted(v, u)) continue;
          if (!vis.has(v)) { vis.add(v); q.push(v); }
        }
      }
      return vis;
    }

    function isEdgeMutedByNode(from, to) {
      return ui.muteEdgeNodes.has(from) || ui.muteEdgeNodes.has(to);
    }

    function applyVisibility() {
      const focusSet = computeFocusSet(ui.focusId);
      for (const [id, g] of nodeItems.entries()) {
        const on = !focusSet || focusSet.has(id);
        g.style.display = on ? '' : 'none';
      }
      for (const it of edgeItems) {
        const a = it.from;
        const b = it.to;
        const onFocus = !focusSet || (focusSet.has(a) && focusSet.has(b));
        const onMute = !isEdgeMutedByNode(a, b);
        it.path.style.display = (onFocus && onMute) ? '' : 'none';
      }

      for (const [id, ic] of focusIcons.entries()) {
        const active = (ui.focusId === id);
        if (active) ic.classList.add('active');
        else ic.classList.remove('active');
      }
      for (const [id, ic] of muteIcons.entries()) {
        const active = ui.muteEdgeNodes.has(id);
        if (active) ic.classList.add('active');
        else ic.classList.remove('active');
      }

      resizeSvgToFit(focusSet);
    }

    function updateAllEdges() {
      for (const it of edgeItems) {
        const r = edgeGeom(it.from, it.to);
        it.path.setAttribute('d', r.d);
      }
    }

    function updateNode(id) {
      const p = pos.get(id);
      const g = nodeItems.get(id);
      if (!p || !g) return;
      g.setAttribute('transform', `translate(${p.x},${p.y})`);
    }

    updateAllEdges();
    applyVisibility();
    resizeSvgToFit(computeFocusSet(ui.focusId));

    let drag = null;
    function onPointerDown(ev) {
      const id = ev.currentTarget.getAttribute('data-id');
      if (!id) return;
      const p0 = pos.get(id);
      if (!p0) return;
      const s = clientToSvg(ev.clientX, ev.clientY);
      drag = {id, startX: s.x, startY: s.y, origX: p0.x, origY: p0.y, pointerId: ev.pointerId};
      ev.currentTarget.setPointerCapture(ev.pointerId);
    }
    function onPointerMove(ev) {
      if (!drag) return;
      if (ev.pointerId !== drag.pointerId) return;
      const s = clientToSvg(ev.clientX, ev.clientY);
      const nx = drag.origX + (s.x - drag.startX);
      const ny = drag.origY + (s.y - drag.startY);
      pos.set(drag.id, {x: nx, y: ny});
      updateNode(drag.id);
      updateAllEdges();
      resizeSvgToFit(computeFocusSet(ui.focusId));
    }
    function onPointerUp(ev) {
      if (!drag) return;
      if (ev.pointerId !== drag.pointerId) return;
      saveLayout(pos);
      drag = null;
    }

    for (const g of nodeItems.values()) {
      g.addEventListener('pointerdown', onPointerDown);
      g.addEventListener('pointermove', onPointerMove);
      g.addEventListener('pointerup', onPointerUp);
      g.addEventListener('pointercancel', onPointerUp);
    }

    resetBtn.addEventListener('click', function () {
      localStorage.removeItem(storageKey);
      localStorage.removeItem(uiKey);
      ui.focusId = null;
      ui.muteEdgeNodes = new Set();
      for (const [id, p] of defaultPos.entries()) pos.set(id, {x: p.x, y: p.y});
      for (const id of topo) updateNode(id);
      updateAllEdges();
      applyVisibility();
      resizeSvgToFit(null);
    });
  })();
  </script>
</body>
</html>
"##;

    let escaped_title = html_escape_text(&data.title);
    Ok(template
        .replace("__DAG_TITLE__", &escaped_title)
        .replace("__DAG_JSON__", &json))
}

pub fn render_init_steps_html_three_tags(spec: &InitStepsDagSpec) -> Result<String> {
    validate_init_steps_spec(spec)?;

    // Build three tag-specific step DAGs and reuse resources across them.
    //
    // - Steps are duplicated per tag (id-prefixed), so tag-specific layout remains clear.
    // - Resource nodes are *not* duplicated. A resource is a cross-tag coupling point, so it is
    //   rendered once and can have edges from/to steps in different tags.
    //
    // The UI remains generic: all tag/role shaping happens in Rust, not in JS.
    let data_all = build_init_steps_render_data(spec)?;

    let mut node_by_id: BTreeMap<&str, &InitStepNode> = BTreeMap::new();
    for n in &data_all.nodes {
        node_by_id.insert(n.id.as_str(), n);
    }

    let mut resources_once: Vec<InitStepNode> = Vec::new();
    for n in &data_all.nodes {
        if n.kind == "resource" {
            resources_once.push(n.clone());
        }
    }

    let mut nodes: Vec<InitStepNode> = Vec::new();
    nodes.extend(resources_once);

    let mut edges: Vec<InitStepEdge> = Vec::new();

    for tag in ["master", "owner", "external"] {
        let prefix = format!("{}::", tag);

        let mut kept_steps: Vec<InitStepNode> = Vec::new();
        for n in &data_all.nodes {
            if n.kind == "resource" {
                continue;
            }
            if n.tags.iter().any(|t| t == tag) {
                kept_steps.push(n.clone());
            }
        }

        let mut kept_step_ids: BTreeSet<String> = BTreeSet::new();
        for n in &kept_steps {
            kept_step_ids.insert(n.id.clone());
        }

        for n in kept_steps.iter_mut() {
            n.id = format!("{}{}", prefix, n.id);
            // Prefix module to keep deterministic per-tag column grouping.
            n.module = format!("{}/{}", tag, n.module);
            // Keep only this tag for styling/debugging.
            n.tags = vec![tag.to_string()];
        }

        for e in &data_all.edges {
            let from_node = node_by_id
                .get(e.from.as_str())
                .context("edge.from points to unknown node")?;
            let to_node = node_by_id
                .get(e.to.as_str())
                .context("edge.to points to unknown node")?;

            let from_is_res = from_node.kind == "resource";
            let to_is_res = to_node.kind == "resource";

            let keep = match (from_is_res, to_is_res) {
                (false, false) => kept_step_ids.contains(&e.from) && kept_step_ids.contains(&e.to),
                (false, true) => {
                    // step -> resource (publisher edge)
                    kept_step_ids.contains(&e.from) && to_node.publish_tags.iter().any(|t| t == tag)
                }
                (true, false) => {
                    // resource -> step (wait edge)
                    kept_step_ids.contains(&e.to)
                }
                (true, true) => false,
            };
            if !keep {
                continue;
            }

            let mut ee = e.clone();
            if !from_is_res {
                ee.from = format!("{}{}", prefix, ee.from);
            }
            if !to_is_res {
                ee.to = format!("{}{}", prefix, ee.to);
            }
            edges.push(ee);
        }

        nodes.extend(kept_steps);
    }

    let combined = InitStepsRenderData {
        title: data_all.title.clone(),
        nodes,
        edges,
    };

    let mut json = serde_json::to_string(&combined).context("serialize init steps render data")?;
    json = json.replace('<', "\\u003c");

    let meta_line = "This page draws three tag-specific init DAGs in one canvas: master:: / owner:: / external::. Steps are duplicated per tag (one copy per tag). Resource nodes are shared across tags and may connect steps from different tags.";
    render_init_steps_html_one_canvas(&combined.title, meta_line, &json)
}

fn render_init_steps_html_one_canvas(title: &str, meta_line: &str, json: &str) -> Result<String> {
    let template = r##"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>__DAG_TITLE__</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 16px; }
    .meta { color: #555; font-size: 12px; margin-bottom: 12px; }
    #canvas { border: 1px solid #ddd; height: 82vh; overflow: auto; background: #fff; }
    svg { background: #fff; }
    .node { cursor: move; }
    .node rect { stroke: #333; stroke-width: 1; rx: 6; ry: 6; }
    .node text { font-size: 12px; dominant-baseline: middle; }
    .edge { stroke: #555; stroke-width: 1; fill: none; }
    .edge.seq { stroke: #999; stroke-opacity: 0.45; }
    .node .icon-hit { cursor: pointer; }
    .node .icon-hit rect { fill: #fff; stroke: #666; stroke-width: 1; rx: 3; ry: 3; }
    .node .icon-hit.active rect { fill: #222; stroke: #222; }
    .node .icon-hit path { stroke: #222; stroke-width: 1.5; fill: none; stroke-linecap: round; stroke-linejoin: round; }
    .node .icon-hit.active path { stroke: #fff; }
    .node .icon-hit line { stroke: #222; stroke-width: 1.5; stroke-linecap: round; }
    .node .icon-hit.active line { stroke: #fff; }

    .node.selected rect { stroke: #d97706; stroke-width: 2; }
    .select-box { fill: rgba(59,130,246,0.12); stroke: #3b82f6; stroke-width: 1; stroke-dasharray: 4,4; pointer-events: none; }
  </style>
</head>
<body>
  <h2>__DAG_TITLE__</h2>
  <div class="meta">__META_LINE__</div>
  <pre id="dagviz-status" class="meta" style="display:none"></pre>
  <div style="display:flex; gap:8px; align-items:center; margin-bottom: 8px;">
    <button id="reset-layout" type="button">Reset layout</button>
    <span class="meta">Drag nodes to adjust layout. Box-select to multi-select, then drag to move as a group. Click background to clear selection.</span>
  </div>
  <script type="application/json" id="dag-data">__DAG_JSON__</script>
  <div id="canvas">
    <svg id="dag" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker id="arrow" markerWidth="10" markerHeight="10" refX="10" refY="3" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,3 L0,6 Z" fill="#555"></path>
        </marker>
      </defs>
    </svg>
  </div>
  <script>
  (function () {
    const statusEl = document.getElementById('dagviz-status');
    function setStatus(msg) {
      if (!statusEl) return;
      if (msg) {
        statusEl.style.display = 'block';
        statusEl.textContent = msg;
      } else {
        statusEl.style.display = 'none';
        statusEl.textContent = '';
      }
    }

    try {
      setStatus('dagviz: rendering...');
      const raw = document.getElementById('dag-data').textContent;
      const data = JSON.parse(raw);

      // Layout constants: keep them explicit and stable for deterministic output.
      const MARGIN_X = 24;
      const MARGIN_Y = 24;
      const LAYER_Y = 120;
      const COL_X = 360;
      const NODE_W = 340;
      const NODE_H = 64;
      const EDGE_CURVE_C = 120;

      const svg = document.getElementById('dag');
      const resetBtn = document.getElementById('reset-layout');

      function nodeFill(n) {
        if (n.kind === 'resource') return '#f3f4f6';
        if (n.mode === 'AsyncSpawn') return '#eaf9ee';
        if (n.mode === 'BestEffortWait') return '#fff7e6';
        return '#e8f1ff';
      }

      function docPreviewLines(n) {
        if (!n || !n.doc) return [];
        const lines = String(n.doc).split(/\r?\n/)
          .map(s => s.replace(/^\s*[-*]\s*/, '').trim())
          .filter(s => s.length);
        // Keep nodes readable in the SVG: show only a short preview in the box.
        return lines.slice(0, 2).map(s => (s.length > 42 ? (s.slice(0, 42) + '...') : s));
      }

      function edgeGeom(fromId, toId) {
        const a = pos.get(fromId);
        const b = pos.get(toId);
        if (!a || !b) throw new Error('missing node position for edge: ' + fromId + ' -> ' + toId);
        const x1 = a.x + NODE_W / 2;
        const y1 = a.y + NODE_H;
        const x2 = b.x + NODE_W / 2;
        const y2 = b.y;
        const d = `M ${x1} ${y1} C ${x1} ${y1 + EDGE_CURVE_C}, ${x2} ${y2 - EDGE_CURVE_C}, ${x2} ${y2}`;
        return {d, midX: (x1 + x2) / 2, midY: (y1 + y2) / 2};
      }

      function fnv1a(str) {
        let h = 0x811c9dc5;
        for (let i = 0; i < str.length; i++) {
          h ^= str.charCodeAt(i);
          h = (h + (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24)) >>> 0;
        }
        return ('00000000' + h.toString(16)).slice(-8);
      }

      const storageKey = 'fluxon_dagviz_layout:' + fnv1a(raw) + ':three_tags_shared_resources';
      const uiKey = storageKey + ':ui';

      function loadLayout() {
        const s = localStorage.getItem(storageKey);
        if (!s) return null;
        try {
          const obj = JSON.parse(s);
          if (!obj || typeof obj !== 'object') throw new Error('layout is not an object');
          return obj;
        } catch (_) {
          localStorage.removeItem(storageKey);
          return null;
        }
      }

      function saveLayout(pos) {
        const out = {};
        for (const [id, p] of pos.entries()) out[id] = {x: p.x, y: p.y};
        localStorage.setItem(storageKey, JSON.stringify(out));
      }

      function loadUiState() {
        const s = localStorage.getItem(uiKey);
        if (!s) return null;
        try {
          const obj = JSON.parse(s);
          if (!obj || typeof obj !== 'object') throw new Error('ui is not an object');
          if (obj.focusId != null && typeof obj.focusId !== 'string') throw new Error('focusId invalid');
          if (obj.muteEdgeNodes != null && !Array.isArray(obj.muteEdgeNodes)) throw new Error('muteEdgeNodes invalid');
          return { focusId: obj.focusId || null, muteEdgeNodes: new Set(obj.muteEdgeNodes || []) };
        } catch (_) {
          localStorage.removeItem(uiKey);
          return null;
        }
      }

      function saveUiState(ui) {
        const out = { focusId: ui.focusId, muteEdgeNodes: Array.from(ui.muteEdgeNodes) };
        localStorage.setItem(uiKey, JSON.stringify(out));
      }

      function clientToSvg(clientX, clientY) {
        const pt = svg.createSVGPoint();
        pt.x = clientX;
        pt.y = clientY;
        const ctm = svg.getScreenCTM();
        if (!ctm) throw new Error('svg has no screen CTM');
        return pt.matrixTransform(ctm.inverse());
      }

      const nodes = new Map();
      for (const n of data.nodes) nodes.set(n.id, n);

      const out = new Map();
      const inb = new Map();
      const indeg = new Map();
      for (const id of nodes.keys()) { out.set(id, []); inb.set(id, []); indeg.set(id, 0); }
      for (const e of data.edges) {
        out.get(e.from).push(e.to);
        inb.get(e.to).push(e.from);
        indeg.set(e.to, indeg.get(e.to) + 1);
      }

      const q = [];
      for (const [id, d] of indeg.entries()) if (d === 0) q.push(id);
      q.sort();
      const topo = [];
      while (q.length) {
        const u = q.shift();
        topo.push(u);
        const outs = out.get(u) || [];
        for (const v of outs) {
          indeg.set(v, indeg.get(v) - 1);
          if (indeg.get(v) === 0) {
            q.push(v);
            q.sort();
          }
        }
      }

      const defaultPos = new Map();
      const pos = new Map();
      const layout = loadLayout();

      function placeDefault() {
        const colOf = new Map();
        const colIndex = new Map();
        let nextCol = 0;

        function moduleKey(n) {
          if (n.kind === 'resource') return n.module || 'Resource';
          return n.module || 'Unknown';
        }

        for (const id of topo) {
          const n = nodes.get(id);
          const mk = moduleKey(n);
          if (!colIndex.has(mk)) {
            colIndex.set(mk, nextCol);
            nextCol += 1;
          }
          colOf.set(id, colIndex.get(mk));
        }

        const layerOf = new Map();
        for (const id of topo) {
          let maxPred = -1;
          for (const p of (inb.get(id) || [])) {
            const lp = layerOf.get(p);
            if (lp != null && lp > maxPred) maxPred = lp;
          }
          layerOf.set(id, maxPred + 1);
        }

        for (const id of topo) {
          const col = colOf.get(id) || 0;
          const layer = layerOf.get(id) || 0;
          const x = MARGIN_X + col * COL_X;
          const y = MARGIN_Y + layer * LAYER_Y;
          defaultPos.set(id, {x, y});
        }
      }

      placeDefault();

      if (layout) {
        for (const id of topo) {
          const p = layout[id];
          if (p && typeof p.x === 'number' && typeof p.y === 'number') pos.set(id, {x: p.x, y: p.y});
          else {
            const dp = defaultPos.get(id);
            pos.set(id, {x: dp.x, y: dp.y});
          }
        }
      } else {
        for (const id of topo) {
          const dp = defaultPos.get(id);
          pos.set(id, {x: dp.x, y: dp.y});
        }
      }

      // Draw edges first (so nodes are on top).
      const edgeItems = [];
      for (const e of data.edges) {
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('class', 'edge ' + e.kind);
        path.setAttribute('marker-end', 'url(#arrow)');
        if (e.dash) {
          path.setAttribute('stroke-dasharray', e.dash);
        }
        svg.appendChild(path);
        edgeItems.push({from: e.from, to: e.to, kind: e.kind, path});
      }

      const nodeItems = new Map();
      const focusIcons = new Map();
      const muteIcons = new Map();

      function mkIcon(kind, x, y) {
        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', 'icon-hit');
        g.setAttribute('data-kind', kind);
        g.setAttribute('transform', `translate(${x},${y})`);

        const r = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        r.setAttribute('x', 0);
        r.setAttribute('y', 0);
        r.setAttribute('width', 14);
        r.setAttribute('height', 14);
        g.appendChild(r);

        if (kind === 'focus') {
          const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          p.setAttribute('d', 'M4,6 a3,3 0 1,1 6,0 a3,3 0 1,1 -6,0 M9,10 L12,13');
          g.appendChild(p);
        } else if (kind === 'mute') {
          const l1 = document.createElementNS('http://www.w3.org/2000/svg', 'line');
          l1.setAttribute('x1', 3);
          l1.setAttribute('y1', 3);
          l1.setAttribute('x2', 11);
          l1.setAttribute('y2', 11);
          g.appendChild(l1);
          const l2 = document.createElementNS('http://www.w3.org/2000/svg', 'line');
          l2.setAttribute('x1', 11);
          l2.setAttribute('y1', 3);
          l2.setAttribute('x2', 3);
          l2.setAttribute('y2', 11);
          g.appendChild(l2);
        }
        return g;
      }

      for (const id of topo) {
        const n = nodes.get(id);
        const p = pos.get(id);
        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', 'node');
        g.setAttribute('data-id', id);
        g.setAttribute('transform', `translate(${p.x},${p.y})`);

        const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        rect.setAttribute('x', 0);
        rect.setAttribute('y', 0);
        rect.setAttribute('width', NODE_W);
        rect.setAttribute('height', NODE_H);
        rect.setAttribute('fill', nodeFill(n));
        g.appendChild(rect);

        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', 8);
        text.setAttribute('y', 6);
        text.setAttribute('dominant-baseline', 'hanging');

        const t0 = document.createElementNS('http://www.w3.org/2000/svg', 'tspan');
        t0.setAttribute('x', '8');
        t0.setAttribute('dy', '0');
        t0.setAttribute('font-weight', '600');
        t0.textContent = id;
        text.appendChild(t0);

        const preview = docPreviewLines(n);
        for (let i = 0; i < preview.length; i++) {
          const t = document.createElementNS('http://www.w3.org/2000/svg', 'tspan');
          t.setAttribute('x', '8');
          t.setAttribute('dy', '14');
          t.setAttribute('font-size', '11');
          t.textContent = preview[i];
          text.appendChild(t);
        }

        g.appendChild(text);

        const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
        const mode = n.mode ? ('mode: ' + n.mode + '\n') : '';
        const module = n.module ? ('module: ' + n.module + '\n') : '';
        const call = n.exec_call ? ('exec: ' + n.exec_call + '\n') : '';
        title.textContent = mode + module + call + '\n' + (n.doc || '');
        g.appendChild(title);

        const focus = mkIcon('focus', NODE_W - 36, 6);
        const mute = mkIcon('mute', NODE_W - 18, 6);
        g.appendChild(focus);
        g.appendChild(mute);
        focusIcons.set(id, focus);
        muteIcons.set(id, mute);

        svg.appendChild(g);
        nodeItems.set(id, g);
      }

      function updateNode(id) {
        const p = pos.get(id);
        const g = nodeItems.get(id);
        if (p && g) g.setAttribute('transform', `translate(${p.x},${p.y})`);
      }

      function updateAllEdges() {
        for (const e of edgeItems) {
          const geom = edgeGeom(e.from, e.to);
          e.path.setAttribute('d', geom.d);
        }
      }

      function resizeSvgToFit() {
        let maxX = 0;
        let maxY = 0;
        for (const p of pos.values()) {
          maxX = Math.max(maxX, p.x + NODE_W + MARGIN_X);
          maxY = Math.max(maxY, p.y + NODE_H + MARGIN_Y);
        }
        svg.setAttribute('width', maxX);
        svg.setAttribute('height', maxY);
      }

      const uiLoaded = loadUiState();
      const ui = uiLoaded || { focusId: null, muteEdgeNodes: new Set() };

      if (ui.focusId && !nodes.has(ui.focusId)) {
        ui.focusId = null;
        saveUiState(ui);
      }

      const selected = new Set();
      let suppressNextBackgroundClick = false;
      let selecting = null;

      function syncSelectionStyles() {
        for (const [id, g] of nodeItems.entries()) {
          if (selected.has(id)) g.classList.add('selected');
          else g.classList.remove('selected');
        }
      }

      function clearSelection() {
        if (selected.size === 0) return;
        selected.clear();
        syncSelectionStyles();
      }

      function computeChain(focusId) {
        const seen = new Set();
        const stack = [focusId];
        while (stack.length) {
          const x = stack.pop();
          if (seen.has(x)) continue;
          seen.add(x);
          for (const p of (inb.get(x) || [])) stack.push(p);
          for (const c of (out.get(x) || [])) stack.push(c);
        }
        return seen;
      }

      function applyVisibility() {
        const chain = ui.focusId ? computeChain(ui.focusId) : null;
        for (const [id, g] of nodeItems.entries()) {
          const hidden = (chain && !chain.has(id));
          g.style.display = hidden ? 'none' : '';
        }

        for (const e of edgeItems) {
          const mute = ui.muteEdgeNodes.has(e.from) || ui.muteEdgeNodes.has(e.to);
          const hidden = (chain && (!chain.has(e.from) || !chain.has(e.to))) || mute;
          e.path.style.display = hidden ? 'none' : '';
        }

        for (const [id, ic] of focusIcons.entries()) {
          if (ui.focusId === id) ic.classList.add('active');
          else ic.classList.remove('active');
        }
        for (const [id, ic] of muteIcons.entries()) {
          if (ui.muteEdgeNodes.has(id)) ic.classList.add('active');
          else ic.classList.remove('active');
        }

        syncSelectionStyles();
      }

      let drag = null;

      function onPointerDown(ev) {
        const id = ev.currentTarget.getAttribute('data-id');
        if (!id) return;

        // Ignore drag start when clicking the node's small UI icons.
        let t = ev.target;
        while (t && t !== ev.currentTarget) {
          if (t.classList && t.classList.contains('icon-hit')) return;
          t = t.parentNode;
        }

        // Clicking an unselected node collapses selection to that node.
        if (!selected.has(id)) {
          selected.clear();
          selected.add(id);
          syncSelectionStyles();
        }

        const start = clientToSvg(ev.clientX, ev.clientY);
        const ids = Array.from(selected);
        const orig = new Map();
        for (const sid of ids) {
          const p = pos.get(sid);
          orig.set(sid, {x: p.x, y: p.y});
        }
        drag = { ids, pointerId: ev.pointerId, start, orig };
        ev.currentTarget.setPointerCapture(ev.pointerId);
      }

      function onPointerMove(ev) {
        if (!drag) return;
        if (ev.pointerId !== drag.pointerId) return;
        const cur = clientToSvg(ev.clientX, ev.clientY);
        const dx = cur.x - drag.start.x;
        const dy = cur.y - drag.start.y;
        for (const sid of drag.ids) {
          const o = drag.orig.get(sid);
          pos.set(sid, {x: o.x + dx, y: o.y + dy});
          updateNode(sid);
        }
        updateAllEdges();
        resizeSvgToFit();
      }

      function onPointerUp(ev) {
        if (!drag) return;
        if (ev.pointerId !== drag.pointerId) return;
        saveLayout(pos);
        drag = null;
      }

      for (const g of nodeItems.values()) {
        g.addEventListener('pointerdown', onPointerDown);
        g.addEventListener('pointermove', onPointerMove);
        g.addEventListener('pointerup', onPointerUp);
        g.addEventListener('pointercancel', onPointerUp);
      }

      function isBackgroundEventTarget(ev) {
        if (!ev || !ev.target) return true;
        if (ev.target.closest) {
          if (ev.target.closest('.icon-hit')) return false;
          if (ev.target.closest('.node')) return false;
        }
        return true;
      }

      svg.addEventListener('pointerdown', function (ev) {
        if (!isBackgroundEventTarget(ev)) return;
        if (ev.button !== 0) return;

        selecting = { pointerId: ev.pointerId, start: clientToSvg(ev.clientX, ev.clientY), rect: null };
        const r = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        r.setAttribute('class', 'select-box');
        r.setAttribute('x', selecting.start.x);
        r.setAttribute('y', selecting.start.y);
        r.setAttribute('width', 0);
        r.setAttribute('height', 0);
        svg.appendChild(r);
        selecting.rect = r;
        svg.setPointerCapture(ev.pointerId);
        ev.preventDefault();
      });

      svg.addEventListener('pointermove', function (ev) {
        if (!selecting) return;
        if (ev.pointerId !== selecting.pointerId) return;
        const cur = clientToSvg(ev.clientX, ev.clientY);
        const x1 = Math.min(selecting.start.x, cur.x);
        const y1 = Math.min(selecting.start.y, cur.y);
        const x2 = Math.max(selecting.start.x, cur.x);
        const y2 = Math.max(selecting.start.y, cur.y);

        selecting.rect.setAttribute('x', x1);
        selecting.rect.setAttribute('y', y1);
        selecting.rect.setAttribute('width', x2 - x1);
        selecting.rect.setAttribute('height', y2 - y1);

        selected.clear();
        for (const [id, g] of nodeItems.entries()) {
          if (g.style.display === 'none') continue;
          const p = pos.get(id);
          const nx1 = p.x;
          const ny1 = p.y;
          const nx2 = p.x + NODE_W;
          const ny2 = p.y + NODE_H;
          const overlap = !(nx2 < x1 || nx1 > x2 || ny2 < y1 || ny1 > y2);
          if (overlap) selected.add(id);
        }
        syncSelectionStyles();
        ev.preventDefault();
      });

      function endSelecting(ev) {
        if (!selecting) return;
        if (ev.pointerId !== selecting.pointerId) return;
        if (selecting.rect && selecting.rect.parentNode) selecting.rect.parentNode.removeChild(selecting.rect);
        selecting = null;
        suppressNextBackgroundClick = true;
      }

      svg.addEventListener('pointerup', endSelecting);
      svg.addEventListener('pointercancel', endSelecting);

      svg.addEventListener('click', function (ev) {
        if (!isBackgroundEventTarget(ev)) return;
        if (suppressNextBackgroundClick) {
          suppressNextBackgroundClick = false;
          return;
        }
        clearSelection();
      });

      function onIconClick(ev) {
        ev.stopPropagation();
        ev.preventDefault();
        const nodeId = ev.currentTarget.parentNode.getAttribute('data-id');
        const kind = ev.currentTarget.getAttribute('data-kind');
        if (!nodeId || !kind) return;
        if (kind === 'focus') {
          ui.focusId = (ui.focusId === nodeId) ? null : nodeId;
        } else if (kind === 'mute') {
          if (ui.muteEdgeNodes.has(nodeId)) ui.muteEdgeNodes.delete(nodeId);
          else ui.muteEdgeNodes.add(nodeId);
        } else {
          return;
        }
        saveUiState(ui);
        applyVisibility();
      }

      for (const ic of focusIcons.values()) {
        ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
        ic.addEventListener('click', onIconClick);
      }
      for (const ic of muteIcons.values()) {
        ic.addEventListener('pointerdown', (ev) => { ev.stopPropagation(); });
        ic.addEventListener('click', onIconClick);
      }

      resetBtn.addEventListener('click', function () {
        localStorage.removeItem(storageKey);
        localStorage.removeItem(uiKey);
        ui.focusId = null;
        ui.muteEdgeNodes = new Set();
        selected.clear();
        syncSelectionStyles();
        for (const [id, p] of defaultPos.entries()) pos.set(id, {x: p.x, y: p.y});
        for (const id of topo) updateNode(id);
        updateAllEdges();
        applyVisibility();
        resizeSvgToFit();
      });

      updateAllEdges();
      applyVisibility();
      resizeSvgToFit();

      setStatus('');
    } catch (e) {
      const msg = (e && e.stack) ? e.stack : String(e);
      // Avoid embedding raw newlines in JS string literals (easy to break during template edits).
      setStatus(['dagviz render error:', msg].join('\n'));
      throw e;
    }
  })();
  </script>
</body>
</html>
"##;

    let escaped_title = html_escape_text(title);
    let escaped_meta = html_escape_text(meta_line);
    Ok(template
        .replace("__DAG_TITLE__", &escaped_title)
        .replace("__META_LINE__", &escaped_meta)
        .replace("__DAG_JSON__", json))
}

pub fn render_init_steps_html_variants(
    spec: &InitStepsDagSpec,
    variants: &[InitDagVariantSpec],
) -> Result<String> {
    validate_init_steps_spec(spec)?;
    if variants.is_empty() {
        bail!("variants must not be empty");
    }

    let mut variant_ids: BTreeSet<&str> = BTreeSet::new();
    for v in variants {
        if v.id.trim().is_empty() {
            bail!("variant id must not be empty");
        }
        if v.id.trim() != v.id {
            bail!(
                "variant id must not contain surrounding whitespace: {}",
                v.id
            );
        }
        if v.tags.is_empty() {
            bail!("variant {} tags must not be empty", v.id);
        }
        if !variant_ids.insert(v.id.as_str()) {
            bail!("duplicate variant id: {}", v.id);
        }
    }

    let data_all = build_init_steps_render_data(spec)?;
    let mut node_by_id: BTreeMap<&str, &InitStepNode> = BTreeMap::new();
    for n in &data_all.nodes {
        node_by_id.insert(n.id.as_str(), n);
    }

    let mut resources_once: Vec<InitStepNode> = Vec::new();
    for n in &data_all.nodes {
        if n.kind == "resource" {
            resources_once.push(n.clone());
        }
    }

    let mut nodes: Vec<InitStepNode> = Vec::new();
    nodes.extend(resources_once);
    let mut edges: Vec<InitStepEdge> = Vec::new();

    for v in variants {
        let prefix = format!("{}::", v.id);
        let mut variant_tags: BTreeSet<&str> = BTreeSet::new();
        for t in &v.tags {
            if t.trim().is_empty() {
                bail!("variant {} tags must not contain empty", v.id);
            }
            if t.trim() != t {
                bail!(
                    "variant {} tags must not contain surrounding whitespace: {}",
                    v.id,
                    t
                );
            }
            variant_tags.insert(t.as_str());
        }

        let mut kept_steps: Vec<InitStepNode> = Vec::new();
        for n in &data_all.nodes {
            if n.kind == "resource" {
                continue;
            }
            if n.tags.iter().any(|t| variant_tags.contains(t.as_str())) {
                kept_steps.push(n.clone());
            }
        }

        let mut kept_step_ids: BTreeSet<String> = BTreeSet::new();
        for n in &kept_steps {
            kept_step_ids.insert(n.id.clone());
        }

        for n in kept_steps.iter_mut() {
            n.id = format!("{}{}", prefix, n.id);
            n.module = format!("{}/{}", v.id, n.module);
            // Use variant id as the only tag for styling/debugging.
            n.tags = vec![v.id.clone()];
        }

        for e in &data_all.edges {
            let from_node = node_by_id
                .get(e.from.as_str())
                .context("edge.from points to unknown node")?;
            let to_node = node_by_id
                .get(e.to.as_str())
                .context("edge.to points to unknown node")?;

            let from_is_res = from_node.kind == "resource";
            let to_is_res = to_node.kind == "resource";

            let keep = match (from_is_res, to_is_res) {
                (false, false) => kept_step_ids.contains(&e.from) && kept_step_ids.contains(&e.to),
                (false, true) => {
                    // step -> resource (publisher edge)
                    kept_step_ids.contains(&e.from)
                        && to_node
                            .publish_tags
                            .iter()
                            .any(|t| variant_tags.contains(t.as_str()))
                }
                (true, false) => {
                    // resource -> step (wait edge)
                    kept_step_ids.contains(&e.to)
                }
                (true, true) => false,
            };
            if !keep {
                continue;
            }

            let mut ee = e.clone();
            if !from_is_res {
                ee.from = format!("{}{}", prefix, ee.from);
            }
            if !to_is_res {
                ee.to = format!("{}{}", prefix, ee.to);
            }
            edges.push(ee);
        }

        nodes.extend(kept_steps);
    }

    let combined = InitStepsRenderData {
        title: data_all.title.clone(),
        nodes,
        edges,
    };

    let mut json = serde_json::to_string(&combined).context("serialize init steps render data")?;
    json = json.replace('<', "\\u003c");

    let variant_list = variants
        .iter()
        .map(|v| format!("{}::", v.id))
        .collect::<Vec<_>>()
        .join(" / ");
    let meta_line = format!(
        "This page draws variant-specific init DAGs in one canvas: {}. Steps are duplicated per variant (one copy per variant). Resource nodes are shared across variants and may connect steps from different variants.",
        variant_list
    );

    render_init_steps_html_one_canvas(&combined.title, &meta_line, &json)
}

pub fn write_init_steps_html_file(path: &std::path::Path, spec: &InitStepsDagSpec) -> Result<()> {
    let html = render_init_steps_html(spec)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir: {}", parent.display()))?;
    }
    std::fs::write(path, html).with_context(|| format!("write: {}", path.display()))?;
    Ok(())
}

fn validate_init_steps_spec(spec: &InitStepsDagSpec) -> Result<()> {
    if spec.steps.is_empty() {
        bail!("init-steps dag spec has no steps");
    }
    let mut ids = BTreeSet::new();
    for s in &spec.steps {
        if s.id.trim().is_empty() {
            bail!("init step id must not be empty");
        }
        if s.module.trim().is_empty() {
            bail!("init step {} has empty module", s.id);
        }
        if s.doc.trim().is_empty() {
            bail!("init step {} has empty doc", s.id);
        }
        if s.exec_call.trim().is_empty() {
            bail!("init step {} has empty exec_call", s.id);
        }
        if !ids.insert(s.id.clone()) {
            bail!("duplicate init step id: {}", s.id);
        }
    }

    let mut res_ids: BTreeSet<String> = BTreeSet::new();
    for r in &spec.resources {
        if r.id.trim().is_empty() {
            bail!("resource id must not be empty");
        }
        if r.published_by.trim().is_empty() {
            bail!("resource {} has empty published_by", r.id);
        }
        if r.hook_call.trim().is_empty() {
            bail!("resource {} has empty hook_call", r.id);
        }
        if r.doc.trim().is_empty() {
            bail!("resource {} has empty doc", r.id);
        }
        if !res_ids.insert(r.id.clone()) {
            bail!("duplicate resource id: {}", r.id);
        }
        if !ids.contains(r.published_by.as_str()) {
            bail!(
                "resource {} published_by unknown step id: {}",
                r.id,
                r.published_by
            );
        }
    }
    // Verify that all deps references exist.
    for s in &spec.steps {
        for r in &s.deps {
            if !ids.contains(r) {
                bail!("init step {} deps unknown step id: {}", s.id, r);
            }
        }
    }

    for s in &spec.steps {
        for w in &s.waits {
            if !res_ids.contains(w.as_str()) {
                bail!("init step {} waits unknown resource id: {}", s.id, w);
            }
        }
    }
    Ok(())
}

fn build_init_steps_render_data(spec: &InitStepsDagSpec) -> Result<InitStepsRenderData> {
    fn dash_for_mode(mode: &StepMode) -> Option<String> {
        match mode {
            StepMode::Blocking => None,
            StepMode::AsyncSpawn => Some("6,4".to_string()),
            StepMode::BestEffortWait => Some("2,4".to_string()),
        }
    }

    // Group steps by module for deterministic intra-module sequencing.
    let mut by_module: BTreeMap<String, Vec<InitStepSpec>> = BTreeMap::new();
    for s in &spec.steps {
        by_module
            .entry(s.module.clone())
            .or_default()
            .push(s.clone());
    }
    for steps in by_module.values_mut() {
        steps.sort_by(|a, b| a.order.cmp(&b.order).then_with(|| a.id.cmp(&b.id)));
        // Disallow duplicate order inside a module to avoid ambiguity.
        let mut seen: BTreeSet<u32> = BTreeSet::new();
        for s in steps.iter() {
            if !seen.insert(s.order) {
                bail!("module {} has duplicate init order {}", s.module, s.order);
            }
        }
    }

    // Build nodes.
    let mut nodes: Vec<InitStepNode> = Vec::new();
    for s in &spec.steps {
        nodes.push(InitStepNode {
            id: s.id.clone(),
            kind: "step".to_string(),
            tags: s.tags.clone(),
            publish_tags: Vec::new(),
            published_by: None,
            module: s.module.clone(),
            order: s.order,
            mode: s.mode.clone(),
            exec_call: Some(s.exec_call.clone()),
            doc: s.doc.clone(),
        });
    }

    for r in &spec.resources {
        nodes.push(InitStepNode {
            id: r.id.clone(),
            kind: "resource".to_string(),
            tags: r.tags.clone(),
            publish_tags: r.publish_tags.clone(),
            published_by: Some(r.published_by.clone()),
            module: "Resource".to_string(),
            order: 0,
            mode: StepMode::Blocking,
            exec_call: Some(r.hook_call.clone()),
            doc: r.doc.clone(),
        });
    }

    // Build edges:
    // - step deps (explicit)
    // - intra-module sequence edges (implicit)
    // - resource publish edges: step -> resource
    // - resource wait edges: resource -> step
    // Deduplicate edges by (from,to,kind) and keep the target step's mode as dash style.
    let mut edges: BTreeMap<(String, String, String), Option<String>> = BTreeMap::new();

    let mut mode_by_id: BTreeMap<String, StepMode> = BTreeMap::new();
    for s in &spec.steps {
        mode_by_id.insert(s.id.clone(), s.mode.clone());
    }

    for s in &spec.steps {
        for req in &s.deps {
            let dash = dash_for_mode(mode_by_id.get(&s.id).expect("mode must exist"));
            edges.insert((req.clone(), s.id.clone(), "dep".to_string()), dash);
        }
    }
    for (_m, steps) in &by_module {
        for w in steps.windows(2) {
            let from = w[0].id.clone();
            let to = w[1].id.clone();
            let dash = dash_for_mode(mode_by_id.get(&to).expect("mode must exist"));
            edges.insert((from, to, "seq".to_string()), dash);
        }
    }

    // Resource edges.
    let mut published_by: BTreeMap<&str, &str> = BTreeMap::new();
    for r in &spec.resources {
        published_by.insert(r.id.as_str(), r.published_by.as_str());
        edges.insert(
            (r.published_by.clone(), r.id.clone(), "pub".to_string()),
            None,
        );
    }
    for s in &spec.steps {
        for w in &s.waits {
            let dash = dash_for_mode(mode_by_id.get(&s.id).expect("mode must exist"));
            // Draw semantic waiting through the resource node.
            let _pub = published_by.get(w.as_str()).expect("resource must exist");
            edges.insert((w.clone(), s.id.clone(), "wait".to_string()), dash);
        }
    }

    // Cycle check on the combined dependency graph.
    assert_init_steps_acyclic(&nodes, &edges)?;

    let mut edge_vec: Vec<InitStepEdge> = Vec::new();
    for ((from, to, kind), dash) in edges {
        edge_vec.push(InitStepEdge {
            from,
            to,
            dash,
            kind,
        });
    }

    Ok(InitStepsRenderData {
        title: spec.title.clone(),
        nodes,
        edges: edge_vec,
    })
}

fn assert_init_steps_acyclic(
    nodes: &[InitStepNode],
    edges: &BTreeMap<(String, String, String), Option<String>>,
) -> Result<()> {
    let mut indeg: BTreeMap<&str, usize> = BTreeMap::new();
    let mut out: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for n in nodes {
        indeg.insert(n.id.as_str(), 0);
        out.insert(n.id.as_str(), Vec::new());
    }
    for ((from, to, _kind), _dash) in edges {
        let fs = from.as_str();
        let ts = to.as_str();
        out.get_mut(fs).expect("from node must exist").push(ts);
        *indeg.get_mut(ts).expect("to node must exist") += 1;
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
        let outs = out.get(u).expect("node must exist");
        for &v in outs {
            let dv = indeg.get_mut(v).expect("node must exist");
            *dv -= 1;
            if *dv == 0 {
                q.push_back(v);
            }
        }
    }
    if visited != nodes.len() {
        let mut remaining: Vec<&str> = indeg
            .iter()
            .filter_map(|(id, d)| if *d > 0 { Some(*id) } else { None })
            .collect();
        remaining.sort();
        bail!(
            "init-step graph has a cycle or unresolved dependencies; remaining nodes: {:?}",
            remaining
        );
    }
    Ok(())
}

fn html_escape_text(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn validate_spec(spec: &DagSpec) -> Result<()> {
    if spec.resources.is_empty() {
        bail!("dag spec has no resources");
    }
    if spec.steps.is_empty() {
        bail!("dag spec has no steps");
    }

    let mut ids = BTreeSet::new();
    for r in &spec.resources {
        if r.id.trim().is_empty() {
            bail!("resource id must not be empty");
        }
        if !ids.insert(r.id.clone()) {
            bail!("duplicate resource id: {}", r.id);
        }
    }

    let mut step_ids = BTreeSet::new();
    for s in &spec.steps {
        if s.id.trim().is_empty() {
            bail!("step id must not be empty");
        }
        if !step_ids.insert(s.id.clone()) {
            bail!("duplicate step id: {}", s.id);
        }
        if s.provides.is_empty() {
            bail!(
                "step {} provides empty; every step must provide at least one resource",
                s.id
            );
        }
    }

    // Ensure all references exist.
    for s in &spec.steps {
        for r in s.requires.iter().chain(s.provides.iter()) {
            if !ids.contains(r) {
                bail!("step {} references unknown resource: {}", s.id, r);
            }
        }
    }
    Ok(())
}

fn build_render_data(spec: &DagSpec) -> Result<RenderData> {
    let mut kind_by_id: BTreeMap<String, ResourceKind> = BTreeMap::new();
    for r in &spec.resources {
        kind_by_id.insert(r.id.clone(), r.kind.clone());
    }

    let mut provided_by: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut required_by: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for r in &spec.resources {
        provided_by.insert(r.id.clone(), BTreeSet::new());
        required_by.insert(r.id.clone(), BTreeSet::new());
    }

    #[derive(Clone, Debug)]
    struct EdgeAgg {
        labels: BTreeSet<String>,
        mode_rank: u8,
    }

    fn rank_step_mode(mode: &StepMode) -> u8 {
        match mode {
            StepMode::Blocking => 0,
            StepMode::AsyncSpawn => 1,
            StepMode::BestEffortWait => 2,
        }
    }

    fn dash_for_rank(rank: u8) -> Option<String> {
        match rank {
            0 => None,
            1 => Some("6,4".to_string()),
            2 => Some("2,4".to_string()),
            _ => None,
        }
    }

    // Edge aggregation: (from, to) -> labels + style
    let mut edge_labels: BTreeMap<(String, String), EdgeAgg> = BTreeMap::new();

    for step in &spec.steps {
        for p in &step.provides {
            provided_by
                .get_mut(p)
                .expect("resource id must exist")
                .insert(step.id.clone());
        }
        for r in &step.requires {
            required_by
                .get_mut(r)
                .expect("resource id must exist")
                .insert(step.id.clone());
        }

        let step_rank = rank_step_mode(&step.mode);
        for req in &step.requires {
            for prov in &step.provides {
                let key = (req.clone(), prov.clone());
                let entry = edge_labels.entry(key).or_insert_with(|| EdgeAgg {
                    labels: BTreeSet::new(),
                    mode_rank: step_rank,
                });
                entry.labels.insert(step.id.clone());
                if entry.mode_rank < step_rank {
                    entry.mode_rank = step_rank;
                }
            }
        }
    }

    // Ensure the resource-only edge graph is acyclic.
    // Ensure the resource-only edge graph is acyclic.
    // Note: we do not treat style (best-effort/async) as removing an edge.
    assert_resource_graph_acyclic(&kind_by_id, &edge_labels)?;

    let mut nodes: Vec<ResourceNode> = Vec::new();
    for (id, kind) in kind_by_id.iter() {
        let pb = provided_by.get(id).expect("resource id must exist");
        let rb = required_by.get(id).expect("resource id must exist");
        nodes.push(ResourceNode {
            id: id.clone(),
            kind: kind.clone(),
            provided_by_steps: pb.iter().cloned().collect(),
            required_by_steps: rb.iter().cloned().collect(),
        });
    }

    let mut edges: Vec<ResourceEdge> = Vec::new();
    for ((from, to), agg) in edge_labels {
        edges.push(ResourceEdge {
            from,
            to,
            step_labels: agg.labels.into_iter().collect(),
            dash: dash_for_rank(agg.mode_rank),
        });
    }

    Ok(RenderData {
        title: spec.title.clone(),
        nodes,
        edges,
    })
}

fn assert_resource_graph_acyclic<V>(
    kind_by_id: &BTreeMap<String, ResourceKind>,
    edges: &BTreeMap<(String, String), V>,
) -> Result<()> {
    let mut indeg: BTreeMap<&str, usize> = BTreeMap::new();
    let mut out: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for id in kind_by_id.keys() {
        indeg.insert(id.as_str(), 0);
        out.insert(id.as_str(), Vec::new());
    }

    for ((from, to), _) in edges {
        let from_s = from.as_str();
        let to_s = to.as_str();
        out.get_mut(from_s)
            .expect("from node must exist")
            .push(to_s);
        *indeg.get_mut(to_s).expect("to node must exist") += 1;
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
        let outs = out.get(u).expect("node must exist");
        for &v in outs {
            let dv = indeg.get_mut(v).expect("node must exist");
            *dv -= 1;
            if *dv == 0 {
                q.push_back(v);
            }
        }
    }

    if visited != kind_by_id.len() {
        // Provide a compact hint: list nodes still having indegree > 0.
        let mut remaining: Vec<&str> = indeg
            .iter()
            .filter_map(|(id, d)| if *d > 0 { Some(*id) } else { None })
            .collect();
        remaining.sort();
        bail!(
            "resource graph has a cycle or unresolved dependencies; remaining nodes: {:?}",
            remaining
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn r(id: &str, kind: ResourceKind) -> ResourceSpec {
        ResourceSpec {
            id: id.to_string(),
            kind,
        }
    }

    fn step(id: &str, mode: StepMode, requires: &[&str], provides: &[&str]) -> StepSpec {
        StepSpec {
            id: id.to_string(),
            mode,
            requires: requires.iter().map(|s| s.to_string()).collect(),
            provides: provides.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn generate_fluxon_kv_init_mock_html() {
        // This is a mock init DAG derived from fluxon_kv's current framework init flow.
        //
        // Rules for this mock:
        // - Resources are modules ("module ready"), not per-RPC readiness.
        // - A module being ready implies its init-time RPC registration/startup is complete.
        // - Only model real module-to-module dependencies; do not serialize by framework order.

        let steps = vec![
            // Master cluster manager init steps.
            InitStepSpec {
                id: "master.cluster_manager.init.0.start".to_string(),
                module: "master.cluster_manager".to_string(),
                tags: vec![],
                order: 0,
                mode: StepMode::Blocking,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec![],
                doc: "- 启动: master cluster_manager\n- 产出: cluster_state_rw".to_string(),
            },
            InitStepSpec {
                id: "master.cluster_manager.init.1.spawn_prom_broadcast".to_string(),
                module: "master.cluster_manager".to_string(),
                tags: vec![],
                order: 1,
                mode: StepMode::AsyncSpawn,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec![],
                doc: "- 启动: prom urls broadcast loop\n- 产出: prom_urls_ready".to_string(),
            },
            // Client cluster manager init steps.
            InitStepSpec {
                id: "client.cluster_manager.init.0.start".to_string(),
                module: "client.cluster_manager".to_string(),
                tags: vec![],
                order: 0,
                mode: StepMode::Blocking,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec![],
                doc: "- 启动: client cluster_manager\n- 产出: cluster_state_rw".to_string(),
            },
            InitStepSpec {
                id: "client.cluster_manager.init.1.spawn_accessible_ip_probe".to_string(),
                module: "client.cluster_manager".to_string(),
                tags: vec![],
                order: 1,
                mode: StepMode::AsyncSpawn,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec![],
                doc: "- 启动: accessible_ip probe loop".to_string(),
            },
            InitStepSpec {
                id: "client.cluster_manager.init.2.wait_accessible_ip".to_string(),
                module: "client.cluster_manager".to_string(),
                tags: vec![],
                order: 2,
                mode: StepMode::Blocking,
                exec_call: "mock".to_string(),
                deps: vec![
                    // This is a cross-step dependency beyond intra-module ordering: we assume
                    // the probe runs on master-driven broadcast/membership to determine accessible_ip.
                    "client.cluster_manager.init.0.start".to_string(),
                ],
                waits: vec![],
                doc: "- 等待: accessible_ip_ready".to_string(),
            },
            // Client P2P init (async/non-blocking).
            InitStepSpec {
                id: "client.p2p.init.0.start_transport_control".to_string(),
                module: "client.p2p".to_string(),
                tags: vec![],
                order: 0,
                mode: StepMode::AsyncSpawn,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec!["cluster_state_rw".to_string()],
                doc: "- 启动: p2p transport control loop\n- 产出: p2p_rpc_ready".to_string(),
            },
            // Client transfer engine blocks on accessible_ip (non-P2P backends).
            InitStepSpec {
                id: "client.transfer_engine.init.0.build".to_string(),
                module: "client.transfer_engine".to_string(),
                tags: vec![],
                order: 0,
                mode: StepMode::Blocking,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec![
                    "accessible_ip_ready".to_string(),
                    "p2p_rpc_ready".to_string(),
                ],
                doc: "- 构建: transfer engine backend\n- 产出: transfer_engine_ready".to_string(),
            },
            // Client segpool depends on transfer engine; publishes transfer_ready as part of init.
            InitStepSpec {
                id: "client.segpool.init.0.register_segments".to_string(),
                module: "client.segpool".to_string(),
                tags: vec![],
                order: 0,
                mode: StepMode::Blocking,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec!["transfer_engine_ready".to_string()],
                doc: "- 调用: transfer_engine.register_segments".to_string(),
            },
            InitStepSpec {
                id: "client.segpool.init.1.publish_transfer_ready".to_string(),
                module: "client.segpool".to_string(),
                tags: vec![],
                order: 1,
                mode: StepMode::Blocking,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec![],
                doc: "- 写入: cluster_state.transfer_ready".to_string(),
            },
            // MetricReporter: init has two steps; the best-effort wait times out and still completes init.
            InitStepSpec {
                id: "client.metric_reporter.init.0.spawn_loop".to_string(),
                module: "client.metric_reporter".to_string(),
                tags: vec![],
                order: 0,
                mode: StepMode::AsyncSpawn,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec!["cluster_state_rw".to_string(), "p2p_rpc_ready".to_string()],
                doc: "- 启动: metrics reporter loop".to_string(),
            },
            InitStepSpec {
                id: "client.metric_reporter.init.1.best_effort_wait_prom_urls".to_string(),
                module: "client.metric_reporter".to_string(),
                tags: vec![],
                order: 1,
                mode: StepMode::BestEffortWait,
                exec_call: "mock".to_string(),
                deps: vec![],
                waits: vec!["prom_urls_ready".to_string()],
                doc: "- 等待: prom_urls_ready(best-effort)".to_string(),
            },
        ];

        let spec = InitStepsDagSpec {
            title: "fluxon_kv init mock (step-nodes, module multi-init)".to_string(),
            steps,
            resources: vec![
                InitResourceSpec {
                    id: "cluster_state_rw".to_string(),
                    tags: vec![],
                    publish_tags: vec![],
                    hook_call: "mock".to_string(),
                    published_by: "client.cluster_manager.init.0.start".to_string(),
                    doc: "- etcd: cluster_state 可读写".to_string(),
                },
                InitResourceSpec {
                    id: "accessible_ip_ready".to_string(),
                    tags: vec![],
                    publish_tags: vec![],
                    hook_call: "mock".to_string(),
                    published_by: "client.cluster_manager.init.2.wait_accessible_ip".to_string(),
                    doc: "- etcd: accessible_ip 就绪".to_string(),
                },
                InitResourceSpec {
                    id: "p2p_rpc_ready".to_string(),
                    tags: vec![],
                    publish_tags: vec![],
                    hook_call: "mock".to_string(),
                    published_by: "client.p2p.init.0.start_transport_control".to_string(),
                    doc: "- p2p: rpc transport ready".to_string(),
                },
                InitResourceSpec {
                    id: "transfer_engine_ready".to_string(),
                    tags: vec![],
                    publish_tags: vec![],
                    hook_call: "mock".to_string(),
                    published_by: "client.transfer_engine.init.0.build".to_string(),
                    doc: "- transfer engine backend ready".to_string(),
                },
                InitResourceSpec {
                    id: "prom_urls_ready".to_string(),
                    tags: vec![],
                    publish_tags: vec![],
                    hook_call: "mock".to_string(),
                    published_by: "master.cluster_manager.init.1.spawn_prom_broadcast".to_string(),
                    doc: "- broadcast: prom remote_write urls".to_string(),
                },
            ],
        };

        let out = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("target")
            .join("dagviz")
            .join("fluxonkv_init_mock.html");

        write_init_steps_html_file(&out, &spec).expect("write html");

        let meta = std::fs::metadata(&out).expect("stat html");
        assert!(meta.len() > 1024, "html seems too small: {}", meta.len());

        // For manual verification:
        // cargo test -p fluxon_util generate_fluxon_kv_init_mock_html -- --nocapture
        println!("dag viz html written: {}", out.display());
    }
}
