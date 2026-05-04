const form = document.getElementById("stream-form");
const disconnectButton = document.getElementById("disconnect");
const statusEl = document.getElementById("status");
const graphEl = document.getElementById("graph");
const streamUrlEl = document.getElementById("stream-url");
const seedIdsEl = document.getElementById("seed-ids");
const hopsEl = document.getElementById("hops");
const edgeTypesEl = document.getElementById("edge-types");

const state = {
  source: null,
  nodes: new Map(),
  edges: new Map(),
  order: 0,
  lastEvent: null,
};

function setStatus(text) {
  statusEl.textContent = text;
}

function disconnect() {
  if (state.source) {
    state.source.close();
    state.source = null;
  }
  setStatus("disconnected");
}

function entityFromPayload(payload) {
  return payload.entity ?? payload;
}

function normalizeNode(kind, payload) {
  const entity = entityFromPayload(payload);
  const id = String(entity.entity_id ?? entity.id ?? "");
  if (!id) {
    return null;
  }
  const existing = state.nodes.get(id);
  const node = existing ?? {
    id,
    kind,
    payload,
    order: state.order++,
  };
  node.kind = existing?.kind === "seed_node" ? "seed_node" : kind;
  node.payload = payload;
  node.label = entity.name ?? entity.entity_type ?? entity.type ?? id;
  node.depth = payload.depth ?? 0;
  state.nodes.set(id, node);
  return node;
}

function normalizeEdge(payload) {
  const relation = payload.relation ?? payload;
  const source = payload.source ?? {};
  const target = payload.target ?? {};
  const id = String(
    relation.relation_id ??
      relation.id ??
      `${source.entity_id ?? relation.source_id}:${target.entity_id ?? relation.target_id}:${relation.relation_type ?? relation.type}`,
  );
  const edge = state.edges.get(id) ?? { id, payload };
  edge.payload = payload;
  edge.label = relation.relation_type ?? relation.type ?? id;
  state.edges.set(id, edge);
  return edge;
}

function layoutNodes(nodes) {
  const width = 1000;
  const height = 760;
  const cx = width / 2;
  const cy = height / 2;
  const seedNodes = nodes.filter((node) => node.kind === "seed_node");
  const others = nodes.filter((node) => node.kind !== "seed_node");
  const positions = new Map();

  const seedSpread = Math.max(1, seedNodes.length);
  seedNodes.forEach((node, index) => {
    const x = cx + (index - (seedSpread - 1) / 2) * 150;
    positions.set(node.id, { x, y: cy - 120 });
  });

  const goldenAngle = Math.PI * (3 - Math.sqrt(5));
  others.forEach((node, index) => {
    const radius = 150 + index * 24;
    const angle = index * goldenAngle;
    const x = cx + Math.cos(angle) * radius;
    const y = cy + Math.sin(angle) * radius;
    positions.set(node.id, { x, y });
  });

  return positions;
}

function svgEl(name, attrs = {}) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== null && value !== undefined) {
      el.setAttribute(key, String(value));
    }
  }
  return el;
}

function render() {
  const nodes = [...state.nodes.values()].sort((a, b) => a.order - b.order);
  const positions = layoutNodes(nodes);
  graphEl.replaceChildren();

  const defs = svgEl("defs");
  defs.append(
    svgEl("marker", {
      id: "arrow",
      viewBox: "0 0 10 10",
      refX: "8",
      refY: "5",
      markerWidth: "8",
      markerHeight: "8",
      orient: "auto-start-reverse",
    }),
  );
  defs.firstElementChild?.append(svgEl("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: "rgba(118,213,255,0.7)" }));
  graphEl.append(defs);

  graphEl.append(
    svgEl("rect", {
      x: 0,
      y: 0,
      width: 1000,
      height: 760,
      fill: "transparent",
    }),
  );

  for (const edge of state.edges.values()) {
    const payload = edge.payload;
    const relation = payload.relation ?? payload;
    const sourceId = String(payload.source?.entity_id ?? relation.source_id ?? "");
    const targetId = String(payload.target?.entity_id ?? relation.target_id ?? "");
    const source = positions.get(sourceId);
    const target = positions.get(targetId);
    if (!source || !target) {
      continue;
    }
    graphEl.append(
      svgEl("line", {
        x1: source.x,
        y1: source.y,
        x2: target.x,
        y2: target.y,
        stroke: "rgba(118,213,255,0.28)",
        "stroke-width": 2,
        "stroke-linecap": "round",
        "marker-end": "url(#arrow)",
      }),
    );
  }

  for (const node of nodes) {
    const pos = positions.get(node.id);
    if (!pos) {
      continue;
    }

    const isSeed = node.kind === "seed_node";
    const fill = isSeed ? "rgba(255,211,110,0.96)" : "rgba(124,240,200,0.92)";
    const stroke = isSeed ? "rgba(255,211,110,0.35)" : "rgba(124,240,200,0.28)";
    const radius = isSeed ? 18 : 15;

    const group = svgEl("g");
    group.append(
      svgEl("circle", {
        cx: pos.x,
        cy: pos.y,
        r: radius + 8,
        fill: "rgba(255,255,255,0.02)",
        stroke,
        "stroke-width": 1,
      }),
      svgEl("circle", {
        cx: pos.x,
        cy: pos.y,
        r: radius,
        fill,
      }),
      svgEl("text", {
        x: pos.x,
        y: pos.y + 30,
        fill: "rgba(232,236,255,0.92)",
        "text-anchor": "middle",
        "font-size": "14",
        "font-weight": "600",
      }),
    );
    group.lastElementChild.textContent = node.label;
    graphEl.append(group);
  }

  if (state.lastEvent) {
    setStatus(state.lastEvent.kind);
  }
}

function connect(url, params) {
  disconnect();
  state.nodes.clear();
  state.edges.clear();
  state.order = 0;
  state.lastEvent = null;

  const stream = new URL(url, window.location.href);
  stream.search = params.toString();

  setStatus("connecting");
  const source = new EventSource(stream.toString());
  state.source = source;

  source.onopen = () => {
    setStatus("connected");
  };

  source.addEventListener("seed_node", (event) => {
    const payload = JSON.parse(event.data);
    state.lastEvent = { kind: "seed_node" };
    normalizeNode("seed_node", payload);
    render();
  });

  source.addEventListener("node", (event) => {
    const payload = JSON.parse(event.data);
    state.lastEvent = { kind: "node" };
    normalizeNode("node", payload);
    render();
  });

  source.addEventListener("edge", (event) => {
    const payload = JSON.parse(event.data);
    state.lastEvent = { kind: "edge" };
    normalizeEdge(payload);
    render();
  });

  source.addEventListener("done", (event) => {
    const payload = JSON.parse(event.data);
    state.lastEvent = { kind: `done ${payload.kind ?? ""}`.trim() };
    render();
    source.close();
    state.source = null;
  });

  source.onerror = () => {
    if (state.source) {
      setStatus("stream error");
    }
  };
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const params = new URLSearchParams();
  params.set("seed_ids", seedIdsEl.value.trim());
  params.set("hops", hopsEl.value.trim() || "2");
  const edgeTypes = edgeTypesEl.value.trim();
  if (edgeTypes) {
    params.set("edge_types", edgeTypes);
  }
  connect(streamUrlEl.value.trim(), params);
});

disconnectButton.addEventListener("click", () => {
  disconnect();
});

render();
