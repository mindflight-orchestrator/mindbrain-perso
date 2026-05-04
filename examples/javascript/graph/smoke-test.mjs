import assert from "node:assert/strict";
import path from "node:path";
import { pathToFileURL } from "node:url";

function makeElement(tagName, id = null) {
  const listeners = new Map();
  const element = {
    tagName,
    id,
    listeners,
    children: [],
    attrs: new Map(),
    textContent: "",
    value: "",
    setAttribute(name, value) {
      this.attrs.set(name, String(value));
    },
    append(...nodes) {
      this.children.push(...nodes);
    },
    replaceChildren(...nodes) {
      this.children = [...nodes];
    },
    addEventListener(type, handler) {
      listeners.set(type, handler);
    },
    dispatchEvent(event) {
      const handler = listeners.get(event.type);
      if (handler) handler(event);
    },
    get firstElementChild() {
      return this.children.find((child) => child && typeof child === "object") ?? null;
    },
    get lastElementChild() {
      for (let i = this.children.length - 1; i >= 0; i -= 1) {
        const child = this.children[i];
        if (child && typeof child === "object") return child;
      }
      return null;
    },
  };
  return element;
}

const elements = new Map([
  ["stream-form", makeElement("form", "stream-form")],
  ["disconnect", makeElement("button", "disconnect")],
  ["status", makeElement("div", "status")],
  ["graph", makeElement("svg", "graph")],
  ["stream-url", makeElement("input", "stream-url")],
  ["seed-ids", makeElement("input", "seed-ids")],
  ["hops", makeElement("input", "hops")],
  ["edge-types", makeElement("input", "edge-types")],
]);

elements.get("stream-url").value = "http://localhost:8091/api/mindbrain/graph/subgraph";
elements.get("seed-ids").value = "1,2";
elements.get("hops").value = "2";
elements.get("edge-types").value = "requires,writes_about";
elements.get("status").textContent = "idle";

const createdSources = [];

globalThis.window = { location: { href: "http://localhost:8000/" } };
globalThis.document = {
  getElementById(id) {
    return elements.get(id) ?? null;
  },
  createElementNS(namespace, name) {
    return makeElement(name);
  },
};
globalThis.EventSource = class {
  constructor(url) {
    this.url = url;
    this.listeners = new Map();
    this.closed = false;
    createdSources.push(this);
  }
  addEventListener(type, handler) {
    this.listeners.set(type, handler);
  }
  close() {
    this.closed = true;
  }
};

await import(pathToFileURL(path.resolve(new URL(".", import.meta.url).pathname, "./app.js")).href);

assert.equal(elements.get("status").textContent, "idle");

const submit = elements.get("stream-form").listeners.get("submit");
assert.ok(submit, "submit handler registered");
submit({
  type: "submit",
  preventDefault() {},
});

assert.equal(createdSources.length, 1);
assert.equal(
  createdSources[0].url,
  "http://localhost:8091/api/mindbrain/graph/subgraph?seed_ids=1%2C2&hops=2&edge_types=requires%2Cwrites_about",
);
assert.equal(elements.get("status").textContent, "connecting");

createdSources[0].listeners.get("seed_node")?.({
  data: JSON.stringify({
    entity: {
      entity_id: 1,
      name: "Ada",
      entity_type: "person",
    },
  }),
});

assert.ok(elements.get("graph").children.length > 0);

createdSources[0].listeners.get("done")?.({
  data: JSON.stringify({
    kind: "subgraph",
    seed_count: 2,
  }),
});

assert.equal(elements.get("status").textContent, "done subgraph");
assert.equal(createdSources[0].closed, true);
