"use strict";

// Fleet Commander — status UI.
// Renders from the data embedded by the Jinja template (so the page shows the
// fleet immediately) and then polls /api/status to stay current. Manual-override
// controls appear only when the backend reports overrides are enabled.

const POLL_MS = 5000;

// Read the initial payload from the non-executable JSON data block the template
// renders. A data block (not an inline script) keeps the page compatible with a
// strict CSP that forbids inline script execution.
function bootstrapData() {
  const node = document.getElementById("fc-bootstrap");
  if (!node) return null;
  try {
    return JSON.parse(node.textContent);
  } catch (err) {
    return null;
  }
}

function el(tag, attrs, children) {
  const node = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") node.className = v;
      else if (k === "text") node.textContent = v;
      else node.setAttribute(k, v);
    }
  }
  for (const child of children || []) {
    node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return node;
}

function stateClass(value) {
  if (!value) return "unknown";
  const v = value.toLowerCase();
  if (v === "alive" || v === "healthy") return v;
  if (v.startsWith("dead") || v === "unhealthy") return v.startsWith("dead") ? "dead" : "unhealthy";
  if (v === "starting") return "starting";
  return "unknown";
}

function fmtBps(bps) {
  if (bps == null) return "—";
  const mbps = (bps * 8) / 1e6;
  return mbps.toFixed(1) + " Mbps";
}

function fmtPct(v) {
  return v == null ? "—" : v.toFixed(0) + "%";
}

function overrideSecret() {
  const input = document.getElementById("override-secret");
  return input ? input.value : "";
}

async function postOverride(path, body) {
  const resp = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-FC-Override-Secret": overrideSecret() },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    alert("Override failed: " + resp.status);
  }
  await refresh();
}

function renderConnectorRow(rn, c, overridesEnabled) {
  const flags = [];
  if (c.janus_locked) flags.push(el("span", { class: "flag", text: "janus" }));
  if (c.cordoned) flags.push(el("span", { class: "flag", text: "cordoned" }));

  const nameCell = el("td", null, [c.name || c.connector_id || "—", ...flags]);
  const tg = el("td", null, [
    el("span", { class: "state " + stateClass(c.twingate_state), text: c.twingate_state || "unknown" }),
  ]);
  const health = el("td", null, [
    el("span", { class: "state " + stateClass(c.docker_health), text: c.docker_health || "unknown" }),
  ]);
  const cells = [
    nameCell,
    tg,
    health,
    el("td", { text: fmtPct(c.cpu_pct_norm) }),
    el("td", { text: fmtBps(c.throughput_bps) }),
  ];
  if (overridesEnabled) {
    const btn = el("button", {
      "data-testid": "cordon-" + c.connector_id,
      text: c.cordoned ? "Uncordon" : "Cordon",
    });
    btn.addEventListener("click", () =>
      postOverride("/api/overrides/cordon", { connector_id: c.connector_id, cordoned: !c.cordoned })
    );
    cells.push(el("td", null, [btn]));
  }
  return el("tr", { class: "connectors", "data-testid": "connector-" + c.connector_id }, cells);
}

function renderRn(rn, overridesEnabled) {
  const atFloor = rn.count <= rn.min_connectors;
  const count = el("span", {
    class: "rn-count" + (atFloor ? " at-floor" : ""),
    "data-testid": "count-" + rn.rn_id,
    text: `${rn.count} / min ${rn.min_connectors} · max ${rn.max_connectors}`,
  });

  const headChildren = [el("h3", { text: rn.name }), count];
  if (overridesEnabled) {
    const up = el("button", { "data-testid": "scale-up-" + rn.rn_id, text: "+1" });
    up.addEventListener("click", () =>
      postOverride("/api/overrides/scale", { rn_id: rn.rn_id, direction: "up" })
    );
    const down = el("button", { "data-testid": "scale-down-" + rn.rn_id, text: "−1" });
    down.addEventListener("click", () =>
      postOverride("/api/overrides/scale", { rn_id: rn.rn_id, direction: "down" })
    );
    headChildren.push(el("div", { class: "rn-actions" }, [up, down]));
  }

  const head = el("div", { class: "rn-head" }, headChildren);

  const headerRow = el("tr", null, [
    el("th", { text: "connector" }),
    el("th", { text: "twingate" }),
    el("th", { text: "docker" }),
    el("th", { text: "cpu" }),
    el("th", { text: "throughput" }),
    ...(overridesEnabled ? [el("th", { text: "" })] : []),
  ]);
  const tbody = el("tbody", null, rn.connectors.map((c) => renderConnectorRow(rn, c, overridesEnabled)));
  const table = el("table", { class: "data" }, [el("thead", null, [headerRow]), tbody]);

  return el("div", { class: "rn", "data-testid": "rn-" + rn.rn_id }, [head, table]);
}

function render(data) {
  const overridesEnabled = !!data.overrides_enabled;

  document.getElementById("overrides-panel").classList.toggle("hidden", !overridesEnabled);

  const snapshot = data.snapshot;
  const fleet = document.getElementById("fleet");
  fleet.innerHTML = "";
  const noData = document.getElementById("no-data");

  if (!snapshot) {
    noData.classList.remove("hidden");
  } else {
    noData.classList.add("hidden");
    document.getElementById("cycle-id").textContent = "cycle: " + snapshot.cycle_id;
    document.getElementById("updated").textContent = "updated: " + snapshot.ts;
    for (const rn of snapshot.remote_networks) {
      fleet.appendChild(renderRn(rn, overridesEnabled));
    }
  }

  const actions = document.getElementById("actions-body");
  actions.innerHTML = "";
  for (const a of data.actions || []) {
    actions.appendChild(
      el("tr", null, [
        el("td", { text: a.ts }),
        el("td", { text: a.rn_id }),
        el("td", { text: a.action }),
        el("td", { text: a.outcome }),
        el("td", { text: a.actor }),
        el("td", { text: a.reason }),
      ])
    );
  }

  const events = document.getElementById("events-body");
  events.innerHTML = "";
  for (const e of data.events || []) {
    events.appendChild(
      el("li", null, [
        el("span", { class: "ev-ts", text: e.ts || "" }),
        el("span", { class: "ev-name", text: e.event || "" }),
      ])
    );
  }

  document.getElementById("config").textContent = JSON.stringify(data.config || {}, null, 2);
}

async function refresh() {
  try {
    const resp = await fetch("/api/status", { headers: { Accept: "application/json" } });
    if (resp.ok) render(await resp.json());
  } catch (err) {
    /* transient; keep the last render */
  }
}

document.addEventListener("DOMContentLoaded", () => {
  const initial = bootstrapData();
  if (initial) render(initial);
  setInterval(refresh, POLL_MS);
});
