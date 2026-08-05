// Generic client-side renderer for .defs/ TOML files. Copyable across repos.
//
// Usage in index.html:
//   <tbody data-defs-file="terms.toml" data-defs-kind="terms"></tbody>
//   <tbody data-defs-file="contracts.toml" data-defs-kind="contracts"></tbody>
//   <script type="module" src="defs-render.js"></script>
//
// Requires serving over HTTP (fetch of local TOML fails on file://).

import { parse } from "./smol-toml.js";

function el(tag, className, children) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  for (const child of children ?? []) {
    node.append(child);
  }
  return node;
}

function code(text) {
  return el("code", null, [text]);
}

function termRow(term) {
  const definition = el("td", null, [term.definition.trim()]);
  if (term.exported_symbols?.length) {
    const symbols = term.exported_symbols.flatMap((s, i) =>
      i === 0 ? [code(s)] : [", ", code(s)],
    );
    definition.append(el("div", "defs-symbols", symbols));
  }
  const name = el("dfn", "term-name", [term.name]);
  name.id = `term-${term.name.replaceAll(" ", "-")}`;
  return el("tr", null, [el("td", null, [name]), definition]);
}

function contractRow(contract) {
  const title = el("td", null, [
    el("span", "term-name", [contract.title]),
    el("div", "defs-date", [contract.date]),
  ]);
  const body = el("td", null, [
    el("p", "must", [contract.statement.trim()]),
    el("p", "defs-rationale", [contract.rationale.trim()]),
  ]);
  if (contract.check) {
    body.append(el("p", "defs-check", ["Check: ", code(contract.check)]));
  }
  return el("tr", null, [title, body]);
}

const ROW_BUILDERS = { terms: termRow, contracts: contractRow };

async function fillSlot(slot) {
  const { defsFile, defsKind } = slot.dataset;
  const build = ROW_BUILDERS[defsKind];
  try {
    const response = await fetch(defsFile);
    if (!response.ok) throw new Error(`${defsFile}: HTTP ${response.status}`);
    const entries = parse(await response.text())[defsKind] ?? [];
    slot.replaceChildren(...entries.map(build));
  } catch (error) {
    slot.replaceChildren(
      el("tr", null, [el("td", "defs-error", [`Failed to load ${defsFile}: ${error.message}`])]),
    );
  }
}

for (const slot of document.querySelectorAll("[data-defs-file]")) {
  fillSlot(slot);
}
