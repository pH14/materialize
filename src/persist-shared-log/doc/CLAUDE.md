## Code style

- Delete unused code outright — no "legacy" stubs, no removed-but-kept functions, no comments explaining what used to exist.
- Do not write comments that describe what was removed or how things previously worked.

## Doc style

These docs are plain reference documentation. Match the voice of `reference/05_horizontal_sharding.md`: short declarative sentences, present tense, minimal emphasis. When editing or adding content:

- No em-dash connectors like `**X** -- description`. Use a colon or a plain sentence.
- No bolded "reveal" sentences like `**The most important boundary is this:**` or `**The acceptor knows nothing about the metashard.**`. State it as a normal sentence under the relevant heading.
- No "it is not X; it is Y" / negation-heavy constructions. Say what the thing is.
- No stylistic "intentionally" for emphasis (e.g. "intentionally metashard-blind"). Describe what the code does, not that it does it on purpose.
- Do keep: backticks around types, traits, RPCs, and constants; mermaid sequence diagrams where they clarify a protocol; ASCII dependency/relationship blocks; bolded property identifiers like `**L4. Differential log semantics.**` in `02_invariants.md`.
- Describe the implementation that exists. Speculation and "planned" hedging belong only in sections explicitly marked as such.

## Reference docs

The `reference/` directory contains the architectural design docs for the persist shared log. Read these when planning or building:

- `00_overview.md` — architecture overview, system decomposition, core insight
- `01_protocol.md` — protocol specification, data model, write/read paths, pseudocode
- `02_invariants.md` — safety, liveness, and performance properties
- `03_testing.md` — verification strategy (Stateright, DST, stress testing)
- `04_virtual_log.md` — virtual log and write scaling (Delos-style)
- `05_horizontal_sharding.md` — implemented horizontal sharding design: `MetaState`, leader fencing, `plan_reconfiguration` + `reconcile`, range-overlap predecessors, acceptor-written batch 1/batch 2 setup, and routing commit only after `start_state=None`
