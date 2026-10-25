# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root, or
- **`CONTEXT-MAP.md`** at the repo root if it exists: it points at one `CONTEXT.md` per context. Read each one relevant to the topic.
- **`docs/adr/`**: read ADRs that touch the area you're about to work in. In multi-context repos, also check `src/<context>/docs/adr/` for context-scoped decisions.

If a document does not exist, continue from the available code and documentation. Add domain documentation when a concrete decision or vocabulary gap needs recording.

## File structure

Single-context repo (most repos):

```
/
├── CONTEXT.md
├── docs/adr/
│   ├── 0001-event-sourced-orders.md
│   └── 0002-postgres-for-write-model.md
└── src/
```

Multi-context repo (presence of `CONTEXT-MAP.md` at the root):

```
/
├── CONTEXT-MAP.md
├── docs/adr/                          ← system-wide decisions
└── src/
    ├── ordering/
    │   ├── CONTEXT.md
    │   └── docs/adr/                  ← context-specific decisions
    └── billing/
        ├── CONTEXT.md
        └── docs/adr/
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need is not in the glossary, check whether an existing term fits. Record a new term when it represents a real domain distinction.

The glossary records the names chosen so far, not the reasons for them: where a term strains against what it names or against the industry's word for it, propose changing the glossary rather than citing it.

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders), but worth reopening because…_

## Write ADRs for what the code cannot show

An ADR records the decision's reasons, the rejected options with what ruled each out, and when to reconsider; it does not describe the code. Write it in the pull request that builds the decision; a planned design stays in its issue. The pull request that changes a decision rewrites or deletes its ADR, and git keeps the old text. A choice confined to one place in the code is explained by a comment there instead.
