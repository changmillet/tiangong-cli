---
title: cli Validation Guide
docType: guide
scope: repo
status: active
authoritative: false
owner: cli
language: en
whenToUse:
  - when a tiangong-lca-cli change is ready for local validation
  - when deciding the minimum proof required for command, session, artifact, test, or release-gate changes
  - when writing PR validation notes for tiangong-lca-cli work
whenToUpdate:
  - when the repo gains a new canonical validation command or wrapper
  - when change categories require different minimum proof
  - when the protected-branch or coverage contract changes
checkPaths:
  - ai/validation.md
  - ai/task-router.md
  - package.json
  - bin/**
  - src/**
  - test/**
  - scripts/**
  - .github/workflows/**
lastReviewedAt: 2026-04-18
lastReviewedCommit: 8a2184bd17dd796a7f13704a085ffe538605f0fe
related:
  - ../AGENTS.md
  - ./repo.yaml
  - ./task-router.md
  - ./architecture.md
  - ../README.md
---

## Default Baseline

Unless the change is doc-only, the minimum local baseline is:

```bash
npm run lint
npm test
npm run build
```

For protected-branch parity, the authoritative full gate is:

```bash
npm run prepush:gate
```

## Validation Matrix

| Change type | Minimum local proof | Additional proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `bin/**`, `src/main.ts`, or `src/cli.ts` | `npm run lint`; `npm test`; `npm run build` | run the relevant `tiangong --help` or subcommand help path after build | Launcher and dispatch changes affect the public command surface directly. |
| session, auth, env, or remote adapter helpers under `src/lib/{dotenv,env,user-api-key,supabase-*,remote,http}*` | `npm run lint`; `npm test`; `npm run build` | run focused tests for the touched helper plus one command that exercises the changed path | Record any required live env assumptions in the PR note. |
| flow, process, lifecyclemodel, review, publish, or run command families | `npm run lint`; `npm test`; `npm run build` | run focused tests for the touched command family; run `npm run test:coverage:assert-full` if the change touched uncovered branches | Preserve the low-entropy command contract and structured artifact outputs. |
| artifact, IO, or state-lock behavior | `npm run lint`; `npm test`; `npm run build` | run one representative command path that writes the changed artifact layout, if safe | Path and file layout regressions matter for downstream automation. |
| `test/**` or coverage gate scripts | `npm run lint`; `npm test`; `npm run test:coverage`; `npm run test:coverage:assert-full` | run `npm run prepush:gate` when the change affects the protected-branch gate directly | Coverage for `src/**/*.ts` is expected to remain at `100%`. |
| `package.json`, `.nvmrc`, or `scripts/ci/**` | `npm run lint`; `npm test`; `npm run build` | run `npm run prepush:gate` when the change affects release or protected-branch checks | Release-tag checks and dependency baselines change the repo contract. |
| AI docs only | run repo-local `ai-doc-lint` against touched files or the equivalent local PR check | do one scenario-based routing check from root into this repo | Refresh review metadata even when prose-only docs change. |

## Coverage Notes

Facts that matter:

- `npm run test:coverage` is the full coverage proof
- `npm run test:coverage:assert-full` verifies the latest coverage artifact without rerunning coverage
- `npm run prepush:gate` is the exact protected-branch gate

If the task changes control flow, add or update tests instead of using coverage-ignore pragmas.

## Minimum PR Note Quality

A good PR note for this repo should say:

1. which commands ran
2. which focused tests or help paths were exercised when the change touched one command family
3. whether the full protected-branch gate was run or deferred
