---
title: cli Task Router
docType: router
scope: repo
status: active
authoritative: false
owner: cli
language: en
whenToUse:
  - when you already know the task belongs in tiangong-lca-cli but need the right next file or next doc
  - when deciding whether a change belongs in the launcher, one command family, session helpers, release gates, or another repo
  - when routing between CLI work and handoffs to skills, MCP, or runtime repos
whenToUpdate:
  - when new high-frequency command families appear
  - when repo boundaries or deep-doc links change
  - when validation routing becomes misleading
checkPaths:
  - AGENTS.md
  - ai/repo.yaml
  - ai/task-router.md
  - ai/validation.md
  - ai/architecture.md
  - package.json
  - bin/**
  - src/**
  - test/**
  - scripts/**
lastReviewedAt: 2026-04-18
lastReviewedCommit: 8a2184bd17dd796a7f13704a085ffe538605f0fe
related:
  - ../AGENTS.md
  - ./repo.yaml
  - ./validation.md
  - ./architecture.md
  - ../README.md
  - ../docs/IMPLEMENTATION_GUIDE_CN.md
---

## Repo Load Order

When working inside `tiangong-lca-cli`, load docs in this order:

1. `AGENTS.md`
2. `ai/repo.yaml`
3. this file
4. `ai/validation.md` or `ai/architecture.md`
5. `README.md` only for public invocation examples
6. `docs/IMPLEMENTATION_GUIDE_CN.md` only for deeper historical maintainer context

## High-Frequency Task Routing

| Task intent | First code paths to inspect | Next docs to load | Notes |
| --- | --- | --- | --- |
| Change launcher behavior, `--help`, exit codes, or top-level command routing | `bin/tiangong.js`, `src/main.ts`, `src/cli.ts` | `ai/validation.md`, `ai/architecture.md` | This is the core public command surface. |
| Change env loading, session caching, or user API key exchange | `src/lib/dotenv.ts`, `src/lib/env.ts`, `src/lib/user-api-key.ts`, `src/lib/supabase-session.ts`, `src/lib/supabase-client.ts` | `ai/validation.md`, `ai/architecture.md` | Remote auth and session behavior belong here, not in skill wrappers. |
| Change generic REST or Edge request behavior | `src/lib/http.ts`, `src/lib/remote.ts`, `src/lib/supabase-rest.ts` | `ai/validation.md`, `ai/architecture.md` | If the remote API contract itself changes, coordinate with the owning runtime repo. |
| Change flow governance, dedupe, or reviewed-data commands | `src/lib/flow-*.ts` | `ai/validation.md`, `ai/architecture.md` | Keep public command semantics and artifact outputs aligned. |
| Change process review or process build flows | `src/lib/process-*.ts`, `src/lib/review-process.ts` | `ai/validation.md`, `ai/architecture.md` | Use focused tests for the affected process command family. |
| Change lifecycle model automation or publish flows | `src/lib/lifecyclemodel-*.ts`, `src/lib/publish.ts`, `src/lib/run.ts` | `ai/validation.md`, `ai/architecture.md` | These commands often touch artifact layout and remote orchestration together. |
| Change local artifact, lockfile, or output path behavior | `src/lib/artifacts.ts`, `src/lib/io.ts`, `src/lib/state-lock.ts` | `ai/validation.md`, `ai/architecture.md` | Preserve file-first usage and deterministic output layout. |
| Change TIDAS SDK validation inside the CLI | `src/lib/tidas-sdk-package-validator.ts` | `ai/validation.md`, `ai/architecture.md` | If the SDK package contract itself changes, coordinate with `tidas-sdk`. |
| Change coverage, release tag checks, or protected-branch gates | `scripts/assert-full-coverage.ts`, `scripts/ci/**`, `package.json`, `test/**` | `ai/validation.md` | `npm run prepush:gate` remains the full protected-branch contract. |
| Add a capability that only exists today in skills wrappers | `tiangong-lca-cli`, then `tiangong-lca-skills` | root `ai/task-router.md` | Add the native CLI command first, then update the skill wrapper repo. |
| Change MCP transport or inspector behavior | `tiangong-lca-mcp`, not this repo | root `ai/task-router.md` | CLI and MCP are separate surfaces. |
| Change repo-local AI-doc maintenance only | `AGENTS.md`, `ai/**`, `.github/workflows/ai-doc-lint.yml`, `.github/scripts/ai-doc-lint.*` | `ai/validation.md` when present, otherwise `ai/repo.yaml` | Keep the repo-local maintenance gate aligned with root `ai/ci-lint-spec.md` and `ai/review-matrix.md`. |
| Decide whether work is delivery-complete after merge | root workspace docs, not repo code paths | root `AGENTS.md`, `_docs/workspace-branch-policy-contract.md` | Root integration remains a separate phase. |

## Wrong Turns To Avoid

### Implementing command semantics in skills first

If the public command does not exist yet, add it here first. Skills are wrappers, not the primary CLI truth.

### Weakening the coverage gate to land a feature

Do not bypass `npm run prepush:gate` with coverage ignores. Either test the branch or remove dead code.

### Treating remote runtime drift as a CLI-only problem

If the CLI behavior is wrong because the remote API or schema changed, coordinate with the owning runtime repo instead of inventing a local-only contract.

## Cross-Repo Handoffs

Use these handoffs when work crosses boundaries:

1. new native command needed by skills
   - start here
   - then update `tiangong-lca-skills`
2. CLI bug caused by MCP transport behavior
   - route to `tiangong-lca-mcp`
3. CLI bug caused by remote API or schema truth
   - route to the owning runtime repo
4. merged repo PR still needs to ship through the workspace
   - return to `lca-workspace`
   - do the submodule pointer bump there
