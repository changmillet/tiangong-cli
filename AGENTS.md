---
title: cli AI Working Guide
docType: contract
scope: repo
status: active
authoritative: true
owner: cli
language: en
whenToUse:
  - when a task may change the public `tiangong` command surface, CLI runtime behavior, session handling, or release gating
  - when routing work from the workspace root into tiangong-lca-cli
  - when deciding whether a change belongs here, in tiangong-lca-skills, in tiangong-lca-mcp, or in a remote runtime repo
whenToUpdate:
  - when command ownership or repo boundaries change
  - when validation, packaging, or coverage rules change
  - when the repo-local AI bootstrap docs under ai/ change
checkPaths:
  - AGENTS.md
  - README.md
  - DEV_CN.md
  - docs/IMPLEMENTATION_GUIDE_CN.md
  - ai/**/*.md
  - ai/**/*.yaml
  - package.json
  - .nvmrc
  - bin/**
  - src/**
  - test/**
  - scripts/**
  - .github/workflows/**
lastReviewedAt: 2026-04-18
lastReviewedCommit: 8a2184bd17dd796a7f13704a085ffe538605f0fe
related:
  - ai/repo.yaml
  - ai/task-router.md
  - ai/validation.md
  - ai/architecture.md
  - README.md
  - docs/IMPLEMENTATION_GUIDE_CN.md
---

## Repo Contract

`tiangong-lca-cli` owns the checked-in public `tiangong` CLI contract: command nouns and verbs, launcher behavior, local artifact workflow, remote session/auth handling, and the repo-level release gate. Start here when the task may change what the CLI does or how it is validated.

## AI Load Order

Load docs in this order:

1. `AGENTS.md`
2. `ai/repo.yaml`
3. `ai/task-router.md`
4. `ai/validation.md`
5. `ai/architecture.md`
6. `README.md` only for user-facing invocation examples
7. `docs/IMPLEMENTATION_GUIDE_CN.md` only when you need deeper historical maintainer notes

Do not start with scattered subcommands or tests before you know which command family owns the task.

## Repo Ownership

This repo owns:

- `bin/tiangong.js` as the stable launcher entrypoint
- `src/cli.ts` and `src/main.ts` for command dispatch, process entry, help, and exit behavior
- `src/lib/**` for reusable CLI command logic, session handling, artifacts, and remote adapters
- `test/**` and `scripts/assert-full-coverage.ts` for the hard validation gate
- package metadata, build output contract, and tag/release checks in `package.json` and `scripts/ci/**`

This repo does not own:

- skill packaging and skill wrapper metadata
- MCP transport or inspector surfaces
- remote product or Edge Function business logic
- workspace integration state after merge

Route those tasks to:

- `tiangong-lca-skills` for skill wrappers and `SKILL.md` packages
- `tiangong-lca-mcp` for MCP transports and tool registration
- the owning runtime repo for API, schema, or product behavior
- `lca-workspace` for root integration after merge

## Runtime Facts

- Repo-local AI-doc maintenance is enforced by `.github/workflows/ai-doc-lint.yml` using the vendored `.github/scripts/ai-doc-lint.*` files.
- Package manager: `npm`
- Node baseline: `>=24 <25`
- Runtime style: TypeScript source, Node-native CLI, direct REST and Edge Function access only
- The canonical minimum validation command is `npm run lint`
- The authoritative full gate is `npm run prepush:gate`
- Coverage for `src/**/*.ts` is expected to stay at `100%` statements, branches, functions, and lines

## Hard Boundaries

- Do not add orchestration frameworks or new npm dependencies without explicit approval
- Do not move business logic into skill wrappers when the native `tiangong` CLI should own it
- Do not weaken the coverage gate with ignore pragmas; cover the branch or remove dead code
- Do not treat a merged repo PR here as workspace-delivery complete if the root repo still needs a submodule bump

## Workspace Integration

A merged PR in `tiangong-lca-cli` is repo-complete, not delivery-complete.

If the change must ship through the workspace:

1. merge the child PR into `tiangong-lca-cli`
2. update the `lca-workspace` submodule pointer deliberately
3. complete any later workspace-level validation that depends on the updated CLI snapshot
