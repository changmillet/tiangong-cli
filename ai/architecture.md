---
title: cli Architecture Notes
docType: guide
scope: repo
status: active
authoritative: false
owner: cli
language: en
whenToUse:
  - when you need a compact mental model of the CLI before editing command routing, helper modules, or release gates
  - when deciding which file family owns a behavior change
  - when launcher, session, review, publish, or artifact hotspots are mentioned without exact paths
whenToUpdate:
  - when major repo paths or command families change
  - when session or artifact architecture moves
  - when coverage or release gating becomes materially different
checkPaths:
  - ai/architecture.md
  - ai/repo.yaml
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
  - ./task-router.md
  - ./validation.md
  - ../README.md
---

## Repo Shape

This repo is organized around one stable launcher plus a library-style `src/lib/**` tree that implements command families and shared helpers.

## Stable Path Map

| Path group | Role |
| --- | --- |
| `bin/tiangong.js` | stable launcher entrypoint exposed as the public `tiangong` executable |
| `src/main.ts` | process entry, dotenv loading, stdout and stderr wiring |
| `src/cli.ts` | top-level command dispatch, parsing, and help routing |
| `src/lib/**` | command-family implementations plus shared auth, IO, artifact, and remote helpers |
| `test/**` | unit and launcher tests that back the coverage gate |
| `scripts/assert-full-coverage.ts` | strict coverage enforcement |
| `scripts/ci/**` | release-tag and package publication checks |

## Current Architectural Clusters

### Launcher and entry contract

The public `tiangong` surface starts in:

- `bin/tiangong.js`
- `src/main.ts`
- `src/cli.ts`

If a task changes help output, exit behavior, or how subcommands are registered, start here.

### Session and remote access layer

The CLI talks to remote services directly through helper modules such as:

- `src/lib/env.ts`
- `src/lib/dotenv.ts`
- `src/lib/user-api-key.ts`
- `src/lib/supabase-session.ts`
- `src/lib/supabase-client.ts`
- `src/lib/supabase-rest.ts`
- `src/lib/remote.ts`
- `src/lib/http.ts`

This is where the CLI-owned remote access contract lives.

### Workflow command families

The widest feature families currently live in:

- `src/lib/flow-*.ts`
- `src/lib/review-*.ts`
- `src/lib/process-*.ts`
- `src/lib/lifecyclemodel-*.ts`
- `src/lib/publish.ts`
- `src/lib/run.ts`

These files own the public CLI semantics for those workflows.

### Artifact and filesystem behavior

Artifact materialization and local state handling cluster around:

- `src/lib/artifacts.ts`
- `src/lib/io.ts`
- `src/lib/state-lock.ts`

If a task changes output layout, locking, or local run roots, inspect these first.

## Cross-Repo Boundaries

- `tiangong-lca-skills` wraps CLI commands but does not own the native command contract
- `tiangong-lca-mcp` owns MCP transports and tool exposure, not the CLI executable
- runtime API, schema, or product behavior still belong in their owning repos
- `lca-workspace` owns root delivery completion after a child PR merges

## Common Misreads

- a skill wrapper is not the source of truth for a missing command
- the CLI should not absorb MCP transport behavior
- a merged child PR does not finish workspace delivery
