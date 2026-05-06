# TianGong LCA CLI

Package: `@tiangong-lca/cli` Executable: `tiangong` Node: `24.x`

## Run

One-off published run:

```bash
npm exec --yes --package=@tiangong-lca/cli@latest -- tiangong --help
npm exec --yes --package=@tiangong-lca/cli@latest -- tiangong doctor
npm exec --yes --package=@tiangong-lca/cli@latest -- tiangong flow --help
```

Install the published CLI:

```bash
npm install --global @tiangong-lca/cli
tiangong --help
tiangong doctor
tiangong flow --help
```

Run from this repository:

```bash
npm ci
npm run build
node ./bin/tiangong.js --help
```

## Env

Remote commands require:

```bash
TIANGONG_LCA_API_BASE_URL=
TIANGONG_LCA_API_KEY=
TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY=
TIANGONG_LCA_REGION=us-east-1
```

Notes:

- `TIANGONG_LCA_API_BASE_URL` accepts the project root, `/functions/v1`, or `/rest/v1`.
- `TIANGONG_LCA_API_KEY` is the TianGong user API key from the account page, not a Supabase project key.
- The CLI exchanges `TIANGONG_LCA_API_KEY` for a user session, then reuses the access token for both Edge Functions and direct Supabase access.

Optional session control:

```bash
TIANGONG_LCA_SESSION_FILE=
TIANGONG_LCA_DISABLE_SESSION_CACHE=false
TIANGONG_LCA_FORCE_REAUTH=false
```

Optional LLM review env, only for `review process --enable-llm` or `review flow --enable-llm`:

```bash
TIANGONG_LCA_REVIEW_LLM_BASE_URL=
TIANGONG_LCA_REVIEW_LLM_API_KEY=
TIANGONG_LCA_REVIEW_LLM_MODEL=
```

## Search

Minimal `search flow` request:

```json
{
  "query": "soda lime glass",
  "filter": {
    "flowType": "Product flow"
  }
}
```

Run:

```bash
tiangong search flow --input ./search-flow.request.json --json
tiangong search process --input ./search-process.request.json --json
tiangong search lifecyclemodel --input ./search-lifecyclemodel.request.json --json
```

Empty search results should be treated as empty whether the response is `[]` or `{"data":[]}`.

## Read

```bash
tiangong flow get --id <flow-id> --version <version> --json
tiangong flow list --id <flow-id> --state-code 100 --limit 20 --json
tiangong process get --id <process-id> --version <version> --json
tiangong process list --state-code 100 --limit 20 --json
```

## Real DB Flow Review

1. Search or otherwise collect exact flow refs.
2. Materialize DB rows into local review input.
3. Review the materialized rows.
4. Materialize approved decisions into downstream artifacts.

`flow fetch-rows` input:

```json
[
  {
    "id": "7a285e9a-a9f6-4b86-ab17-6ea17367400c",
    "version": "01.01.001",
    "state_code": 100,
    "cluster_id": "cluster-0001",
    "source": "search-flow"
  }
]
```

`flow materialize-decisions` input:

```json
[
  {
    "cluster_id": "cluster-0001",
    "decision": "merge_keep_one",
    "canonical_flow": {
      "id": "7a285e9a-a9f6-4b86-ab17-6ea17367400c",
      "version": "01.01.001"
    },
    "flow_refs": [
      "7a285e9a-a9f6-4b86-ab17-6ea17367400c@01.01.001",
      "017acdd0-7fd7-44cb-a410-1d559e59c506@01.01.001"
    ],
    "reason": "approved_same_product_flow"
  }
]
```

Run:

```bash
tiangong flow fetch-rows \
  --refs-file ./flow-refs.json \
  --out-dir ./flow-fetch

tiangong review flow \
  --rows-file ./flow-fetch/review-input-rows.jsonl \
  --out-dir ./flow-review

tiangong flow materialize-decisions \
  --decision-file ./approved-decisions.json \
  --flow-rows-file ./flow-fetch/review-input-rows.jsonl \
  --out-dir ./flow-decisions
```

Key `flow fetch-rows` outputs:

- `review-input-rows.jsonl`
- `fetch-summary.json`
- `missing-flow-refs.jsonl`
- `ambiguous-flow-refs.jsonl`

Key `flow materialize-decisions` outputs:

- `flow-dedup-canonical-map.json`
- `flow-dedup-rewrite-plan.json`
- `manual-semantic-merge-seed.current.json`
- `decision-summary.json`
- `blocked-clusters.json`

## Other Common Commands

```bash
tiangong process auto-build --input ./examples/process-auto-build.request.json --out-dir /abs/path/to/process-run --json
tiangong process resume-build --run-dir /abs/path/to/process-run --json
tiangong process publish-build --run-dir /abs/path/to/process-run --json
tiangong process batch-build --input ./examples/process-batch-build.request.json --out-dir /abs/path/to/process-batch --json
tiangong lifecyclemodel auto-build --input ./examples/lifecyclemodel-auto-build.request.json --out-dir /abs/path/to/lifecyclemodel-run --json
tiangong lifecyclemodel validate-build --run-dir /abs/path/to/lifecyclemodel-run --json
tiangong lifecyclemodel publish-build --run-dir /abs/path/to/lifecyclemodel-run --json
tiangong lifecyclemodel orchestrate plan --input ./lifecyclemodel-orchestrate.request.json --out-dir /abs/path/to/lifecyclemodel-recursive-run --json
tiangong review process --rows-file ./process-list-report.json --out-dir ./review
tiangong review process --run-root /abs/path/to/process-run --run-id <run_id> --out-dir ./review
tiangong process save-draft --input ./patched-processes.jsonl --out-dir /abs/path/to/process-save-draft --dry-run --json
tiangong process save-draft --input ./patched-processes.jsonl --out-dir /abs/path/to/process-save-draft --commit --json
tiangong publish run --input ./publish-request.json --dry-run
tiangong doctor --json
```

For `publish run`, relative `out_dir` values from either the request body or `--out-dir` are resolved against the request file directory, not the shell `cwd`. Use an absolute path when you want a fixed destination independent of the request file location.

For `review process`, `--rows-file` accepts either raw process rows as JSON/JSONL or the full JSON report emitted by `tiangong process list --json`, as long as it contains a `rows` array.

For `process save-draft`, canonical process payloads are validated locally with `ProcessSchema` before any `--commit` write. Schema-invalid rows remain in `outputs/save-draft-rpc/failures.jsonl` instead of being persisted.

## More Docs

- `docs/IMPLEMENTATION_GUIDE_CN.md`: maintainer-facing command contract and implementation notes
- `--help`: the canonical command surface for `tiangong`, `tiangong flow`, `tiangong review`, `tiangong process`, `tiangong lifecyclemodel`, and `tiangong publish`
- `tiangong-lca-skills`: use the skill-specific `SKILL.md` and wrapper docs for agent workflows; the CLI README only covers the public invocation contract

## Help

```bash
tiangong --help
tiangong flow --help
tiangong review --help
tiangong process --help
tiangong lifecyclemodel --help
tiangong publish --help
```
