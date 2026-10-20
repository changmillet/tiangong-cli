# Stable owner draft preparation evidence

Source candidate: `d053d6dcac55f7bfa8a089344c37ee0bc9075ad9`, based on canonical `main` at `2b2eba10c68ff66d8f018b551c6c9b853a85267f`. Related: [CLI #372](https://github.com/tiangong-lca/cli/issues/372), [Database #855](https://github.com/tiangong-lca/database/issues/855), [Edge #500](https://github.com/tiangong-lca/edge-functions/issues/500).

`dataset prepare-draft` observes one typed UUID under the authenticated owner. It returns a complete draft and review census, selects the unique existing draft or an explicit reviewed conflict choice, and never guesses a successor version. Explicit preparation delegates creation and request recovery to the Database contract through Edge. An owner review in states 20–99 blocks preparation and returns its exact review identity. Lost responses and consumed attempts permit only original-request lookup.

The initial-family policy, native Model/result groups, Elementary/LCIA changes and global uniqueness for excluded Model/Elementary-only paths remain outside this slice. It does not close all of #372. The new backend must be separately deployed and qualified before production preparation can be used; the ordinary read-only command requires only existing relation reads.

## Recorded checks

| Evidence | Observed result |
| --- | --- |
| [Canonical full gate](prepush.txt) | 12 package tests; 1,839 source tests passed, 9 existing platform skips, no failures. Source statements, branches, functions and lines all 100%. |
| [Installed CLI → actual Edge → local PostgreSQL integration](local-integration.json) | One real prepare POST, one draft and one receipt despite a deliberately lost success response; two read-only lookups, no save. Reusing the original request from another output directory emitted no additional prepare. |
| Review-family integration branch | No new draft; exact review conflict returned with zero dispatch. |
| Offline binding integration | Exact assigned identity and reviewed predecessor were bound without changing the scientific projection. Semantic Flow documents were preserved and only their candidate binding changed. `save_ready=false`. |
| [Independent Spec regression execution](independent-spec-tests.txt) | Nine focused tests passed after correcting two P2 findings. Frozen draft→review transitions retain review return points; unavailable/currently excluded targets retain the validated original receipt without becoming ready. Invalid receipts remain rejected. |
| Independent Standards review | No remaining material documented-standard violations; final Spec review also reports none. These are local non-author reviews, not hosted review-bot execution. |
| [Original-workflow acceptance](original-workflow-summary.json) | Independent native live read-only zero-draft and one-draft branches passed with zero writes/attempts. A missing-target bind was rejected. A bound real Process proposal passed native four-layer and allocation validation with the candidate dependencies. |

The local integration used real authenticated Edge handlers and actual PostgreSQL command functions. Two external network-only Process AFTER webhooks were temporarily disabled in that disposable fixture and restored; admission, JSON, lifecycle and review guards remained enabled. This proves the scoped preparation/binding path, not derivative execution or scientific acceptance. Database's separate real concurrent-session and populated-upgrade proofs are in its companion change.

The original-workflow summary deliberately omits real account/dataset identifiers, bodies and private execution files. Public-source ownership and current-owner draft ownership are distinct: one qualified case reused a draft owned by the editor from a public source owned by another account, without changing either owner. The observed production sources were state 100; no state-200/commercial authority is inferred.

## Runtime binding

The isolated package retains version `0.1.28`; it is a source candidate, not a release or a default-runtime replacement.

- CLI runtime content SHA-256: `bdadf02e73076f9da5a1dd24ecbe7445469e5c71cc51017b0a44525a7f6e0ca2`
- Packed archive SHA-256: `0e28dbc2566b45d837ca608e5e124579f9bb4af624bef6c50d16d3569deac1e9`
- Node: `24.19.0`; pnpm: `11.24.0`
- Frozen dependencies include TIDAS SDK `0.5.1`, Supabase JS `2.118.0` and Zod `4.4.3`. The original maintenance installation uses Zod `4.6.7` and was left unchanged; the actual candidate combination was independently revalidated rather than assumed equivalent.

Independent qualification checked the packed file membership/bytes, all 15 production packages and 1,754 dependency files, 32 internal links, the trusted Node executable and the public runtime assertion. Subsequent public evidence-only commits leave the candidate product/runtime bytes unchanged. Final preflight receipts bind the actual submitted commit and freshly fetched base; they supplement these recorded outputs.

## Reproduction and limits

```sh
pnpm install --frozen-lockfile
pnpm build
node --import tsx --test test/dataset-prepare-draft.test.ts test/dataset-prepare-draft-bind.test.ts
pnpm prepush:gate
scripts/docpact-gate.sh --base origin/main
```

The checked-in tests contain synthetic counterexamples, including actual public-bin invocation and transport counts. Read-only original-workflow qualification does not prove a production successor exists. Positive creation, recovery and binding remain local integration evidence until the backend and a separately admitted data operation are qualified.

Offline `prepare-draft bind` preserves the typed UUID and scientific projection while changing only the actual version, permanent URI and explicitly reviewed exact published predecessor. It can preserve a valid review bound to the same scientific proposal; identity rebinding alone does not require another full scientific review. It does not infer that review passed or suppress native schema/reference/owner/state/preimage failures. All outputs remain `save_ready=false`.

[Evidence index](evidence-index.json) records the retained source revision, review outcome and file hashes. Local path redactions are stated there; no test outcome was relabeled as production adoption.
