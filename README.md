# TianGong LCA CLI

`tiangong-lca-cli` is the unified TianGong command-line entrypoint.

Current implementation choices:

- TypeScript on Node 24
- ship built JavaScript artifacts from `dist/`
- direct REST / Edge Function calls instead of MCP
- file-first input and JSON-first output
- one stable command surface for humans, agents, CI, and skills
- zero npm production runtime dependencies

## Implemented commands

- `tiangong doctor`
- `tiangong search flow`
- `tiangong search process`
- `tiangong search lifecyclemodel`
- `tiangong admin embedding-run`

The stable launcher is `bin/tiangong.js`. It loads the compiled runtime at `dist/src/main.js`, while `npm start -- ...` rebuilds and dogfoods the same launcher path.

## Quality gate

The repository enforces:

- `npm run typecheck`
- `npm test`
- `npm run test:coverage`
- `npm run test:coverage:assert-full`
- `npm run prepush:gate`

Coverage is enforced at 100% for `src/**/*.ts`. Launcher smoke tests remain in the normal test suite.

## Quick start

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

nvm install
nvm alias default 24
nvm use

npm install

npm update && npm ci
```

Create `.env`:

```bash
cp .env.example .env
```

Run the CLI:

```bash
npm start -- --help
npm start -- doctor
npm start -- doctor --json
npm start -- search flow --input ./request.json --dry-run
npm start -- admin embedding-run --input ./jobs.json --dry-run
```

Run the built artifact directly:

```bash
node ./bin/tiangong.js doctor
node ./dist/src/main.js doctor --json
```

## Workspace usage

`tiangong-lca-skills` should converge on this CLI instead of keeping separate transport scripts. The current migration strategy is:

- thin remote wrappers move first
- heavier Python workflows stay in place temporarily
- future skill execution should call `tiangong` as the stable entrypoint

## Docs

- Chinese setup guide: [DEV_CN.md](./DEV_CN.md)
- Detailed implementation guide: [docs/IMPLEMENTATION_GUIDE_CN.md](./docs/IMPLEMENTATION_GUIDE_CN.md)
