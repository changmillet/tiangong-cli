# 项目配置

本项目是 TianGong 的统一 CLI 仓库，运行时基线固定为 Node 24，源码直接使用 TypeScript。

设计原则：

- 统一入口：所有 TianGong 平台能力最终收敛到 `tiangong` 一个命令树
- 原生优先：优先使用 Node 24 原生能力，不默认引入高级包
- 直连 REST：不再以内置 MCP 作为 CLI 传输层
- 文件优先：输入优先走 JSON / JSONL / 本地文件，输出优先走结构化 JSON

当前已落地的命令：

- `tiangong doctor`
- `tiangong search flow`
- `tiangong search process`
- `tiangong search lifecyclemodel`
- `tiangong admin embedding-run`

## 安装依赖

参考 `tiangong-lca-next/DEV_CN.md`，本项目初始化命令保持一致：

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

nvm install
nvm alias default 24
nvm use

npm install

npm update && npm ci
```

## 配置文件

本项目会自动加载仓库根目录下的 `.env` 文件。

初始化：

```bash
cp .env.example .env
```

推荐优先使用新的统一环境变量名：

```bash
TIANGONG_API_BASE_URL=
TIANGONG_API_KEY=
TIANGONG_REGION=us-east-1

OPENAI_API_KEY=
OPENAI_MODEL=gpt-5

TIANGONG_KB_BASE_URL=
TIANGONG_KB_API_KEY=

TIANGONG_MINERU_BASE_URL=
TIANGONG_MINERU_API_KEY=
```

迁移期兼容旧变量名，但只作为 alias：

- `SUPABASE_FUNCTIONS_URL -> TIANGONG_API_BASE_URL`
- `SUPABASE_FUNCTION_REGION -> TIANGONG_REGION`
- `TIANGONG_LCA_APIKEY -> TIANGONG_API_KEY`
- `TIANGONG_MINERU_WITH_IMAGE_URL -> TIANGONG_MINERU_BASE_URL`
- `TIANGONG_MINERU_WITH_IMAGE_API_KEY -> TIANGONG_MINERU_API_KEY`

## 调试项目

```bash
npm start -- --help
npm start -- doctor
npm start -- doctor --json
npm start -- search flow --input ./request.json --dry-run
npm start -- admin embedding-run --input ./jobs.json --dry-run
```

## 开发模式

```bash
npm run dev -- --help
```

## 检查与测试

```bash
npm run lint
npm test
npm run test:coverage
npm run test:coverage:assert-full
npm run prepush:gate
```

说明：

- `npm test` 包含普通单元测试和 `bin` / 入口 smoke test
- `npm run test:coverage` 对 `src/**/*.ts` 执行 100% 覆盖率门
- `npm run prepush:gate` 是提交前的完整质量门

## 构建项目

当前不做额外 bundle，`build` 只执行语法校验：

```bash
npm run build
```

## 可执行入口

仓库内有两个稳定入口：

- `npm start -- ...`
- `node ./bin/tiangong.js ...`

`package.json` 也声明了 `bin.tiangong`，所以在本仓库内可直接通过 `npm exec tiangong -- ...` 调用。

## 与 skills 的联动约定

`tiangong-lca-skills` 后续不再各自维护独立 HTTP/MCP 入口，而是逐步收敛到这个 CLI。

当前建议：

- 轻量远程 skill 直接调用 `tiangong search ...` 或 `tiangong admin ...`
- 重型 Python workflow 先保留原执行器，但由 `tiangong` 统一调度
- 所有新脚本优先使用统一环境变量名，不再扩散旧变量名

## 当前目录约定

```text
tiangong-lca-cli/
  .env.example
  .nvmrc
  DEV_CN.md
  README.md
  bin/
  docs/
  scripts/
  src/
  test/
```

## 详细说明

- [docs/IMPLEMENTATION_GUIDE_CN.md](./docs/IMPLEMENTATION_GUIDE_CN.md)
