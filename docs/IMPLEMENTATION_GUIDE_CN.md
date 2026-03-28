# TianGong LCA CLI 实施指南

## 1. 目标

`tiangong-lca-cli` 是 TianGong 的统一执行面。

它解决的不是“有没有脚本”，而是：

- agent 的动作空间过大
- skills、shell、HTTP、MCP、Python 执行器的入口过碎
- 同类能力缺少统一参数风格和环境变量约定
- 质量门没有收敛到一个稳定仓库

本仓库的设计结论很明确：

- 用 TypeScript 直接实现 CLI
- 不把 MCP 作为 CLI 内部传输层
- 优先 Node 24 原生能力
- 优先文件输入、结构化 JSON 输出
- 把 `tiangong-lca-skills` 收敛成这个 CLI 的调用方，而不是并行产品面

## 2. 当前落地范围

### 2.1 已实现命令

```text
tiangong
  doctor
  search
    flow
    process
    lifecyclemodel
  admin
    embedding-run
```

对应关系：

| CLI 命令 | 当前后端能力 |
| --- | --- |
| `tiangong doctor` | 本地环境诊断、`.env` 加载、旧变量 alias 检查 |
| `tiangong search flow` | `flow_hybrid_search` |
| `tiangong search process` | `process_hybrid_search` |
| `tiangong search lifecyclemodel` | `lifecyclemodel_hybrid_search` |
| `tiangong admin embedding-run` | `embedding_ft` |

### 2.2 已经固定的工程约束

- 运行时：Node 24
- 源码：TypeScript
- 包管理：npm
- 测试：`node:test`
- 覆盖率：`c8`
- 构建产物：`dist/`
- 开发期运行器：`tsx`

这里的边界现在很明确：

- 运行时不再依赖 `tsx`
- `bin` 入口只加载 `dist/src/main.js`
- `tsx` 只保留给开发期和测试期

## 3. 目录职责

```text
tiangong-lca-cli/
  bin/
    tiangong.js
    tiangong.d.ts
  dist/
  src/
    cli.ts
    main.ts
    lib/
  test/
  scripts/
    assert-full-coverage.ts
  docs/
```

职责边界：

- `bin/`：稳定启动器，只负责把 `tiangong` 命令接到 `dist/src/main.js`
- `dist/`：构建产物，供正式运行路径使用
- `src/cli.ts`：命令分发、参数解析、命令帮助、错误出口
- `src/main.ts`：进程入口源码，构建后输出到 `dist/src/main.js`
- `src/lib/`：纯功能模块
- `test/`：单元测试和 smoke test
- `scripts/assert-full-coverage.ts`：覆盖率硬门源码，构建后输出到 `dist/scripts/`

## 4. 命令设计原则

### 4.1 不按 skill 名直接暴露命令

不推荐：

```bash
tiangong flow-hybrid-search
tiangong process-hybrid-search
tiangong embedding-ft
```

推荐：

```bash
tiangong search flow
tiangong search process
tiangong admin embedding-run
```

这能显著降低 agent 的搜索空间和误操作概率。

### 4.2 读操作偏通用，写操作必须带业务语义

这个仓库没有实现“万能 CRUD”。

原因很简单：

- 搜索是搜索
- 发布是发布
- review/build 是 workflow
- 长任务是 job

如果为了“统一”再做一个泛化 CRUD 协议，只会重新制造熵。

### 4.3 文件优先

优先形式：

```bash
tiangong search flow --input ./request.json --json
tiangong admin embedding-run --input ./jobs.json --dry-run
```

而不是长自然语言参数和不稳定的 shell 拼接。

## 5. 环境变量策略

### 5.1 统一命名

新的标准变量名：

```bash
TIANGONG_API_BASE_URL=
TIANGONG_API_KEY=
TIANGONG_REGION=us-east-1

TIANGONG_KB_BASE_URL=
TIANGONG_KB_API_KEY=

TIANGONG_MINERU_BASE_URL=
TIANGONG_MINERU_API_KEY=
```

### 5.2 迁移期 alias

CLI 当前兼容旧变量名：

- `SUPABASE_FUNCTIONS_URL -> TIANGONG_API_BASE_URL`
- `SUPABASE_FUNCTION_REGION -> TIANGONG_REGION`
- `TIANGONG_LCA_APIKEY -> TIANGONG_API_KEY`
- `TIANGONG_MINERU_WITH_IMAGE_URL -> TIANGONG_MINERU_BASE_URL`
- `TIANGONG_MINERU_WITH_IMAGE_API_KEY -> TIANGONG_MINERU_API_KEY`

规则是：

- 新代码只写新变量名
- 旧变量名只作为迁移兼容，不再扩散

## 6. 质量门

### 6.1 当前质量门

```bash
npm run typecheck
npm test
npm run test:coverage
npm run test:coverage:assert-full
npm run prepush:gate
```

### 6.2 为什么覆盖率门只卡 `src/**/*.ts`

这是一个有意设计，不是偷懒。

原因：

- `bin/tiangong.js` 是极薄启动器
- 运行时真正执行的是由 `src/**/*.ts` 编译出来的 `dist/` 产物
- 它的价值主要在 smoke test，而不是复杂业务逻辑

所以当前做法是：

- `src/**/*.ts` 必须 100% lines / branches / functions / statements
- `bin` 入口由普通测试做 smoke 保证

这比“把所有文件都硬塞进 coverage，结果统计失真”更可靠。

## 7. 与 `tiangong-lca-skills` 的关系

### 7.1 定位分工

- `tiangong-lca-cli`：统一执行面
- `tiangong-lca-skills`：agent 安装面、任务包装面

### 7.2 第一批迁移对象

最适合先迁移到统一 CLI 的，是当前的薄远程 skill：

| 当前 skill | 目标 CLI |
| --- | --- |
| `flow-hybrid-search` | `tiangong search flow` |
| `process-hybrid-search` | `tiangong search process` |
| `lifecyclemodel-hybrid-search` | `tiangong search lifecyclemodel` |
| `embedding-ft` | `tiangong admin embedding-run` |

### 7.3 暂不全量重写的对象

这类能力先不做 JS/TS 全量重写：

- `process-automated-builder`
- `lifecycleinventory-review`
- 其他重型 Python workflow

更合理的路径是：

1. 先让 CLI 成为统一入口
2. 由 CLI 调度现有本地执行器
3. 再逐步把值得平台化的环节抽成 REST 能力

## 8. 推荐的 skills 调用方式

在 workspace 内部，skill wrapper 应优先把 CLI 仓库作为相邻 repo 调用。

推荐约定：

- 默认路径：`${WORKSPACE_ROOT}/tiangong-lca-cli`
- 可覆盖路径：`TIANGONG_CLI_DIR`

调用方式优先顺序：

1. `node "${TIANGONG_CLI_DIR}/bin/tiangong.js" ...`
2. `node "${TIANGONG_CLI_DIR}/dist/src/main.js" ...`
3. `npm exec --prefix "${TIANGONG_CLI_DIR}" tiangong -- ...`

不要再在 skill 内部重复实现一套 `curl` 参数解析和环境变量规则。

## 9. 下一阶段路线

### Phase 1

- 完成当前薄远程命令
- 完成 skills 对这批命令的收敛
- 固定统一环境变量名和帮助文本

### Phase 2

- 引入 `review` / `job` / `flow` / `process` 的更多业务子命令
- 用 CLI 调度现有 Python workflow
- 统一 run-dir / artifact / manifest 输入输出格式

### Phase 3

- 把重型 workflow 中真正稳定的远程能力逐步服务化
- 继续减少 skill 仓库里的 transport logic
- 让 agent 主要理解 `tiangong` 命令树，而不是 repo 内部脚本细节

## 10. 结论

这次实施的核心不是“又做一个工具”，而是收敛执行面：

- CLI 负责统一能力抽象
- skills 负责任务包装
- REST 负责明确远程边界
- MCP 不再进入 CLI 内部

如果后续继续扩能力，也必须遵守同一条原则：

先判断它是不是稳定的业务动作，
再决定它是不是应该进入 `tiangong` 命令树。
