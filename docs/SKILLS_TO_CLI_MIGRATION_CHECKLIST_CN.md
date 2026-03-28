# TianGong Skills -> CLI 迁移清单

## 1. 目标结论

这份清单服务于一个明确判断：

- `tiangong-lca-cli` 应该成为唯一执行入口
- 业务执行逻辑应以 TypeScript 为主，收敛到 CLI
- `tiangong-lca-skills` 最终只保留安装面、提示面、调用约定
- Python workflow 只能视为迁移中的遗留层，不再是目标架构

换句话说，未来不应该再有“skill 自己维护一套 transport / env / orchestration / publish 逻辑”的情况。

## 2. 硬规则

- [ ] 不再新增任何 Python 业务 workflow
- [ ] 不再新增任何 skill 自带 HTTP / MCP / env parsing
- [ ] 不再新增任何基于 MCP 的 CLI 内部传输层
- [ ] 所有新能力只能先定义成 `tiangong <noun> <verb>` 命令，再实现
- [ ] `tiangong-lca-skills` 中的 wrapper 只能调用 `tiangong`，不能再直接 `curl`、直接调 MCP、直接跑业务 Python
- [ ] CLI 的 env 只能按实际已实现命令逐步增加，不预埋未来猜测接口

## 3. Repo 边界

### 3.1 `tiangong-lca-cli`

唯一执行面，负责：

- 命令树
- 参数解析
- env 合同
- REST 客户端
- 本地运行态管理
- artifact / manifest / run-id / job 输出契约
- 测试与 100% 覆盖率质量门

### 3.2 `tiangong-lca-skills`

最终只负责：

- `SKILL.md`
- 使用说明
- 示例输入
- 对 `tiangong` 的薄调用

最终不负责：

- transport
- CRUD 逻辑
- env 合同
- LLM / KB / OCR / publish 主逻辑
- 独立 workflow runtime

### 3.3 `tidas-sdk` / `tidas-tools`

继续作为库层存在，不需要把它们“并入 CLI 仓库”。

正确做法是：

- CLI 调用这些库
- 不在 skills 里重复实现一遍
- 不在 CLI 里手抄 schema / validation / export 逻辑

## 4. 当前 Skill 盘点

| Skill | 当前状态 | 当前技术形态 | 目标状态 | 优先级 |
| --- | --- | --- | --- | --- |
| `flow-hybrid-search` | 已有等价 CLI | shell wrapper，历史 token/env 兼容 | 只保留 skill 文档，调用 `tiangong search flow` | P0 |
| `process-hybrid-search` | 已有等价 CLI | shell wrapper，历史 token/env 兼容 | 只保留 skill 文档，调用 `tiangong search process` | P0 |
| `lifecyclemodel-hybrid-search` | 已有等价 CLI | shell wrapper，历史 token/env 兼容 | 只保留 skill 文档，调用 `tiangong search lifecyclemodel` | P0 |
| `embedding-ft` | 已有等价 CLI | shell wrapper | 只保留 skill 文档，调用 `tiangong admin embedding-run` | P0 |
| `process-automated-builder` | 仍是重 workflow | shell + Python + LangGraph + MCP + OpenAI + KB + MinerU | 迁成 `tiangong process ...` 主链 | P1 |
| `lifecyclemodel-automated-builder` | 仍是重 workflow | shell + Python + MCP + OpenAI | 迁成 `tiangong lifecyclemodel ...` 主链 | P1 |
| `lifecyclemodel-resulting-process-builder` | 仍是重 workflow | Python builder + 可选 MCP lookup | 迁成 `tiangong lifecyclemodel ...` 或 `tiangong process ...` 构建子命令 | P1 |
| `lifecycleinventory-review` | 仍是 review workflow | Python review script | 迁成 `tiangong review process` | P2 |
| `flow-governance-review` | 仍是治理 workflow | shell + 多个 Python helper + 可选 MCP | 迁成 `tiangong flow ...` / `tiangong review flow` | P2 |
| `lifecyclemodel-recursive-orchestrator` | 仍是 orchestrator | Python orchestrator，串联多个技能 | 迁成 CLI 编排命令 | P3 |
| `lca-publish-executor` | 仍是 publish contract layer | Python publish executor | 迁成 CLI publish / handoff 层 | P3 |

## 5. 现状证据

当前 workspace 里，重逻辑确实还留在 skills / Python 层：

- `process-automated-builder` 仍要求 standalone Python env，并显式依赖 `TIANGONG_LCA_REMOTE_*`、`OPENAI_*`、`TIANGONG_KB_REMOTE_*`、`TIANGONG_MINERU_WITH_IMAGE_*`。[`tiangong-lca-skills/process-automated-builder/SKILL.md`](../../tiangong-lca-skills/process-automated-builder/SKILL.md)
- 其运行时配置代码也仍围绕 MCP 连接组织，而不是 CLI 内部 TS 客户端。[`tiangong-lca-skills/process-automated-builder/tiangong_lca_spec/core/config.py`](../../tiangong-lca-skills/process-automated-builder/tiangong_lca_spec/core/config.py)
- `lifecyclemodel-automated-builder` 仍是 Python 脚本 + MCP/OpenAI 路径。[`tiangong-lca-skills/lifecyclemodel-automated-builder/SKILL.md`](../../tiangong-lca-skills/lifecyclemodel-automated-builder/SKILL.md)
- `skills` 仓库 README 也明确写着“thin remote skills are being migrated to the unified `tiangong` CLI”，说明迁移尚未完成。[`tiangong-lca-skills/README.md`](../../tiangong-lca-skills/README.md)

这说明：

- “应该统一到 CLI”是目标态
- “现在还有 Python / skills 逻辑”是现状债务

## 6. 分批迁移清单

## Phase 0：冻结旧世界

- [ ] 明确宣布：不再新增 Python 业务 workflow
- [ ] 明确宣布：不再新增 skill 自带 transport / env parsing
- [ ] 明确宣布：不再新增基于 MCP 的 CLI 内部能力
- [ ] 将 `tiangong-lca-skills` 中所有新需求默认路由到 CLI issue

## Phase 1：清掉薄 remote 技能

这批已经有 CLI 对应能力，应该最先清掉。

- [x] `flow-hybrid-search` wrapper 改为只调用 `tiangong search flow`
- [x] `process-hybrid-search` wrapper 改为只调用 `tiangong search process`
- [x] `lifecyclemodel-hybrid-search` wrapper 改为只调用 `tiangong search lifecyclemodel`
- [x] `embedding-ft` wrapper 改为只调用 `tiangong admin embedding-run`
- [x] 删除这些 skill 中的旧 token / env 兼容文案
- [x] 删除这些 skill 中的直接 `curl` / shell transport 逻辑
- [x] 统一 skill 调用路径变量为 `TIANGONG_LCA_CLI_DIR`

完成定义：

- [x] 调用链只剩 `skill -> tiangong`
- [x] 不再出现 `TIANGONG_API_KEY`、`TIANGONG_LCA_APIKEY`、`SUPABASE_FUNCTIONS_URL` 之类旧名
- [x] 不再出现 skill 自己解析 HTTP header / base URL

## Phase 2：先补 CLI 基础模块，再迁重 workflow

在移植重 workflow 前，CLI 需要先有可复用的 TS 基础能力。

- [x] `run` 基础模块：`run_id`、目录布局、manifest、resume 元数据
- [x] `artifacts` 模块：统一 JSON / JSONL / audit / report 输出
- [x] `state-lock` 模块：本地单写者锁
- [x] `http` / `rest-client` 模块：统一 REST 调用、重试、超时、错误格式
- [x] `llm` 模块：统一模型调用抽象，不再直接暴露 `OPENAI_*`
- [ ] `kb` 模块：统一知识库 REST 客户端
- [ ] `mineru` 模块：统一 OCR / SI 解析客户端
- [ ] `publish` 模块：统一 dry-run / commit / publish report
- [ ] `validation` 模块：把 `tidas-sdk` / `tidas-tools` 校验调用收口到 CLI

完成定义：

- [ ] 重 workflow 不再需要自己管理 env 解析
- [x] 重 workflow 不再需要自己管理 artifact 目录约定
- [ ] 后续命令只是在复用 CLI 模块，而不是复制 Python 脚本

## Phase 3：迁 `process-automated-builder`

这是最高优先级的重 workflow。

建议目标命令：

- [ ] `tiangong process auto-build`
- [ ] `tiangong process resume-build`
- [ ] `tiangong process publish-build`
- [ ] `tiangong process batch-build`

迁移内容：

- [ ] 流程编排迁到 TS
- [ ] flow search 改为直接 REST，而不是 MCP
- [ ] publish 改为直接 REST，而不是 MCP CRUD
- [ ] LLM 调用改为 CLI 自己的 provider abstraction
- [ ] KB 检索改为 CLI 自己的 KB client
- [ ] MinerU 调用改为 CLI 自己的 client
- [ ] 本地状态锁、cache、resume 逻辑迁到 CLI
- [ ] 保留 artifact 契约，不保留 Python 实现

迁移完成后应删除：

- [ ] `scripts/origin/process_from_flow_langgraph.py`
- [ ] 对 LangGraph 的硬依赖
- [ ] 对 `OPENAI_*`、`TIANGONG_LCA_REMOTE_*`、`TIANGONG_KB_REMOTE_*`、`TIANGONG_MINERU_WITH_IMAGE_*` 的依赖

## Phase 4：迁 `lifecyclemodel-automated-builder`

建议目标命令：

- [ ] `tiangong lifecyclemodel auto-build`
- [ ] `tiangong lifecyclemodel validate-build`
- [ ] `tiangong lifecyclemodel publish-build`

迁移内容：

- [ ] process discovery 改为 CLI 统一查询面
- [ ] 本地 `json_ordered` 组装改为 TS
- [ ] 本地校验改为 CLI 调用 `tidas-sdk` / `tidas-tools`
- [ ] publish 改为直接 REST
- [ ] 去掉只为 MCP insert 保留的分支

## Phase 5：迁 `lifecyclemodel-resulting-process-builder`

建议目标命令：

- [ ] `tiangong lifecyclemodel build-resulting-process`
- [ ] `tiangong lifecyclemodel publish-resulting-process`

迁移内容：

- [ ] lifecycle model 读取、拓扑解析、聚合投影改为 TS
- [ ] process catalog / local run 解析改为 CLI 模块
- [ ] 远程 process lookup 改为直接 REST 查询
- [ ] `publish-bundle.json` 生成契约保留，Python builder 删除

## Phase 6：迁 review / governance / publish

这一批偏治理和交付层，复杂度高，但也最能体现 CLI 统一入口价值。

- [ ] `lifecycleinventory-review` -> `tiangong review process`
- [ ] `flow-governance-review` -> `tiangong flow ...` / `tiangong review flow`
- [ ] `lca-publish-executor` -> `tiangong publish ...`

重点原则：

- [ ] review 输出保持本地 artifact-first
- [ ] publish 统一 dry-run / commit 语义
- [ ] 不再允许某个 skill 自带一套 publish 契约

## Phase 7：迁 orchestrator

- [ ] `lifecyclemodel-recursive-orchestrator` 迁成 CLI 编排命令
- [ ] 所有子步骤只通过 `tiangong` 子命令调用
- [ ] 不再保留 Python orchestrator 作为总入口

这一步必须放在前面重 workflow 完成后再做，否则只是把 Python 总控换个壳。

## Phase 8：删除遗留层

- [ ] 删除 skills 中的业务 Python 运行时
- [ ] 删除 skills 中的业务 shell wrapper
- [ ] 删除 skills 中的 transport/env parsing 逻辑
- [ ] 删除 skills 中的 MCP-only 实现
- [ ] 删除所有旧 env 名文档
- [ ] 删除对 `TIANGONG_CLI_DIR` 旧变量名的依赖

最终 `skills` 仓库应只剩：

- [ ] `SKILL.md`
- [ ] 示例 request / assets
- [ ] 对 `tiangong` 的单行调用

## 7. Env 收敛清单

### 7.1 已经落地

- [x] `TIANGONG_LCA_API_BASE_URL`
- [x] `TIANGONG_LCA_API_KEY`
- [x] `TIANGONG_LCA_REGION`

### 7.2 后续若 CLI 真正实现对应模块，再增加

- [x] `TIANGONG_LCA_LLM_API_KEY`
- [x] `TIANGONG_LCA_LLM_MODEL`
- [x] `TIANGONG_LCA_LLM_BASE_URL`
- [ ] `TIANGONG_LCA_KB_API_BASE_URL`
- [ ] `TIANGONG_LCA_KB_API_KEY`
- [ ] `TIANGONG_LCA_KB_DATASET_ID`
- [ ] `TIANGONG_LCA_MINERU_API_BASE_URL`
- [ ] `TIANGONG_LCA_MINERU_API_KEY`
- [ ] `TIANGONG_LCA_CLI_DIR`

### 7.3 应彻底淘汰

- [ ] `TIANGONG_API_BASE_URL`
- [ ] `TIANGONG_API_KEY`
- [ ] `TIANGONG_REGION`
- [ ] `TIANGONG_LCA_APIKEY`
- [ ] `SUPABASE_FUNCTIONS_URL`
- [ ] `SUPABASE_FUNCTION_REGION`
- [ ] `OPENAI_*`
- [ ] `TIANGONG_KB_*`（旧直连命名）
- [ ] `TIANGONG_LCA_REMOTE_*`
- [ ] `TIANGONG_KB_REMOTE_*`
- [ ] `TIANGONG_MINERU_WITH_IMAGE_*`

## 8. 每个 Skill 的完成定义

一个 skill 只有在满足下面条件后，才算迁移完成：

- [ ] skill 不再直接执行业务 Python
- [ ] skill 不再直接访问 REST / MCP
- [ ] skill 不再解析 env
- [ ] skill 不再持有独立 publish 逻辑
- [ ] skill 调用统一 `tiangong` 命令
- [ ] 对应 CLI 子命令有测试
- [ ] 对应 CLI 子命令有文档
- [ ] 对应 CLI 子命令纳入 `npm run prepush:gate`

## 9. 建议执行顺序

推荐严格按这个顺序推进：

1. 先清掉薄 remote skill
2. 再补 CLI 基础模块
3. 再迁 `process-automated-builder`
4. 再迁 `lifecyclemodel-automated-builder`
5. 再迁 `lifecyclemodel-resulting-process-builder`
6. 再迁 review / governance / publish
7. 最后迁 orchestrator
8. 全量删除 Python 遗留层

不建议的顺序：

- 先迁 orchestrator
- 先保留 Python 主逻辑，只包一层 TS 壳
- 先统一文档，不统一执行面

## 10. 一句话标准

判断迁移是否走在正确方向上，只问一句：

> 一个 agent 要完成工作时，是否只需要知道 `tiangong` 命令树，而不需要知道 skills 内部 shell、Python、MCP、OpenAI、KB、MinerU 的实现细节？

如果答案还是“否”，说明迁移还没完成。
