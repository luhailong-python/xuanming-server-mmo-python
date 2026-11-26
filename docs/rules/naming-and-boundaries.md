# 命名规范、关联仓库与权限边界（Python 版）

> 对应根索引 [AGENTS.md](../../AGENTS.md)，移植自 C++/Go 服务端仓 `docs/rules/naming-and-boundaries.md`。

## 1. 命名规范与语言约定

- **第一原则（强制）**：所有命名（模块、类、函数、变量、配置字段、错误码、指标等）**必须清晰易懂且语义完全准确**，做到见名知意、无歧义；带准单位（`_ms` / `_sec` / `_count` / `_bytes`），拒绝模糊缩写、歧义布尔值、泛化名（`data` / `info` / `flag` / `temp` / `handle` / `process`）及混层命名。
- **语言约定**：AI 协作回复、注释、commit message 与文档统一用**中文**；服务端运行期日志与标准输出/错误诊断信息使用结构化**英文**。

## 2. 关联 Git 仓库与开发分支

核心关联仓库（按 GitHub 地址定位，不写死本机盘符）：
- Python 服务端（本仓库）：`https://github.com/luhailong-python/xuanming-server-mmo-python`
- C++/Go 服务端（权威契约源）：`https://github.com/luyuan-cpp/xuanming-server-mmo`
- Java 服务端：`https://github.com/luyuan-java/xuanming-server-mmo`
- Unity 客户端：`https://github.com/luhailong-cpp/mmorpg-client`
- 协议转 MySQL 工具库：`https://github.com/luyuan-cpp/proto2mysql`

开发分支：所有改动直接在 `main` 分支进行，禁止创建临时分支或工作树。

## 3. 硬禁令

- ❌ **严禁引入可能导致游戏不过审、被迫下架或广告/支付封号的任何设计、配置与文案（一票否决，负责人 2026-11-26 强制）**：禁止站内收款码/捐赠/外链切支付及无版号内购；激励视频首审默认关闭且仅发不可交易绑定银两（`3`），严禁诱导点击/支持作者文案；禁止削弱实名防沉迷、账号 7 天真注销与 UGC 内容安全。
- ❌ 禁止手改由 `tools/build_protocol.py` 自动生成的协议产物与 `src/xuanming/contracts/*`、`src/xuanming/data/*` 下的契约/配置表/导航数据；需先改权威源再重新生成。
- ❌ 禁止把 `player_id` 等高基数值作为 Prometheus 指标 label。
- ❌ 禁止未经确认私自新开后台线程（除非压测证实瓶颈并先与程序/负责人确认说明原因）。
- ❌ 禁止在仓库中写入密钥、口令或未脱敏的运行日志。
- ❌ 禁止 `--no-verify`、跳过失败测试或销毁他人工作区改动。
- ❌ 未经用户明确要求，禁止执行 `git push` / `git tag` / `git push --force` 或改写已推送历史。
