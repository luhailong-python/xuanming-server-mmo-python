# xuanming-server-mmo-python 协作守则与门禁命令

> 本文件是本仓库对 AI Agent（Claude Code / Codex / Gemini / Cursor / Copilot 等）和人类开发者的**唯一规范总入口**。
> `CLAUDE.md` 与 `.github/copilot-instructions.md` 只通过 `@AGENTS.md` 单行导入本文件，不在多处重复维护规则；分章细则见 `docs/rules/`。

## 会话启动门禁

0. **新电脑 / 新 AI Agent 环境必先安装必装 Skills**（`mattpocock/skills`、`DietrichGebert/ponytail` 及精选 ≥2 万 Star 代码工程 Skills，共 8 仓 618 项），清单与一键复原脚本见 → [docs/rules/required-skills.md](docs/rules/required-skills.md)
1. 阅读 `AGENTS.md` 及本次任务涉及的 `docs/rules/*.md` 分章细则；
2. 执行 `git status -sb` 与 `git log -20 --oneline`，确认既有未提交改动后再动手。

## 规范分章索引（`docs/rules/`）

| 领域 | 核心约束摘要 | 完整规范文档 |
|---|---|---|
| 必装 Skills | 新电脑 / 新 Agent 必装 8 仓 618 个 Skills（含一键脚本与逐项明细） | [docs/rules/required-skills.md](docs/rules/required-skills.md) |
| 工程设计与编码原则 | SOLID、KISS/YAGNI、**系统/模块间 Actor 消息通信（不可变消息、禁过度拆内部函数）**、**23 种设计模式主动采用并说明原因**、**高 Star 开源库（门槛按语言与领域生态浮动）交程序决定并说明原因**、实例与布局分层 | [docs/rules/engineering-principles.md](docs/rules/engineering-principles.md) |
| 位图（Bit）与删表 ID | 纯布尔完成/领取状态优先 bit 存储、领取归属业务表、删表 ID 策划与程序红线 | [docs/rules/bit-storage-and-table-ids.md](docs/rules/bit-storage-and-table-ids.md) |
| 命名与权限边界 | 命名清晰准确带单位、关联 GitHub 仓库清单、硬禁令 | [docs/rules/naming-and-boundaries.md](docs/rules/naming-and-boundaries.md) |
| 运行模式与启动规范 | 平替 C++ 服务端定位、Windows 单进程一键启动、PB 直转 ProtoJSON 存档、条件路由严禁静默降级、多节点并行启动器 | [docs/rules/runtime-modes-and-startup.md](docs/rules/runtime-modes-and-startup.md) |
| AI 协作与模型档位 | 分工约定、无证据禁称通过、强制高低档模型切换、设计方案强制落盘 `.md` | [docs/rules/ai-workflow.md](docs/rules/ai-workflow.md) |

## 1. 开发分支与通用工程原则

- **只在 `main` 分支开发**：所有改动直接在 `main` 分支进行，禁止创建临时分支或工作树。
- **未上线阶段前提**：游戏尚未上线，玩家数据与库表结构可完全清空重构；按最标准、最合理的设计直接改，不做冗余历史兼容层。
- **架构与质量要求**（完整细则见 `docs/rules/`）：
  - 保证**数据强一致性**与边界校验，不留“等压测/等以后再修”的已知隐患，发现问题当场修完。
  - 代码可读易懂、命名清晰准确、**可水平扩展**，严格遵守**单一职责（SRP）**等软件工程原则；适合用 23 种经典设计模式（策略、状态、工厂、责任链、观察者等）简化与解耦的场景主动采用并向程序说明原因。
  - **系统 / 模块间消息通信（Actor 隔离）**：跨系统、跨协程队列、跨进程交互一律通过**不可变消息**（Mailbox / 事件 / RPC / Kafka）通信，各模块独占状态、禁止传递可变对象引用或借全局单例绕过通道；仅限跨模块/跨边界使用，单个模块内部保持正常直接方法调用，严禁把内部普通函数过度拆成消息（详见 [docs/rules/engineering-principles.md](docs/rules/engineering-principles.md)）。
  - 某功能有成熟高 Star 开源库可用时（Star 门槛按语言与细分领域生态体量浮动，不机械死卡固定阈值），交由程序决定是否引入并说明原因。
  - 纯布尔完成/领取状态优先用 bit 位图存储，严格遵守删表 ID 红线（见 [docs/rules/bit-storage-and-table-ids.md](docs/rules/bit-storage-and-table-ids.md)）。
- **分工约定**：只聚焦服务端代码与单测实现时，协议生成、全量编译与冒烟可交由 Gemini 执行；**Codex 仅负责作图（UI / 美术素材 / 视觉精修）**，不参与编码、分析、编译、协议生成、测试、压测与运维收尾（交接时写明命令与验证范围，模型档位见 [docs/rules/ai-workflow.md](docs/rules/ai-workflow.md)）。

## 2. 门禁命令

```powershell
# 1. 安装/更新依赖（建议使用项目 .venv）
python -m pip install -e ".[dev]"

# 2. 协议生成
python tools/build_protocol.py

# 3. 单元测试（全量 / 单文件）
pytest
pytest tests/test_battle.py
```

- 没有实际运行证据时，不得声称“测试通过”或“协议已对齐”。

## 3. 禁止改动目录与事项

- ❌ 禁止手改由 `tools/build_protocol.py` 自动生成的协议产物与 `src/xuanming/contracts/*`、`src/xuanming/data/*` 下的契约/配置表/导航数据；需先改权威源再重新生成。
- ❌ 禁止在仓库中写入密钥、口令或未脱敏的运行日志。
- ❌ 未经用户明确要求，禁止执行 `git push` / `git tag` / `git push --force` 或改写已推送历史。

## 4. 对照 C++ 仓增量移植流程

当用户在本仓库触发「继续移植」等增量同步指令时，固定按以下闭环执行（只读对照 `../xuanming-server-mmo-cpp`，严禁修改 C++ 仓）：

1. 读取 [docs/design/python-full-service-port-plan.md](docs/design/python-full-service-port-plan.md) 末尾记录的已校对 C++ 基线 Commit SHA（当前为 `0f9bc81d68f0af285497d6ffc5cea8a87c5c5fc9`）；
2. 在 C++ 仓执行 `git log --oneline <baseline>..HEAD`，面向客户端可无缝平替 C++ 服务端的目标，对全协议、全配置表、全微服务及核心玩法/战斗逻辑（含传输/协议、存储/锁/围栏、鉴权/安全、停机疏散/故障切换、弹性加固、玩法与战斗结算逻辑及运维/离线工具）逐项纯 Python 增量对齐，确保契约与行为与 C++ 仓一致；
3. 跑完回归测试并验证通过后，将最新已校对的 C++ `HEAD` Commit SHA 同步更新到本节、[docs/rules/runtime-modes-and-startup.md](docs/rules/runtime-modes-and-startup.md)、[README.md](README.md) 与 [docs/design/python-full-service-port-plan.md](docs/design/python-full-service-port-plan.md)，供下一次增量移植衔接。
