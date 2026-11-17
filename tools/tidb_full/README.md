# 隔离 TiDB 全链联调（Windows）

本工具在随机 Docker Compose 项目中启动独立 TiDB、TiKV 和 PD，并在该数据库上运行 Python 门户、Gate、Scene、Battle 与原生 Go Login、DataService、PlayerStore、Match、PlayerInbox、Router 和 SceneManager 的完整验收。覆盖真实登录分票、两个账号的申请/邀请组队、棋盘、跨区、战斗与数据库回读。TiDB 的本地资格探针验证仅有 SELECT 权限的账号不能执行 UPDATE，再在 `information_schema.DATA_LOCK_WAITS` 观察悲观事务的 `SELECT ... FOR UPDATE` 阻塞其他写入；writer grant 绑定本次探针结果并使用 `SELECT_ONLY`。

探针从准备好的干净源码提取 `writer_slot` 的权威 DDL 并记录 SHA-256，覆盖 64 个槽位、`SELECT_ONLY` 和列级 `LOCK_TOUCH` 两种原语，以及生产使用的 READ COMMITTED 和额外的 REPEATABLE READ 对照；同时验证无关列更新被拒绝和锁触碰回滚。每个竞争写都必须在 TiDB 锁等待视图中实际出现，不能仅以等待时间推断。

这是单机隔离联调，不是部署环境的生产 I3 锁资格认证。Go 业务服务按测试模式运行，数据库后端使用真实 TiDB/TiKV；商城生产模式测试见 `tools/tidb_interop/`。不要将测试资格文件用于任何其他部署。

## 前置条件

- Windows、Docker Desktop、可用 Docker Compose，及项目 Python 环境 `.[cluster,dev]`。
- 先按 [MySQL 整栈工具](../interop/README.md) 运行 `tools/interop/prepare.py`，得到来自单个已提交 C++/Go 版本且带 SHA-256 清单的原生二进制和源码快照。
- 本地 etcd、Redis、MySQL 兼容命令行客户端、Kafka 与其 Java 运行时、Go 及原生写者校验所需的可信 helper。Python 门户和游戏节点不依赖 Java；Kafka 使用 Java。
- Docker 能获取 `tools/tidb_interop/run.py` 中固定摘要的 TiDB、TiKV 和 PD 镜像，并有足够内存。默认 Docker 地址池耗尽时用 `--subnet` 指定未被其他网络占用的私有 IPv4 子网。

```powershell
.venv/Scripts/python.exe tools/tidb_full/run.py `
  --prepared 'D:/path/to/prepared-native-snapshot' `
  --work-dir 'D:/path/to/new-disposable-work-directory' `
  --go 'D:/path/to/go.exe' --etcd 'D:/path/to/etcd.exe' `
  --redis 'D:/path/to/redis-server.exe' --mysql 'D:/path/to/mysql' `
  --kafka-java 'D:/path/to/java.exe' --kafka-libs 'D:/path/to/kafka/libs' `
  --private-helper 'D:/path/to/trusted/private_control_file.exe' `
  --subnet '10.248.174.0/24'
```

`--mysql` 只需要该目录内的 `bin/mysql.exe`，不会启动 MySQL。`--work-dir` 必须不存在。执行前会核对准备目录的源码树和所有二进制摘要；每次 SQL 命令会验证 TiDB 容器归属、随机项目标签及唯一的 IPv4 回环绑定。数据库初始化会在该专属实例中启用 TiDB CHECK 约束，并保留原生迁移和 writer 身份校验。driver 及其子孙进程放入独立 Windows Job，正常退出、超时或中断都会关闭该 Job；随后仅清理本次随机项目的容器和网络。独立工作目录中的日志和测试凭据保留供检查，应存放在受信任位置且不要提交到 Git。

完整结果见工作目录下的 `tidb-full-acceptance.json`，它引用更详细的 `run-.../acceptance.json` 和本地锁资格证据。详细记录和底层工具中部分历史字段名仍含 `mysql`；本工具实际数据库类型以总结果中的 `backend: tidb/tikv` 和检测到的 TiDB 版本为准。只有完整业务验证及容器清理都通过，结果才为 `ok: true`。复验预检、进程树与 SQL 监听隔离：`.venv/Scripts/python.exe -m pytest -q tools/tidb_full/test_tidb_full_preflight.py tools/tidb_full/test_windows_job.py tools/tidb_full/test_safety.py`。

需要同时验证同区 Scene 计划停机时，在上述 `run.py` 命令末尾追加 `--scene-drain`。工具会在真实账号进入源 Scene 后，启动第二个 Python Scene，执行源节点停机并核验目标入场、Gate 路由、归属 epoch 和 TiDB 存档。此选项要求准备的原生 Go SceneManager 支持 `EnterSceneRequest.evacuation`；对照当前基线时，请按 [MySQL 整栈工具](../interop/README.md) 使用当前 C++ main 提交重新准备快照。输出的总结果包含 `scene_drain_requested` 和 `scene_planned_shutdown` 证据；如果指定选项后缺少或未通过迁移证据，总结果不会标记成功。历史字段 `native_mysql_verified` / `mysql_save_seq` 在此链路中实际表示通过 TiDB 验证的存档。

在 `tools/tidb_ha` 创建的隔离多副本实例上，先针对其 `ready.json` 生成绑定本轮容器的资格证据，再运行原生 Go 旧 epoch 围栏用例：

```powershell
.venv/Scripts/python.exe tools/tidb_full/run_qualification.py `
  --identity 'D:/path/to/ha-work/ready.json' --prepared 'D:/path/to/prepared-native-snapshot' `
  --output 'D:/path/to/ha-work/qualification.json'

.venv/Scripts/python.exe tools/tidb_full/native_epoch.py `
  --identity 'D:/path/to/ha-work/ready.json' --qualification 'D:/path/to/ha-work/qualification.json' `
  --prepared 'D:/path/to/prepared-native-snapshot' --output 'D:/path/to/new-epoch-output'
```

工具先核对容器标签、唯一 loopback 端口、两种原语证据和权威 DDL SHA，再运行 Go 测试。输出与锁资格仅适用于这次隔离集群；失败时保留私有回执，避免覆盖不确定的清理结果。
