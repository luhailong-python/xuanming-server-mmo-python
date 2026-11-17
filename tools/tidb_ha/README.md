# 隔离 TiDB 多副本故障演练

该工具仅在本机新建的随机 Docker Compose 项目中运行：3 个 PD、3 个带独立逻辑 zone 标签的 TiKV、2 个 TiDB。SQL 和 PD 端口仅发布到随机 IPv4 回环端口，所有停止、启动和清理操作先核对本轮容器 ID、项目标签、随机 run 标签及网络。它不会操作现有集群。

脚本创建独立账本，确认其所在 Region 有跨三个逻辑 zone 的三个健康副本，分别停止 PD leader、账本 Region 的 TiKV leader 和一个 TiDB 实例。每次故障时写入并在恢复后核对已确认的账本；TiKV 故障还会对已发起、结果未知的事务使用同一个业务键幂等重试，TiDB 故障验证另一实例可重新建立连接并重放。输出记录选中的节点、Region、leader 转移和各次耗时。

这是**单机缩配机制测试**：三个逻辑 zone 仍在一台宿主机，不能证明实际跨可用区隔离、生产配置、真实流量下的 RTO/RPO、负载均衡器自动切换或跨机房灾备。该工具不连接生产数据，不是生产环境 I3 资格。故障演练依次进行，不模拟多个节点同时故障。

需要 Windows、Docker Desktop/Compose、Python 3.11+ 和本项目 `.[cluster]` 依赖。各实例有硬内存上限，逐个启动；Windows 可用内存低于 4 GiB 会立即停止并清理本轮资源。启动前至少需要 6 GiB 可用物理内存，且仍需为本机其他进程预留容量。默认 Docker 地址池用尽时可指定未被占用的私有网段。

```powershell
.venv/Scripts/python.exe tools/tidb_ha/run.py `
  --work-dir 'D:/path/to/new-disposable-work-directory' `
  --subnet '10.248.174.0/24'
```

`--work-dir` 必须不存在。完整结果和内存采样写在该目录；成功或失败均尝试只清理本轮容器、网络和卷。失败时附带容器状态与日志，保留工作目录供排查。**不带 `--wait-for-qualification` 时，`ok: true` 只表示故障演练和清理成功，`qualification` 为 `null`，不表示 I3 已通过。**需要同一实例 I3 与 HA 均通过时必须使用该参数，并检查结果中包含资格报告和原生测试报告的摘要。

如需同一实例先完成 I3 和原生旧 epoch 用例，再进行故障注入，在上面的命令末尾加 `--wait-for-qualification`。保持该终端运行，另开终端依次执行以下命令；`$Prepared` 指向按 [整栈准备步骤](../interop/README.md) 构建并校验的 Go 原生源码及二进制目录。任何一步失败都不要继续下一步，HA 进程最多等待 300 秒。

```powershell
$Work = (Resolve-Path 'D:/path/to/new-disposable-work-directory').Path
$Prepared = 'D:/path/to/prepared-native-snapshot'
.venv/Scripts/python.exe tools/tidb_full/run_qualification.py `
  --identity "$Work/ready.json" --prepared $Prepared --output "$Work/qualification.json"
if ($LASTEXITCODE) { throw 'I3 qualification failed' }
.venv/Scripts/python.exe tools/tidb_full/native_epoch.py `
  --identity "$Work/ready.json" --prepared $Prepared --qualification "$Work/qualification.json" --output "$Work/native-epoch"
if ($LASTEXITCODE) { throw 'Native epoch test failed' }
.venv/Scripts/python.exe tools/tidb_ha/mark_qualified.py `
  --work-dir $Work --qualification "$Work/qualification.json" --native-result "$Work/native-epoch/native-epoch-result.json"
if ($LASTEXITCODE) { throw 'Cannot release fault test' }
```

最后一步检查两个结果与本轮项目、容器、端口和 run ID 的绑定、64 槽和两种锁原语、原生测试结果、清理状态以及报告和日志 SHA-256。只有检查通过才写入标记；HA 进程再次复核该标记，再开始停止节点，结果中包含两份证据的路径与摘要。这些是隔离实例的本地测试凭据，不是生产环境的资格文件。
