# 隔离整栈互操作验收（Windows）

这个工具从指定的 C++/Go Git 提交导出快照，在新目录编译原生 Go 服务，再启动 Python 门户、Gate、Scene、Battle 及配套中间件。它检查 HTTP 登录分票、无夹具组队申请与邀请、场景跟随、棋盘、跨区、战斗、Kafka、Inbox 和数据库回读，并检查门户管理接口和 etcd 租约。

它只用于本机隔离测试：监听 `127.0.0.1` 的随机端口，创建全新的 MySQL 数据目录，核对服务返回的 `@@datadir`，使用随机生成的账号和密钥。测试结束终止启动的进程并保留 `run-<随机值>/acceptance.json`、日志和测试数据库供检查；该目录包含临时凭据，仅放在受信任的工作目录，检查结束由操作者删除。不要把输出目录放进 Git，也不要指向现有生产中间件。

此脚本使用独立 **MySQL**，不声称验证 TiDB。Kafka 本身需要 Java 运行时，Python 门户及游戏节点不需要 Java。商城 TiDB 验收需要对应的独立测试工具和环境。

## 前置条件

- Windows，Python 3.11+，在本项目安装 `.[cluster,dev]`；Git 和本机兼容的 Go 工具链（当前基线需 Go 1.27.2）。
- C++/Go 仓库的 Git checkout，内含要验证的提交；只读取 `git archive`，不会把未提交的工作树改动打包。默认提交为 `098f18ade7a3a8692698add32dc64556a665c7dc`，它包含组队和 Redis 连接池边界修复；用 `--revision` 可指定其他提交。
- etcd 可执行文件、Redis 可执行文件、MySQL 安装目录（含 `bin/mysqld.exe`、`mysql.exe`、`mysqladmin.exe`）、Kafka 发行包的 `libs` 目录和 Kafka 使用的 Java 可执行文件。
- 已正确安装并满足原生 Go 写者启动机制信任检查的 `private_control_file.exe`。脚本只读取这个既有文件，不更改系统 ACL，也不安装或删除它。
- 本机 Go 模块缓存有依赖或能够从可信模块代理下载。准备阶段根据提交的 `go.mod` 编译；测试账户的 Argon2id 生成器额外使用 Go 缓存中的 `golang.org/x/crypto@v0.46.0`。

## 准备和运行

路径都由调用者指定。示例变量应替换成本机实际路径：

```powershell
$CppRepo = 'D:/path/to/cpp-repo'
$GoExe = 'D:/path/to/go/bin/go.exe'
$Prepared = 'D:/path/to/new-disposable-work-directory'
.venv/Scripts/python.exe tools/interop/prepare.py --cpp-repo $CppRepo --work-dir $Prepared --go $GoExe

.venv/Scripts/python.exe tools/interop/run.py --prepared $Prepared `
  --go $GoExe --etcd 'D:/path/to/etcd.exe' --redis 'D:/path/to/redis-server.exe' `
  --mysql 'D:/path/to/mysql' --kafka-java 'D:/path/to/java.exe' `
  --kafka-libs 'D:/path/to/kafka/libs' --private-helper 'D:/path/to/trusted/private_control_file.exe'
```

准备目录必须不存在，程序不会覆盖或递归删除已有目录。准备程序记录源码树及每个原生二进制的 SHA-256；运行前会重新核对，任一文件变化都会拒绝运行。首次构建可能耗时较长。运行结束输出验收 JSON 的路径，退出码为零表示全部启用场景通过。

默认通过 Router 验证组队。给运行命令追加 `--direct-gate` 可验证 Gate 直连 Login/Match 的登录、跨区和战斗链路；原生直连模式不提供 Team 客户端路由，因此该模式不运行组队场景。

回环端口分配存在释放到监听之间的系统竞争窗口；若端口被占，测试应失败并清理，而不应连接已有 SQL 实例。原生服务可能因版本、Windows 安全策略或依赖版本不同拒绝启动；应检查本次独立目录的日志，不要通过削弱文件权限或禁用写者验证来绕过。

## 同区 Scene 计划停机

在上述 `run.py` 命令后追加 `--scene-drain`，工具会在玩家登录并进入源 Scene 后，启动独立节点号的第二个 Python Scene，触发源节点的计划停机，然后核对客户端仍可访问、Gate 路由、角色归属 epoch、地图和原生 MySQL 存档。结果写入本次 `acceptance.json` 的 `scene_planned_shutdown`。它使用同一组隔离中间件，不能指向已有运行环境或生产数据。

此场景依赖 C++ main 的 Go SceneManager 所实现的 `EnterSceneRequest.evacuation` 语义：要验证当前基线，请通过 `prepare.py --revision <当前 C++ main 提交>` 重新准备快照。也可先对已有的 `--prepared` 快照运行，但早期快照可能缺少该语义；其结果不能证明当前 C++ main 的迁移行为。不同快照保存在各自新建的准备目录，不覆盖已有产物。

轻量验证只检查脚本编译和测试，不启动中间件：`.venv/Scripts/python.exe -m pytest -q tools/interop/test_drain.py tools/interop/test_preflight.py`。实际多节点联调必须运行上面的完整验收，不能用轻量测试结果代替。

可单独运行准备产物篡改检测的回归测试：`.venv/Scripts/python.exe -m pytest -q tools/interop/test_preflight.py`。

## 低资源 Match 发现和切换验证

下面的测试只启动一个全新 etcd 进程和两个本机 gRPC 测试副本，不启动 MySQL、Kafka 或 TiDB。etcd 数据位于 pytest 临时目录，监听随机的回环端口；测试检查注册发现、停止一个仍有登记的副本后的实际重试，以及删除登记后的选择和连接清理。将路径替换为本机 etcd 可执行文件：

```powershell
$env:XUANMING_ETCD_EXE = 'D:/path/to/etcd.exe'
.venv/Scripts/python.exe -m pytest -q tests/test_match_failover_etcd.py
Remove-Item Env:XUANMING_ETCD_EXE
```
