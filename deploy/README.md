# 玄冥纯 Python 服务端部署指南

本目录提供全量纯 Python 服务端（共 23 个业务服务 + 15 库幂等 Schema 迁移）的容器镜像、Docker Compose 本地/测试集群编排与 Kubernetes（Kustomize）生产清单，**零 Go/Java 工具链依赖**。

## 1. 构建统一镜像

在仓库根目录执行：

```bash
docker build -f deploy/Dockerfile -t xuanming-python:latest .
```

镜像特性：
- 基于 `python:3.13-slim` 多阶段构建，仅安装 `.[cluster]` 纯 Python 运行时依赖；
- 默认以非 root 用户 `xuanming`（UID/GID `10001:10001`）运行；
- 入口固定为 `ENTRYPOINT ["python", "-m", "xuanming"]`，默认执行 `smoke`。

本地快速验证镜像：

```bash
docker run --rm xuanming-python:latest smoke
```

## 2. 密钥与环境变量准备

仓库严禁写入任何生产密钥。启动前需通过 `.env`（仅供本机且加入 `.gitignore`）或 Secret 管理系统注入密钥：

1. 生成 Gate 内部 Ed25519 密钥对：
   ```bash
   python -m xuanming identitykeygen
   ```
2. 设置数据库密码与各服务间 ≥32 字节 HMAC 密钥（例如 `MYSQL_ROOT_PASSWORD`、`MYSQL_PASSWORD`、`MMORPG_GATE_TOKEN_SECRET`、`MMORPG_INTERNAL_AUTH_SIGNING_KEY`、`MMORPG_INTERNAL_AUTH_PUBLIC_KEYS`、`MMORPG_MATCH_OPS_CALLER_SECRET`、`COMMERCE_PAYMENT_SECRET`、`COMMERCE_ADMIN_SECRET`、`GM_ADMIN_JWT_SECRET` 等）。

## 3. Docker Compose 启动（本地 / 集成联调）

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

编排包含：
- **基础设施**：`mysql`（8.0）、`redis`（7，附带 `redis-player`/`redis-login`/`redis-scene-control` 别名）、`etcd`（v3.5）、`kafka`（KRaft 单节点开发态）；
- **初始化容器**：`schemamigrate` 在 MySQL 健康后运行 `schemamigrate --host mysql --port 3306 ... --schema-dir /app/config`，覆盖全部 15 份 `*_schema.sql` 建表定义；
- **23 个业务服务**：`portal`、`data-service`、`db`、`player-locator`、`player-inbox`、`router`、`scene-manager`、`match`、`content-guard`、`chat`、`mail`、`rank`、`friend`、`payment`、`trade`、`guild`、`mall-game`、`gm-admin`、`config-node`、`login`、`gate`、`scene`、`battle`。

若生产采用物理分库，可按库逐个执行幂等迁移：

```bash
docker run --rm xuanming-python:latest schemamigrate \
  --host mysql --port 3306 --user appuser --password "$MYSQL_PASSWORD" \
  --db match --database mmorpg_match --schema-dir /app/config
```

## 4. Kubernetes（Kustomize）部署

1. 先创建命名空间与 `xuanming-secrets` Secret（以下仅为字段名示例，请由外部 KMS / ExternalSecrets 注入真实值）：
   ```bash
   kubectl apply -f deploy/k8s/namespace.yaml
   kubectl -n xuanming create secret generic xuanming-secrets \
     --from-literal=mysql-user=appuser \
     --from-literal=mysql-password='<REPLACE_ME>' \
     --from-literal=mysql-database=mmorpg_global \
     --from-literal=portal-mysql-password='<REPLACE_ME>' \
     --from-literal=portal-cdn-signing-secret='<REPLACE_ME>' \
     --from-literal=portal-admin-api-key='<REPLACE_ME>' \
     --from-literal=commerce-admin-secret='<REPLACE_ME>' \
     --from-literal=commerce-payment-secret='<REPLACE_ME>' \
     --from-literal=gm-admin-jwt-secret='<REPLACE_ME>' \
     --from-literal=gate-token-secret='<REPLACE_ME>' \
     --from-literal=internal-auth-signing-key='<REPLACE_ME>' \
     --from-literal=internal-auth-public-keys='<REPLACE_ME>' \
     --from-literal=account-directory-secret-login='<REPLACE_ME>' \
     --from-literal=account-directory-secret-scene='<REPLACE_ME>' \
     --from-literal=account-directory-secret-payment='<REPLACE_ME>' \
     --from-literal=player-store-secret-scene='<REPLACE_ME>' \
     --from-literal=player-inbox-secret-match='<REPLACE_ME>' \
     --from-literal=match-outcome-secret-battle='<REPLACE_ME>' \
     --from-literal=match-ops-caller-secret='<REPLACE_ME>' \
     --from-literal=rank-producer-secret-match='<REPLACE_ME>' \
     --from-literal=role-fence-secret-coordinator='<REPLACE_ME>'
   ```
2. 应用全量清单（含 `Namespace`、`schemamigrate` Job 与 23 组 `Deployment` + `ClusterIP Service`）：
   ```bash
   kubectl apply -k deploy/k8s
   kubectl -n xuanming wait --for=condition=complete job/schemamigrate --timeout=180s
   kubectl -n xuanming get pods,svc
   ```

所有 Pod 均强制配置：
- `runAsNonRoot: true`、`runAsUser: 10001`、`allowPrivilegeEscalation: false`、`readOnlyRootFilesystem: true`、`capabilities.drop: ["ALL"]`；
- HTTP 服务（`portal` / `mall-game` / `gm-admin` / `config-node`）配置 `/actuator/health*` HTTP 探针；gRPC/TCP 服务配置 `tcpSocket` 探针。

## 5. 常用运维 CLI 命令

```bash
# 1) 死信 JSONL 巡检与重放计划
python -m xuanming.ops.admin_cli kafka-dlq inspect --file dlq.jsonl --json
python -m xuanming.ops.admin_cli kafka-dlq replay-plan --file dlq.jsonl --topic match-results --json

# 2) 开发环境支付回调签名投递（支持 --dry-run）
PAYMENT_DEVPAY_SECRET='<32+ bytes secret>' python -m xuanming.ops.admin_cli devpay-notify \
  --url http://127.0.0.1:18130/notify/devpay \
  --order-id 11111111-2222-3333-4444-555555555555 \
  --amount-minor 600 --currency CNY --dry-run --json

# 3) 运营审核玩家战斗剧本（真实 gRPC 调用 match.MatchInternal）
MMORPG_MATCH_OPS_CALLER_SECRET='<32+ bytes secret>' python -m xuanming.ops.admin_cli battle-script-review \
  list --endpoint 127.0.0.1:50500 --limit 20 --json
MMORPG_MATCH_OPS_CALLER_SECRET='<32+ bytes secret>' python -m xuanming.ops.admin_cli battle-script-review \
  approve --endpoint 127.0.0.1:50500 --script-id 1001 --revision 1 --reviewer ops-alice --note "ok" --json
```
