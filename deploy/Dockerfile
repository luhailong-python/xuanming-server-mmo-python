# 玄冥纯 Python 服务端统一多阶段容器镜像（零 Go/Java 工具链）
# 构建：docker build -f deploy/Dockerfile -t xuanming-python:latest .
# 运行：docker run --rm xuanming-python:latest <子命令> [参数...]

FROM python:3.13-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY src ./src

# 安装纯 Python 集群运行依赖（含 aiokafka / redis / aiohttp / aiomysql）
RUN pip install --upgrade pip setuptools wheel \
    && pip install ".[cluster]"

FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# 固定非 root 运行身份 UID/GID 10001
RUN groupadd --gid 10001 --system xuanming \
    && useradd --uid 10001 --gid 10001 --system --no-create-home --shell /usr/sbin/nologin xuanming \
    && mkdir -p /app/config /var/lib/xuanming /tmp \
    && chown -R 10001:10001 /app /var/lib/xuanming

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=10001:10001 config ./config

USER 10001:10001

ENTRYPOINT ["python", "-m", "xuanming"]
CMD ["smoke"]
