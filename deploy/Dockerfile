# gocdc 镜像（前端资源 + Go 多阶段构建、非 root、HEALTHCHECK）
# 构建期禁止 go mod tidy（16 §6/R15）——GOFLAGS=-mod=readonly 强制。
FROM node:22-alpine AS web-build
WORKDIR /workspace
COPY web/package.json web/package-lock.json ./web/
RUN npm ci --prefix web
COPY web/ ./web/
RUN VITE_OUT_DIR=/workspace/internal/api/dist npm run build --prefix web

FROM docker.cnb.cool/qingflyfei/edu.image.base:gobuild-1.24.11 AS build

# 版本信息由 CI 注入：--build-arg VERSION=dev-v1.2.3 COMMIT=abc1234
ARG VERSION=dev
ARG COMMIT=unknown

ENV GOFLAGS=-mod=readonly
ADD . /data/build/
COPY --from=web-build /workspace/internal/api/dist/ /data/build/internal/api/dist/
WORKDIR /data/build

RUN CGO_ENABLED=0 GOOS=linux \
    go build -ldflags "-s -w -X main.buildVersion=${VERSION} -X main.buildCommit=${COMMIT}" \
    -o /data/wwwRoot/gocdc ./cmd/gocdc

# 运行层：alpine + tzdata + Asia/Shanghai（gorun 基础镜像）
FROM docker.cnb.cool/qingflyfei/edu.image.base:gorun-1.0.1

RUN adduser -D -u 10001 gocdc 2>/dev/null || true
COPY --from=build /data/wwwRoot/gocdc /data/wwwRoot/gocdc
RUN chmod +x /data/wwwRoot/gocdc && chown -R gocdc:gocdc /data/wwwRoot

USER gocdc
WORKDIR /data/wwwRoot

# 健康检查打 metrics 端口的 liveness（config 里 metrics_addr 改了就用 HEALTH_URL 覆盖）
ENV HEALTH_URL=http://127.0.0.1:9595/healthz
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -qO- "$HEALTH_URL" || exit 1

ENTRYPOINT ["/data/wwwRoot/gocdc"]
