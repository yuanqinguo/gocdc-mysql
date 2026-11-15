FROM node:22-alpine AS web-build
WORKDIR /workspace
COPY web/package.json web/package-lock.json ./web/
RUN npm ci --prefix web
COPY web/ ./web/
RUN VITE_OUT_DIR=/workspace/internal/api/dist npm run build --prefix web

FROM golang:1.24.11 AS build

ARG VERSION=dev
ARG COMMIT=unknown

ENV GOFLAGS=-mod=readonly
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web-build /workspace/internal/api/dist/ ./internal/api/dist/
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w -X main.buildVersion=${VERSION} -X main.buildCommit=${COMMIT}" \
    -o /out/gocdc ./cmd/gocdc

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 gocdc

ENV TZ=Asia/Shanghai
COPY --from=build /out/gocdc /data/wwwRoot/gocdc
RUN chmod +x /data/wwwRoot/gocdc && chown -R gocdc:gocdc /data/wwwRoot

USER gocdc
WORKDIR /data/wwwRoot
ENTRYPOINT ["/data/wwwRoot/gocdc"]
