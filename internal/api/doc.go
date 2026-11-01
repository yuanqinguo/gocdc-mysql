// Package api 提供 admin REST API（gin，13 §3 全端点 + auth + 审计）。
//
// 目录语义：本包只放 API 实现；前端源码在仓库根 web/（React 18 + AntD 5），
// 构建产物 go:embed 挂载进本包（13 §6）。
//
// 原则（13 §1）：API 优先；看图归 Grafana；默认安全（单凭据必配，写操作
// confirm + 审计）；无旁路写（除 position/etcd 配置两个受控入口）。
//
// 鉴权：HTTP Basic 单用户名密码（observability.web.username/password，
// 支持 ${ENV}），一套凭据全权限；危险操作另有 confirm 门禁与审计兜底。
//
// 红线：不做配置热更新——Web 改配置只写 etcd 并提示重启生效（03 §3.1-5）。
//
// 文件布局：router.go 只留 Deps 依赖契约与路由表；出入参结构体与契约常量在
// dto.go；统一出参信封在 respond.go；handler 按资源拆分（status/rules/
// position/ddl/verify/leader，audit 端点随审计实现在 auth.go）。
package api
