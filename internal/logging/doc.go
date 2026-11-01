// Package logging 统一日志入口：zap 封装（级别动态可调、JSON/console 双编码）、
// 生命周期事件固定 event 字段（11 §3 清单，见 events.go）、敏感值过滤器接口
// （见 redact.go）。
//
// 输出统一写标准输出：部署形态为容器（docker-compose 集群/单节点），日志由
// docker logging driver 收集，不做文件轮转。
//
// 红线：禁止打印未脱敏行列值——行数据日志必须经 RowRedactor 视图。
package logging
