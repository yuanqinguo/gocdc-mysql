// binlog 拉取实现（04 §1）：go-mysql-org/go-mysql 的 BinlogSyncer——
// 仅做协议握手、事件流读取、binlog 事件解码，不使用 canal；断线由自建
// 监督循环退避重连——重连起点只取安全位点（resume 回调 = Watermark 当前值，
// 否则内部最后完整事件位点），重放只会重复不会丢失（04 §5 位点不变式）。
//
// 红线：心跳/读超时强制默认，不得裸连；
// go-mysql 内部 retrySync 从「最后解析位点」重连（存在丢未推送事件窗口），
// 必须以 DisableRetrySync=true 禁用。
// 包级概述见 doc.go。

package source

import (
	"errors"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"gocdc/internal/model"
)

// ErrInvalidPosition 起始位点不满足启动条件（GTID 集合缺失 / 文件名为空）。
// 位点获取途径：etcd 位点 key（重启）、全量快照协议 P、SHOW MASTER STATUS（首启）。
var ErrInvalidPosition = errors.New("source: 起始位点无效")

// Config binlog 拉取参数（与 config.Source 一一对应，装配层映射）。
type Config struct {
	Addr     string // host:port
	User     string
	Password string
	Charset  string // 默认 utf8mb4
	Flavor   string // mysql | mariadb；空 = mysql

	ServerID uint32 // 伪从库 ID，全拓扑唯一

	Timezone *time.Location // TIMESTAMP 字符串的格式化时区（source.timezone）；nil = UTC

	UseGTID           bool
	HeartbeatInterval time.Duration // 0 = 3s（03 §3.2：防静默分区挂起）
	ReadTimeout       time.Duration // 0 = 60s
}

// Kind 原始事件类别（Dispatcher 据此分派；04 §3 控制事件表）。
type Kind uint8

const (
	KindRow       Kind = iota + 1 // 行事件（一个 RowsEvent，Dispatcher 拆成行级单元）
	KindRotate                    // 文件切换（含连接首包的 fake rotate，Artificial=true）
	KindXID                       // 事务提交（位点推进的权威锚点）
	KindGTID                      // GTID 事务标记
	KindQuery                     // Query 语句（ROW 模式下 DDL 才会出现；BEGIN/COMMIT 已过滤）
	KindHeartbeat                 // 心跳（长时间无写入时喂狗/位点推进检查，04 §3）
)

// Event 源头事件（Dispatcher 输入通道的元素）。
// 位点字段：LogPos = 本事件在 binlog 中的结束偏移；XID/Rotate 是位点推进的
// 权威锚点，行事件仅作事务内轨迹（04 §5：位点取 XID 映射）。
type Event struct {
	Kind       Kind
	Artificial bool // fake rotate 等人工事件（连接首包）

	Ts     uint64 // header 秒级时间戳
	LogPos uint32 // 事件尾偏移
	File   string // 当前 binlog 文件

	Schema string                        // KindRow
	Table  string                        // KindRow
	Rows   *replication.RowsEvent        // KindRow（Rows() 值已按 04 §7 期望解码）
	RowOp  replication.EnumRowsEventType // KindRow：insert/update/delete

	Query   string // KindQuery
	XID     uint64 // KindXID
	GTIDSet string // 当前 GTID 集合文本（use_gtid 时随 XID/Query 更新）
}

// ResumeFunc 每次重连前取回放起点（接入 Watermark 当前位点；nil = 用内部位点）。
// 返回零值位点表示暂无已确认位点，保持内部位点。
type ResumeFunc func() model.Position
