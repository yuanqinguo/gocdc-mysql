package source

import (
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	"gocdc/internal/logging"
	"gocdc/internal/model"
)

// Source binlog 拉取器：单 goroutine 语义（04 §1），内部位点带锁维护、
// 快照读，供 Run（监督循环）与外部排障读取共用。
type Source struct {
	cfg Config
	log *zap.Logger

	// DebugQueryHook QueryEvent 观测钩子（nil=关闭；诊断/测试用，非业务路径）。
	DebugQueryHook func(query, schema string)

	mu       sync.Mutex
	lastFile string
	lastPos  uint32
	gtid     string // 最近一次 XID/Query 事件的 GTID 集合文本（use_gtid）
}

// New 构造（缺省值在此填平：charset/flavor/heartbeat/read timeout/timezone）。
func New(cfg Config, log *zap.Logger) *Source {
	if cfg.Charset == "" {
		cfg.Charset = "utf8mb4"
	}
	if cfg.Flavor == "" {
		cfg.Flavor = "mysql"
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeat
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = defaultReadTodo
	}
	if cfg.Timezone == nil {
		cfg.Timezone = time.UTC
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Source{cfg: cfg, log: log}
}

// flavor 返回小写 flavor（mysql/mariadb，供 GTID 集合解析）。
func (s *Source) flavor() string { return strings.ToLower(s.cfg.Flavor) }

// snapshotPos 内部位点快照（重连基线；连接首包 fake rotate 之前可能为空）。
func (s *Source) snapshotPos() model.Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	return model.Position{Name: s.lastFile, Pos: s.lastPos, GTIDSet: s.gtid}
}

func (s *Source) trackGSet(gset interface{ String() string }) {
	if gset == nil {
		return
	}
	s.mu.Lock()
	s.gtid = gset.String()
	s.mu.Unlock()
}

func (s *Source) currentGTID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gtid
}

// syncerConfig go-mysql 配置（04 §1）：
//   - ParseTime=false：DATETIME/TIMESTAMP 以服务端格式化字符串交付（transform
//     字符串透传路径，时区语义由 TimestampStringLocation 显式锚定）；
//   - UseDecimal=false：DECIMAL 以文本 []byte 交付（transform string 无损路径）；
//   - IgnoreJSONDecode 保持 false：JSON 二进制在解析层转文本（transform 校验文本）；
//   - DisableRetrySync=true：禁用内部「从最后解析位点重连」（会跳过未推送事件，
//     破坏 at-least-once），重连统一由本包 Run 从安全位点发起。
func (s *Source) syncerConfig() replication.BinlogSyncerConfig {
	host, portStr, err := net.SplitHostPort(s.cfg.Addr)
	if err != nil {
		host = s.cfg.Addr
		portStr = "3306"
	}
	port64, _ := strconv.ParseUint(portStr, 10, 16)

	return replication.BinlogSyncerConfig{
		ServerID:                s.cfg.ServerID,
		Flavor:                  s.flavor(),
		Host:                    host,
		Port:                    uint16(port64),
		User:                    s.cfg.User,
		Password:                s.cfg.Password,
		Charset:                 s.cfg.Charset,
		ParseTime:               false,
		TimestampStringLocation: s.cfg.Timezone,
		HeartbeatPeriod:         s.cfg.HeartbeatInterval,
		ReadTimeout:             s.cfg.ReadTimeout,
		DisableRetrySync:        true,
		Logger:                  slog.New(logging.SlogHandler(s.log)),
	}
}
