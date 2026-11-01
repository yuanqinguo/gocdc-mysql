// client.go：etcd 客户端构造（多 endpoint/TLS/auth/超时选项）与共享常量；
// 包说明见 doc.go。

package etcdx

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// 预期错误（fail-fast 校验用）。
var (
	ErrNoEndpoints = errors.New("etcd endpoints 不能为空")
)

const (
	defaultDialTimeout = 5 * time.Second // 03 §5.3
	defaultOpTimeout   = 3 * time.Second // 每操作超时，03 §5.3
	defaultRetries     = 3               // 读写重试次数，03 §5.3
	defaultMaxSend     = 8 << 20         // MaxCallSendMsgSize 8MB（配置 JSON 等足够）
)

// Options 客户端构造选项（03 §5.3：多 endpoint/TLS/auth/超时）。
type Options struct {
	Endpoints []string
	Username  string
	Password  string
	TLS       TLSOptions

	// DialTimeout 建连超时，0 = 默认 5s。
	DialTimeout time.Duration
	// MaxCallSendMsgSize 单请求上限（字节），0 = 默认 8MB。
	MaxCallSendMsgSize int
}

// TLSOptions TLS 配置（03 §3.2 source/kafka 同构：ca/cert/key/skip_verify）。
type TLSOptions struct {
	Enabled    bool
	SkipVerify bool
	CA         string
	Cert       string
	Key        string
}

// New 构造 etcd 客户端。连接是惰性的（New 不真正建连），首个操作才会暴露
// 连接问题——调用方用 KV 包装层的重试吸收瞬时故障。
func New(opts Options) (*clientv3.Client, error) {
	if len(opts.Endpoints) == 0 {
		return nil, ErrNoEndpoints
	}
	cfg := clientv3.Config{
		Endpoints:          opts.Endpoints,
		DialTimeout:        defaultDialTimeout,
		MaxCallSendMsgSize: orDefault(opts.MaxCallSendMsgSize, defaultMaxSend),
		Username:           opts.Username,
		Password:           opts.Password,
	}
	if opts.DialTimeout > 0 {
		cfg.DialTimeout = opts.DialTimeout
	}
	if opts.TLS.Enabled {
		tlsCfg, err := buildTLS(opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("构建 etcd TLS 配置: %w", err)
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("创建 etcd 客户端: %w", err)
	}
	return cli, nil
}

// buildTLS 由 CA/证书/私钥文件构造 *tls.Config。
func buildTLS(o TLSOptions) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: o.SkipVerify, //nolint:gosec // 显式配置项，默认 false
	}
	if o.CA != "" {
		pem, err := os.ReadFile(o.CA)
		if err != nil {
			return nil, fmt.Errorf("读取 CA %q: %w", o.CA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA %q 中无有效证书", o.CA)
		}
		cfg.RootCAs = pool
	}
	if o.Cert != "" || o.Key != "" {
		if o.Cert == "" || o.Key == "" {
			return nil, errors.New("双向 TLS 需同时提供 cert 与 key")
		}
		pair, err := tls.LoadX509KeyPair(o.Cert, o.Key)
		if err != nil {
			return nil, fmt.Errorf("加载客户端证书 %q/%q: %w", o.Cert, o.Key, err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

func orDefault(v, d int) int {
	if v <= 0 {
		return d
	}
	return v
}
