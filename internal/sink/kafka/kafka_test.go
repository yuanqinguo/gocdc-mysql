package kafka

import (
	"strings"
	"testing"

	"gocdc/internal/config"
)

func TestCompression_Unknown(t *testing.T) {
	if _, err := compression("brotli"); err == nil {
		t.Fatal("未知压缩应报错")
	}
	for _, ok := range []string{"", "snappy", "none", "gzip", "lz4", "zstd"} {
		if _, err := compression(ok); err != nil {
			t.Fatalf("%s 应合法: %v", ok, err)
		}
	}
}

func TestSaslMechanism(t *testing.T) {
	if m, err := saslMechanism(config.SASLOptions{Enabled: false}); m != nil || err != nil {
		t.Fatalf("未启用应 nil: %v %v", m, err)
	}
	for _, mech := range []string{"plain", "scram-sha256", "scram-sha512"} {
		if _, err := saslMechanism(config.SASLOptions{Enabled: true, Mechanism: mech, User: "u"}); err != nil {
			t.Fatalf("%s 应合法: %v", mech, err)
		}
	}
	if _, err := saslMechanism(config.SASLOptions{Enabled: true, Mechanism: "kerberos"}); err == nil {
		t.Fatal("未知机制应报错")
	}
}

func TestNew_Validation(t *testing.T) {
	if _, err := New(Options{}); err == nil || !strings.Contains(err.Error(), "brokers") {
		t.Fatalf("缺 brokers 应报错: %v", err)
	}
}

func TestClassify(t *testing.T) {
	if classify(nil) != nil {
		t.Fatal("nil 不应包装")
	}
	if !isPermanent(classify(errOf("MSG_SIZE_TOO_LARGE"))) {
		t.Fatal("消息过大应永久")
	}
	if isPermanent(classify(errOf("broker: not available"))) {
		t.Fatal("broker 不可达应可重试")
	}
}

type strErr struct{ s string }

func (e *strErr) Error() string { return e.s }

func errOf(s string) error { return &strErr{s} }

func isPermanent(err error) bool {
	return err != nil && strings.Contains(err.Error(), "不可重试")
}
