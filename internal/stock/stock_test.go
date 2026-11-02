// 无 Docker 单测：全量扫描的纯函数层（PK 游标编解码、绑定值归一、位点比较、
// 锁语句派生、SQL 标识符）。需要 MySQL/etcd 的场景见 *_integration_test.go。
package stock

import (
	"context"
	"strings"
	"testing"

	"gocdc/internal/model"
)

// TestEncodeDecodePKRoundTrip 游标文本形态 ↔ 绑定值往返（断点续扫的持久化契约）。
func TestEncodeDecodePKRoundTrip(t *testing.T) {
	cols := []model.Column{
		{Name: "id", Type: model.TypeBigint},
		{Name: "uid", Type: model.TypeInt, Unsigned: true},
		{Name: "code", Type: model.TypeVarchar},
		{Name: "y", Type: model.TypeYear},
	}
	vals := []string{"42", "18446744073709551615", "ab`cd", "2024"}

	decoded, err := decodePK(cols, vals)
	if err != nil {
		t.Fatal(err)
	}
	if decoded[0] != int64(42) {
		t.Errorf("有符号整型还原 = %T(%v), want int64(42)", decoded[0], decoded[0])
	}
	if decoded[1] != uint64(18446744073709551615) {
		t.Errorf("无符号整型还原 = %T(%v), want uint64 max", decoded[1], decoded[1])
	}
	if decoded[2] != "ab`cd" {
		t.Errorf("字符串还原 = %v", decoded[2])
	}
	if decoded[3] != int64(2024) {
		t.Errorf("year 还原 = %T(%v), want int64(2024)", decoded[3], decoded[3])
	}

	// 往返：encode(decoded) 应回到原文本
	if got := encodePK(decoded); !equalStrings(got, vals) {
		t.Errorf("encode(decode(%v)) = %v，往返不一致", vals, got)
	}
}

// TestDecodePKMismatch 断点列数不匹配必须报错（不得半还原续扫）。
func TestDecodePKMismatch(t *testing.T) {
	cols := []model.Column{{Name: "id", Type: model.TypeInt}}
	if _, err := decodePK(cols, []string{"1", "2"}); err == nil {
		t.Fatal("列数不匹配应报错")
	}
	// 脏文本：非数字进整型列必须报错（不得静默 0 续扫——会死循环重复扫同一块）
	if _, err := decodePK(cols, []string{"abc"}); err == nil {
		t.Fatal("整型列脏文本应报错")
	}
}

// TestNormalizeForBind 文本协议回读值 → 游标绑定值（类型分支覆盖）。
func TestNormalizeForBind(t *testing.T) {
	intCol := model.Column{Name: "id", Type: model.TypeInt}
	uintCol := model.Column{Name: "id", Type: model.TypeBigint, Unsigned: true}
	decCol := model.Column{Name: "amt", Type: model.TypeDecimal}
	strCol := model.Column{Name: "name", Type: model.TypeVarchar}

	cases := []struct {
		note string
		col  model.Column
		in   any
		want any
	}{
		{"nil 保持 nil（游标边界：PK 非空约束下不应出现，防御性）", intCol, nil, nil},
		{"int64 直通", intCol, int64(7), int64(7)},
		{"[]byte 文本 → 有符号 int64", intCol, []byte("123"), int64(123)},
		{"string 文本 → 无符号 uint64", uintCol, "999", uint64(999)},
		{"[]byte 文本 → 无符号 uint64", uintCol, []byte("999"), uint64(999)},
		{"decimal []byte → 字符串绑定（04 §7）", decCol, []byte("12.34"), "12.34"},
		{"varchar []byte → 字符串", strCol, []byte("abc"), "abc"},
		{"string 直通", strCol, "abc", "abc"},
	}
	for _, c := range cases {
		if got := normalizeForBind(c.col, c.in); got != c.want {
			t.Errorf("%s: normalizeForBind(%v) = %T(%v), want %T(%v)", c.note, c.in, got, got, c.want, c.want)
		}
	}
}

// TestLaterPos 位点比较（P2 = max(已记录 P, 当前)，06 §4.1）。
func TestLaterPos(t *testing.T) {
	p := func(name string, pos uint32) model.Position { return model.Position{Name: name, Pos: pos} }
	cases := []struct {
		note string
		a, b model.Position
		want bool
	}{
		{"b 未记录 → a 胜", p("mysql-bin.000001", 4), model.Position{}, true},
		{"文件序 > 偏移序（跨文件回绕）", p("mysql-bin.000002", 4), p("mysql-bin.000001", 9999), true},
		{"同文件偏移更大", p("mysql-bin.000001", 200), p("mysql-bin.000001", 100), true},
		{"同文件相等（max 语义含等号：位点不回退）", p("mysql-bin.000001", 100), p("mysql-bin.000001", 100), true},
		{"a 更早 → false", p("mysql-bin.000001", 4), p("mysql-bin.000002", 4), false},
	}
	for _, c := range cases {
		if got := laterPos(c.a, c.b); got != c.want {
			t.Errorf("%s: laterPos(%v, %v) = %v, want %v", c.note, c.a, c.b, got, c.want)
		}
	}
}

// TestReleaseLockSQL 锁语句与模式配对（backup 锁的释放语句不同，错用会语法错）。
func TestReleaseLockSQL(t *testing.T) {
	cases := map[string]string{
		LockBackup: "UNLOCK INSTANCE",
		LockFTWRL:  "UNLOCK TABLES",
		LockNone:   "SELECT 1",
	}
	for mode, want := range cases {
		if got := releaseLockSQL(mode); got != want {
			t.Errorf("releaseLockSQL(%q) = %q, want %q", mode, got, want)
		}
	}
}

// TestAcquireLockExplicitModes 显式 none 直通、未知模式报错（不触库，conn 可为 nil）。
func TestAcquireLockExplicitModes(t *testing.T) {
	mode, err := acquireLock(context.Background(), nil, LockNone)
	if err != nil || mode != LockNone {
		t.Fatalf("none 应直通: %q %v", mode, err)
	}
	if _, err := acquireLock(context.Background(), nil, "mystery"); err == nil {
		t.Fatal("未知 lock_mode 应报错（防无感降级）")
	}
}

// TestQuoting SQL 标识符派生：反引号包裹 + 内嵌反引号翻倍（与 sink/mysql 同规）。
func TestQuoting(t *testing.T) {
	if got := quoteJoin([]string{"id", "na`me"}); got != "`id`,`na``me`" {
		t.Errorf("quoteJoin = %q", got)
	}
	if got := quoteTable("db", "tb"); got != "`db`.`tb`" {
		t.Errorf("quoteTable = %q", got)
	}
	if got := placeholdersN(3); got != "?,?,?" {
		t.Errorf("placeholdersN(3) = %q", got)
	}
	if got := placeholdersN(1); got != "?" {
		t.Errorf("placeholdersN(1) = %q", got)
	}
	if got := placeholdersN(0); got != "" {
		t.Errorf("placeholdersN(0) = %q", got)
	}
}

// TestGtids SHOW MASTER STATUS 的 GTID 列跨驱动是 string/[]byte/nil 三态。
func TestGtids(t *testing.T) {
	if got := gtids("uuid:1-1:1"); got != "uuid:1-1:1" {
		t.Errorf("string = %q", got)
	}
	if got := gtids([]byte("uuid:1-1:1")); got != "uuid:1-1:1" {
		t.Errorf("[]byte = %q", got)
	}
	if got := gtids(nil); got != "" {
		t.Errorf("nil = %q, want 空", got)
	}
}

// TestNewSessionID 会话 ID 随机性（16 hex 字符；恢复期 session_id 不匹配判定依赖非恒定）。
func TestNewSessionID(t *testing.T) {
	seen := map[string]bool{}
	for range 16 {
		id := newSessionID()
		if len(id) != 16 || strings.ToLower(id) != id {
			t.Fatalf("会话 ID 形态异常: %q", id)
		}
		seen[id] = true
	}
	if len(seen) < 16 {
		t.Fatal("会话 ID 出现碰撞（随机性不足）")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
