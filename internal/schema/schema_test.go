package schema

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gocdc/internal/model"
)

func TestParseColumnType(t *testing.T) {
	cases := []struct {
		in       string
		base     string
		unsigned bool
		m1, m2   int
		hasLen   bool
		vals     []string
	}{
		{"int", "int", false, 0, 0, false, nil},
		{"bigint unsigned", "bigint", true, 0, 0, false, nil},
		{"decimal(10,2)", "decimal", false, 10, 2, true, nil},
		{"decimal(65,30)", "decimal", false, 65, 30, true, nil},
		{"decimal(65,30) unsigned", "decimal", true, 65, 30, true, nil},
		{"varchar(191)", "varchar", false, 191, 0, true, nil},
		{"bit(12)", "bit", false, 12, 0, true, nil},
		{"datetime(3)", "datetime", false, 3, 0, true, nil},
		{"text", "text", false, 0, 0, false, nil},
		{`enum('a','b''q','c\\d')`, "enum", false, 0, 0, false, []string{"a", "b'q", "c\\d"}},
		{"set('x','y')", "set", false, 0, 0, false, []string{"x", "y"}},
		{"int(11)", "int", false, 11, 0, true, nil},
	}
	for _, tc := range cases {
		p := parseColumnType(tc.in)
		if p.base != tc.base || p.unsigned != tc.unsigned || p.m1 != tc.m1 || p.m2 != tc.m2 || p.hasLen != tc.hasLen {
			t.Fatalf("%s → %+v，期望 base=%s unsigned=%v m1=%d m2=%d hasLen=%v",
				tc.in, p, tc.base, tc.unsigned, tc.m1, tc.m2, tc.hasLen)
		}
		if len(p.vals) != len(tc.vals) {
			t.Fatalf("%s vals=%v 期望 %v", tc.in, p.vals, tc.vals)
		}
		for i := range tc.vals {
			if p.vals[i] != tc.vals[i] {
				t.Fatalf("%s vals[%d]=%q 期望 %q", tc.in, i, p.vals[i], tc.vals[i])
			}
		}
	}
}

// stubLoader 单测用加载器。
type stubLoader struct {
	tbl   map[string]*model.TableMeta
	err   error
	calls int
}

func (s *stubLoader) Load(_ context.Context, schemaName, table string) (*model.TableMeta, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	m, ok := s.tbl[schemaName+"."+table]
	if !ok {
		return nil, ErrTableNotFound
	}
	cp := *m
	return &cp, nil
}

func newStub() *stubLoader {
	return &stubLoader{tbl: map[string]*model.TableMeta{
		"db1.users": {Schema: "db1", Table: "users", Columns: []model.Column{
			{Name: "id", Type: model.TypeBigint},
		}},
	}}
}

func TestManager_LoadAndVersion(t *testing.T) {
	s := newStub()
	m := NewManager(s)
	ctx := context.Background()

	t1, err := m.Get(ctx, "db1", "users")
	if err != nil {
		t.Fatal(err)
	}
	if t1.Version != 1 {
		t.Fatalf("首次加载版本应为 1，实际 %d", t1.Version)
	}
	// 命中缓存不再触发查询
	if _, err := m.Get(ctx, "db1", "users"); err != nil {
		t.Fatal(err)
	}
	if s.calls != 1 {
		t.Fatalf("缓存命中不应重查，calls=%d", s.calls)
	}
	// Refresh 版本递增且实例替换
	t2, err := m.Refresh(ctx, "db1", "users")
	if err != nil {
		t.Fatal(err)
	}
	if t2.Version != 2 || t1.Version != 1 {
		t.Fatalf("Refresh 版本应为 2，实际 %d（旧实例保持 %d）", t2.Version, t1.Version)
	}
	if t1 == t2 {
		t.Fatal("Refresh 应产出新实例")
	}
	// 大小写不敏感 key
	if _, err := m.Get(ctx, "DB1", "USERS"); err != nil {
		t.Fatal(err)
	}
	if s.calls != 2 {
		t.Fatalf("大小写归一后应命中缓存，calls=%d", s.calls)
	}
	// 未加载表的错误直传
	if _, err := m.Get(ctx, "db1", "ghost"); !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("期望 ErrTableNotFound: %v", err)
	}
}

func TestManager_PreloadFailFast(t *testing.T) {
	s := newStub()
	m := NewManager(s)
	err := m.Preload(context.Background(), []TableRef{
		{"db1", "users"}, {"db1", "ghost"},
	})
	if err == nil || !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("预热应聚合 ErrTableNotFound: %v", err)
	}
	// 失败也应有成功者的缓存
	if _, gerr := m.Get(context.Background(), "db1", "users"); gerr != nil {
		t.Fatalf("预热成功的表应可 Get: %v", gerr)
	}
}

func TestManager_ConcurrentGetRefresh(t *testing.T) {
	s := newStub()
	m := NewManager(s)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := m.Get(ctx, "db1", "users"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := m.Refresh(ctx, "db1", "users"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if latest, _ := m.Get(ctx, "db1", "users"); latest.Version == 0 {
		t.Fatal("版本应 >0")
	}
	_ = time.Now
}
