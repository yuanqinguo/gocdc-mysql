package schema

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gocdc/internal/model"
)

// Loader 表结构加载器（MySQLLoader 之外可注入 stub 供单测）。
type Loader interface {
	Load(ctx context.Context, schemaName, table string) (*model.TableMeta, error)
}

// snapshot 不可变快照：整体原子替换，读方无锁（versioned, atomic swap）。
type snapshot struct {
	tables map[string]*model.TableMeta // key = "db.table" 小写
}

// Manager SchemaManager 实现：
//
//	Get   命中缓存即返回；未命中懒加载（管线预热后稳态为纯内存读）
//	Refresh 强制重查并递增该表版本（DDL barrier 协议调用）
//
// 版本语义（05 §2.1 schema_version）：每表独立计数，首次加载 = 1，每次 Refresh +1；
// 同一版本的 TableMeta 实例不可变，Event.SchemaVersion 即取自此。
type Manager struct {
	loader Loader
	now    func() time.Time

	mu       sync.Mutex        // 串行化加载/刷新（查询路径互斥；读路径无锁）
	versions map[string]uint64 // 每表当前版本
	cur      atomic.Pointer[snapshot]
}

// NewManager 构造（loader 为 MySQLLoader 或测试 stub）。
func NewManager(loader Loader) *Manager {
	m := &Manager{loader: loader, versions: map[string]uint64{}, now: time.Now}
	m.cur.Store(&snapshot{tables: map[string]*model.TableMeta{}})
	return m
}

func key(schemaName, table string) string {
	return strings.ToLower(schemaName) + "." + strings.ToLower(table)
}

// Get 返回当前版本的 TableMeta；缓存未命中时懒加载（含首次版本 = 1）。
func (m *Manager) Get(ctx context.Context, schemaName, table string) (*model.TableMeta, error) {
	k := key(schemaName, table)
	if t, ok := m.cur.Load().tables[k]; ok {
		return t, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.cur.Load().tables[k]; ok { // 双检：等待期间他 goroutine 已加载
		return t, nil
	}
	return m.load(ctx, schemaName, table)
}

// Refresh 强制重查（DDL 后调用）：新版本 TableMeta 原子替换，旧版本对
// 在途事件保持有效（转换期间引用的快照不受影响，08 §3）。
func (m *Manager) Refresh(ctx context.Context, schemaName, table string) (*model.TableMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.load(ctx, schemaName, table)
}

// Preload 预热规则覆盖的全部表（启动期 fail-fast：任一表不可加载即报错清单）。
type TableRef struct{ Schema, Table string }

func (m *Manager) Preload(ctx context.Context, refs []TableRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for _, r := range refs {
		if _, err := m.load(ctx, r.Schema, r.Table); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("schema: 预热 %d 张表失败: %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// load 实际查询并入缓存（调用方必须持 m.mu）。列数变化之外的 DDL（改类型/注释）
// 也应刷新版本——统一以 Refresh 次数为准，不做内容 diff。
func (m *Manager) load(ctx context.Context, schemaName, table string) (*model.TableMeta, error) {
	meta, err := m.loader.Load(ctx, schemaName, table)
	if err != nil {
		return nil, err
	}
	k := key(schemaName, table)
	m.versions[k]++ // 首次 = 1
	meta.Version = m.versions[k]
	meta.LoadedAt = m.now()

	next := &snapshot{tables: make(map[string]*model.TableMeta, len(m.cur.Load().tables)+1)}
	for kk, v := range m.cur.Load().tables {
		next.tables[kk] = v
	}
	next.tables[k] = meta
	m.cur.Store(next)
	return meta, nil
}
