package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"gocdc/internal/model"
)

// planGTIDMigration 决策矩阵（P3：use_gtid 切换的 Web 迁移路径）。
func TestPlanGTIDMigration(t *testing.T) {
	pos := model.Position{Name: "binlog.000004", Pos: 100}
	master := model.Position{Name: "binlog.000004", Pos: 5000, GTIDSet: "uuid:1-9"}

	// 合法：同文件差距在门槛内 → 写入 executed 集合
	gset, gap, aerr := planGTIDMigration(true, true, pos, master)
	if aerr != nil || gset != "uuid:1-9" || gap != 4900 {
		t.Fatalf("合法迁移: %v %q %d", aerr, gset, gap)
	}

	// use_gtid 未开启
	if _, _, aerr := planGTIDMigration(false, true, pos, master); aerr == nil || !strings.Contains(aerr.msg, "use_gtid 未开启") {
		t.Fatalf("use_gtid=false 应拒绝: %+v", aerr)
	}
	// 位点不存在
	if _, _, aerr := planGTIDMigration(true, false, model.Position{}, master); aerr == nil || !strings.Contains(aerr.msg, "未初始化") {
		t.Fatalf("无位点应拒绝: %+v", aerr)
	}
	// 位点已有 gtid_set
	if _, _, aerr := planGTIDMigration(true, true, model.Position{Name: "f", Pos: 4, GTIDSet: "u:1-1"}, master); aerr == nil || !strings.Contains(aerr.msg, "无需迁移") {
		t.Fatalf("已含集合应拒绝: %+v", aerr)
	}
	// master 未返回集合
	if _, _, aerr := planGTIDMigration(true, true, pos, model.Position{Name: "binlog.000004", Pos: 5000}); aerr == nil || !strings.Contains(aerr.msg, "gtid_mode") {
		t.Fatalf("master 无集合应拒绝: %+v", aerr)
	}
	// 文件已轮转
	if _, _, aerr := planGTIDMigration(true, true, pos, model.Position{Name: "binlog.000009", Pos: 4, GTIDSet: "uuid:1-9"}); aerr == nil || !strings.Contains(aerr.msg, "已轮转") {
		t.Fatalf("轮转应拒绝: %+v", aerr)
	}
	// 差距超门槛
	masterFar := model.Position{Name: "binlog.000004", Pos: pos.Pos + gtidMigrateMaxGap + 1, GTIDSet: "uuid:1-9"}
	if _, gap, aerr := planGTIDMigration(true, true, pos, masterFar); aerr == nil || gap != int64(gtidMigrateMaxGap+1) || !strings.Contains(aerr.msg, "先追平") {
		t.Fatalf("超门槛应拒绝: %+v gap=%d", aerr, gap)
	}
}

// planFilePosMigration 反向决策矩阵。
func TestPlanFilePosMigration(t *testing.T) {
	pos := model.Position{Name: "binlog.000004", Pos: 4, GTIDSet: "uuid:1-9"}

	newPos, aerr := planFilePosMigration(false, true, pos, "binlog.000001")
	if aerr != nil || newPos.Name != "binlog.000001" || newPos.Pos != 4 || newPos.GTIDSet != "" {
		t.Fatalf("反向迁移: %+v %+v", aerr, newPos)
	}
	if _, aerr := planFilePosMigration(true, true, pos, "binlog.000001"); aerr == nil || !strings.Contains(aerr.msg, "仍为 true") {
		t.Fatalf("use_gtid=true 应拒绝: %+v", aerr)
	}
	if _, aerr := planFilePosMigration(false, true, model.Position{Name: "f", Pos: 4}, "binlog.000001"); aerr == nil || !strings.Contains(aerr.msg, "无需反向") {
		t.Fatalf("无 gtid_set 应拒绝: %+v", aerr)
	}
	if _, aerr := planFilePosMigration(false, true, pos, ""); aerr == nil || !strings.Contains(aerr.msg, "SHOW BINARY LOGS 为空") {
		t.Fatalf("空清单应拒绝: %+v", aerr)
	}
}

// TestAuditTypedNilSafety typed-nil 防线：nil *apiError 直接作为 error 接口
// 是非 nil 接口包 nil 指针，调 result.Error() 即 panic。apiErrToError 归一
// nil 语义，本测试锁定：迁移决策的 nil 结果经转换后进 audit 不得 panic。
func TestAuditTypedNilSafety(t *testing.T) {
	if apiErrToError(nil) != nil {
		t.Fatal("nil *apiError 须归一为 nil error 接口")
	}
	var e *apiError = &apiError{code: 400, msg: "x"}
	if apiErrToError(e) == nil {
		t.Fatal("非 nil *apiError 不得归一为 nil")
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/api/v1/sources/default/position/migrate-gtid?confirm=true", nil)
	var verr *apiError // 模拟 planGTIDMigration 成功路径的 typed-nil 返回
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("typed-nil 进 audit 触发 panic（apiErrToError 未生效？）: %v", r)
			}
		}()
		audit(nil, c, "test.what", "d", apiErrToError(verr))
	}()
}

// TestRouter_MigrateGuards 路由守卫：缺 confirm / use_gtid 未开（不触 Store）。
func TestRouter_MigrateGuards(t *testing.T) {
	deps, _ := testDeps()
	defaultSD(deps).MasterPos = func(context.Context) (model.Position, error) { return model.Position{}, nil }
	defaultSD(deps).OldestBinlog = func(context.Context) (string, error) { return "binlog.000001", nil }
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	post := func(path string) string {
		req := httptest.NewRequest("POST", path, nil)
		req.SetBasicAuth("admin", "pw")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w.Body.String()
	}
	if body := post("/api/v1/sources/default/position/migrate-gtid"); !strings.Contains(body, "confirm=true") {
		t.Fatalf("缺 confirm 应 400: %s", body)
	}
	// use_gtid=false（默认基线）→ gtid 迁移拒绝、filepos 迁移进入位点读取
	//（testDeps 的 Store 为 nil 客户端包装，Load 会失败 → 500 而非 400，可接受：
	// 守卫路径已由纯函数单测覆盖）
	if body := post("/api/v1/sources/default/position/migrate-gtid?confirm=true"); !strings.Contains(body, "use_gtid 未开启") {
		t.Fatalf("use_gtid 未开应拒绝: %s", body)
	}
}

// mergeSetPosition 决策矩阵：SET-POSITION 只覆盖 name/pos，gtid_set/seq/任期
// 以 etcd 现值为底沿用——Store.Save 是整值覆盖，只写请求三件套会把 GTID 集
// 抹空（恢复后管线必因位点缺集合熔断）；无旧位点（首启引导态）按请求裸写。
func TestMergeSetPosition(t *testing.T) {
	body := SetPositionRequest{Name: "binlog.000007", Pos: 42, Confirm: ConfirmSetPosition}

	// 有旧位点：沿用沿用字段，只覆盖 name/pos
	merged := mergeSetPosition(true, model.Position{
		Name: "binlog.000004", Pos: 100, GTIDSet: "uuid:1-9", Seq: 7, Term: 1130,
	}, body)
	if merged.Name != "binlog.000007" || merged.Pos != 42 {
		t.Fatalf("name/pos 应被覆盖: %+v", merged)
	}
	if merged.GTIDSet != "uuid:1-9" || merged.Seq != 7 || merged.Term != 1130 {
		t.Fatalf("沿用字段不得丢失（GTID 模式下抹空=恢复即熔断）: %+v", merged)
	}

	// 无旧位点（首启引导态）：按请求裸写
	fresh := mergeSetPosition(false, model.Position{}, body)
	if fresh.Name != "binlog.000007" || fresh.Pos != 42 || fresh.GTIDSet != "" || fresh.Seq != 0 {
		t.Fatalf("无旧位点应裸写: %+v", fresh)
	}
}
