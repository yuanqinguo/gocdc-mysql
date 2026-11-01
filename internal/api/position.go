package api

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"gocdc/internal/model"
)

// getPosition GET /api/v1/position：当前同步位点（无位点 data=null）。
func (h apiHandlers) getPosition(c *gin.Context) {
	p, ok, err := h.Store.Load(c.Request.Context())
	if err != nil {
		respondErr(c, 500, err.Error())
		return
	}
	if !ok {
		respondOK(c, nil)
		return
	}
	respondOK(c, PositionData{
		Name: p.Name, Pos: p.Pos, GTIDSet: p.GTIDSet,
		Seq: p.Seq, Term: p.Term, NodeID: p.NodeID, UpdatedAt: p.UpdatedAt,
	})
}

// mergeSetPosition SET-POSITION 融合决策（纯函数可单测）：请求只携带 name/pos，
// 其余字段以 etcd 现值为底沿用——Store.Save 是整值覆盖，若只写请求三件套，
// GTID 模式下会把 gtid_set 抹空（恢复后管线必因位点缺集合熔断），seq/任期
// 同理；与 etcdctl 直写指引「v、seq、leader_term 沿用旧值」语义对齐。
// 无旧位点（首启引导态）按请求裸写。
func mergeSetPosition(haveOld bool, old model.Position, body SetPositionRequest) model.Position {
	if !haveOld {
		return model.Position{Name: body.Name, Pos: body.Pos}
	}
	old.Name, old.Pos = body.Name, body.Pos
	return old
}

// setPosition POST /api/v1/position：手工覆盖位点（危险操作——confirm 口令
// 门禁 + 审计，绕过水位连续性检查）。以 etcd 现值为底只覆盖 name/pos
// （mergeSetPosition）；现值读取失败拒绝盲写。
func (h apiHandlers) setPosition(c *gin.Context) {
	var body SetPositionRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		abort(c, 400, err.Error())
		return
	}
	if body.Confirm != ConfirmSetPosition {
		abort(c, 400, `危险操作：body.confirm 须为 "SET-POSITION" 原样字符串（13 §3）`)
		return
	}
	old, haveOld, lerr := h.Store.Load(c.Request.Context())
	if lerr != nil {
		err := fmt.Errorf("读取现有位点失败，拒绝盲写（防止沿用字段丢失）: %w", lerr)
		audit(auditOrNil(h.Audit), c, auditPositionSet,
			fmt.Sprintf("%s:%d", body.Name, body.Pos), err)
		respondErr(c, 500, err.Error())
		return
	}
	p := mergeSetPosition(haveOld, old, body)
	p.NodeID = h.NodeID
	err := h.Store.Save(c.Request.Context(), p)
	audit(auditOrNil(h.Audit), c, auditPositionSet,
		fmt.Sprintf("%s:%d", body.Name, body.Pos), err)
	respondResult(c, err)
}

// gtidMigrateMaxGap migrate-gtid 的追平门槛：位点距 master 尾部最大容忍字节差。
// 窗口内未确认事务将被跳过（响应明示 gap_bytes）；超出则拒绝并指引先追平。
const gtidMigrateMaxGap = 8 << 20 // 8 MiB

// apiError 带 HTTP 状态码的处理错误（migrate 决策函数用，纯逻辑可单测）。
type apiError struct {
	code int
	msg  string
}

func (e *apiError) Error() string { return e.msg }

// apiErrToError *apiError → error 接口的 nil 归一（防 typed-nil）：nil 的
// *apiError 直接作为 error 传入 audit 时是非 nil 接口包 nil 指针，调
// result.Error() 即空指针 panic——迁移决策的成功路径返回的正是 nil
// *apiError，故任何进 audit 的 *apiError 必先经本函数归一。
func apiErrToError(e *apiError) error {
	if e == nil {
		return nil
	}
	return e
}

// planGTIDMigration 迁移决策（纯函数）：校验前置与追平门槛，返回应写入位点
// 的 gtid_set 与 gap。安全语义：master 三元组须与位点同文件、差距 ≤ 门槛，
// 否则拒绝（窗口内未确认事务会被 GTID 过滤跳过，等价静默丢数据）。
func planGTIDMigration(useGTID bool, havePos bool, pos, mp model.Position) (gtidSet string, gap int64, apiErr *apiError) {
	if !useGTID {
		return "", 0, &apiError{400, "use_gtid 未开启：先在配置中心把 source.use_gtid 改为 true 并重启，管线会因位点缺 gtid_set 暂停，届时回到本页执行迁移"}
	}
	if !havePos {
		return "", 0, &apiError{400, "位点未初始化（etcd 无持久化位点），无需迁移"}
	}
	if pos.GTIDSet != "" {
		return "", 0, &apiError{400, "位点已含 gtid_set，无需迁移"}
	}
	if pos.Name == "" {
		return "", 0, &apiError{400, "位点缺 binlog 文件名，无法迁移"}
	}
	if mp.GTIDSet == "" {
		return "", 0, &apiError{400, "源库未返回 executed gtid_set（gtid_mode 未开启？）"}
	}
	if mp.Name != pos.Name {
		return "", 0, &apiError{400, fmt.Sprintf("位点文件已轮转（位点在 %s，master 尾部在 %s）：请先切回 use_gtid=false 让位点追平后再执行迁移", pos.Name, mp.Name)}
	}
	if mp.Pos > pos.Pos {
		gap = int64(mp.Pos - pos.Pos)
	}
	if gap > gtidMigrateMaxGap {
		return "", gap, &apiError{400, fmt.Sprintf("位点距 master 尾部 %d 字节（门槛 %d）：此窗口内未确认事务将被跳过，请先追平（切回 use_gtid=false 跑到 lag≈0）再迁移", gap, gtidMigrateMaxGap)}
	}
	return mp.GTIDSet, gap, nil
}

// planFilePosMigration 反向迁移决策（纯函数）：位点置最老保留 binlog 文件头，
// 接受整段重放（at-least-once + 下游幂等吸收重复）。
func planFilePosMigration(useGTID bool, havePos bool, pos model.Position, oldest string) (model.Position, *apiError) {
	if useGTID {
		return model.Position{}, &apiError{400, "use_gtid 仍为 true：先在配置中心改回 false 并重启，再执行反向迁移"}
	}
	if !havePos || pos.GTIDSet == "" {
		return model.Position{}, &apiError{400, "位点不含 gtid_set，无需反向迁移"}
	}
	if oldest == "" {
		return model.Position{}, &apiError{400, "源库 SHOW BINARY LOGS 为空，无法确定重放起点"}
	}
	pos.Name = oldest
	pos.Pos = 4 // binlog 魔数之后的第一个事件起点
	pos.GTIDSet = ""
	return pos, nil
}

// migrateGTID POST /api/v1/position/migrate-gtid：file:pos 存量位点 → GTID 基线
// （use_gtid 切换后管线因 ErrInvalidPosition 暂停的恢复路径，P3）。
func (h apiHandlers) migrateGTID(c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	var (
		pos     model.Position
		havePos bool
		mp      model.Position
		verr    *apiError
	)
	if h.UseGTID && h.MasterPos != nil {
		p, ok, err := h.Store.Load(c.Request.Context())
		if err != nil {
			respondErr(c, 500, err.Error())
			return
		}
		pos, havePos = p, ok
		mp, err = h.MasterPos(c.Request.Context())
		if err != nil {
			respondErr(c, 500, "读取源库尾部位点失败: "+err.Error())
			return
		}
	} else if !h.UseGTID {
		verr = &apiError{400, "use_gtid 未开启：先在配置中心把 source.use_gtid 改为 true 并重启，管线会因位点缺 gtid_set 暂停，届时回到本页执行迁移"}
	} else {
		respondErr(c, 500, "未装配源库位点采样")
		return
	}

	var gtidSet string
	var gap int64
	if verr == nil {
		gtidSet, gap, verr = planGTIDMigration(h.UseGTID, havePos, pos, mp)
	}
	audit(auditOrNil(h.Audit), c, auditPositionMigrateGTID, "migrate-gtid", apiErrToError(verr))
	if verr != nil {
		abort(c, verr.code, verr.msg)
		return
	}
	pos.GTIDSet = gtidSet
	if err := h.Store.Save(c.Request.Context(), pos); err != nil {
		respondErr(c, 500, "位点写入失败（lease 门槛未通过？）: "+err.Error())
		return
	}
	respondOK(c, gin.H{
		"name": pos.Name, "pos": pos.Pos, "gtid_set": pos.GTIDSet,
		"gap_bytes": gap, "next": "重启进程后增量将按 GTID 集合回放起点继续",
	})
}

// migrateFilePos POST /api/v1/position/migrate-filepos：GTID → file:pos（反向）。
func (h apiHandlers) migrateFilePos(c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	var (
		pos     model.Position
		havePos bool
		oldest  string
		verr    *apiError
	)
	if h.UseGTID {
		verr = &apiError{400, "use_gtid 仍为 true：先在配置中心改回 false 并重启，再执行反向迁移"}
	} else if h.OldestBinlog == nil {
		respondErr(c, 500, "未装配源库 binlog 清单采样")
		return
	} else {
		p, ok, err := h.Store.Load(c.Request.Context())
		if err != nil {
			respondErr(c, 500, err.Error())
			return
		}
		pos, havePos = p, ok
		oldest, err = h.OldestBinlog(c.Request.Context())
		if err != nil {
			respondErr(c, 500, "读取源库最老 binlog 失败: "+err.Error())
			return
		}
	}

	var newPos model.Position
	if verr == nil {
		newPos, verr = planFilePosMigration(h.UseGTID, havePos, pos, oldest)
	}
	audit(auditOrNil(h.Audit), c, auditPositionMigrateFilePos, "migrate-filepos", apiErrToError(verr))
	if verr != nil {
		abort(c, verr.code, verr.msg)
		return
	}
	if err := h.Store.Save(c.Request.Context(), newPos); err != nil {
		respondErr(c, 500, "位点写入失败（lease 门槛未通过？）: "+err.Error())
		return
	}
	respondOK(c, gin.H{
		"name": newPos.Name, "pos": newPos.Pos,
		"next": "重启进程；增量将从最老 binlog 头重放，下游幂等吸收重复",
	})
}
