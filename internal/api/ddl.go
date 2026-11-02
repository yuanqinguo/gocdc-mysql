package api

import (
	"github.com/gin-gonic/gin"

	"gocdc/internal/pipeline"
)

// ddlEventsOf GET 源作用域 DDL 审批挂起列表（schema/table 均空 = 全部挂起记录）。
func ddlEventsOf(sd *SourceDeps, c *gin.Context) {
	lister := sd.DDLLister()
	if lister == nil { // 未装配（装配中/失败）：空列表诚实口径（列表页不因单源未就绪 409）
		respondOK(c, []pipeline.DDLRecord{})
		return
	}
	recs, err := lister.PendingByTable(c.Request.Context(),
		c.Query("schema"), c.Query("table"))
	if err != nil {
		respondErr(c, 500, err.Error())
		return
	}
	if recs == nil {
		recs = []pipeline.DDLRecord{}
	}
	for i := range recs {
		if recs[i].Source == "" { // 旧记录无 source 字段：列表侧按所属源补齐
			recs[i].Source = sd.ID
		}
	}
	respondOK(c, recs)
}

// sourceDDLEvents GET /api/v1/sources/:sid/ddl-events。
func (h apiHandlers) sourceDDLEvents(c *gin.Context) {
	if sd := h.resolveSource(c); sd != nil {
		ddlEventsOf(sd, c)
	}
}

// ddlEventsAll GET /api/v1/ddl-events（cluster 级）：全部源 pending 合并
//（记录带 source 字段，按源分组排序——聚合页/角标数据源，多源 PRD 06 §1.2）。
func (h apiHandlers) ddlEventsAll(c *gin.Context) {
	out := []pipeline.DDLRecord{}
	for _, sid := range h.SourceOrder {
		sd := h.Sources[sid]
		if sd == nil {
			continue
		}
		lister := sd.DDLLister()
		if lister == nil {
			continue
		}
		recs, err := lister.PendingByTable(c.Request.Context(),
			c.Query("schema"), c.Query("table"))
		if err != nil {
			respondErr(c, 500, sid+": "+err.Error())
			return
		}
		for i := range recs {
			if recs[i].Source == "" {
				recs[i].Source = sid
			}
			out = append(out, recs[i])
		}
	}
	respondOK(c, out)
}

// settleDDLOf DDL 审批（approve/skip 共用骨架，13 §3，源作用域）：
// confirm 门禁 + 审计（带 source 字段）。
func (h apiHandlers) settleDDLOf(sd *SourceDeps, decision string) func(c *gin.Context) {
	what, note := auditDDLApprove, noteWebApprove
	if decision == pipeline.DecisionSkip {
		what, note = auditDDLSkip, noteWebSkip
	}
	return func(c *gin.Context) {
		if !requireConfirm(c) {
			return
		}
		gate := sd.Gate()
		if gate == nil { // 源数据面未装配（装配中/失败）：显式报错不 panic
			abort(c, 409, "该源组件未就绪（装配中或装配失败，稍后重试或查看日志）")
			return
		}
		err := gate.Settle(c.Request.Context(), c.Param(paramEventID), decision, note)
		auditSource(auditOrNil(h.Audit), c, sd.ID, what, c.Param(paramEventID), err)
		respondResult(c, err)
	}
}

// sourceSettleDDL POST /api/v1/sources/:sid/ddl/:event_id/approve|skip。
func (h apiHandlers) sourceSettleDDL(decision string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if sd := h.resolveSource(c); sd != nil {
			h.settleDDLOf(sd, decision)(c)
		}
	}
}

