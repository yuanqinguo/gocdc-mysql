package api

import (
	"github.com/gin-gonic/gin"

	"gocdc/internal/pipeline"
)

// ddlEventsOf GET 源作用域 DDL 审批挂起列表（schema/table 均空 = 全部挂起记录）。
func ddlEventsOf(sd *SourceDeps, c *gin.Context) {
	if sd.DDLLister == nil {
		respondOK(c, []pipeline.DDLRecord{})
		return
	}
	recs, err := sd.DDLLister.PendingByTable(c.Request.Context(),
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
		if sd == nil || sd.DDLLister == nil {
			continue
		}
		recs, err := sd.DDLLister.PendingByTable(c.Request.Context(),
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
		err := sd.Gate.Settle(c.Request.Context(), c.Param(paramEventID), decision, note)
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

// legacySettleDDL 兼容层 POST /ddl/:event_id/approve|skip。
func (h apiHandlers) legacySettleDDL(decision string) func(c *gin.Context, sd *SourceDeps) {
	return func(c *gin.Context, sd *SourceDeps) {
		h.settleDDLOf(sd, decision)(c)
	}
}
