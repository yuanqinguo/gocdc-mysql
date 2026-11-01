package api

import (
	"github.com/gin-gonic/gin"

	"gocdc/internal/pipeline"
)

// ddlEvents GET /api/v1/ddl-events：DDL 审批挂起列表（schema/table 均空 =
// 全部挂起记录）。
func (h apiHandlers) ddlEvents(c *gin.Context) {
	if h.DDLLister == nil {
		respondOK(c, []pipeline.DDLRecord{})
		return
	}
	recs, err := h.DDLLister.PendingByTable(c.Request.Context(),
		c.Query("schema"), c.Query("table"))
	if err != nil {
		respondErr(c, 500, err.Error())
		return
	}
	if recs == nil {
		recs = []pipeline.DDLRecord{}
	}
	respondOK(c, recs)
}

// settleDDL DDL 审批（approve/skip 共用骨架，13 §3）：confirm 门禁 + 审计。
func (h apiHandlers) settleDDL(decision string) gin.HandlerFunc {
	what, note := auditDDLApprove, noteWebApprove
	if decision == pipeline.DecisionSkip {
		what, note = auditDDLSkip, noteWebSkip
	}
	return func(c *gin.Context) {
		if !requireConfirm(c) {
			return
		}
		err := h.Gate.Settle(c.Request.Context(), c.Param(paramEventID), decision, note)
		audit(auditOrNil(h.Audit), c, what, c.Param(paramEventID), err)
		respondResult(c, err)
	}
}
