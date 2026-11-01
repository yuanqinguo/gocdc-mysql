package api

import (
	"github.com/gin-gonic/gin"
)

// verify POST /api/v1/verify：抽样对账（Web 形态——运行中进程自带
// 源/目标连接，Docker 部署无需进节点执行 CLI）。大表扫描代价 → 写组 +
// confirm 门禁。数据差异（Mismatch>0）走 200——差异在 data 里而非报 500。
func (h apiHandlers) verify(c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	if h.Verify == nil {
		respondErr(c, 500, "verify 未装配")
		return
	}
	var body VerifyRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		abort(c, 400, err.Error())
		return
	}
	if body.Database == "" || body.Table == "" {
		abort(c, 400, "database/table 必填")
		return
	}
	if body.PK == "" {
		body.PK = DefaultPK
	}
	res, err := h.Verify(c.Request.Context(), body.Database, body.Table, body.PK, body.Sample)
	audit(auditOrNil(h.Audit), c, auditVerifyRun, body.Database+"."+body.Table, err)
	if err != nil {
		respondErrData(c, 500, err.Error(), res)
		return
	}
	respondOK(c, res)
}
