package api

import (
	"github.com/gin-gonic/gin"
)

// verifyOf 源作用域抽样对账（POST verify，Web 形态——运行中进程自带该源
// 源/目标连接，Docker 部署无需进节点执行 CLI）。大表扫描代价 → 写组 +
// confirm 门禁。数据差异（Mismatch>0）走 200——差异在 data 里而非报 500。
func (h apiHandlers) verifyOf(sd *SourceDeps, c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	if sd.Verify == nil {
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
	res, err := sd.Verify(c.Request.Context(), body.Database, body.Table, body.PK, body.Sample)
	auditSource(auditOrNil(h.Audit), c, sd.ID, auditVerifyRun, body.Database+"."+body.Table, err)
	if err != nil {
		respondErrData(c, 500, err.Error(), res)
		return
	}
	respondOK(c, res)
}

// sourceVerify POST /api/v1/sources/:sid/verify。
func (h apiHandlers) sourceVerify(c *gin.Context) {
	if sd := h.resolveSource(c); sd != nil {
		h.verifyOf(sd, c)
	}
}

// legacyVerify 兼容层 POST /verify。
func (h apiHandlers) legacyVerify(c *gin.Context, sd *SourceDeps) { h.verifyOf(sd, c) }
