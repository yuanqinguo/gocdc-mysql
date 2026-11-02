package api

import (
	"github.com/gin-gonic/gin"
)

// listSources GET /api/v1/sources：源清单统计（聚合总览页数据源，
// 多源 PRD 06 §1.2——id、源/目标地址、规则/表数、sink 集、状态摘要）。
func (h apiHandlers) listSources(c *gin.Context) {
	if h.SourceList == nil {
		respondOK(c, []SourceSummary{})
		return
	}
	list := h.SourceList()
	if list == nil {
		list = []SourceSummary{}
	}
	respondOK(c, list)
}
