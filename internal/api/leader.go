package api

import (
	"github.com/gin-gonic/gin"
)

// resign POST /api/v1/leader/resign：主动让位（触发重选举；单节点模式
// Resign 未装配时为幂等空操作）。
func (h apiHandlers) resign(c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	if h.Resign != nil {
		h.Resign()
	}
	audit(auditOrNil(h.Audit), c, auditLeaderResign, "", nil)
	respondResult(c, nil)
}
