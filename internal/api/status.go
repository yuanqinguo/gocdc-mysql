package api

import (
	"time"

	"github.com/gin-gonic/gin"

	"gocdc/internal/elector"
)

// status GET /api/v1/status：角色三态 + 进程标识 + 全部源摘要数组
//（多源 PRD 06 §1.2；管线运行态以 sources[] 为准）。
func (h apiHandlers) status(c *gin.Context) {
	d := StatusData{
		NodeID:        h.NodeID,
		Version:       h.Version,
		Cluster:       h.Cluster,
		ConfigSource:  h.ConfigSrc,
		UptimeSeconds: int64(time.Since(h.StartedAt).Seconds()),
		Sources:       []SourceStatus{},
	}
	// 全部源摘要（cmd 层闭包；nil = 未装配，空数组诚实口径）
	if h.SourceStatuses != nil {
		if ss := h.SourceStatuses(); ss != nil {
			d.Sources = ss
		}
	}
	// role 三态：Elector 缺席 = single（ha.enabled: false）；在选 = follower/leader
	if h.Elector == nil {
		d.Role = RoleSingle
		respondOK(c, d)
		return
	}
	d.Term = h.Elector.Term()
	d.Leader = h.Elector.LeaderPtr()
	if h.Elector.Role() == elector.RoleLeader {
		d.Role = RoleLeader
	} else {
		d.Role = RoleFollower
	}
	respondOK(c, d)
}

// sourceStatus GET /api/v1/sources/:sid/status：单源完整管线状态
//（= 现行 status 的管线部分 + 该源位点详情）。
func (h apiHandlers) sourceStatus(c *gin.Context) {
	sd := h.resolveSource(c)
	if sd == nil {
		return
	}
	out := SourceStatus{ID: sd.ID, PipelineState: PipeIdle, UseGTID: sd.UseGTID, Phase: "ready"}
	if h.SourceStatuses != nil {
		for _, s := range h.SourceStatuses() {
			if s.ID == sd.ID {
				out = s
				break
			}
		}
	}
	respondOK(c, out)
}

// config GET /api/v1/config：打码后的生效配置（装配层负责打码）。
func (h apiHandlers) config(c *gin.Context) {
	c.Data(200, "application/json", h.ConfigJSON)
}
