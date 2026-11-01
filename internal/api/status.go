package api

import (
	"time"

	"github.com/gin-gonic/gin"

	"gocdc/internal/elector"
)

// status GET /api/v1/status：角色三态 + 进程标识 + 管线运行状态。
func (h apiHandlers) status(c *gin.Context) {
	d := StatusData{
		NodeID:        h.NodeID,
		Version:       h.Version,
		Cluster:       h.Cluster,
		ConfigSource:  h.ConfigSrc,
		UptimeSeconds: int64(time.Since(h.StartedAt).Seconds()),
		PipelineState: PipeIdle,
		UseGTID:       h.UseGTID,
	}
	// 管线状态（装配层闭包；nil = 未装配，保持 idle——未参与同步的诚实口径）
	if h.Pipeline != nil {
		ps := h.Pipeline()
		d.PipelineState = ps.State
		d.PipelineError = ps.Error
		d.PipelineSince = ps.Since
		d.PausedByOp = ps.PausedByOp
		if ps.HasLag {
			lag := ps.EventLag
			d.EventLagSeconds = &lag
		}
		if ps.HasCatchup {
			lagBytes := ps.MasterLag
			up := lagBytes == 0
			d.MasterLagBytes = &lagBytes
			d.CaughtUp = &up
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

// config GET /api/v1/config：打码后的生效配置（装配层负责打码）。
func (h apiHandlers) config(c *gin.Context) {
	c.Data(200, "application/json", h.ConfigJSON)
}
