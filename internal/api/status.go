package api

import (
	"time"

	"github.com/gin-gonic/gin"

	"gocdc/internal/elector"
)

// status GET /api/v1/status：角色三态 + 进程标识 + 全部源摘要数组
//（多源 PRD 06 §1.2）。顶层管线字段为兼容镜像：单源部署镜像唯一源
//（行为与旧版一致），多源部署置空（idle）以 sources[] 为准。
func (h apiHandlers) status(c *gin.Context) {
	d := StatusData{
		NodeID:        h.NodeID,
		Version:       h.Version,
		Cluster:       h.Cluster,
		ConfigSource:  h.ConfigSrc,
		UptimeSeconds: int64(time.Since(h.StartedAt).Seconds()),
		PipelineState: PipeIdle,
		Sources:       []SourceStatus{},
	}
	// 全部源摘要（cmd 层闭包；nil = 未装配，空数组诚实口径）
	if h.SourceStatuses != nil {
		if ss := h.SourceStatuses(); ss != nil {
			d.Sources = ss
		}
	}
	// 兼容镜像：单源部署把唯一源摘要镜像到顶层管线字段（外部脚本/监控探针
	// 的弃用窗口 ≥ 2 个小版本）；多源置空以 sources[] 为准
	if sd := h.singleSource(); sd != nil && len(d.Sources) == 1 {
		mirrorSourceStatus(&d, d.Sources[0])
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

// mirrorSourceStatus 单源摘要 → 顶层兼容镜像字段（旧脚本/探针契约不变）。
func mirrorSourceStatus(d *StatusData, s SourceStatus) {
	d.PipelineState = s.PipelineState
	d.PipelineError = s.PipelineError
	d.PipelineSince = s.PipelineSince
	d.PausedByOp = s.PausedByOp
	d.EventLagSeconds = s.EventLagSeconds
	d.CaughtUp = s.CaughtUp
	d.MasterLagBytes = s.MasterLagBytes
	d.UseGTID = s.UseGTID
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
