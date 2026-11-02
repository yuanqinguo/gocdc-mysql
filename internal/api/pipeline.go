package api

import (
	"errors"

	"github.com/gin-gonic/gin"
)

// pausePipelineOf 源作用域人工暂停（13 §3 逐源版）——cmd 层闭包写该源 etcd
// 持久标志并优雅停该源管线（保持 Leader 与 Web）；跨重启保持暂停直到显式
// resume。非 leader 节点/源未承担返回错误。其余源不受影响。
func (h apiHandlers) pausePipelineOf(sd *SourceDeps, c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	var err error
	if sd.PausePipeline != nil {
		err = sd.PausePipeline(c.Request.Context())
	} else {
		err = errors.New("暂停管线未装配")
	}
	auditSource(auditOrNil(h.Audit), c, sd.ID, auditPipelinePause, "", err)
	respondResult(c, err)
}

// sourcePausePipeline POST /api/v1/sources/:sid/pipeline/pause。
func (h apiHandlers) sourcePausePipeline(c *gin.Context) {
	if sd := h.resolveSource(c); sd != nil {
		h.pausePipelineOf(sd, c)
	}
}


// resumePipelineOf 源作用域恢复人工暂停——删该源持久标志并从该源 etcd 位点
// 重启全量/增量流程。
func (h apiHandlers) resumePipelineOf(sd *SourceDeps, c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	var err error
	if sd.ResumePipeline != nil {
		err = sd.ResumePipeline(c.Request.Context())
	} else {
		err = errors.New("恢复管线未装配")
	}
	auditSource(auditOrNil(h.Audit), c, sd.ID, auditPipelineResume, "", err)
	respondResult(c, err)
}

// sourceResumePipeline POST /api/v1/sources/:sid/pipeline/resume。
func (h apiHandlers) sourceResumePipeline(c *gin.Context) {
	if sd := h.resolveSource(c); sd != nil {
		h.resumePipelineOf(sd, c)
	}
}

