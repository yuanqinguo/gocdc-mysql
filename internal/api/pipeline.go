package api

import (
	"errors"

	"github.com/gin-gonic/gin"
)

// pausePipeline POST /api/v1/pipeline/pause：人工暂停管线（13 §3）——
// cmd 层闭包写 etcd 持久标志并优雅停管线（保持 Leader 与 Web）；跨重启
// 保持暂停直到显式 resume。非 leader 节点返回错误。
func (h apiHandlers) pausePipeline(c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	var err error
	if h.PausePipeline != nil {
		err = h.PausePipeline(c.Request.Context())
	} else {
		err = errors.New("暂停管线未装配")
	}
	audit(auditOrNil(h.Audit), c, auditPipelinePause, "", err)
	respondResult(c, err)
}

// resumePipeline POST /api/v1/pipeline/resume：恢复人工暂停的管线——
// 删持久标志并从 etcd 位点重启全量/增量流程。
func (h apiHandlers) resumePipeline(c *gin.Context) {
	if !requireConfirm(c) {
		return
	}
	var err error
	if h.ResumePipeline != nil {
		err = h.ResumePipeline(c.Request.Context())
	} else {
		err = errors.New("恢复管线未装配")
	}
	audit(auditOrNil(h.Audit), c, auditPipelineResume, "", err)
	respondResult(c, err)
}
