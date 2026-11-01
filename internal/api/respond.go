package api

import (
	"github.com/gin-gonic/gin"
)

// Response /api/v1 统一出参信封（13 §3）：成功 code=0；失败 code=HTTP 状态
// 码 + message。data 恒输出（无数据为 null）；message 仅失败与写成功（ok）
// 时出现——只读成功为 {"code":0,"data":...}。
type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data"`
}

// respondOK 只读端点成功出参。
func respondOK(c *gin.Context, data any) {
	c.JSON(200, Response{Code: 0, Data: data})
}

// respondDone 写操作成功出参（message=ok）。
func respondDone(c *gin.Context) {
	c.JSON(200, Response{Code: 0, Message: "ok"})
}

// respondErr 业务失败（连不上/未装配/参数拒绝等，HTTP 4xx/5xx）。
func respondErr(c *gin.Context, code int, msg string) {
	c.JSON(code, Response{Code: code, Message: msg})
}

// respondErrData 失败但携带部分数据（verify：数据差异不是工具故障，明细
// 放 data 返回而非吞掉）。
func respondErrData(c *gin.Context, code int, msg string, data any) {
	c.JSON(code, Response{Code: code, Message: msg, Data: data})
}

// respondResult 写操作统一收尾：err != nil → 500 + message；否则 200 ok。
func respondResult(c *gin.Context, err error) {
	if err != nil {
		respondErr(c, 500, err.Error())
		return
	}
	respondDone(c)
}

// abort 门禁/鉴权拦截：输出信封并终止后续 handler。
func abort(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(code, Response{Code: code, Message: msg})
}
