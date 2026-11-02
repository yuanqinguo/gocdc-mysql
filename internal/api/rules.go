package api

import (
	"strings"

	"github.com/gin-gonic/gin"

	"gocdc/internal/model"
)

// effectiveRulesOf GET 源作用域规则编译产物（每表合并生效视图）。
func effectiveRulesOf(sd *SourceDeps, c *gin.Context) {
	views := sd.Rules.EffectiveRules()
	out := make([]EffectiveRule, 0, len(views))
	for _, v := range views {
		out = append(out, EffectiveRule{
			Key: v.Key, Schema: v.Schema, Table: v.Table,
			TargetDatabase: v.TargetDatabase, DoStock: v.DoStock,
			DDLPolicy: v.DDLPolicy, OrderByColumn: v.OrderByColumn,
			ExcludeColumns: v.ExcludeColumns, MaskColumns: summarizeMasks(v.MaskColumns),
			Sinks: EffectiveRuleSinks{
				MySQL: v.Sinks.MySQL, Kafka: v.Sinks.Kafka, RocketMQ: v.Sinks.RocketMQ,
				KafkaTopic: v.Sinks.KafkaTopic, RocketMQTopic: v.Sinks.RocketMQTopic, RocketMQTag: v.Sinks.RocketMQTag,
			},
		})
	}
	respondOK(c, out)
}

// sourceEffectiveRules GET /api/v1/sources/:sid/rules/effective。
func (h apiHandlers) sourceEffectiveRules(c *gin.Context) {
	if sd := h.resolveSource(c); sd != nil {
		effectiveRulesOf(sd, c)
	}
}

// legacyEffectiveRules 兼容层 GET /rules/effective。
func (h apiHandlers) legacyEffectiveRules(c *gin.Context, sd *SourceDeps) { effectiveRulesOf(sd, c) }

func summarizeMasks(in map[string]model.MaskSpec) map[string]EffectiveMaskSpec {
	out := make(map[string]EffectiveMaskSpec, len(in))
	for column, spec := range in {
		out[column] = EffectiveMaskSpec{Type: spec.Type, Keep: spec.Keep}
	}
	return out
}

// restockOf 源作用域全量重跑（POST restock，源作用域）：只清该源标记，
// 重启后仅该源该表重扫（多源 PRD 05 §4-4）。
func (h apiHandlers) restockOf(sd *SourceDeps, c *gin.Context, schema, table string) {
	if !requireConfirm(c) {
		return
	}
	if schema == "" || table == "" {
		abort(c, 400, "schema/table 必填")
		return
	}
	if sd.StockReset == nil {
		// 非 leader 节点/源流程未启动：显式 409 语义（PRD 06 §1.5——原为
		// stockSvc nil panic 被 Recovery 兜成 500）
		abort(c, 409, "本节点当前不承担该源管线（热备/未当选/源未启动），请向 leader 节点发起")
		return
	}
	detail := schema + "." + table
	err := sd.StockReset(c.Request.Context(), schema, table)
	auditSource(auditOrNil(h.Audit), c, sd.ID, auditRuleRestock, detail, err)
	respondResult(c, err)
}

// sourceRestock POST /api/v1/sources/:sid/rules/by-schema/:schema/:table/restock。
func (h apiHandlers) sourceRestock(c *gin.Context) {
	if sd := h.resolveSource(c); sd == nil {
		return
	} else {
		h.restockOf(sd, c, c.Param(paramSchema), c.Param(paramTable))
	}
}

// legacyRestock 是兼容旧客户端的简写路由；schema 可含点，table 按配置契约
// 不含点，因此从最后一个点拆分。
func (h apiHandlers) legacyRestock(c *gin.Context, sd *SourceDeps) {
	key := c.Param(paramTableKey)
	separator := strings.LastIndex(key, ".")
	if separator <= 0 || separator == len(key)-1 {
		abort(c, 400, "key 须为 schema.table")
		return
	}
	h.restockOf(sd, c, key[:separator], key[separator+1:])
}

// legacyRestockBySchemaTable 是无歧义的管理端路由，支持库名包含点号。
func (h apiHandlers) legacyRestockBySchemaTable(c *gin.Context, sd *SourceDeps) {
	h.restockOf(sd, c, c.Param(paramSchema), c.Param(paramTable))
}
