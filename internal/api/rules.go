package api

import (
	"strings"

	"github.com/gin-gonic/gin"

	"gocdc/internal/model"
)

// effectiveRules GET /api/v1/rules/effective：规则编译产物（每表合并
// 生效视图）。
func (h apiHandlers) effectiveRules(c *gin.Context) {
	views := h.Rules.EffectiveRules()
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

func summarizeMasks(in map[string]model.MaskSpec) map[string]EffectiveMaskSpec {
	out := make(map[string]EffectiveMaskSpec, len(in))
	for column, spec := range in {
		out[column] = EffectiveMaskSpec{Type: spec.Type, Keep: spec.Keep}
	}
	return out
}

// restock 是兼容旧客户端的简写路由；schema 可含点，table 按配置契约不含点，
// 因此从最后一个点拆分。
func (h apiHandlers) restock(c *gin.Context) {
	key := c.Param(paramTableKey)
	separator := strings.LastIndex(key, ".")
	if separator <= 0 || separator == len(key)-1 {
		abort(c, 400, "key 须为 schema.table")
		return
	}
	h.restockTable(c, key[:separator], key[separator+1:])
}

// restockBySchemaTable 是无歧义的管理端路由，支持库名包含点号。
func (h apiHandlers) restockBySchemaTable(c *gin.Context) {
	h.restockTable(c, c.Param(paramSchema), c.Param(paramTable))
}

func (h apiHandlers) restockTable(c *gin.Context, schema, table string) {
	if !requireConfirm(c) {
		return
	}
	if schema == "" || table == "" {
		abort(c, 400, "schema/table 必填")
		return
	}
	if h.StockReset == nil {
		respondErr(c, 500, "restock 未装配")
		return
	}
	detail := schema + "." + table
	err := h.StockReset(c.Request.Context(), schema, table)
	audit(auditOrNil(h.Audit), c, auditRuleRestock, detail, err)
	respondResult(c, err)
}
