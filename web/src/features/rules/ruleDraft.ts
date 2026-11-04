// 规则编辑器的纯逻辑（可单测）：服务端组形态 JSON ⇄ 编辑器草稿形态的转换、
// 客户端必填校验、exclude/mask 文本编解码、diff 摘要。
// 服务端校验（config.LoadFromRaw + 活体预检）是权威——这里只做提交前的
// 快速反馈，两边规则不一致时以服务端为准。
import type { PutRulesResult, RuleGroup } from '@/shared/api/types'

/** 编辑器草稿：单张表。mask/exclude 以文本形态编辑（列=值 逗号分隔）。 */
export interface TableDraft {
  table: string
  doStock: boolean
  orderBy: string
  targetDatabase: string
  excludeText: string
  maskText: string
}

/** 编辑器草稿：单个规则组。 */
export interface GroupDraft {
  schema: string
  ddlPolicy: 'auto' | 'pause' | 'ignore'
  targetDatabase: string
  sinks: { mysql: boolean; kafka: boolean; rocketmq: boolean }
  tables: TableDraft[]
}

/** 服务端规则组 → 草稿（sinks 对象/bool 双形态、mask 对象/字符串双形态归一）。 */
export function draftFromRules(rules: RuleGroup[]): GroupDraft[] {
  return rules.map((group) => ({
    schema: group.schema,
    ddlPolicy: normalizePolicy(group.ddl_policy),
    targetDatabase: group.target_database ?? '',
    sinks: {
      mysql: sinkEnabled(group.sinks?.mysql),
      kafka: sinkEnabled(group.sinks?.kafka),
      rocketmq: sinkEnabled(group.sinks?.rocketmq),
    },
    tables: (group.tables ?? []).map((t) => ({
      table: t.table,
      doStock: t.do_stock ?? false,
      orderBy: t.order_by_column ?? '',
      targetDatabase: t.target_database ?? '',
      excludeText: formatExclude(t.exclude_columns ?? []),
      maskText: formatMask(t.mask_columns ?? {}),
    })),
  }))
}

/** 草稿 → PUT body 的 rules（服务端契约形态；mask 走字符串简写）。 */
export function payloadFromDraft(drafts: GroupDraft[]): RuleGroup[] {
  return drafts.map((g) => ({
    schema: g.schema.trim(),
    ddl_policy: g.ddlPolicy,
    target_database: g.targetDatabase.trim() || undefined,
    sinks: {
      mysql: { enabled: g.sinks.mysql },
      kafka: { enabled: g.sinks.kafka },
      rocketmq: { enabled: g.sinks.rocketmq },
    },
    tables: g.tables.map((t) => ({
      table: t.table.trim(),
      do_stock: t.doStock,
      order_by_column: t.orderBy.trim() || undefined,
      target_database: t.targetDatabase.trim() || undefined,
      exclude_columns: parseExclude(t.excludeText).length > 0 ? parseExclude(t.excludeText) : undefined,
      mask_columns: Object.keys(parseMask(t.maskText)).length > 0 ? parseMask(t.maskText) : undefined,
    })),
  }))
}

/** 客户端快速校验（服务端全量校验是权威；返回错误清单）。 */
export function validateDraft(drafts: GroupDraft[]): string[] {
  const errors: string[] = []
  const seenSchema = new Set<string>()
  drafts.forEach((g, gi) => {
    const label = g.schema.trim() || `规则组 ${gi + 1}`
    if (!g.schema.trim()) errors.push(`规则组 ${gi + 1}：源库名（schema）必填`)
    else if (seenSchema.has(g.schema.trim().toLowerCase())) errors.push(`源库 ${g.schema} 重复（一组一库）`)
    else seenSchema.add(g.schema.trim().toLowerCase())
    if (!g.sinks.mysql && !g.sinks.kafka && !g.sinks.rocketmq) errors.push(`源库 ${label}：至少启用一个下游 sink`)
    if (g.tables.length === 0) errors.push(`源库 ${label}：tables 不能为空（白名单逐表列出）`)
    const seenTable = new Set<string>()
    g.tables.forEach((t, ti) => {
      const tlabel = t.table.trim() || `表 ${ti + 1}`
      if (!t.table.trim()) errors.push(`源库 ${label}：第 ${ti + 1} 张表表名必填`)
      else if (seenTable.has(t.table.trim().toLowerCase())) errors.push(`源库 ${label}：表 ${t.table} 重复`)
      else seenTable.add(t.table.trim().toLowerCase())
      if (t.doStock && !t.orderBy.trim()) errors.push(`源库 ${label}.${tlabel}：do_stock=true 需要配 order_by_column`)
    })
  })
  return errors
}

/** diff + 预检摘要（确认框文案）。 */
export function diffSummaryText(result: PutRulesResult): string {
  const parts: string[] = []
  if (result.added.length > 0) parts.push(`新增 ${result.added.length} 表`)
  if (result.removed.length > 0) parts.push(`移除 ${result.removed.length} 表`)
  if (result.modified.length > 0) parts.push(`修改 ${result.modified.length} 表`)
  return parts.length > 0 ? parts.join('，') : '无表级变化'
}

/** "a,b c" → ["a","b","c"]（逗号/空白分隔，去空）。 */
export function parseExclude(text: string): string[] {
  return text
    .split(/[,，\s]+/)
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
}

export function formatExclude(list: string[]): string {
  return list.join(', ')
}

/** "phone=phone, name=truncate" → {phone:"phone", name:"truncate"}（=/: 分隔，含全角）。 */
export function parseMask(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const line of text.split(/[,，\n]+/)) {
    const item = line.trim()
    if (!item) continue
    const m = item.match(/^([^\s=＝:：]+)\s*[=＝:：]\s*(.+)$/)
    if (m) out[m[1]!.trim()] = m[2]!.trim()
  }
  return out
}

export function formatMask(map: Record<string, string | { type: string }>): string {
  return Object.entries(map)
    .map(([col, spec]) => `${col}=${typeof spec === 'string' ? spec : spec.type}`)
    .join(', ')
}

function normalizePolicy(policy?: string): 'auto' | 'pause' | 'ignore' {
  if (policy === 'auto' || policy === 'ignore') return policy
  return 'pause'
}

function sinkEnabled(spec: boolean | { enabled: boolean } | undefined): boolean {
  if (typeof spec === 'boolean') return spec
  return spec?.enabled ?? false
}
