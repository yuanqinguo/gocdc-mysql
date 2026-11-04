import type { RuleGroup, RuleTable } from '@/shared/api/types'

// RulesManage 的纯逻辑（无 UI 依赖，可单测）：草稿组的 sink 形态保留、表清单
// 取用、本地 diff 摘要。与 ruleDraft.ts 同一「纯逻辑可单测」模式。

export const SINK_KEYS = ['mysql', 'kafka', 'rocketmq'] as const
export type SinkKey = (typeof SINK_KEYS)[number]

export function cloneGroups(rules: RuleGroup[]): RuleGroup[] {
  return rules.map((g) => ({ ...g, tables: (g.tables ?? []).map((t) => ({ ...t })) }))
}

export function tablesOf(g: RuleGroup): RuleTable[] {
  return Array.isArray(g.tables) ? g.tables : []
}

export function sinkOn(g: RuleGroup, key: SinkKey): boolean {
  const v = (g.sinks ?? {})[key]
  if (v === true) return true
  // 对象形态（{enabled,topic,tag} 覆盖）也算启用——编辑时须原样保留
  if (v && typeof v === 'object' && v.enabled === true) return true
  return false
}

/** 开 = 保留原形态（bool true 或对象覆盖原样带回，topic/tag 不丢）；关 = false。 */
export function keepSink(
  orig: RuleGroup['sinks'],
  key: SinkKey,
  on: boolean,
): boolean | { enabled: boolean; topic?: string; tag?: string } {
  if (!on) return false
  return orig?.[key] ?? true
}

/** 本地 diff 摘要（保存确认框文案用；与 ruleDraft.diffSummaryText 同口径，但输入是草稿组）。 */
export function diffSummaryTextOf(groups: RuleGroup[], original: RuleGroup[]): string {
  const keyOf = (g: { schema?: string }, t: { table?: string }) => `${g.schema ?? ''}.${t.table ?? ''}`
  const tableIndex = (rules: RuleGroup[]) => {
    const map = new Map<string, string>()
    for (const g of rules) for (const t of tablesOf(g)) map.set(keyOf(g, t), JSON.stringify(t))
    return map
  }
  const before = tableIndex(original)
  const after = tableIndex(groups)
  let added = 0
  let removed = 0
  let modified = 0
  for (const [key, value] of after) {
    if (!before.has(key)) added += 1
    else if (before.get(key) !== value) modified += 1
  }
  for (const key of before.keys()) if (!after.has(key)) removed += 1
  const parts: string[] = []
  if (added > 0) parts.push(`新增 ${added} 表`)
  if (removed > 0) parts.push(`移除 ${removed} 表`)
  if (modified > 0) parts.push(`修改 ${modified} 表`)
  return parts.length > 0 ? parts.join('，') : '无表级变化'
}
