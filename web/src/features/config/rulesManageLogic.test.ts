import { describe, expect, it } from 'vitest'
import type { RuleGroup } from '@/shared/api/types'
import { SINK_KEYS, diffSummaryTextOf, keepSink, sinkOn, tablesOf } from './rulesManageLogic'

// RulesManage 纯逻辑（rulesManageLogic.ts）单测：sink 双形态保留（bool 简写 /
// {enabled,topic,tag} 对象覆盖）与本地 diff 摘要——这两处丢语义 = topic 配置
// 静默丢失 / 确认框误导，是编辑器的核心不变式。

const g = (over: Partial<RuleGroup>): RuleGroup => ({ schema: 'db1', tables: [], ...over })

describe('sinkOn', () => {
  it('bool 简写与对象形态都算启用', () => {
    expect(sinkOn(g({ sinks: { mysql: true } }), 'mysql')).toBe(true)
    expect(sinkOn(g({ sinks: { kafka: { enabled: true, topic: 't' } } }), 'kafka')).toBe(true)
  })
  it('关/缺省/对象 enabled=false 都不算启用', () => {
    expect(sinkOn(g({ sinks: { mysql: false } }), 'mysql')).toBe(false)
    expect(sinkOn(g({}), 'mysql')).toBe(false)
    expect(sinkOn(g({ sinks: { mysql: { enabled: false, topic: 't' } } }), 'mysql')).toBe(false)
    expect(sinkOn(g({ sinks: {} }), 'rocketmq')).toBe(false)
  })
})

describe('keepSink（开=原样带回防 topic 丢失；关=false）', () => {
  it('关恒为 false——对象覆盖也被显式关闭', () => {
    expect(keepSink({ mysql: { enabled: true, topic: 't' } }, 'mysql', false)).toBe(false)
    expect(keepSink(undefined, 'mysql', false)).toBe(false)
  })
  it('开且无原值 = true（bool 简写）', () => {
    expect(keepSink(undefined, 'kafka', true)).toBe(true)
    expect(keepSink({}, 'kafka', true)).toBe(true)
  })
  it('开且原值 bool = true', () => {
    expect(keepSink({ mysql: true }, 'mysql', true)).toBe(true)
  })
  it('开且原值对象 = 原对象原样带回（topic/tag 不丢）', () => {
    const orig = { enabled: true, topic: 'cdc_custom', tag: 'tag1' }
    expect(keepSink({ kafka: orig }, 'kafka', true)).toBe(orig)
  })
})

describe('tablesOf', () => {
  it('数组透传；缺失/非数组兜底空数组', () => {
    const t = [{ table: 't1', do_stock: true }]
    expect(tablesOf(g({ tables: t }))).toBe(t)
    expect(tablesOf(g({ tables: undefined })).length).toBe(0)
  })
})

describe('diffSummaryTextOf', () => {
  const t1 = { table: 't1', do_stock: true, order_by_column: 'id' }
  const t2 = { table: 't2', do_stock: false }
  const base: RuleGroup[] = [g({ schema: 'db1', tables: [t1, t2] })]

  it('增/删/改计数', () => {
    const next: RuleGroup[] = [g({
      schema: 'db1',
      tables: [
        { ...t1, do_stock: false }, // 修改
        t2, // 不变
        { table: 't3', do_stock: true }, // 新增
      ],
    })]
    expect(diffSummaryTextOf(next, base)).toBe('新增 1 表，修改 1 表')
    expect(diffSummaryTextOf([g({ schema: 'db1', tables: [t1] })], base)).toBe('移除 1 表')
  })
  it('表重排不算变化；整组移除计入移除', () => {
    expect(diffSummaryTextOf([g({ schema: 'db1', tables: [t2, t1] })], base)).toBe('无表级变化')
    expect(diffSummaryTextOf([], base)).toBe('移除 2 表')
  })
  it('多组混合计数；组级字段变化（sinks/ddl_policy）不算表级', () => {
    const next: RuleGroup[] = [
      g({ schema: 'db1', tables: [t1, t2], ddl_policy: 'auto' }), // 组级变，表不变
      g({ schema: 'db2', tables: [{ table: 'x', do_stock: true }] }), // 新组
    ]
    expect(diffSummaryTextOf(next, base)).toBe('新增 1 表')
  })
})

describe('SINK_KEYS', () => {
  it('三下游齐全（新增源库默认开关的遍历基底）', () => {
    expect(SINK_KEYS).toEqual(['mysql', 'kafka', 'rocketmq'])
  })
})
