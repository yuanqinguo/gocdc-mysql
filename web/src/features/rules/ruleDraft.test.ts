import { describe, expect, it } from 'vitest'
import {
  diffSummaryText,
  draftFromRules,
  formatExclude,
  formatMask,
  parseExclude,
  parseMask,
  payloadFromDraft,
  validateDraft,
} from './ruleDraft'
import type { PutRulesResult, RuleGroup } from '@/shared/api/types'

const serverRules: RuleGroup[] = [
  {
    schema: 'edu.mall.admin',
    sinks: { mysql: { enabled: true }, kafka: { enabled: false } },
    ddl_policy: 'auto',
    target_database: 'merge.admin',
    tables: [
      { table: 'users', order_by_column: 'id', do_stock: true },
      { table: 'logs', do_stock: false, exclude_columns: ['ext', 'tmp'], mask_columns: { phone: 'phone', name: { type: 'truncate', keep: 4 } } },
    ],
  },
]

describe('draftFromRules', () => {
  it('归一 sinks 对象/bool 双形态与 mask 对象形态', () => {
    const draft = draftFromRules([
      { schema: 'db1', sinks: { mysql: true }, tables: [{ table: 't1', do_stock: true, order_by_column: 'id' }] },
      ...serverRules,
    ])
    expect(draft[0]!.sinks.mysql).toBe(true)
    expect(draft[0]!.sinks.kafka).toBe(false)
    expect(draft[1]!.sinks.mysql).toBe(true)
    expect(draft[1]!.ddlPolicy).toBe('auto')
    expect(draft[1]!.targetDatabase).toBe('merge.admin')
    // mask 对象形态 → col=type 文本
    expect(draft[1]!.tables[1]!.maskText).toBe('phone=phone, name=truncate')
    expect(draft[1]!.tables[1]!.excludeText).toBe('ext, tmp')
    // 省略 ddl_policy → pause 安全默认
    expect(draft[0]!.ddlPolicy).toBe('pause')
  })
})

describe('payloadFromDraft', () => {
  const mixed = [...draftFromRules([{ schema: 'db1', sinks: { mysql: true }, tables: [{ table: 't1', do_stock: true, order_by_column: 'id' }] }]), ...draftFromRules(serverRules)]
  it('草稿 → 服务端契约形态（空值收敛为 undefined；mask 走字符串简写）', () => {
    const payload = payloadFromDraft(mixed)
    expect(payload[1]!.schema).toBe('edu.mall.admin')
    expect(payload[1]!.ddl_policy).toBe('auto')
    expect(payload[1]!.sinks?.mysql).toEqual({ enabled: true })
    expect(payload[1]!.tables?.[1]!.mask_columns).toEqual({ phone: 'phone', name: 'truncate' })
    expect(payload[1]!.tables?.[1]!.exclude_columns).toEqual(['ext', 'tmp'])
    expect(payload[1]!.tables?.[0]!.mask_columns).toBeUndefined()
    expect(payload[1]!.tables?.[0]!.order_by_column).toBe('id')
  })
  it('往返无损：draft → payload → draft', () => {
    const once = draftFromRules(mixed)
    const back = draftFromRules(payloadFromDraft(once))
    expect(back[1]!.tables[1]!.maskText).toBe(once[1]!.tables[1]!.maskText)
    expect(back[1]!.tables[1]!.excludeText).toBe(once[1]!.tables[1]!.excludeText)
    expect(back[1]!.sinks).toEqual(once[1]!.sinks)
  })
})

describe('validateDraft', () => {
  const valid: GroupDraft0 = {
    schema: 'db1', ddlPolicy: 'pause', targetDatabase: '', sinks: { mysql: true, kafka: false, rocketmq: false },
    tables: [{ table: 't1', doStock: true, orderBy: 'id', targetDatabase: '', excludeText: '', maskText: '' }],
  }
  it('合法草稿零错误', () => {
    expect(validateDraft([valid])).toEqual([])
  })
  it('缺 order_by（do_stock=true）/ 重复库 / 重复表 / 空 sink / 空表', () => {
    const bad: GroupDraft0[] = [
      { ...valid, tables: [{ ...valid.tables[0]!, orderBy: '' }] },
      { ...valid },
      { ...valid, schema: '', sinks: { mysql: false, kafka: false, rocketmq: false }, tables: [] },
    ]
    const errors = validateDraft(bad)
    expect(errors.some((e) => e.includes('order_by_column'))).toBe(true)
    expect(errors.some((e) => e.includes('重复'))).toBe(true)
    expect(errors.some((e) => e.includes('sink'))).toBe(true)
    expect(errors.some((e) => e.includes('tables 不能为空'))).toBe(true)
    expect(errors.some((e) => e.includes('schema）必填'))).toBe(true)
  })
})

type GroupDraft0 = ReturnType<typeof draftFromRules>[number]

describe('文本编解码', () => {
  it('parseExclude 逗号/空白分隔去空', () => {
    expect(parseExclude('a, b  c，d')).toEqual(['a', 'b', 'c', 'd'])
    expect(parseExclude('  ')).toEqual([])
  })
  it('parseMask 支持 = 与 : 与中文冒号', () => {
    expect(parseMask('phone=phone, name:truncate')).toEqual({ phone: 'phone', name: 'truncate' })
    expect(parseMask('p＝q')).toEqual({ p: 'q' })
    expect(parseMask('')).toEqual({})
  })
  it('format 双向', () => {
    expect(formatExclude(['a', 'b'])).toBe('a, b')
    expect(formatMask({ phone: 'phone', n: { type: 'null' } })).toBe('phone=phone, n=null')
  })
})

describe('diffSummaryText', () => {
  const mk = (over: Partial<PutRulesResult>): PutRulesResult => ({
    revision: 1, added: [], removed: [], modified: [], report: { checked: 0, errors: [], warnings: [] }, ...over,
  })
  it('计数与空态', () => {
    expect(diffSummaryText(mk({}))).toBe('无表级变化')
    expect(diffSummaryText(mk({ added: [{ schema: 'a', table: 'b' }], modified: [{ schema: 'a', table: 'c' }] })))
      .toBe('新增 1 表，修改 1 表')
    expect(diffSummaryText(mk({ removed: [{ schema: 'a', table: 'b' }, { schema: 'a', table: 'c' }] })))
      .toBe('移除 2 表')
  })
})
