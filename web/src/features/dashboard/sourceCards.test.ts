import { describe, expect, it } from 'vitest'
import type { SourceStatus } from '@/shared/api/types'
import { classifySource, sortSourceCards, type SourceCardData } from './sourceCards'

function source(overrides: Partial<SourceStatus> & { id: string }): SourceStatus {
  return { phase: 'ready', pipeline_state: 'running', use_gtid: false, ...overrides }
}

function card(overrides: Partial<SourceStatus> & { id: string }): SourceCardData {
  return { source: source(overrides), pendingDDL: 0 }
}

describe('sortSourceCards', () => {
  it('orders abnormal sources first: failed > paused > DDL waiting > catching up', () => {
    const ordered = sortSourceCards([
      card({ id: 'ok', pipeline_state: 'running', caught_up: true }),
      card({ id: 'catchup', pipeline_state: 'running', caught_up: false }),
      card({ id: 'ddl', pipeline_state: 'paused', pipeline_error: 'DDL 等待人工决策（管线暂停中）: ALTER' }),
      card({ id: 'failed', phase: 'failed', pipeline_state: 'idle' }),
      card({ id: 'paused', pipeline_state: 'paused', pipeline_error: 'fatal' }),
    ])
    expect(ordered.map((item) => item.source.id)).toEqual(['failed', 'paused', 'ddl', 'catchup', 'ok'])
  })

  it('breaks ties within the same severity by event_lag_seconds descending', () => {
    const ordered = sortSourceCards([
      card({ id: 'fresh', event_lag_seconds: 3 }),
      card({ id: 'stale', event_lag_seconds: 900 }),
      card({ id: 'nolag' }),
    ])
    expect(ordered.map((item) => item.source.id)).toEqual(['stale', 'fresh', 'nolag'])
  })

  it('keeps fatal pause above DDL waiting even with a larger lag on the waiting source', () => {
    const ordered = sortSourceCards([
      card({ id: 'ddl', pipeline_state: 'paused', pipeline_error: 'DDL 等待人工决策: x', event_lag_seconds: 5000 }),
      card({ id: 'fatal', pipeline_state: 'paused', pipeline_error: 'fatal' }),
    ])
    expect(ordered.map((item) => item.source.id)).toEqual(['fatal', 'ddl'])
  })

  it('treats op-paused sources as the paused severity', () => {
    const ordered = sortSourceCards([
      card({ id: 'catchup', pipeline_state: 'running', caught_up: false }),
      card({ id: 'op-paused', pipeline_state: 'paused', pipeline_paused_by_op: true }),
    ])
    expect(ordered.map((item) => item.source.id)).toEqual(['op-paused', 'catchup'])
  })
})

describe('classifySource', () => {
  it('detects failed assembly and ddl waiting precisely', () => {
    expect(classifySource(source({ id: 'x', phase: 'failed' }))).toBe('failed')
    expect(classifySource(source({ id: 'x', pipeline_state: 'paused', pipeline_error: 'DDL 等待人工决策（管线暂停中）: DROP' }))).toBe('ddlWaiting')
    expect(classifySource(source({ id: 'x', pipeline_state: 'paused', pipeline_error: '别的错误' }))).toBe('paused')
    expect(classifySource(source({ id: 'x', pipeline_state: 'running', caught_up: false }))).toBe('catchingUp')
    expect(classifySource(source({ id: 'x', pipeline_state: 'stock' }))).toBe('ok')
  })
})
