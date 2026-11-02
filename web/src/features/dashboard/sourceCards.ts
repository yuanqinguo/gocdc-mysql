import type { SourceStatus, SourceSummary } from '@/shared/api/types'

/** 聚合源卡数据（/status 摘要 + /sources 清单统计 + 该源 pending DDL 数）。 */
export interface SourceCardData {
  source: SourceStatus
  summary?: SourceSummary
  pendingDDL: number
}

/** 异常等级：聚合排序用（failed > paused > DDL 等待 > 追赶中 > 正常）。 */
export type SourceCardSeverity = 'failed' | 'paused' | 'ddlWaiting' | 'catchingUp' | 'ok'

/** 沿用总控台彩带分类口径的判定函数。 */
export function isDDLWaiting(source: SourceStatus): boolean {
  return source.pipeline_state === 'paused' && /DDL 等待人工决策/.test(source.pipeline_error ?? '')
}

export function isOpPaused(source: SourceStatus): boolean {
  return source.pipeline_state === 'paused' && source.pipeline_paused_by_op === true
}

export function isCatchingUp(source: SourceStatus): boolean {
  return source.pipeline_state === 'running' && source.caught_up === false
}

export function classifySource(source: SourceStatus): SourceCardSeverity {
  if (source.phase === 'failed' || source.pipeline_state === 'failed') return 'failed'
  if (isDDLWaiting(source)) return 'ddlWaiting'
  if (source.pipeline_state === 'paused') return 'paused'
  if (isCatchingUp(source)) return 'catchingUp'
  return 'ok'
}

const SEVERITY_RANK: Record<SourceCardSeverity, number> = { failed: 0, paused: 1, ddlWaiting: 2, catchingUp: 3, ok: 4 }

/** 聚合源卡排序：异常优先，同级按 event_lag_seconds 降序。 */
export function sortSourceCards(cards: SourceCardData[]): SourceCardData[] {
  return [...cards].sort((a, b) => {
    const rank = SEVERITY_RANK[classifySource(a.source)] - SEVERITY_RANK[classifySource(b.source)]
    if (rank !== 0) return rank
    return (b.source.event_lag_seconds ?? -1) - (a.source.event_lag_seconds ?? -1)
  })
}
