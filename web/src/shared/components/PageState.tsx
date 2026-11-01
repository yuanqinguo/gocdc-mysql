import type { ReactNode } from 'react'
import { Button } from 'antd'
import { WarningOutlined } from '@ant-design/icons'

export function LoadingState({ rows = 4 }: { rows?: number }) {
  return (
    <div className="page-state loading-state" aria-label="正在加载">
      <div className="loading-scan"><span /></div>
      <div className="loading-rows">
        {Array.from({ length: rows }).map((_, index) => (
          <i key={index} style={{ width: `${86 - (index % 4) * 13}%` }} />
        ))}
      </div>
    </div>
  )
}

export function ErrorState({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const message = error instanceof Error ? error.message : '请求失败，请稍后重试'
  return (
    <div className="error-state" role="alert">
      <span className="error-state-icon"><WarningOutlined /></span>
      <div className="error-state-copy">
        <b>暂时无法加载数据</b>
        <p>{message}</p>
        {onRetry && <Button size="small" onClick={onRetry}>重试</Button>}
      </div>
    </div>
  )
}

export function EmptyState({ description = '暂无数据' }: { description?: ReactNode }) {
  return <div className="empty-state"><span className="empty-glyph">∅</span><p>{description}</p></div>
}
