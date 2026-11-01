import type { ReactNode } from 'react'
import { LoadingState } from './PageState'

export function DataCard({ title, extra, children, loading = false, className }: {
  title?: ReactNode
  extra?: ReactNode
  children: ReactNode
  loading?: boolean
  className?: string
}) {
  return (
    <section className={`panel data-card ${className ?? ''}`}>
      {(title || extra) && (
        <div className="panel-head">
          <div className="panel-title">{title}</div>
          {extra && <div className="panel-extra">{extra}</div>}
        </div>
      )}
      <div className="panel-body">
        {loading ? <LoadingState rows={3} /> : children}
      </div>
    </section>
  )
}
