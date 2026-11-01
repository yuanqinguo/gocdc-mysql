import type { ReactNode } from 'react'

export function PageHeader({ code, eyebrow, title, description, extra }: {
  code: string
  eyebrow: string
  title: string
  description?: string
  extra?: ReactNode
}) {
  return (
    <header className="page-header" data-code={code}>
      <div className="page-header-main">
        <div className="page-eyebrow">
          <span className="eyebrow-code">{code}</span>
          <span className="eyebrow-rule" />
          <span className="eyebrow-text">{eyebrow}</span>
        </div>
        <h1 className="page-title">{title}</h1>
        {description && <p className="page-description">{description}</p>}
      </div>
      {extra && <div className="page-header-extra">{extra}</div>}
    </header>
  )
}
