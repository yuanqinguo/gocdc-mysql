import type { ReactNode } from 'react'
import { CheckCircleOutlined, CloseCircleOutlined, InfoCircleOutlined, WarningOutlined } from '@ant-design/icons'

type CalloutTone = 'info' | 'warning' | 'danger' | 'success'

const ICONS: Record<CalloutTone, ReactNode> = {
  info: <InfoCircleOutlined />,
  warning: <WarningOutlined />,
  danger: <CloseCircleOutlined />,
  success: <CheckCircleOutlined />,
}

export function Callout({ tone = 'info', title, children, className, action }: {
  tone?: CalloutTone
  title?: ReactNode
  children?: ReactNode
  className?: string
  action?: ReactNode
}) {
  return (
    <div className={`callout callout-${tone} ${className ?? ''}`} role={tone === 'info' || tone === 'success' ? 'status' : 'alert'}>
      <span className="callout-icon">{ICONS[tone]}</span>
      <div className="callout-body">
        {title && <b>{title}</b>}
        {children !== undefined && children !== null && <div className="callout-text">{children}</div>}
      </div>
      {action && <div className="callout-action">{action}</div>}
    </div>
  )
}
