/* eslint-disable react-refresh/only-export-components */
import type { ReactNode } from 'react'
import type { NodeRole } from '@/shared/api/types'

type ChipTone = 'ok' | 'warn' | 'danger' | 'info' | 'muted' | 'signal'

export function roleLabel(role: NodeRole) {
  return role === 'leader' ? '主节点' : role === 'follower' ? '热备节点' : role === 'single' ? '单节点' : role
}

export function Chip({ tone = 'muted', dot = false, title, children, className }: {
  tone?: ChipTone
  dot?: boolean
  title?: string
  children: ReactNode
  className?: string
}) {
  return <span title={title} className={`chip chip-${tone} ${className ?? ''}`}>{dot && <i className="led" />}{children}</span>
}

export function RoleChip({ role }: { role: NodeRole }) {
  return <Chip className="role-chip" tone={role === 'leader' || role === 'single' ? 'ok' : 'muted'} dot>{roleLabel(role)}</Chip>
}

export function SinkTag({ label, enabled, title }: { label: string; enabled: boolean; title?: string }) {
  return <Chip tone={enabled ? 'ok' : 'muted'} dot={enabled} title={title}>{label}</Chip>
}

const POLICY_LABELS: Record<string, string> = { pause: '暂停等待', auto: '自动执行', ignore: '忽略' }

/** DDL 策略枚举的中文展示名（未知值原样显示）。 */
export function policyLabel(policy?: string) {
  return POLICY_LABELS[policy || 'pause'] ?? policy
}

export function PolicyChip({ policy }: { policy?: string }) {
  const value = policy || 'pause'
  const tone: ChipTone = value === 'auto' ? 'ok' : value === 'pause' ? 'warn' : value === 'ignore' ? 'muted' : 'info'
  return <Chip tone={tone}>{policyLabel(policy)}</Chip>
}

export function CodeValue({ children }: { children?: string | number | null }) {
  return <code className="code-value">{children === '' || children === null || children === undefined ? '—' : children}</code>
}
