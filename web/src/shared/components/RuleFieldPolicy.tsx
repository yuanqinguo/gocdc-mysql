import { Button, Popover, Space } from 'antd'
import type { EffectiveMaskSpec, EffectiveRule } from '@/shared/api/types'

export function RuleFieldPolicy({ rule }: { rule: EffectiveRule }) {
  const excluded = rule.exclude_columns ?? []
  const masks = Object.entries(rule.mask_columns ?? {})
  if (excluded.length === 0 && masks.length === 0) return <span className="text-muted">无</span>

  const details = (
    <div className="field-policy-detail">
      {masks.length > 0 && <section><b>脱敏列</b>{masks.map(([column, spec]) => <div className="field-policy-row" key={column}><code>{column}</code><span>{maskLabel(spec)}{spec.type === 'truncate' && spec.keep ? ` · 保留 ${spec.keep}` : ''}</span></div>)}</section>}
      {excluded.length > 0 && <section><b>排除列</b><div className="field-policy-excluded">{excluded.map((column) => <code key={column}>{column}</code>)}</div></section>}
    </div>
  )

  return (
    <Popover title="字段处理策略" content={details} trigger="click" placement="left">
      <Button type="link" size="small" aria-label={`查看 ${rule.schema}.${rule.table} 的字段处理策略`}>
        <Space size={3}>{masks.length} 脱敏<span className="field-policy-separator">·</span>{excluded.length} 排除</Space>
      </Button>
    </Popover>
  )
}

function maskLabel(spec: EffectiveMaskSpec) {
  switch (spec.type) {
    case 'fixed': return '固定值替换'
    case 'hash_md5': return 'MD5 摘要'
    case 'hash_sha256': return 'SHA-256 摘要'
    case 'phone': return '手机号掩码'
    case 'email': return '邮箱掩码'
    case 'truncate': return '截断'
    case 'null': return '置空'
    default: return spec.type
  }
}
