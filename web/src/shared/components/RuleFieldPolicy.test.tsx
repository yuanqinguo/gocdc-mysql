import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import type { EffectiveRule } from '@/shared/api/types'
import { RuleFieldPolicy } from './RuleFieldPolicy'

const rule: EffectiveRule = {
  key: 'db.users',
  schema: 'db',
  table: 'users',
  target_database: 'db',
  do_stock: true,
  ddl_policy: 'pause',
  order_by_column: 'id',
  exclude_columns: ['internal_note'],
  mask_columns: {
    phone: { type: 'hash_sha256' },
    name: { type: 'truncate', keep: 4 },
  },
  sinks: { mysql: true, kafka: false, rocketmq: false, kafka_topic: '', rocketmq_topic: '', rocketmq_tag: '' },
}

describe('RuleFieldPolicy', () => {
  it('shows safe mask algorithms and excluded columns on demand', async () => {
    const user = userEvent.setup()
    render(<RuleFieldPolicy rule={rule} />)
    await user.click(screen.getByRole('button', { name: '查看 db.users 的字段处理策略' }))

    expect(await screen.findByText('SHA-256 摘要')).toBeInTheDocument()
    expect(screen.getByText('截断 · 保留 4')).toBeInTheDocument()
    expect(screen.getByText('internal_note')).toBeInTheDocument()
  })

  it('uses a compact empty state when the rule has no column transforms', () => {
    render(<RuleFieldPolicy rule={{ ...rule, exclude_columns: [], mask_columns: {} }} />)
    expect(screen.getByText('无')).toBeInTheDocument()
  })
})
