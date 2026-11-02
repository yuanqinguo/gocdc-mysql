import { useState } from 'react'
import { App as AntApp, Button, Modal, Table, Tooltip } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { BranchesOutlined, CheckOutlined, ClockCircleOutlined, CloseOutlined, DatabaseOutlined, ReloadOutlined } from '@ant-design/icons'
import { useApi } from '@/app/api'
import { useSourceScope } from '@/app/sourceScope'
import { queryKeys, useDDLEvents, useDDLEventsAll, useStatus } from '@/shared/api/queries'
import type { DDLRecord } from '@/shared/api/types'
import { Callout } from '@/shared/components/Callout'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip, CodeValue, policyLabel } from '@/shared/components/presentation'

export function DDLPage() {
  const { currentSid } = useSourceScope()
  const { data: status } = useStatus()
  const sources = status?.sources ?? []
  const effectiveSid = currentSid ?? (sources.length === 1 ? sources[0]?.id ?? null : null)
  // 单源视图拉该源挂起；全部视图拉集群合并列表（记录带 source 字段）
  const scoped = useDDLEvents(effectiveSid)
  const merged = useDDLEventsAll()
  const query = effectiveSid ? scoped : merged
  const records = query.data ?? []
  const { isPending, error, refetch } = query
  const [pendingDecision, setPendingDecision] = useState<{ record: DDLRecord; decision: 'approve' | 'skip' } | null>(null)
  const api = useApi()
  const queryClient = useQueryClient()
  const { message } = AntApp.useApp()
  const settle = useMutation({
    mutationFn: ({ sid, id, decision }: { sid: string; id: string; decision: 'approve' | 'skip' }) =>
      decision === 'approve' ? api.approveDDLOf(sid, id) : api.skipDDLOf(sid, id),
    onSuccess: async (_, variables) => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.ddlAll })
      await queryClient.invalidateQueries({ queryKey: ['ddl-events'] })
      void message.success(variables.decision === 'approve' ? '已提交审批，管线将继续处理。' : '已跳过该 DDL 事件。')
      setPendingDecision(null)
    },
    onError: (cause) => void message.error(cause.message),
  })

  const columns: ColumnsType<DDLRecord> = [
    {
      title: '事件', key: 'event', width: 220,
      render: (_, row) => (
        <div className="entity">
          <span className="entity-icon is-warn"><BranchesOutlined /></span>
          <span><b>{row.schema}.{row.table}</b><small>{policyLabel(row.policy)} 策略 · 待审批</small></span>
        </div>
      ),
      sorter: (a, b) => `${a.schema}.${a.table}`.localeCompare(`${b.schema}.${b.table}`),
    },
    {
      title: '源', dataIndex: 'source', key: 'source', width: 130,
      render: (value?: string) => value
        ? <span className="entity"><span className="entity-icon"><DatabaseOutlined /></span><span><b>{value}</b></span></span>
        : <span className="text-muted">—</span>,
      sorter: (a, b) => (a.source ?? '').localeCompare(b.source ?? ''),
      defaultSortOrder: !effectiveSid ? 'ascend' : undefined,
    },
    { title: '源位点', dataIndex: 'position', key: 'position', width: 160, render: (value: string) => <CodeValue>{value}</CodeValue> },
    {
      title: '产生时间', dataIndex: 'created_at', key: 'created_at', width: 170,
      render: (value: string) => <span className="time-cell"><ClockCircleOutlined />{formatDate(value)}</span>,
      sorter: (a, b) => Date.parse(a.created_at) - Date.parse(b.created_at),
    },
    {
      title: '事件 ID', dataIndex: 'id', key: 'id', width: 230,
      render: (value: string) => <Tooltip title={value}><span className="truncate-code"><CodeValue>{value}</CodeValue></span></Tooltip>,
    },
    {
      title: '操作', key: 'actions', fixed: 'right', width: 170,
      render: (_, row) => (
        <div className="row-actions">
          <Button type="primary" ghost size="small" icon={<CheckOutlined />} disabled={settle.isPending} onClick={() => setPendingDecision({ record: row, decision: 'approve' })}>批准</Button>
          <Button danger type="text" size="small" icon={<CloseOutlined />} disabled={settle.isPending} onClick={() => setPendingDecision({ record: row, decision: 'skip' })}>跳过</Button>
        </div>
      ),
    },
  ]

  return <>
    <PageHeader
      code="04"
      eyebrow={effectiveSid ? `复制 / 结构变更 · 源 ${effectiveSid}` : '复制 / 结构变更'}
      title="结构变更"
      description={effectiveSid
        ? `源「${effectiveSid}」的结构变更在此等待人工决策；审批状态由集群通过 etcd 共享。`
        : '全部源的结构变更在此等待人工决策（按源分组排序）；审批状态由集群通过 etcd 共享。'}
      extra={<Button icon={<ReloadOutlined />} onClick={() => void refetch()}>刷新</Button>}
    />
    {!error && !isPending && (
      <div className={`queue-readout ${records.length ? 'has-pending' : 'is-clear'}`}>
        <div className="queue-number">{String(records.length).padStart(2, '0')}</div>
        <div className="queue-copy">
          <b>{records.length ? '条结构变更等待决策' : '当前没有待决事件'}</b>
          <small>{records.length ? '审批前请阅读完整 SQL 和同步策略。' : '管线无需等待 DDL 决策。'}</small>
        </div>
        <Chip tone={records.length ? 'warn' : 'ok'} dot>{records.length ? '待决策' : '无待决'}</Chip>
      </div>
    )}
    {error ? <ErrorState error={error} onRetry={() => void refetch()} /> : isPending ? <LoadingState /> : (
      <div className="table-shell">
        <Table<DDLRecord> className="admin-table ddl-table" rowKey="id" columns={columns} dataSource={records} loading={isPending}
          scroll={{ x: 1080 }} pagination={{ pageSize: 10, showTotal: (total) => `共 ${total} 条待审批事件` }}
          expandable={{
            expandedRowRender: (row) => (
              <div className="ddl-sql">
                <div className="sql-row"><span className="panel-kicker">DDL 语句</span>{row.note && <span className="text-muted">{row.note}</span>}</div>
                <pre>{row.sql || '未提供 SQL 文本'}</pre>
              </div>
            ),
            rowExpandable: (row) => Boolean(row.sql || row.note),
          }}
          locale={{ emptyText: '暂无待审批 DDL 事件' }} />
      </div>
    )}
    <Modal
      open={pendingDecision !== null}
      title={pendingDecision?.decision === 'approve' ? '确认批准结构变更' : '确认跳过结构变更'}
      okText={pendingDecision?.decision === 'approve' ? '确认批准' : '确认跳过'}
      cancelText="返回检查"
      okButtonProps={{ danger: pendingDecision?.decision === 'skip', loading: settle.isPending }}
      cancelButtonProps={{ disabled: settle.isPending }}
      closable={!settle.isPending}
      onCancel={() => { if (!settle.isPending) setPendingDecision(null) }}
      onOk={() => pendingDecision ? settle.mutateAsync({ sid: pendingDecision.record.source || effectiveSid || '', id: pendingDecision.record.id, decision: pendingDecision.decision }) : undefined}
    >
      {pendingDecision && <>
        <Callout tone={pendingDecision.decision === 'approve' ? 'warning' : 'danger'} title={pendingDecision.decision === 'approve' ? '批准语义' : '跳过语义'}>
          {pendingDecision.decision === 'approve'
            ? '白名单内结构由系统同步；DROP、RENAME、缩容或类型变更等非白名单操作不会自动执行。请先在目标库完成相同变更，再批准以复验并恢复管线。'
            : '目标端将保留旧结构，源端变更可能导致字段无法同步。跳过后该事件会被记入审计。'}
        </Callout>
        <div className="confirm-meta">
          <span><b>所属源</b><code>{pendingDecision.record.source || effectiveSid || '—'}</code></span>
          <span><b>源表</b>{pendingDecision.record.schema}.{pendingDecision.record.table}</span>
          <span><b>策略</b>{policyLabel(pendingDecision.record.policy)}</span>
          <span><b>源位点</b><code>{pendingDecision.record.position || '—'}</code></span>
          <span><b>事件 ID</b><code>{pendingDecision.record.id}</code></span>
        </div>
        <div className="confirm-label">待处理 SQL</div>
        <pre className="confirm-sql">{pendingDecision.record.sql || '服务端未返回 SQL 文本'}</pre>
        {pendingDecision.record.note && <div className="modal-footnote">{pendingDecision.record.note}</div>}
      </>}
    </Modal>
    <div className="help-strip"><b>审批语义提示</b><span>白名单 DDL 可自动执行；DROP、RENAME、缩容或类型变更等操作不会由 approve 自动执行。请先在目标库完成相同变更，再审批以触发复验。</span></div>
  </>
}

function formatDate(value: string) {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat('zh-CN', { dateStyle: 'medium', timeStyle: 'short' }).format(date)
}
