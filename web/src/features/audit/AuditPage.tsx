import { Button, Table, Tooltip } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import { ReloadOutlined, UserOutlined } from '@ant-design/icons'
import { useSourceScope } from '@/app/sourceScope'
import { useAudit } from '@/shared/api/queries'
import type { AuditEntry } from '@/shared/api/types'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip, CodeValue } from '@/shared/components/presentation'

export function AuditPage() {
  const { currentSid } = useSourceScope()
  const { data: allEntries = [], isPending, error, refetch } = useAudit()
  // 源作用域真过滤：选中源时只显示该源的操作（source 为空=集群级操作，单源视图不显示）
  const entries = currentSid ? allEntries.filter((entry) => entry.source === currentSid) : allEntries
  const successCount = entries.filter((entry) => entry.result === 'ok').length
  const failureCount = entries.length - successCount
  const columns: ColumnsType<AuditEntry> = [
    {
      title: '操作时间', dataIndex: 'at', key: 'at', width: 200,
      render: (value: string) => <span className="audit-time">{formatDate(value)}</span>,
      sorter: (a, b) => Date.parse(a.at) - Date.parse(b.at),
      defaultSortOrder: 'descend',
    },
    {
      title: '操作人', dataIndex: 'who', key: 'who', width: 160,
      render: (value: string) => (
        <span className="audit-actor"><span className="actor-mark"><UserOutlined /></span>{value || 'anonymous'}</span>
      ),
    },
    { title: '操作内容', dataIndex: 'what', key: 'what', render: (value: string) => <Tooltip title={value}><span className="audit-action">{value}</span></Tooltip> },
    { title: '源', dataIndex: 'source', key: 'source', width: 120, render: (value?: string) => value ? <CodeValue>{value}</CodeValue> : <span className="text-muted">—</span> },
    { title: '执行节点', dataIndex: 'node', key: 'node', width: 170, render: (value: string) => <CodeValue>{value}</CodeValue> },
    {
      title: '结果', dataIndex: 'result', key: 'result', width: 130,
      render: (value: string) => {
        const success = value === 'ok'
        return <Chip tone={success ? 'ok' : 'danger'} dot>{success ? '成功' : '失败'}</Chip>
      },
    },
  ]

  return <>
    <PageHeader
      code="06"
      eyebrow={currentSid ? `系统 / 操作审计 · 源 ${currentSid}` : '系统 / 操作审计'}
      title="审计日志"
      description={currentSid
        ? `源「${currentSid}」的管理操作记录（已按源过滤）；失败操作同样留痕。查看全部源请切回「全部源」。`
        : '最近 100 条管理操作记录，由服务端写入 etcd；失败操作同样留痕。「源」列为空表示集群级操作。'}
      extra={<Button icon={<ReloadOutlined />} onClick={() => void refetch()}>刷新记录</Button>}
    />
    {error ? <ErrorState error={error} onRetry={() => void refetch()} /> : isPending ? <LoadingState /> : <>
      <div className="audit-summary">
        <div className="audit-summary-main"><b>{entries.length}</b><span>条近期记录</span></div>
        <div className="audit-summary-chips">
          <Chip tone="ok" dot>{successCount} 成功</Chip>
          <Chip tone="danger" dot>{failureCount} 失败</Chip>
          <span className="audit-retention">保留最近 100 条</span>
        </div>
      </div>
      <div className="table-shell">
        <Table<AuditEntry> className="admin-table audit-table" rowKey={(row) => `${row.at}-${row.node}-${row.what}`}
          columns={columns} dataSource={entries} loading={isPending} scroll={{ x: 970 }}
          pagination={{ pageSize: 15, showTotal: (total) => `共 ${total} 条审计记录`, pageSizeOptions: [15, 30, 50] }}
          locale={{ emptyText: currentSid ? '该源暂无操作记录（集群级操作不在此列示）' : '暂无管理操作记录' }} />
      </div>
    </>}
  </>
}

function formatDate(value: string) {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat('zh-CN', { dateStyle: 'medium', timeStyle: 'medium' }).format(date)
}
