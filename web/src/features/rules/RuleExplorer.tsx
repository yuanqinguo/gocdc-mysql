import { useMemo, useState } from 'react'
import { Button, Empty, Input, Table, Tooltip } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import { ArrowLeftOutlined, DatabaseOutlined, SearchOutlined, TableOutlined } from '@ant-design/icons'
import type { EffectiveRule } from '@/shared/api/types'
import { Chip, CodeValue, PolicyChip, SinkTag } from '@/shared/components/presentation'
import { RuleFieldPolicy } from '@/shared/components/RuleFieldPolicy'

export interface RuleExplorerProps {
  rules: EffectiveRule[]
  /** 挂起 DDL 事件（/ddl-events 返回的就是 pending 集合），用于库级角标 */
  pendingDDL?: { schema: string; table: string }[]
  /** 库名搜索占位文案 */
  schemaSearchPlaceholder?: string
  /** false = 只读视图（配置中心页）；true = 带重跑/对账操作列（复制规则页） */
  showActions?: boolean
  onRestock?: (rule: EffectiveRule) => void
  onVerify?: (rule: EffectiveRule) => void
  actionPending?: boolean
}

interface SchemaGroup {
  schema: string
  tables: EffectiveRule[]
  targetDatabase: string
  ddlPolicy: string
  sinks: EffectiveRule['sinks']
  pendingCount: number
}

/** 组→表两级探索器：库级摘要（一级）→ 点库下钻表规则（二级，带表名搜索）。
 * 规则来自后端编译产物（组形态白名单全量生效视图），前端只做展示聚合。 */
export function RuleExplorer({
  rules, pendingDDL = [], schemaSearchPlaceholder = '搜索源库…',
  showActions = false, onRestock, onVerify, actionPending = false,
}: RuleExplorerProps) {
  const [schema, setSchema] = useState<string | null>(null)
  const [schemaSearch, setSchemaSearch] = useState('')
  const [tableSearch, setTableSearch] = useState('')

  const groups = useMemo<SchemaGroup[]>(() => {
    const bySchema = new Map<string, SchemaGroup>()
    for (const rule of rules) {
      let group = bySchema.get(rule.schema)
      if (!group) {
        group = {
          schema: rule.schema, tables: [], targetDatabase: rule.target_database,
          ddlPolicy: rule.ddl_policy || 'pause', sinks: rule.sinks, pendingCount: 0,
        }
        bySchema.set(rule.schema, group)
      }
      group.tables.push(rule)
    }
    const pending = new Map<string, number>()
    for (const event of pendingDDL) {
      pending.set(event.schema, (pending.get(event.schema) ?? 0) + 1)
    }
    const output = [...bySchema.values()]
    for (const group of output) {
      group.pendingCount = pending.get(group.schema) ?? 0
      group.tables.sort((a, b) => a.table.localeCompare(b.table))
    }
    output.sort((a, b) => a.schema.localeCompare(b.schema))
    return output
  }, [rules, pendingDDL])

  // ── 二级：某库的表规则 ──
  if (schema !== null) {
    const group = groups.find((item) => item.schema === schema)
    const needle = tableSearch.trim().toLocaleLowerCase()
    const tables = (group?.tables ?? []).filter((rule) =>
      !needle || rule.table.toLocaleLowerCase().includes(needle))
    return (
      <>
        <div className="toolbar drill-toolbar">
          <div className="drill-title">
            <Button className="btn-back" icon={<ArrowLeftOutlined />} onClick={() => { setSchema(null); setTableSearch('') }}>返回库列表</Button>
            <span className="entity-icon"><DatabaseOutlined /></span>
            <div className="drill-copy">
              <b>{schema}</b>
              <small>{group?.tables.length ?? 0} 张表 · {group?.targetDatabase || '同名库'}</small>
            </div>
          </div>
          <Input allowClear prefix={<SearchOutlined />} placeholder="搜索表名…" value={tableSearch}
            onChange={(event) => setTableSearch(event.target.value)} className="search-input" />
        </div>
        <div className="table-shell">
          <Table<EffectiveRule> className="admin-table" rowKey={(row) => row.key || `${row.schema}.${row.table}`}
            columns={tableColumns(showActions, onRestock, onVerify, actionPending)}
            dataSource={tables} scroll={{ x: 1105 }}
            pagination={{ pageSize: 10, showSizeChanger: true, showTotal: (total) => `共 ${total} 张表`, pageSizeOptions: [10, 20, 50] }}
            locale={{ emptyText: needle ? '没有匹配的表' : '暂无表规则' }} />
        </div>
      </>
    )
  }

  // ── 一级：库摘要 ──
  const needle = schemaSearch.trim().toLocaleLowerCase()
  const visible = needle ? groups.filter((group) => group.schema.toLocaleLowerCase().includes(needle)) : groups
  const totalPending = groups.reduce((sum, group) => sum + group.pendingCount, 0)
  return (
    <>
      <div className="toolbar">
        <div className="toolbar-stats">
          <span className="toolbar-stat"><b>{groups.length}</b><small>源库</small></span>
          <span className="toolbar-divider" />
          <span className="toolbar-stat"><b>{rules.length}</b><small>表白名单</small></span>
          <span className="toolbar-divider" />
          <span className={`toolbar-stat ${totalPending > 0 ? 'is-warn' : ''}`}><b>{totalPending}</b><small>待决 DDL</small></span>
        </div>
        <Input allowClear prefix={<SearchOutlined />} placeholder={schemaSearchPlaceholder} value={schemaSearch}
          onChange={(event) => setSchemaSearch(event.target.value)} className="search-input" />
      </div>
      <div className="table-shell">
        <Table<SchemaGroup> className="admin-table" rowKey="schema"
          columns={[
            {
              title: '源库', key: 'schema', render: (_, group) => (
                <div className="entity">
                  <span className="entity-icon"><DatabaseOutlined /></span>
                  <span><b>{group.schema}</b><small>{group.targetDatabase || '同名库'}</small></span>
                </div>
              ),
            },
            { title: '表数', key: 'count', width: 90, render: (_, group) => <span className="num-cell"><TableOutlined /> {group.tables.length}</span> },
            { title: 'DDL 策略', key: 'ddl', width: 110, render: (_, group) => <PolicyChip policy={group.ddlPolicy} /> },
            {
              title: '下游', key: 'sinks', width: 190, render: (_, group) => (
                <div className="chip-row">
                  <SinkTag label="MySQL" enabled={group.sinks?.mysql ?? false} />
                  <SinkTag label="Kafka" enabled={group.sinks?.kafka ?? false} />
                  <SinkTag label="RocketMQ" enabled={group.sinks?.rocketmq ?? false} />
                </div>
              ),
            },
            {
              title: '挂起 DDL', key: 'pending', width: 110, render: (_, group) => group.pendingCount > 0
                ? <Tooltip title="有待审批的结构变更，去「结构变更」页处理"><Chip tone="warn" dot>{group.pendingCount} 待审</Chip></Tooltip>
                : <span className="text-muted">0</span>,
            },
            {
              title: '操作', key: 'actions', width: 110, render: (_, group) => (
                <Button type="link" size="small" icon={<TableOutlined />}
                  onClick={() => { setSchema(group.schema); setTableSearch('') }}>
                  {group.tables.length} 张表
                </Button>
              ),
            },
          ]}
          dataSource={visible} loading={false} scroll={{ x: 860 }}
          pagination={groups.length > 10 ? { pageSize: 10, showTotal: (total) => `共 ${total} 个库` } : false}
          locale={{ emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={needle ? '没有匹配的库' : '暂无规则'} /> }}
          onRow={(group) => ({ onClick: () => { setSchema(group.schema); setTableSearch('') }, style: { cursor: 'pointer' } })}
        />
      </div>
    </>
  )
}

/** 二级表规则列（复制规则页带操作；生效配置页只读）。 */
function tableColumns(
  showActions: boolean,
  onRestock?: (rule: EffectiveRule) => void,
  onVerify?: (rule: EffectiveRule) => void,
  actionPending = false,
): ColumnsType<EffectiveRule> {
  const columns: ColumnsType<EffectiveRule> = [
    {
      title: '源表', key: 'source', fixed: 'left', width: 185,
      render: (_, row) => (
        <div className="entity">
          <span className="entity-icon"><DatabaseOutlined /></span>
          <span><b>{row.table}</b><small>{row.schema}</small></span>
        </div>
      ),
      sorter: (a, b) => a.table.localeCompare(b.table),
    },
    { title: '目标库', dataIndex: 'target_database', key: 'target_database', width: 125, render: (value?: string) => <CodeValue>{value || '同名库'}</CodeValue> },
    { title: '快照排序列', dataIndex: 'order_by_column', key: 'order_by_column', width: 130, render: (value: string) => <CodeValue>{value || '主键顺序'}</CodeValue> },
    {
      title: '下游', key: 'sinks', width: 185, render: (_, row) => (
        <div className="chip-row">
          <SinkTag label="MySQL" enabled={row.sinks?.mysql ?? false} />
          <SinkTag label="Kafka" enabled={row.sinks?.kafka ?? false} title={row.sinks?.kafka ? (row.sinks.kafka_topic ? `Topic: ${row.sinks.kafka_topic}` : 'Topic 按配置前缀与库表名自动派生') : undefined} />
          <SinkTag label="RocketMQ" enabled={row.sinks?.rocketmq ?? false} title={row.sinks?.rocketmq ? [row.sinks.rocketmq_topic ? `Topic: ${row.sinks.rocketmq_topic}` : 'Topic 按配置前缀与库表名自动派生', row.sinks.rocketmq_tag && `Tag: ${row.sinks.rocketmq_tag}`].filter(Boolean).join(' · ') : undefined} />
        </div>
      ),
    },
    { title: '字段处理', key: 'field_policy', width: 150, render: (_, row) => <RuleFieldPolicy rule={row} /> },
    { title: 'DDL 策略', dataIndex: 'ddl_policy', key: 'ddl_policy', width: 110, render: (value: string) => <PolicyChip policy={value} /> },
    {
      title: '全量同步', dataIndex: 'do_stock', key: 'do_stock', width: 100, render: (value: boolean) => (
        <span className={value ? 'state-text is-on' : 'state-text is-off'}><i className="led" />{value ? '启用' : '关闭'}</span>
      ),
    },
  ]
  if (showActions) {
    columns.push({
      title: '操作', key: 'actions', fixed: 'right', width: 160, render: (_, row) => (
        <div className="row-actions">
          <Tooltip title="清除该表的全量完成标记并重新执行">
            <Button type="text" size="small" disabled={actionPending}
              onClick={() => onRestock?.(row)}>重跑</Button>
          </Tooltip>
          <Button type="text" size="small" onClick={() => onVerify?.(row)}>对账</Button>
        </div>
      ),
    })
  }
  return columns
}
