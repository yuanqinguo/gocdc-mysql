import { useState, type ReactNode } from 'react'
import { Alert, App as AntApp, Button, Empty, Form, Input, Select, Space, Switch, Table, Tag, Typography } from 'antd'
import type { ColumnsType } from 'antd/es/table'
import {
  ArrowLeftOutlined, DatabaseOutlined, DeleteOutlined, EditOutlined,
  PlusOutlined, SaveOutlined, SearchOutlined, TableOutlined, UndoOutlined,
} from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useApi } from '@/app/api'
import { ApiError } from '@/shared/api/client'
import { queryKeys } from '@/shared/api/queries'
import type { PutRulesResult, RuleGroup, RuleTable } from '@/shared/api/types'
import { Chip, CodeValue, PolicyChip, SinkTag } from '@/shared/components/presentation'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { draftFromRules, formatExclude, parseExclude, validateDraft } from '@/features/rules/ruleDraft'
import { SINK_KEYS, cloneGroups, diffSummaryTextOf, keepSink, sinkOn, tablesOf, type SinkKey } from './rulesManageLogic'

const { Text } = Typography

// 「规则视图（按库）」的在线编辑视图（配置中心页签，按源分节渲染，sid 作用域）：
// 在 库 → 下钻表 两级结构上直接提供 新增/修改/移除源库 与 增加/修改/移除表（弹窗编辑）。
// 多源多目标语义：本组件只编辑当前源（sid）的规则组；目标库靠 组级/表级 target_database
// 覆盖表达（空 = 与源库同名 / 组级默认），下游 sinks 三开关按该源 targets 实际启用的给默认。
// 草稿 = 配置中心组形态原样（spread 修补）：sink 的 topic/tag 对象覆盖、脱敏对象形态等
// 未知字段原样保留，不做归一化重建（防静默丢失）。
// 保存走服务端三道护栏（全量校验 + 变更表活体预检 + CAS）；保存成功后需「应用配置并重启」
// 生效——重启是进程级，会一并应用其他源已保存的变更（多源共享同一份配置正文与 revision）。
// 纯逻辑（sink 形态保留/diff 摘要）在 ./rulesManageLogic，可单测。

const DDL_POLICIES = [
  { value: 'pause', label: 'pause（DDL 挂起待审批，默认）' },
  { value: 'auto', label: 'auto（白名单 DDL 自动执行）' },
  { value: 'ignore', label: 'ignore（DDL 不同步继续跑）' },
]

export interface RulesManageProps {
  sid: string
  /** 该源 pending DDL 事件（按 sid 过滤后），用于库级角标 */
  pendingDDL?: { schema: string; table: string }[]
  /** 该源 targets 已启用的下游（新增源库的 sinks 默认值与提示）；空 = 未知，默认只开 mysql */
  enabledSinks?: string[]
  /** 无法在线编辑（本地文件配置形态）时回退渲染的只读视图 */
  readOnlyView?: ReactNode
}

export function RulesManage({ sid, pendingDDL = [], enabledSinks = [], readOnlyView }: RulesManageProps) {
  const api = useApi()
  const queryClient = useQueryClient()
  const { message, modal } = AntApp.useApp()
  const load = useQuery({
    queryKey: ['source-rules', sid],
    queryFn: ({ signal }) => api.getRulesOf(sid, signal),
    staleTime: 0,
  })

  const [revision, setRevision] = useState<number | null>(null)
  const [original, setOriginal] = useState<RuleGroup[]>([])
  const [groups, setGroups] = useState<RuleGroup[]>([])
  const [schema, setSchema] = useState<string | null>(null)
  const [schemaSearch, setSchemaSearch] = useState('')
  const [tableSearch, setTableSearch] = useState('')

  const dirty = revision !== null && JSON.stringify(groups) !== JSON.stringify(original)

  // 基线播种（渲染期同源 state 初始化，React 认可模式）：干净态随最新数据刷新
  // （含保存后 fetchQuery 与并发保存后的 invalidate）；一旦有未保存修改即冻结，绝不覆盖草稿。
  if (load.data && !dirty && (revision === null || load.data.revision !== revision)) {
    setRevision(load.data.revision)
    setOriginal(cloneGroups(load.data.rules))
    setGroups(cloneGroups(load.data.rules))
  }

  const save = useMutation({
    mutationFn: () => api.putRulesOf(sid, revision ?? 0, groups, false),
    onSuccess: async (report: PutRulesResult) => {
      const fresh = await queryClient.fetchQuery({
        queryKey: ['source-rules', sid],
        queryFn: ({ signal }) => api.getRulesOf(sid, signal),
      })
      setRevision(fresh.revision)
      setOriginal(cloneGroups(fresh.rules))
      setGroups(cloneGroups(fresh.rules))
      // 配置正文是全源共享一份：本源保存会推进 revision，其余源缓存全部失效重读
      await queryClient.invalidateQueries({ queryKey: ['source-rules'] })
      await queryClient.invalidateQueries({ queryKey: queryKeys.rules(sid) })
      showSaved(report)
    },
    onError: (e) => void message.error(e.message),
  })

  const restart = useMutation({
    mutationFn: () => api.applyRestart(),
    onSuccess: () => {
      void message.info('已触发重启：进程优雅退出（位点终值落盘），由编排层拉起后新配置生效。')
    },
    onError: (e) => void message.error(e.message),
  })

  function showSaved(report: PutRulesResult) {
    const diffRow = (label: string, list: { schema: string; table: string }[] | undefined, tone: string) =>
      Array.isArray(list) && list.length > 0 ? (
        <div style={{ marginBottom: 8 }}>
          <Tag color={tone}>{label} {list.length}</Tag>
          {list.map((ref) => <Tag key={`${ref.schema}.${ref.table}`} style={{ marginBottom: 4 }}><Text>{ref.schema}.{ref.table}</Text></Tag>)}
        </div>
      ) : null
    modal.confirm({
      title: '规则已保存到配置中心（重启后生效）',
      width: 640,
      icon: null,
      content: (
        <div>
          {diffRow('新增', report.added, 'green')}
          {diffRow('移除', report.removed, 'red')}
          {diffRow('修改', report.modified, 'blue')}
          {(report.report.warnings?.length ?? 0) > 0 && (
            <Alert type="warning" showIcon style={{ marginBottom: 8 }} message={report.report.warnings.join('；')} />
          )}
          <Alert type="info" showIcon
            message="重启为进程级：优雅停机（位点终值落盘、resign）→ 编排拉起 → 按新配置续跑并补齐积压。多源部署下会一并应用其他源已保存的变更。配置只在进程启动时读取（无热更）：集群形态请按序滚动——先逐个重启其余节点（带新配置当热备），最后重启 leader，避免 failover 晋升者仍持旧配置（加表=少同步安全方向；移除=本应停掉的表继续同步）。" />
        </div>
      ),
      okText: '立即重启（不等自动）',
      okButtonProps: { danger: true },
      cancelText: '稍后手动重启',
      onOk: () => restart.mutateAsync(),
    })
  }

  function confirmReset() {
    modal.confirm({
      title: '放弃全部未保存修改？',
      width: 460,
      icon: null,
      content: '将恢复为配置中心当前保存的版本。',
      okText: '放弃修改',
      okButtonProps: { danger: true },
      cancelText: '继续编辑',
      onOk: () => {
        setRevision(load.data?.revision ?? null)
        setOriginal(cloneGroups(load.data?.rules ?? []))
        setGroups(cloneGroups(load.data?.rules ?? []))
      },
    })
  }

  function confirmSave() {
    modal.confirm({
      title: '校验并保存规则变更？',
      width: 560,
      icon: null,
      content: (
        <div style={{ display: 'grid', gap: 8 }}>
          <Alert type="info" showIcon message="保存前服务端会执行三道护栏：" description={
            <ol style={{ margin: '4px 0 0', paddingLeft: 20 }}>
              <li>全量配置校验（与启动加载同一管线）</li>
              <li>变更表活体预检（目标表须预建、列集一致）</li>
              <li>防并发覆盖（版本不一致返回 409）</li>
            </ol>
          } />
          <Text type="secondary">任一失败写入被拒并展示全部错误明细；本次变更：{diffSummaryTextOf(groups, original)}。保存成功后需「应用配置并重启」生效。</Text>
        </div>
      ),
      okText: '校验并保存',
      okButtonProps: { icon: <SaveOutlined /> },
      cancelText: '取消',
      onOk: () => save.mutateAsync(),
    })
  }

  // 行操作按 schema/table 键定位（不按对象引用/下标）：干净态下后台会随最新数据
  // 重新播种（多源共享 revision，别的源保存会推进），引用/下标会静默失配。
  const patchGroup = (schemaKey: string, patch: Partial<RuleGroup>) => {
    setGroups((gs) => gs.map((g) => ((g.schema ?? '') === schemaKey ? { ...g, ...patch } : g)))
  }

  function groupModal(schemaKey: string | null) {
    const editing = schemaKey !== null ? groups.find((g) => (g.schema ?? '') === schemaKey) ?? null : null
    const defaultSinks: Record<SinkKey, boolean> = {
      mysql: editing ? sinkOn(editing, 'mysql') : enabledSinks.includes('mysql'),
      kafka: editing ? sinkOn(editing, 'kafka') : enabledSinks.includes('kafka'),
      rocketmq: editing ? sinkOn(editing, 'rocketmq') : enabledSinks.includes('rocketmq'),
    }
    if (!editing && defaultSinks.mysql === false && defaultSinks.kafka === false && defaultSinks.rocketmq === false) {
      defaultSinks.mysql = true // targets 未知时保守默认（服务端校验为准）
    }
    let form: FormRefLike | null = null
    modal.confirm({
      title: editing ? `修改源库 ${editing.schema || '(未命名)'}` : `在源 ${sid} 新增源库（规则组）`,
      width: 520,
      icon: null,
      content: <GroupForm onReady={(f) => { form = f }} initial={editing ?? { schema: '', ddl_policy: 'pause', tables: [] }} defaultSinks={defaultSinks} enabledSinksHint={enabledSinks} />,
      okText: editing ? '保存修改' : '新增源库',
      cancelText: '取消',
      onOk: async () => {
        const values = await form?.validate()
        if (!values) throw new Error('表单校验未通过')
        if (editing) patchGroup(schemaKey as string, values as Partial<RuleGroup>)
        else setGroups((gs) => [...gs, values as unknown as RuleGroup])
      },
    })
  }

  function removeGroup(schemaKey: string, label: string) {
    modal.confirm({
      title: `移除源库 ${label}？`, width: 480, icon: null,
      content: '该库全部表将停止同步（事件过滤），目标端数据保留不动；全量死标记由后台 GC 清理。保存后需应用重启生效。',
      okText: '移除源库', okButtonProps: { danger: true }, cancelText: '取消',
      onOk: () => setGroups((gs) => gs.filter((g) => (g.schema ?? '') !== schemaKey)),
    })
  }

  function tableModal(g: RuleGroup, existing: RuleTable | null) {
    const schemaKey = g.schema ?? ''
    let form: FormRefLike | null = null
    modal.confirm({
      title: existing ? `修改表 ${existing.table}` : `在源库 ${schemaKey} 下增加表`,
      width: 560,
      icon: null,
      content: <TableForm onReady={(f) => { form = f }} initial={existing ?? { table: '', do_stock: true }} schemaHint={schemaKey} />,
      okText: existing ? '保存修改' : '增加表',
      cancelText: '取消',
      onOk: async () => {
        const values = await form?.validate()
        if (!values) throw new Error('表单校验未通过')
        const patch = values as unknown as RuleTable
        const originalName = existing?.table ?? ''
        setGroups((gs) => gs.map((item) => (item.schema ?? '') !== schemaKey ? item : {
          ...item,
          // 修改 = 按原表名定位替换（表名可改）；新增 = 追加
          tables: existing
            ? tablesOf(item).map((t) => (t.table === originalName ? { ...t, ...patch } : t))
            : [...tablesOf(item), patch],
        }))
      },
    })
  }

  function removeTable(schemaKey: string, tableName: string) {
    modal.confirm({
      title: `移除表 ${tableName}？`, width: 460, icon: null,
      content: '该表将停止同步（事件过滤），目标端数据保留不动；全量死标记由后台 GC 清理。保存后需应用重启生效。',
      okText: '移除表', okButtonProps: { danger: true }, cancelText: '取消',
      onOk: () => setGroups((gs) => gs.map((item) => (item.schema ?? '') !== schemaKey ? item : {
        ...item,
        tables: tablesOf(item).filter((t) => t.table !== tableName),
      })),
    })
  }

  // ── 不可编辑回退：本地文件配置形态（409）→ 只读生效视图 ──
  if (load.error instanceof ApiError && load.error.status === 409 && readOnlyView) {
    return (
      <>
        <Alert type="warning" showIcon style={{ marginBottom: 12 }}
          message="当前配置来源为本地文件，无法在线编辑（请直接编辑文件后重启进程）；以下为只读生效视图。" />
        {readOnlyView}
      </>
    )
  }
  if (load.error) return <ErrorState error={load.error} onRetry={() => void load.refetch()} />
  if (load.isPending || revision === null) return <LoadingState />

  const clientErrors = validateDraft(draftFromRules(groups))
  const pendingBySchema = new Map<string, number>()
  for (const event of pendingDDL) pendingBySchema.set(event.schema, (pendingBySchema.get(event.schema) ?? 0) + 1)

  const dirtyBanner = (dirty || clientErrors.length > 0) && (
    <Alert className="page-notice" type={clientErrors.length > 0 ? 'error' : 'warning'} showIcon
      style={{ marginBottom: 12 }}
      message={clientErrors.length > 0 ? '提交前检查未通过' : '有未保存的规则变更'}
      description={clientErrors.length > 0
        ? <ul style={{ paddingLeft: 18, margin: 0 }}>{clientErrors.map((e, i) => <li key={i}>{e}</li>)}</ul>
        : '保存前服务端会执行：全量配置校验 → 变更表活体预检 → 防并发覆盖；任一失败写入被拒并展示全部错误明细。'}
      action={dirty ? <Space>
        <Button size="small" type="primary" icon={<SaveOutlined />} loading={save.isPending}
          disabled={clientErrors.length > 0} onClick={confirmSave}>校验并保存</Button>
        <Button size="small" icon={<UndoOutlined />} onClick={confirmReset}>放弃修改</Button>
      </Space> : undefined} />
  )

  // ── 二级：某源库的表列表（增加表/修改表/移除表）──
  if (schema !== null) {
    const g = groups.find((item) => (item.schema ?? '') === schema)
    const needle = tableSearch.trim().toLocaleLowerCase()
    const tables = tablesOf(g ?? { schema: '', tables: [] }).filter((t) => !needle || (t.table ?? '').toLocaleLowerCase().includes(needle))
    const tableCols: ColumnsType<RuleTable> = [
      {
        title: '表名', key: 'table', fixed: 'left', width: 200,
        render: (_, t) => (
          <div className="entity">
            <span className="entity-icon"><TableOutlined /></span>
            <span><b>{t.table}</b><small>{schema}</small></span>
          </div>
        ),
      },
      { title: '快照排序键', key: 'order_by', width: 130, render: (_, t) => <CodeValue>{t.order_by_column || '主键顺序'}</CodeValue> },
      {
        title: '参与全量', key: 'do_stock', width: 105,
        render: (_, t) => <span className={t.do_stock ? 'state-text is-on' : 'state-text is-off'}><i className="led" />{t.do_stock ? '全量+增量' : '只增量'}</span>,
      },
      { title: '目标库覆盖', key: 'td', width: 140, render: (_, t) => <CodeValue>{t.target_database || ''}</CodeValue> },
      { title: '排除列', key: 'ex', render: (_, t) => t.exclude_columns?.length ? <Text code>{t.exclude_columns.join(',')}</Text> : <span className="text-muted">—</span> },
      { title: '脱敏列数', key: 'mask', width: 95, render: (_, t) => Object.keys(t.mask_columns ?? {}).length
        ? <Chip tone="info">{Object.keys(t.mask_columns ?? {}).length} 列</Chip>
        : <span className="text-muted">—</span> },
      {
        title: '操作', key: 'op', width: 160, fixed: 'right',
        render: (_, t) => (
          <div className="row-actions">
            <Button type="text" size="small" icon={<EditOutlined />} disabled={!g} onClick={() => g && tableModal(g, t)}>修改表</Button>
            <Button type="text" size="small" danger icon={<DeleteOutlined />} disabled={!g} onClick={() => removeTable(schema, t.table)}>移除表</Button>
          </div>
        ),
      },
    ]
    return (
      <>
        {dirtyBanner}
        <div className="toolbar drill-toolbar">
          <div className="drill-title">
            <Button className="btn-back" icon={<ArrowLeftOutlined />} onClick={() => { setSchema(null); setTableSearch('') }}>返回库列表</Button>
            <span className="entity-icon"><DatabaseOutlined /></span>
            <div className="drill-copy">
              <b>{schema}</b>
              <small>{tablesOf(g ?? { schema: '', tables: [] }).length} 张表 · {g?.target_database || '同名库'}</small>
            </div>
          </div>
          <Space>
            <Input allowClear prefix={<SearchOutlined />} placeholder="搜索表名…" value={tableSearch}
              onChange={(event) => setTableSearch(event.target.value)} className="search-input" />
            <Button type="primary" icon={<PlusOutlined />} disabled={!g} onClick={() => g && tableModal(g, null)}>增加表</Button>
          </Space>
        </div>
        <div className="table-shell">
          <Table<RuleTable> className="admin-table" rowKey={(t, ti) => `${t.table}#${ti}`}
            columns={tableCols} dataSource={tables} scroll={{ x: 1050 }}
            pagination={{ pageSize: 10, showSizeChanger: true, showTotal: (total) => `共 ${total} 张表`, pageSizeOptions: [10, 20, 50] }}
            locale={{ emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={needle ? '没有匹配的表' : '该源库还没有表——点「增加表」接入第一张'} /> }} />
        </div>
      </>
    )
  }

  // ── 一级：源库列表（新增源库/修改源库/移除源库 + 下钻）──
  const needle = schemaSearch.trim().toLocaleLowerCase()
  const visible = groups.filter((g) => !needle || (g.schema ?? '').toLocaleLowerCase().includes(needle))
  const groupCols: ColumnsType<RuleGroup> = [
    {
      title: '源库', key: 'schema', fixed: 'left', width: 220,
      render: (_, g) => (
        <div className="entity">
          <span className="entity-icon"><DatabaseOutlined /></span>
          <span><b>{g.schema || '(未命名)'}</b><small>{g.target_database || '同名库'}</small></span>
        </div>
      ),
    },
    {
      title: '表数', key: 'count', width: 100,
      render: (_, g) => (
        <Button type="link" size="small" icon={<TableOutlined />}
          onClick={() => { setSchema(g.schema ?? ''); setTableSearch('') }}>
          {tablesOf(g).length} 张表
        </Button>
      ),
    },
    { title: 'DDL 策略', key: 'ddl', width: 110, render: (_, g) => <PolicyChip policy={g.ddl_policy} /> },
    {
      title: '下游', key: 'sinks', width: 190,
      render: (_, g) => (
        <div className="chip-row">
          <SinkTag label="MySQL" enabled={sinkOn(g, 'mysql')} />
          <SinkTag label="Kafka" enabled={sinkOn(g, 'kafka')} title={sinkOn(g, 'kafka') && typeof g.sinks?.kafka === 'object' && g.sinks.kafka?.topic ? `Topic: ${g.sinks.kafka.topic}` : undefined} />
          <SinkTag label="RocketMQ" enabled={sinkOn(g, 'rocketmq')} title={sinkOn(g, 'rocketmq') && typeof g.sinks?.rocketmq === 'object' ? [g.sinks.rocketmq?.topic && `Topic: ${g.sinks.rocketmq.topic}`, g.sinks.rocketmq?.tag && `Tag: ${g.sinks.rocketmq.tag}`].filter(Boolean).join(' · ') : undefined} />
        </div>
      ),
    },
    {
      title: '挂起 DDL', key: 'pending', width: 110,
      render: (_, g) => (pendingBySchema.get(g.schema ?? '') ?? 0) > 0
        ? <Chip tone="warn" dot>{pendingBySchema.get(g.schema ?? '')} 待审</Chip>
        : <span className="text-muted">0</span>,
    },
    {
      title: '操作', key: 'op', width: 200,
      render: (_, g) => (
        <div className="row-actions">
          <Button type="text" size="small" icon={<EditOutlined />} onClick={() => groupModal(g.schema ?? '')}>修改源库</Button>
          <Button type="text" size="small" danger icon={<DeleteOutlined />} onClick={() => removeGroup(g.schema ?? '', g.schema || '(未命名)')}>移除源库</Button>
        </div>
      ),
    },
  ]

  return (
    <>
      {dirtyBanner}
      <div className="toolbar">
        <div className="toolbar-stats">
          <span className="toolbar-stat"><b>{groups.length}</b><small>源库</small></span>
          <span className="toolbar-divider" />
          <span className="toolbar-stat"><b>{groups.reduce((sum, g) => sum + tablesOf(g).length, 0)}</b><small>表白名单</small></span>
          <span className="toolbar-divider" />
          <span className="toolbar-stat"><b>{pendingDDL.length}</b><small>待决 DDL</small></span>
        </div>
        <Space>
          <Input allowClear prefix={<SearchOutlined />} placeholder={`在源 ${sid} 中搜索源库…`} value={schemaSearch}
            onChange={(event) => setSchemaSearch(event.target.value)} className="search-input" />
          <Button type="primary" icon={<PlusOutlined />} onClick={() => groupModal(null)}>新增源库</Button>
        </Space>
      </div>
      <div className="table-shell">
        <Table<RuleGroup> className="admin-table" rowKey={(g) => g.schema || `__g${groups.indexOf(g)}`}
          columns={groupCols} dataSource={visible} scroll={{ x: 930 }}
          pagination={visible.length > 10 ? { pageSize: 10, showTotal: (total) => `共 ${total} 个库` } : false}
          locale={{ emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={needle ? '没有匹配的源库' : '暂无规则——点「新增源库」接入第一个库'} /> }}
          onRow={(g) => ({ onClick: () => { setSchema(g.schema ?? ''); setTableSearch('') }, style: { cursor: 'pointer' } })}
        />
      </div>
    </>
  )
}

/** 表单引用（modal.confirm 内容里的受控 Form，onOk 时取校验值）。 */
interface FormRefLike {
  validate: () => Promise<Record<string, unknown>>
}

type GroupFormProps = { onReady: (f: FormRefLike) => void; initial: RuleGroup; defaultSinks: Record<SinkKey, boolean>; enabledSinksHint: string[] }

/** 源库（规则组）弹窗表单：schema / DDL 策略 / 目标库覆盖 / 下游三开关。 */
function GroupForm({ onReady, initial, defaultSinks, enabledSinksHint }: GroupFormProps) {
  const [form] = Form.useForm()
  onReady({
    validate: async () => {
      const v = await form.validateFields()
      // 开 = 保留原值（bool true 或对象形态的 topic/tag 覆盖原样带回）；关 = false。
      // 切忌用开关布尔覆盖——对象覆盖会被静默丢弃（topic 配置丢失）。
      const orig = initial.sinks ?? {}
      return {
        schema: v.schema,
        ddl_policy: v.ddl_policy,
        target_database: v.target_database || undefined,
        sinks: {
          mysql: keepSink(orig, 'mysql', v.mysql),
          kafka: keepSink(orig, 'kafka', v.kafka),
          rocketmq: keepSink(orig, 'rocketmq', v.rocketmq),
        },
      }
    },
  })
  return (
    <Form form={form} layout="vertical" initialValues={{
      schema: initial.schema, ddl_policy: initial.ddl_policy || 'pause', target_database: initial.target_database,
      mysql: defaultSinks.mysql, kafka: defaultSinks.kafka, rocketmq: defaultSinks.rocketmq,
    }}>
      <Form.Item name="schema" label="源库 schema（一组一库，允许点分层）" rules={[{ required: true, message: '源库 schema 必填' }]}>
        <Input placeholder="如 edu.mall.admin" />
      </Form.Item>
      <Form.Item name="ddl_policy" label="DDL 策略">
        <Select options={DDL_POLICIES} />
      </Form.Item>
      <Form.Item name="target_database" label="目标库覆盖（多目标聚合写一个库在此配置）" extra="空 = 与源库同名；表级可再覆盖">
        <Input placeholder="空 = 与源库同名" />
      </Form.Item>
      <Form.Item label={`下游 sinks（该源 targets 已启用：${enabledSinksHint.join(' / ') || '未知'}；至少启用一个，保存时服务端校验）`} required>
        <Space size={20}>
          {SINK_KEYS.map((key) => (
            <Space size={6} key={key}>
              <Text type="secondary">{key}</Text>
              <Form.Item name={key} valuePropName="checked" noStyle><Switch /></Form.Item>
            </Space>
          ))}
        </Space>
      </Form.Item>
    </Form>
  )
}

type TableFormProps = { onReady: (f: FormRefLike) => void; initial: RuleTable; schemaHint: string }

/** 表弹窗表单：表名 / 快照排序键 / 参与全量 / 目标库覆盖 / 排除列 / 脱敏列。 */
function TableForm({ onReady, initial, schemaHint }: TableFormProps) {
  const [form] = Form.useForm()
  onReady({
    validate: async () => {
      const v = await form.validateFields()
      // 排除列留空 = 不排除；脱敏留空 = 不脱敏（整体替换语义，与 ruleDraft 一致）
      const exclude = parseExclude(v.exclude_columns ?? '')
      return {
        table: v.table,
        order_by_column: v.order_by_column || undefined,
        do_stock: v.do_stock,
        target_database: v.target_database || undefined,
        exclude_columns: exclude.length > 0 ? exclude : undefined,
        mask_columns: v.mask_columns ? (JSON.parse(v.mask_columns) as RuleTable['mask_columns']) : undefined,
      }
    },
  })
  return (
    <Form form={form} layout="vertical" initialValues={{
      table: initial.table, order_by_column: initial.order_by_column,
      do_stock: initial.do_stock !== false, target_database: initial.target_database,
      exclude_columns: Array.isArray(initial.exclude_columns) ? formatExclude(initial.exclude_columns) : '',
      mask_columns: initial.mask_columns && Object.keys(initial.mask_columns).length ? JSON.stringify(initial.mask_columns) : '',
    }}>
      <Alert type="info" showIcon style={{ marginBottom: 12 }}
        message={`目标表须已预建且列集一致（保存前服务端自动活体预检，源库 ${schemaHint}）`} />
      <Form.Item name="table" label="表名（须具体表名：禁空白/点/反引号/*通配）" rules={[{ required: true, message: '表名必填' }]}>
        <Input placeholder="表名" />
      </Form.Item>
      <Form.Item name="order_by_column" label="快照排序键" extra="参与全量时必填（用于分块扫描排序）；主键正常时填主键列即可">
        <Input placeholder="如 id" />
      </Form.Item>
      <Form.Item name="do_stock" label="参与全量快照" valuePropName="checked" extra="关闭 = 只同步增量，不回刷存量数据">
        <Switch />
      </Form.Item>
      <Form.Item name="target_database" label="目标库覆盖（空 = 组级）">
        <Input placeholder="空 = 组级" />
      </Form.Item>
      <Form.Item name="exclude_columns" label="排除列（逗号分隔，目标表不应有这些列）">
        <Input placeholder="如 created_at, updated_at" />
      </Form.Item>
      <Form.Item name="mask_columns" label="脱敏列（JSON：列名 → 算法或 {type,...} 对象；留空 = 不脱敏）"
        rules={[{ validator: (_, v) => (!v ? Promise.resolve() : (() => { try { JSON.parse(v); return Promise.resolve() } catch { return Promise.reject(new Error('JSON 格式不正确')) } })()) }]}>
        <Input placeholder='{"phone":"phone","id_card":{"type":"hash_sha256"}}' />
      </Form.Item>
    </Form>
  )
}
