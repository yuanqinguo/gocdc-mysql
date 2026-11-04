import { useEffect, useState } from 'react'
import { App as AntApp, Alert, Button, Checkbox, Drawer, Input, Popconfirm, Select, Space, Table, Typography } from 'antd'
import { CheckCircleOutlined, ExclamationCircleOutlined, SaveOutlined, ThunderboltOutlined } from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useApi } from '@/app/api'
import { ApiError } from '@/shared/api/client'
import { queryKeys } from '@/shared/api/queries'
import type { PutRulesResult } from '@/shared/api/types'
import { Callout } from '@/shared/components/Callout'
import { diffSummaryText, draftFromRules, payloadFromDraft, validateDraft } from './ruleDraft'
import type { GroupDraft } from './ruleDraft'

const policyOptions = [
  { value: 'auto', label: 'auto（白名单 DDL 自动执行）' },
  { value: 'pause', label: 'pause（DDL 暂停待审批）' },
  { value: 'ignore', label: 'ignore（DDL 不同步）' },
]

/**
 * 规则编辑抽屉：读取配置中心该源规则组 → 表格化增删改 → [预检]（活体核对
 * 变更表）/ [保存]（校验+预检+CAS 写配置中心）→ 保存成功后提示「应用并重启」
 * （优雅排空退出，由编排拉起应用新配置）。
 * 配置不热更：保存只落配置中心，重启才生效——抽屉内全程提示该语义。
 */
export function RuleEditor({ sid, open, onClose }: { sid: string; open: boolean; onClose: () => void }) {
  const { message, modal } = AntApp.useApp()
  const api = useApi()
  const queryClient = useQueryClient()
  const [draft, setDraft] = useState<GroupDraft[] | null>(null)
  const [base, setBase] = useState(0)
  const [result, setResult] = useState<PutRulesResult | null>(null)
  const [savedRevision, setSavedRevision] = useState<number | null>(null)

  const load = useQuery({
    queryKey: ['source-rules', sid],
    queryFn: ({ signal }) => api.getRulesOf(sid, signal),
    enabled: open,
    staleTime: 0,
  })
  // 草稿播种（effect，非渲染期 setState）：打开时以配置中心现值初始化；
  // 关闭即清（下次打开重新播种，外部改动以配置中心为准）。保存后不动草稿、
  // 手动以返回的新 revision 更新 base——不 invalidate 本查询，避免 refetch
  // 重置草稿清掉「应用并重启」横幅。
  useEffect(() => {
    if (!open) {
      setDraft(null)
      setResult(null)
      setSavedRevision(null)
      return
    }
    if (load.data) {
      setDraft(draftFromRules(load.data.rules))
      setBase(load.data.revision)
      setResult(null)
      setSavedRevision(null)
    }
  }, [open, load.data])

  const check = useMutation({
    mutationFn: (drafts: GroupDraft[]) => api.putRulesOf(sid, base, payloadFromDraft(drafts), true),
    onSuccess: (res) => setResult(res),
    onError: (e) => void message.error(e.message),
  })
  const save = useMutation({
    mutationFn: (drafts: GroupDraft[]) => api.putRulesOf(sid, base, payloadFromDraft(drafts), false),
    onSuccess: async (res) => {
      setBase(res.revision) // 再次保存用新 base；不动草稿（内容与已保存一致）
      setSavedRevision(res.revision)
      await queryClient.invalidateQueries({ queryKey: queryKeys.rules(sid) }) // 只刷新只读生效视图
      void message.success('已保存到配置中心（重启后生效）。')
    },
    onError: (e) => void message.error(e.message),
  })
  const restart = useMutation({
    mutationFn: () => api.applyRestart(),
    onError: (e) => void message.error(e.message),
  })

  const drafts = draft ?? []
  const clientErrors = validateDraft(drafts)

  const updateGroup = (index: number, patch: Partial<GroupDraft>) => {
    setDraft(drafts.map((g, i) => (i === index ? { ...g, ...patch } : g)))
  }
  const updateTable = (gi: number, ti: number, patch: Partial<GroupDraft['tables'][number]>) => {
    setDraft(drafts.map((g, i) =>
      i === gi ? { ...g, tables: g.tables.map((t, j) => (j === ti ? { ...t, ...patch } : t)) } : g))
  }

  const confirmSave = () => {
    const res = result
    const summary = res ? diffSummaryText(res) : '变更清单以预检结果为准'
    modal.confirm({
      title: '保存规则到配置中心？',
      content: (
        <div>
          <p>本次变更：{summary}。保存前服务端已做全量校验与活体预检；保存成功后<b>需重启进程才生效</b>。</p>
          <ul style={{ paddingLeft: 18, margin: 0 }}>
            <li>加表 = 多同步（重启后自动进入全量快照，do_stock=true 时）；</li>
            <li>减表 = 少同步（目标端数据保留不动，位点照常推进）；</li>
            <li>mask/exclude 修改只作用于后续事件，不影响已同步的历史数据。</li>
          </ul>
        </div>
      ),
      okText: '确认保存', cancelText: '取消',
      onOk: () => save.mutateAsync(drafts).then(() => undefined),
    })
  }
  const confirmRestart = () => {
    modal.confirm({
      title: '应用并重启本节点？',
      content: '本进程将优雅停止（排空队列、位点终值落盘、resign），由编排层拉起以应用新配置。若进程无自动拉起机制（compose restart 策略 / k8s），同步将保持停止。',
      okText: '优雅重启', okButtonProps: { danger: true }, cancelText: '取消',
      onOk: () => restart.mutateAsync().then(() => undefined),
    })
  }

  return (
    <Drawer
      title={<span>编辑规则 · 源 <b>{sid}</b></span>}
      open={open}
      onClose={onClose}
      width={960}
      destroyOnHidden
      footer={
        <Space>
          <Button icon={<ThunderboltOutlined />} loading={check.isPending}
            disabled={clientErrors.length > 0} onClick={() => check.mutate(drafts)}>
            预检（不保存）
          </Button>
          <Button type="primary" icon={<SaveOutlined />} loading={save.isPending}
            disabled={clientErrors.length > 0} onClick={confirmSave}>
            保存到配置中心
          </Button>
          <Button danger icon={<ExclamationCircleOutlined />} disabled={savedRevision === null}
            loading={restart.isPending} onClick={confirmRestart}>
            应用并重启本节点
          </Button>
          <span style={{ marginLeft: 'auto', color: 'var(--text-tertiary, #999)' }}>CAS base: {base}</span>
        </Space>
      }
    >
      {load.isPending && <Alert type="info" showIcon message="正在读取配置中心…" />}
      {load.error && (
        <Alert type={load.error instanceof ApiError && load.error.status === 409 ? 'warning' : 'error'} showIcon
          message={load.error instanceof ApiError && load.error.status === 409
            ? '当前配置来源为本地文件，无法在线编辑（请直接编辑文件后重启进程）'
            : load.error.message} />
      )}
      {savedRevision !== null && (
        <Callout tone="warning" title="配置已保存，重启后生效">
          <p style={{ margin: '0 0 6px' }}>新配置 revision = {savedRevision}。当前进程仍按旧配置运行；重启后按新配置启动（新表 do_stock=true 会触发全量快照）。</p>
          <p style={{ margin: 0 }}>
            <b>集群形态请按序滚动</b>：先逐个重启 follower（带新配置回来当热备），最后重启 leader。顺序颠倒不会产生数据风险（旧配置=少同步，安全方向），只浪费一轮。
          </p>
        </Callout>
      )}
      {clientErrors.length > 0 && (
        <Alert type="error" showIcon message="提交前检查未通过" description={
          <ul style={{ paddingLeft: 18, margin: 0 }}>{clientErrors.map((e, i) => <li key={i}>{e}</li>)}</ul>
        } style={{ marginBottom: 12 }} />
      )}
      {result && <PreflightResult result={result} />}

      <Table<GroupDraft>
        rowKey={(g) => g.schema || `__g${drafts.indexOf(g)}`}
        size="small"
        pagination={false}
        dataSource={drafts}
        columns={[
          {
            title: '源库（schema）', dataIndex: 'schema', width: 200,
            render: (_, g, gi) => <Input value={g.schema} onChange={(e) => updateGroup(gi, { schema: e.target.value })} placeholder="如 edu.mall.admin" />,
          },
          {
            title: '下游 sinks', width: 220,
            render: (_, g, gi) => (
              <Space size={4} wrap>
                <Checkbox checked={g.sinks.mysql} onChange={(e) => updateGroup(gi, { sinks: { ...g.sinks, mysql: e.target.checked } })}>mysql</Checkbox>
                <Checkbox checked={g.sinks.kafka} onChange={(e) => updateGroup(gi, { sinks: { ...g.sinks, kafka: e.target.checked } })}>kafka</Checkbox>
                <Checkbox checked={g.sinks.rocketmq} onChange={(e) => updateGroup(gi, { sinks: { ...g.sinks, rocketmq: e.target.checked } })}>rocketmq</Checkbox>
              </Space>
            ),
          },
          {
            title: 'DDL 策略', width: 210,
            render: (_, g, gi) => (
              <Select value={g.ddlPolicy} options={policyOptions} style={{ width: '100%' }}
                onChange={(v) => updateGroup(gi, { ddlPolicy: v })} />
            ),
          },
          {
            title: '目标库覆盖', dataIndex: 'targetDatabase', width: 160,
            render: (_, g, gi) => <Input value={g.targetDatabase} onChange={(e) => updateGroup(gi, { targetDatabase: e.target.value })} placeholder="留空 = 全局默认" />,
          },
          {
            title: '', width: 60,
            render: (_, g, gi) => (
              <Popconfirm title={`移除源库 ${g.schema || ''} 的全部规则？目标端数据保留不动。`}
                onConfirm={() => setDraft(drafts.filter((_, i) => i !== gi))}>
                <Button size="small" danger type="text">移除</Button>
              </Popconfirm>
            ),
          },
        ]}
        expandedRowRender={(g, gi) => (
          <Table<GroupDraft['tables'][number]>
            rowKey={(t) => t.table || `__t${gi}-${g.tables.indexOf(t)}`}
            size="small"
            pagination={false}
            dataSource={g.tables}
            columns={[
              { title: '表名', width: 160, render: (_, t, ti) => <Input size="small" value={t.table} onChange={(e) => updateTable(gi, ti, { table: e.target.value })} /> },
              { title: '全量', width: 60, render: (_, t, ti) => <Checkbox checked={t.doStock} onChange={(e) => updateTable(gi, ti, { doStock: e.target.checked })} /> },
              { title: 'order_by_column', width: 140, render: (_, t, ti) => <Input size="small" value={t.orderBy} onChange={(e) => updateTable(gi, ti, { orderBy: e.target.value })} placeholder="do_stock 必填" /> },
              { title: '目标库覆盖', width: 140, render: (_, t, ti) => <Input size="small" value={t.targetDatabase} onChange={(e) => updateTable(gi, ti, { targetDatabase: e.target.value })} /> },
              { title: 'exclude_columns', width: 150, render: (_, t, ti) => <Input size="small" value={t.excludeText} onChange={(e) => updateTable(gi, ti, { excludeText: e.target.value })} placeholder="逗号分隔" /> },
              { title: 'mask（列=算法）', render: (_, t, ti) => <Input size="small" value={t.maskText} onChange={(e) => updateTable(gi, ti, { maskText: e.target.value })} placeholder="phone=phone, name=truncate" /> },
              {
                title: '', width: 60,
                render: (_, _t, ti) => (
                  <Button size="small" danger type="text"
                    onClick={() => updateGroup(gi, { tables: g.tables.filter((_, j) => j !== ti) })}>移除</Button>
                ),
              },
            ]}
            footer={() => (
              <Button size="small" onClick={() => updateGroup(gi, { tables: [...g.tables, { table: '', doStock: false, orderBy: '', targetDatabase: '', excludeText: '', maskText: '' }] })}>
                加表
              </Button>
            )}
          />
        )}
        footer={() => (
          <Button size="small" type="dashed"
            onClick={() => setDraft([...drafts, { schema: '', ddlPolicy: 'pause', targetDatabase: '', sinks: { mysql: true, kafka: false, rocketmq: false }, tables: [{ table: '', doStock: false, orderBy: '', targetDatabase: '', excludeText: '', maskText: '' }] }])}>
            加库（新增规则组）
          </Button>
        )}
      />
      <Typography.Paragraph type="secondary" style={{ marginTop: 12, marginBottom: 0, fontSize: 12 }}>
        语义边界：mask/exclude 修改不影响已同步的历史数据；减表目标端数据保留不动、stock 死标记由 GC 清理；
        加表 do_stock=true 时重启后自动进入全量快照。服务端校验与活体预检为权威，本页校验仅做快速反馈。
      </Typography.Paragraph>
    </Drawer>
  )
}

/** 预检/保存结果面板：errors 红（阻断）、warnings 黄（放行）、diff 摘要。 */
function PreflightResult({ result }: { result: PutRulesResult }) {
  const hasErrors = result.report.errors.length > 0
  const hasWarnings = result.report.warnings.length > 0
  if (!hasErrors && !hasWarnings) {
    return (
      <Alert type="success" showIcon icon={<CheckCircleOutlined />}
        message={`预检通过（核对 ${result.report.checked} 张变更表）· ${diffSummaryText(result)}`}
        style={{ marginBottom: 12 }} />
    )
  }
  return (
    <div style={{ marginBottom: 12 }}>
      {hasErrors && (
        <Alert type="error" showIcon message={`活体预检未通过（保存被阻断）· ${diffSummaryText(result)}`} description={
          <ul style={{ paddingLeft: 18, margin: 0 }}>{result.report.errors.map((e, i) => <li key={i}>{e}</li>)}</ul>
        } style={{ marginBottom: hasWarnings ? 8 : 0 }} />
      )}
      {hasWarnings && (
        <Alert type="warning" showIcon message="预检警告（不阻断保存）" description={
          <ul style={{ paddingLeft: 18, margin: 0 }}>{result.report.warnings.map((w, i) => <li key={i}>{w}</li>)}</ul>
        } />
      )}
    </div>
  )
}
