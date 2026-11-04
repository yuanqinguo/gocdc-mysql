import { useState } from 'react'
import { App as AntApp, Button, Collapse, Form, Input, InputNumber, Modal, Space } from 'antd'
import { CheckCircleOutlined, DatabaseOutlined, ReloadOutlined, WarningOutlined } from '@ant-design/icons'
import { useMutation, useQueries, useQueryClient } from '@tanstack/react-query'
import { useApi } from '@/app/api'
import { useSourceScope } from '@/app/sourceScope'
import { queryKeys, useDDLEvents, useDDLEventsAll, useEffectiveRules, useSources, useStatus } from '@/shared/api/queries'
import type { EffectiveRule, VerifyResult } from '@/shared/api/types'
import { Callout } from '@/shared/components/Callout'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { RuleExplorer } from './RuleExplorer'
import { RuleEditor } from './RuleEditor'

export function RulesPage() {
  const { currentSid } = useSourceScope()
  const { data: status } = useStatus()
  // 无选中源时单源部署默认锁定唯一源
  const sources = status?.sources ?? []
  const effectiveSid = currentSid ?? (sources.length === 1 ? sources[0]?.id ?? null : null)
  return effectiveSid ? <SingleSourceRules sid={effectiveSid} /> : <AllSourcesRules />
}

/** 单源视图：该源生效规则 + 重跑/对账操作 + 规则编辑（配置中心 CAS 写）。 */
function SingleSourceRules({ sid }: { sid: string }) {
  const { data: rules = [], isPending, error, refetch } = useEffectiveRules(sid)
  const { data: ddlEvents = [] } = useDDLEvents(sid)
  const [verifyRule, setVerifyRule] = useState<EffectiveRule | null>(null)
  const [verifyResult, setVerifyResult] = useState<VerifyResult | null>(null)
  const [editorOpen, setEditorOpen] = useState(false)
  const [form] = Form.useForm<{ pk: string; sample: number }>()
  const api = useApi()
  const queryClient = useQueryClient()
  const { message, modal } = AntApp.useApp()
  const restock = useMutation({
    mutationFn: (rule: EffectiveRule) => api.restockOf(sid, rule.schema, rule.table),
    onSuccess: async (_res, rule) => {
      // 重跑会重启该源管线（排空→全量回放→续增量）：状态类查询一并失效，
      // 让总控台/状态页立即反映「全量回放」，而不是等轮询周期
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.rules(sid) }),
        queryClient.invalidateQueries({ queryKey: queryKeys.status }),
        queryClient.invalidateQueries({ queryKey: queryKeys.sources }),
        queryClient.invalidateQueries({ queryKey: queryKeys.sourceStatus(sid) }),
      ])
      void message.success(
        `已重置 ${rule.schema}.${rule.table} 的全量标记并触发该源重走：管线进入「全量回放」后重扫该表，完成后续接增量。`,
      )
    },
    onError: (e) => void message.error(e.message),
  })
  const verify = useMutation({
    mutationFn: (input: { rule: EffectiveRule; pk: string; sample: number }) =>
      api.verifyOf(sid, input.rule.schema, input.rule.table, input.pk, input.sample),
    onSuccess: (result) => setVerifyResult(result),
    onError: (e) => void message.error(e.message),
  })

  const confirmRestock = (rule: EffectiveRule) => {
    modal.confirm({
      title: '确认重跑全量同步？',
      content: <span>
        将重置源「{sid}」的 <b>{rule.schema}.{rule.table}</b> 全量标记，并自动重启该源同步管线：排空 → 全量重扫（无 done 标记表）→ 从新快照位点续接增量。
        操作会等待当前管线排空后才返回（最多约 15 秒），期间请不要重复提交。
        目标端为幂等 upsert，已有行按快照值覆盖；重扫不会删除「目标有、源没有」的行。
        请先确认目标表状态和下游负载。
      </span>,
      okText: '确认重跑', okButtonProps: { danger: true }, cancelText: '取消',
      onOk: () => restock.mutateAsync(rule),
    })
  }
  const openVerify = (rule: EffectiveRule) => {
    setVerifyRule(rule)
    setVerifyResult(null)
    form.setFieldsValue({ pk: 'id', sample: 100 })
  }

  return <>
    <PageHeader
      code="02"
      eyebrow={`复制 / 规则目录 · 源 ${sid}`}
      title="复制规则"
      description={`源「${sid}」组形态白名单的库级摘要与逐表生效视图；列表来自后端编译产物，前端不做继承推算。`}
      extra={<Space>
        <Button type="primary" onClick={() => setEditorOpen(true)}>编辑规则</Button>
        <Button icon={<ReloadOutlined />} onClick={() => void refetch()}>刷新</Button>
      </Space>}
    />
    <Callout tone="info" title="规则视图说明">
      点击源库下钻查看该库全部表规则；「编辑规则」保存到配置中心（校验+活体预检+CAS），重启进程后生效。
    </Callout>
    {error ? <ErrorState error={error} onRetry={() => void refetch()} /> : isPending ? <LoadingState /> : (
      <RuleExplorer rules={rules} pendingDDL={ddlEvents} showActions
        onRestock={confirmRestock} onVerify={openVerify} actionPending={restock.isPending} />
    )}
    <RuleEditor sid={sid} open={editorOpen} onClose={() => setEditorOpen(false)} />
    <VerifyModal
      verifyRule={verifyRule}
      verifyResult={verifyResult}
      form={form}
      pending={verify.isPending}
      onSubmit={() => !verifyRule ? undefined : form.validateFields().then((values) => verify.mutateAsync({ rule: verifyRule, pk: values.pk, sample: values.sample })).then(() => undefined)}
      onClose={() => { setVerifyRule(null); setVerifyResult(null) }}
    />
  </>
}

/** 全部视图（多源）：按源分组的折叠列表，只读。 */
function AllSourcesRules() {
  const sources = useSources()
  const ddlAll = useDDLEventsAll()
  const api = useApi()
  const ids = sources.data?.map((item) => item.id) ?? []
  const rulesQueries = useQueries({
    queries: ids.map((sid) => ({
      queryKey: queryKeys.rules(sid),
      queryFn: ({ signal }) => api.getEffectiveRulesOf(sid, signal),
      staleTime: 30_000,
    })),
  })

  return <>
    <PageHeader
      code="02"
      eyebrow="复制 / 规则目录"
      title="复制规则"
      description="按源分组的生效规则只读视图；需要重跑全量或抽样对账时，从源选择器进入单源视图操作。"
      extra={<Button icon={<ReloadOutlined />} onClick={() => void sources.refetch()}>刷新</Button>}
    />
    <Callout tone="info" title="规则视图说明">
      展开各源查看该源的库级摘要与表规则；单表重跑/抽样对账属于源作用域操作，请切换到单源视图执行。
    </Callout>
    {sources.isPending ? <LoadingState /> : sources.error ? <ErrorState error={sources.error} onRetry={() => void sources.refetch()} /> : (
      <Collapse
        className="source-collapse"
        items={ids.map((sid, index) => {
          const query = rulesQueries[index]!
          const tables = query.data?.length ?? 0
          return {
            key: sid,
            label: (
              <span className="entity">
                <span className="entity-icon"><DatabaseOutlined /></span>
                <span><b>{sid}</b><small>{tables} 张表 · 只读视图</small></span>
              </span>
            ),
            children: query.isPending
              ? <LoadingState />
              : query.error
                ? <ErrorState error={query.error} onRetry={() => void query.refetch()} />
                : <RuleExplorer
                    rules={query.data ?? []}
                    pendingDDL={(ddlAll.data ?? []).filter((record) => record.source === sid)}
                    schemaSearchPlaceholder={`在源 ${sid} 中搜索源库…`}
                  />,
          }
        })}
      />
    )}
  </>
}

function VerifyModal({ verifyRule, verifyResult, form, pending, onSubmit, onClose }: {
  verifyRule: EffectiveRule | null
  verifyResult: VerifyResult | null
  form: ReturnType<typeof Form.useForm<{ pk: string; sample: number }>>[0]
  pending: boolean
  onSubmit: () => void | Promise<void>
  onClose: () => void
}) {
  return (
    <Modal
      title={verifyResult ? '抽样对账结果' : `抽样对账 · ${verifyRule?.schema}.${verifyRule?.table}`}
      open={verifyRule !== null}
      onCancel={onClose}
      footer={verifyResult ? <Button type="primary" onClick={onClose}>完成</Button> : undefined}
      okText="开始对账"
      cancelText="关闭"
      confirmLoading={pending}
      onOk={onSubmit}
    >
      {verifyResult ? <div className="verify-result">
        <div className={`verify-summary ${verifyResult.mismatch === 0 ? 'verify-ok' : 'verify-diff'}`}>
          <div className="verify-result-icon">{verifyResult.mismatch === 0 ? <CheckCircleOutlined /> : <WarningOutlined />}</div>
          <div><b>{verifyResult.mismatch === 0 ? '抽样数据一致' : `发现 ${verifyResult.mismatch} 处差异`}</b><span>结果差异属于正常对账结果，不代表请求失败。</span></div>
        </div>
        <div className="verify-counts">
          <div><small>源表行数</small><b>{verifyResult.src_count.toLocaleString()}</b></div>
          <div><small>目标表行数</small><b>{verifyResult.tgt_count.toLocaleString()}</b></div>
          <div><small>抽样检查</small><b>{verifyResult.checked.toLocaleString()}</b></div>
        </div>
        {verifyResult.diffs?.length > 0 && (
          <div className="diff-list">
            {verifyResult.diffs.map((diff, index) => <code key={`${index}-${diff}`}>{diff}</code>)}
            {verifyResult.truncated && <span className="diff-truncated">差异明细过多，仅显示前几项。</span>}
          </div>
        )}
      </div> : <>
        <p className="modal-copy">从源表与目标表抽样比较数据。大表扫描需要一定时间，请在低峰期执行。</p>
        <Callout tone="warning" title="请输入真实的唯一键列">
          抽样会按该列逐行定位并比对，列值必须唯一且有索引。全量同步的 order_by_column 仅用于扫描排序，不一定适合作为对账键。默认值 id 仅为 API 缺省值。
        </Callout>
        <Form form={form} layout="vertical" initialValues={{ pk: 'id', sample: 100 }}>
          <Form.Item name="pk" label="唯一键 / 主键列" rules={[{ required: true, message: '请输入用于定位的唯一键列' }]}><Input placeholder="例如 id" /></Form.Item>
          <Form.Item name="sample" label="抽样行数" rules={[{ required: true, message: '请输入抽样行数' }]}><InputNumber min={1} max={1000} className="full-width" /></Form.Item>
        </Form>
        <div className="modal-footnote">接口最大抽样上限为 1000 行；脱敏字段在目标端值不同属于预期差异。</div>
      </>}
    </Modal>
  )
}
