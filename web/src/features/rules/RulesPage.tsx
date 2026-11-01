import { useState } from 'react'
import { App as AntApp, Button, Form, Input, InputNumber, Modal } from 'antd'
import { CheckCircleOutlined, ReloadOutlined, WarningOutlined } from '@ant-design/icons'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useApi } from '@/app/api'
import { queryKeys, useEffectiveRules, useDDLEvents } from '@/shared/api/queries'
import type { EffectiveRule, VerifyResult } from '@/shared/api/types'
import { Callout } from '@/shared/components/Callout'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { RuleExplorer } from './RuleExplorer'

export function RulesPage() {
  const { data: rules = [], isPending, error, refetch } = useEffectiveRules()
  const { data: ddlEvents = [] } = useDDLEvents()
  const [verifyRule, setVerifyRule] = useState<EffectiveRule | null>(null)
  const [verifyResult, setVerifyResult] = useState<VerifyResult | null>(null)
  const [form] = Form.useForm<{ pk: string; sample: number }>()
  const api = useApi()
  const queryClient = useQueryClient()
  const { message, modal } = AntApp.useApp()
  const restock = useMutation({
    mutationFn: (rule: EffectiveRule) => api.restock(rule.schema, rule.table),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.rules })
      void message.success('已重置该表的全量标记。')
    },
    onError: (e) => void message.error(e.message),
  })
  const verify = useMutation({
    mutationFn: (input: { rule: EffectiveRule; pk: string; sample: number }) =>
      api.verify(input.rule.schema, input.rule.table, input.pk, input.sample),
    onSuccess: (result) => setVerifyResult(result),
    onError: (e) => void message.error(e.message),
  })

  const confirmRestock = (rule: EffectiveRule) => {
    modal.confirm({
      title: '确认重跑全量同步？',
      content: <span>将重置 <b>{rule.schema}.{rule.table}</b> 的全量标记。请先确认目标表状态和下游负载。</span>,
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
      eyebrow="复制 / 规则目录"
      title="复制规则"
      description="组形态白名单的库级摘要与逐表生效视图；列表来自后端编译产物，前端不做继承推算。"
      extra={<Button icon={<ReloadOutlined />} onClick={() => void refetch()}>刷新规则</Button>}
    />
    <Callout tone="info" title="规则视图说明">
      点击源库下钻查看该库全部表规则；当前接口提供规则视图与单表重跑/抽样对账，规则运行状态、暂停/恢复操作尚未由后端 API 提供。
    </Callout>
    {error ? <ErrorState error={error} onRetry={() => void refetch()} /> : isPending ? <LoadingState /> : (
      <RuleExplorer rules={rules} pendingDDL={ddlEvents} showActions
        onRestock={confirmRestock} onVerify={openVerify} actionPending={restock.isPending} />
    )}
    <Modal
      title={verifyResult ? '抽样对账结果' : `抽样对账 · ${verifyRule?.schema}.${verifyRule?.table}`}
      open={verifyRule !== null}
      onCancel={() => { setVerifyRule(null); setVerifyResult(null) }}
      footer={verifyResult ? <Button type="primary" onClick={() => { setVerifyRule(null); setVerifyResult(null) }}>完成</Button> : undefined}
      okText="开始对账"
      cancelText="关闭"
      confirmLoading={verify.isPending}
      onOk={() => !verifyRule ? undefined : form.validateFields().then((values) => verify.mutateAsync({ rule: verifyRule, pk: values.pk, sample: values.sample })).then(() => undefined)}
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
  </>
}
