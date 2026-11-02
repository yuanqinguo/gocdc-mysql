import { useState } from 'react'
import { App as AntApp, Button, Form, Input, InputNumber, Modal } from 'antd'
import { AimOutlined, ExclamationCircleOutlined, LockOutlined, SafetyCertificateOutlined, SwapOutlined } from '@ant-design/icons'
import { Link } from 'react-router-dom'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useApi } from '@/app/api'
import { useSourceScope } from '@/app/sourceScope'
import { queryKeys, usePosition, useStatus } from '@/shared/api/queries'
import { Callout } from '@/shared/components/Callout'
import { DataCard } from '@/shared/components/DataCard'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip, CodeValue } from '@/shared/components/presentation'

export function PositionPage() {
  const { currentSid } = useSourceScope()
  const { data: status } = useStatus()
  const sources = status?.sources ?? []
  const effectiveSid = currentSid ?? (sources.length === 1 ? sources[0]?.id ?? null : null)
  return effectiveSid ? <SingleSourcePosition sid={effectiveSid} /> : <AllSourcesPosition />
}

/** 单源视图：位点卡 / 模式迁移 / SET-POSITION / etcdctl 指引（全部带 sid）。 */
function SingleSourcePosition({ sid }: { sid: string }) {
  const { data: position, isPending, error, refetch } = usePosition(sid)
  const { data: status } = useStatus()
  const [open, setOpen] = useState(false)
  const [form] = Form.useForm<{ name: string; pos: number; confirmation: string }>()
  const api = useApi()
  const queryClient = useQueryClient()
  const { message, modal } = AntApp.useApp()
  const source = status?.sources.find((item) => item.id === sid)
  const useGTID = source?.use_gtid ?? false
  const update = useMutation({
    mutationFn: (input: { name: string; pos: number }) => api.setPositionOf(sid, { ...input, confirm: 'SET-POSITION' }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.position(sid) })
      void message.success('位点修改请求已提交。')
      setOpen(false)
      form.resetFields()
    },
    onError: (cause) => void message.error(cause.message),
  })
  const pausePipe = useMutation({
    mutationFn: () => api.pausePipelineOf(sid),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.success('管线暂停中：位点已落盘，现在可以安全修改位点。')
    },
    onError: (cause) => void message.error(cause.message),
  })
  const refreshPosition = async () => {
    await queryClient.invalidateQueries({ queryKey: queryKeys.position(sid) })
    void message.success('位点迁移完成，按提示重启进程生效。')
  }
  const migrateGTID = useMutation({
    mutationFn: () => api.migrateGTIDOf(sid),
    onSuccess: async (result) => {
      await refreshPosition()
      void message.info(`已写入 gtid_set（窗口 ${result.gap_bytes} 字节）。${result.next}`)
    },
    onError: (cause) => void message.error(cause.message),
  })
  const migrateFilePos = useMutation({
    mutationFn: () => api.migrateFilePosOf(sid),
    onSuccess: async (result) => {
      await refreshPosition()
      void message.info(`${result.name}:${result.pos} — ${result.next}`)
    },
    onError: (cause) => void message.error(cause.message),
  })

  function submit() {
    void form.validateFields().then((values) => {
      modal.confirm({
        title: '最终确认：覆盖同步位点',
        icon: <ExclamationCircleOutlined />,
        content: (
          <div className="danger-confirm-copy">
            <p>即将把源「{sid}」的同步位置设置为：</p>
            <CodeValue>{`${values.name}:${values.pos}`}</CodeValue>
            <p>请确认管线当前未在消费位点（运行中写入会被水位覆盖），且该位点对应的 binlog 仍可用。</p>
          </div>
        ),
        okText: '确认覆盖位点',
        okButtonProps: { danger: true, loading: update.isPending },
        cancelText: '返回检查',
        onOk: () => update.mutateAsync({ name: values.name, pos: values.pos }),
      })
    })
  }

  const pipelineState = source?.pipeline_state
  const canMigrateGTID = useGTID && !!position && !!position.name && !position.gtid_set
  const canMigrateFilePos = useGTID === false && !!position?.gtid_set
  const posKey = `/gocdc/${status?.cluster || '{集群名}'}/sources/${sid}/position`
  const manualValue = JSON.stringify({
    v: 1,
    name: position?.name || 'mysql-bin.000123',
    pos: position?.pos || 4,
    seq: position?.seq ?? 0,
    leader_term: position?.term ?? 0,
    node_id: 'manual',
    updated_at: new Date().toISOString(),
  })

  return <>
    <PageHeader
      code="03"
      eyebrow={`复制 / 同步位点 · 源 ${sid}`}
      title="位点管理"
      description={`源「${sid}」的持久化位点保存在 etcd；修改位点属于危险运维操作。`}
    />
    <Callout tone="danger" title="位点修改前必须确认管线未在运行">
      管线运行中写入的位点会被水位落盘覆盖回去；错误的位点可能造成数据缺失或重复回放。可先在总控台暂停该源管线（优雅停机、保持 Leader），或全部节点停机后经 etcdctl 直接写位点 key <CodeValue>{posKey}</CodeValue>。
    </Callout>
    {error ? <ErrorState error={error} onRetry={() => void refetch()} /> : isPending ? <LoadingState rows={3} /> : <>
      <DataCard
        title={<b>已保存的同步位置 · 源 {sid}</b>}
        extra={<Button onClick={() => void refetch()}>刷新</Button>}
      >
        {position ? <>
          <div className="checkpoint-readout">
            <div className="checkpoint-file">
              <span className="panel-kicker">BINLOG 文件</span>
              <b>{position.name || '—'}</b>
              <small>位置 {position.pos.toLocaleString()}</small>
            </div>
            <div className="checkpoint-gtid">
              <span className="panel-kicker">GTID 集合</span>
              <code>{position.gtid_set || '未启用 GTID'}</code>
            </div>
          </div>
          <div className="spec-grid cols-2">
            <div className="spec-item"><span>序列号</span><CodeValue>{position.seq}</CodeValue></div>
            <div className="spec-item"><span>任期</span><CodeValue>{position.term}</CodeValue></div>
            <div className="spec-item"><span>最后保存节点</span><CodeValue>{position.node_id || '—'}</CodeValue></div>
            <div className="spec-item"><span>更新时间</span><CodeValue>{formatDate(position.updated_at)}</CodeValue></div>
          </div>
        </> : (
          <div className="empty-position">
            <span className="empty-glyph">∅</span>
            <b>尚未保存同步位点</b>
            <span>服务可能尚未完成首次启动或位点引导。</span>
          </div>
        )}
      </DataCard>

      {(canMigrateGTID || canMigrateFilePos) && (
        <section className="panel action-panel is-warn">
          <div className="action-copy">
            <span className="action-icon is-warn"><SwapOutlined /></span>
            <div>
              <b>位点模式迁移 <Chip tone="warn">{canMigrateGTID ? 'use_gtid=true · 位点缺 gtid_set' : 'use_gtid=false · 位点含 gtid_set'}</Chip></b>
              <span>
                {canMigrateGTID
                  ? '切换 use_gtid 后管线因位点缺 gtid_set 暂停。迁移会以源库当前 executed 集合作为 GTID 起点：要求位点与 master 同文件且差距 ≤ 8 MiB，否则拒绝（窗口内未确认事务会被跳过）。'
                  : 'GTID 集合无法反推精确 file:pos。反向迁移把位点置为最老保留 binlog 文件头，重启后增量从头重放一段历史，下游幂等吸收重复。'}
              </span>
            </div>
          </div>
          {canMigrateGTID && (
            <Button type="primary" icon={<SafetyCertificateOutlined />} loading={migrateGTID.isPending}
              onClick={() => modal.confirm({
                title: `确认迁移位点 → GTID · 源 ${sid}`,
                icon: <ExclamationCircleOutlined />,
                content: (
                  <div className="danger-confirm-copy">
                    <p>将读取源库 <b>SHOW BINARY LOG STATUS</b> 的 executed 集合写入位点。前提：位点已追平主库；若差距超出门限（8 MiB）或文件已轮转，操作会被拒绝并给出指引。</p>
                    <p>完成后需<b>重启进程</b>使 use_gtid 生效。</p>
                  </div>
                ),
                okText: '执行迁移',
                okButtonProps: { loading: migrateGTID.isPending },
                cancelText: '取消',
                onOk: () => migrateGTID.mutateAsync(),
              })}>迁移位点 → GTID</Button>
          )}
          {canMigrateFilePos && (
            <Button type="primary" icon={<SafetyCertificateOutlined />} loading={migrateFilePos.isPending}
              onClick={() => modal.confirm({
                title: `确认迁回 file:pos（重放模式）· 源 ${sid}`,
                icon: <ExclamationCircleOutlined />,
                content: (
                  <div className="danger-confirm-copy">
                    <p>位点将置为最老保留 binlog 文件头并清空 gtid_set，重启后增量会<b>重放一段历史事件</b>（下游幂等吸收重复）。</p>
                    <p>完成后需<b>重启进程</b>生效。</p>
                  </div>
                ),
                okText: '执行迁移',
                okButtonProps: { danger: true, loading: migrateFilePos.isPending },
                cancelText: '取消',
                onOk: () => migrateFilePos.mutateAsync(),
              })}>迁回 file:pos</Button>
          )}
        </section>
      )}

      <section className="panel action-panel danger-zone">
        <div className="action-copy">
          <span className="action-icon is-danger"><LockOutlined /></span>
          <div>
            <b>手动修改位点</b>
            <span>只有在迁移或故障恢复时使用。此操作会绕过水位连续性检查，并记入审计日志。</span>
          </div>
        </div>
        <Button danger icon={<SafetyCertificateOutlined />} onClick={() => { form.setFieldsValue({ name: position?.name ?? '', pos: position?.pos ?? 4, confirmation: '' }); setOpen(true) }}>危险操作</Button>
      </section>
    </>}

    <Modal title={`手动覆盖同步位点 · 源 ${sid}`} width={640} open={open} onCancel={() => setOpen(false)} okText="继续确认" cancelText="取消" onOk={submit} okButtonProps={{ danger: true }}>
      {pipelineState === 'running' || pipelineState === 'stock' ? (
        <Callout
          tone="danger"
          title="管线正在运行——现在提交会被水位覆盖"
          action={<Button size="small" loading={pausePipe.isPending} onClick={() => pausePipe.mutate()}>暂停管线</Button>}
        >
          该源当前状态为「{pipelineState === 'running' ? '同步中' : '全量回放'}」，修改会被运行中的管线覆盖回去。先暂停管线（优雅停机、位点落盘、保持 Leader），状态变为「已暂停」后再执行本操作。
        </Callout>
      ) : (
        <Callout tone="warning" title="全部节点已停机？API 不可用时直接写 etcd">
          <p>位点 key 为 <CodeValue>{posKey}</CodeValue>，值为 JSON。以旧值为底，只改 <CodeValue>name</CodeValue> / <CodeValue>pos</CodeValue> / <CodeValue>updated_at</CodeValue>（v、seq、leader_term 沿用旧值）：</p>
          <pre className="cli-block">{`# 1) 读取当前位点
etcdctl get ${posKey}

# 2) 写回（示例已按当前位点预填，改 name / pos 即可）
etcdctl put ${posKey} '${manualValue}'`}</pre>
        </Callout>
      )}
      <Form form={form} layout="vertical" className="position-form">
        <Form.Item name="name" label="Binlog 文件名" rules={[{ required: true, message: '请输入 binlog 文件名' }, { pattern: /^[-A-Za-z0-9_.]+$/, message: '文件名包含不支持的字符' }]}>
          <Input placeholder="mysql-bin.000123" />
        </Form.Item>
        <Form.Item name="pos" label="文件位置" rules={[{ required: true, message: '请输入文件位置' }]}>
          <InputNumber min={4} max={4294967295} precision={0} className="full-width" />
        </Form.Item>
        <Form.Item name="confirmation" label={<span>确认执行：原样输入 <CodeValue>SET-POSITION</CodeValue></span>} rules={[{ required: true, message: '请原样输入 SET-POSITION' }, { validator: async (_, value: string) => { if (value !== 'SET-POSITION') throw new Error('须与 SET-POSITION 完全一致') } }]}>
          <Input autoComplete="off" placeholder="SET-POSITION" />
        </Form.Item>
        <p className="form-note">这会直接写入 etcd 中该源的位点。修改结果不可撤销，请在提交前留存当前位点。最后一栏是接口强制的确认字符串（防误触），不是位点。</p>
      </Form>
    </Modal>
  </>
}

/** 全部视图（多源）：各源位点摘要（只读），操作跳转单源视图。 */
function AllSourcesPosition() {
  const { data: status, isPending, error, refetch } = useStatus()
  const sources = status?.sources ?? []
  return <>
    <PageHeader
      code="03"
      eyebrow="复制 / 同步位点"
      title="位点管理"
      description="全部源的持久化位点摘要（只读）。修改、模式迁移等危险操作请进入单源视图执行。"
      extra={status ? <Button onClick={() => void refetch()}>刷新</Button> : undefined}
    />
    {error ? <ErrorState error={error} onRetry={() => void refetch()} /> : isPending || !status ? <LoadingState rows={3} /> : (
      <DataCard title={<b>各源位点摘要</b>} extra={<span className="live-indicator"><i />自动刷新 · 15 秒</span>}>
        <div className="position-summary-list">
          {sources.map((source) => {
            const position = source.position
            return (
              <div className="position-summary-row" key={source.id}>
                <span className="entity">
                  <span className="entity-icon"><AimOutlined /></span>
                  <span><b>{source.id}</b><small>{position ? `序号 ${position.seq.toLocaleString()} · 任期 ${position.term}` : '尚未保存位点'}</small></span>
                </span>
                <CodeValue>{position ? `${position.name}:${position.pos.toLocaleString()}` : '—'}</CodeValue>
                <span className="time-cell">{position ? formatDate(position.updated_at) : '—'}</span>
                <Link to={`/position?source=${encodeURIComponent(source.id)}`} className="source-card-detail">单源操作</Link>
              </div>
            )
          })}
        </div>
      </DataCard>
    )}
  </>
}

function formatDate(value: string) {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : new Intl.DateTimeFormat('zh-CN', { dateStyle: 'medium', timeStyle: 'medium' }).format(date)
}
