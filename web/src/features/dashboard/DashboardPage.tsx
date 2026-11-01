import { useState } from 'react'
import { App as AntApp, Button, Modal, Tooltip } from 'antd'
import {
  AimOutlined,
  ApartmentOutlined,
  ArrowRightOutlined,
  BranchesOutlined,
  CrownOutlined,
  LineChartOutlined,
  ReloadOutlined,
  SlidersOutlined,
  WarningOutlined,
} from '@ant-design/icons'
import { Link } from 'react-router-dom'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useApi } from '@/app/api'
import { queryKeys, useDDLEvents, useEffectiveRules, usePosition, useStatus } from '@/shared/api/queries'
import type { EffectiveRule, PositionData, StatusData } from '@/shared/api/types'
import { Callout } from '@/shared/components/Callout'
import { DataCard } from '@/shared/components/DataCard'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip, CodeValue, RoleChip, SinkTag, roleLabel } from '@/shared/components/presentation'

interface PipelineHealth { type: 'success' | 'info' | 'warning' | 'error'; text: string }

const PIPELINE_HEALTH: Record<string, PipelineHealth> = {
  running: { type: 'success', text: '增量同步运行中' },
  stock: { type: 'info', text: '全量初始化进行中（增量同步尚未启动）' },
  paused: { type: 'error', text: '管线已停止 —— 需人工介入后重启进程' },
  stopped: { type: 'warning', text: '管线未运行（失位、让位或停机中）' },
  idle: { type: 'info', text: '未参与同步（热备节点或尚未当选）' },
}

const TOPOLOGY_STATES: Record<string, { label: string; copy: string }> = {
  running: { label: '同步中', copy: '增量事件持续写入下游' },
  stock: { label: '全量回放', copy: '全量快照扫描中，增量尚未启动' },
  paused: { label: '已暂停', copy: '管线已停止，等待人工介入' },
  stopped: { label: '待机', copy: '未运行（失位、让位或停机）' },
  idle: { label: '空闲', copy: '热备节点，等待成为 Leader' },
}

function pipelineHealth(state: string): PipelineHealth {
  return PIPELINE_HEALTH[state] ?? { type: 'info', text: '未参与同步（热备节点或尚未当选）' }
}

function healthTone(type: PipelineHealth['type']) {
  return type === 'success' ? 'ok' : type === 'error' ? 'danger' : type === 'warning' ? 'warn' : 'info'
}

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let value = n
  let index = 0
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024
    index += 1
  }
  return `${value >= 100 ? Math.round(value) : value.toFixed(1)} ${units[index]}`
}

function formatUptime(seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) return '—'
  const days = Math.floor(seconds / 86_400)
  const hours = Math.floor((seconds % 86_400) / 3_600)
  const minutes = Math.floor((seconds % 3_600) / 60)
  return days ? `${days} 天 ${hours} 小时` : hours ? `${hours} 小时 ${minutes} 分` : `${minutes} 分钟`
}

function buildHealth(status: StatusData) {
  let health = pipelineHealth(status.pipeline_state)
  const ddlPaused = status.pipeline_state === 'paused' && /DDL 等待人工决策/.test(status.pipeline_error ?? '')
  if (ddlPaused) health = { type: 'warning', text: 'DDL 等待人工决策 —— 管线已暂停' }
  const catchingUp = status.pipeline_state === 'running' && status.caught_up === false
  if (catchingUp) health = { type: 'warning', text: '同步追赶中 —— 落后于源库' }
  const opPaused = status.pipeline_state === 'paused' && status.pipeline_paused_by_op === true
  if (opPaused) health = { type: 'info', text: '管线已人工暂停 —— 恢复后从位点继续同步' }

  const parts: string[] = []
  if (status.pipeline_state === 'running' && typeof status.event_lag_seconds === 'number') {
    parts.push(`距最近业务写入 ${status.event_lag_seconds}s`)
  }
  if (catchingUp && typeof status.master_lag_bytes === 'number') {
    parts.push(`落后 ${formatBytes(status.master_lag_bytes)}`)
  }
  if (status.pipeline_state === 'running' && status.caught_up === true) {
    parts.unshift('已追平')
  }
  return { health, ddlPaused, catchingUp, opPaused, lag: parts.join(' · ') }
}

export function DashboardPage() {
  const { data: status, isPending: statusPending, error: statusError, refetch: refetchStatus } = useStatus()
  const { data: position, isPending: posPending, error: posError } = usePosition()
  const { data: rules = [] } = useEffectiveRules()
  const { data: ddlEvents = [] } = useDDLEvents()
  const [showResign, setShowResign] = useState(false)
  const [showPause, setShowPause] = useState(false)
  const api = useApi()
  const queryClient = useQueryClient()
  const { message } = AntApp.useApp()
  const resign = useMutation({
    mutationFn: () => api.resign(),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.success('已请求让位，集群将重新选主。')
      setShowResign(false)
    },
    onError: (error) => void message.error(error.message),
  })
  const pausePipeline = useMutation({
    mutationFn: () => api.pausePipeline(),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.success('管线暂停中：位点已落盘，进程与 Web 保持在线。')
      setShowPause(false)
    },
    onError: (error) => void message.error(error.message),
  })
  const resumePipeline = useMutation({
    mutationFn: () => api.resumePipeline(),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.info('恢复请求已提交，管线将从持久化位点重新启动。')
    },
    onError: (error) => void message.error(error.message),
  })

  const updatedAt = position ? Date.parse(position.updated_at) : Number.NaN
  const posAgeSec = Number.isFinite(updatedAt) ? Math.max(0, Math.round((Date.now() - updatedAt) / 1000)) : null

  return (
    <>
      <PageHeader
        code="01"
        eyebrow="实时监控"
        title="总控台"
        description="实时观察同步管线、节点角色与持久化位点。复杂性能趋势请在 Grafana 查看。"
        extra={<Button icon={<ReloadOutlined />} onClick={() => void refetchStatus()}>刷新遥测</Button>}
      />
      {statusError ? <ErrorState error={statusError} onRetry={() => void refetchStatus()} /> : statusPending || !status ? <LoadingState rows={4} /> : (
        <DashboardBody
          status={status}
          position={position}
          posPending={posPending}
          posError={posError}
          posAgeSec={posAgeSec}
          rules={rules}
          pendingDDL={ddlEvents.length}
          onResign={() => setShowResign(true)}
          onPause={() => setShowPause(true)}
          onResume={() => resumePipeline.mutateAsync()}
          resuming={resumePipeline.isPending}
        />
      )}
      <Modal
        title="确认 Leader 让位"
        open={showResign}
        onCancel={() => setShowResign(false)}
        onOk={() => resign.mutateAsync()}
        okText="确认让位"
        okButtonProps={{ danger: true, loading: resign.isPending }}
        cancelText="取消"
      >
        <p className="modal-copy">此操作会释放当前 Leader 租约，集群将重新选主。通常仅用于滚动发布或维护。</p>
        <Callout tone="warning" title="操作会影响当前同步主节点">请确认没有正在进行的维护冲突。</Callout>
      </Modal>
      <Modal
        title="确认暂停管线"
        open={showPause}
        onCancel={() => setShowPause(false)}
        onOk={() => pausePipeline.mutateAsync()}
        okText="确认暂停"
        okButtonProps={{ loading: pausePipeline.isPending }}
        cancelText="取消"
      >
        <p className="modal-copy">管线将优雅停机：停止拉取、排空队列、位点终值落盘。进程与 Web 保持在线，集群保持当前 Leader。暂停状态在重启后仍然生效，需手动恢复。</p>
        <Callout tone="warning" title="同步将中断">暂停期间源库变更持续积压在 binlog，恢复后继续消费；下游停写窗口请自行评估。</Callout>
      </Modal>
    </>
  )
}

function DashboardBody({ status, position, posPending, posError, posAgeSec, rules, pendingDDL, onResign, onPause, onResume, resuming }: {
  status: StatusData
  position?: PositionData | null
  posPending: boolean
  posError: unknown
  posAgeSec: number | null
  rules: EffectiveRule[]
  pendingDDL: number
  onResign: () => void
  onPause: () => void
  onResume: () => void
  resuming: boolean
}) {
  const view = buildHealth(status)
  return (
    <>
      <div className={`state-ribbon is-${healthTone(view.health.type)}`}>
        <span className="state-ribbon-led"><i /></span>
        <div className="state-ribbon-copy">
          <div className="state-ribbon-line">
            <b>{view.health.text}</b>
            {view.lag && <span className="state-ribbon-lag">{view.lag}</span>}
          </div>
          {status.pipeline_state === 'paused' && (
            <p>
              {status.pipeline_error || '管线致命错误退出，请排查服务日志，修复后重启进程恢复同步。'}
              {/gtid_set/.test(status.pipeline_error ?? '') && ' 切换 use_gtid 的位点迁移可在「位点管理」页执行。'}
            </p>
          )}
        </div>
        {view.ddlPaused && <Link to="/ddl" className="state-ribbon-cta">处理结构变更 <ArrowRightOutlined /></Link>}
      </div>

      <Topology status={status} position={position} rules={rules} pendingDDL={pendingDDL} />

      <div className="metric-rack">
        <div className="metric-cell">
          <span className="metric-index">01</span>
          <span className="metric-label">节点角色</span>
          <div className="metric-value">{roleLabel(status.role)}</div>
          <span className="metric-foot">
            {status.role === 'leader' ? `主节点 ${status.leader || status.node_id}` : status.role === 'single' ? '单节点运行模式' : `主节点 ${status.leader || '等待选主'}`}
          </span>
        </div>
        <div className="metric-cell">
          <span className="metric-index">02</span>
          <span className="metric-label">同步位点</span>
          <div className="metric-value is-mono is-signal">{posPending ? '读取中…' : position?.name || '未初始化'}</div>
          <span className="metric-foot">
            {position ? `位置 ${position.pos.toLocaleString()} · 序号 ${position.seq.toLocaleString()}${posAgeSec !== null ? ` · ${posAgeSec}s 前写入` : ''}` : '位点 API 暂无持久化记录'}
          </span>
        </div>
        <div className="metric-cell">
          <span className="metric-index">03</span>
          <span className="metric-label">配置来源</span>
          <div className="metric-value is-mono">{status.config_source || '—'}</div>
          <span className="metric-foot">版本 {status.version || '未知'}</span>
        </div>
        <div className="metric-cell">
          <span className="metric-index">04</span>
          <span className="metric-label">运行时长</span>
          <div className="metric-value">{formatUptime(status.uptime_seconds)}</div>
          <span className="metric-foot">任期 {status.term || 0} · 节点 {status.node_id || '—'}</span>
        </div>
      </div>

      {Boolean(posError) && (
        <div className="inline-note is-warn">
          <WarningOutlined /> 位点暂时不可用 · {posError instanceof Error ? posError.message : '请稍后重试。'}
        </div>
      )}

      <div className="content-grid">
        <DataCard
          title={<b>同步状态</b>}
          extra={<span className="live-indicator"><i />自动刷新 · 15 秒</span>}
        >
          <div className="spec-grid">
            <div className="spec-item"><span>集群名称</span><CodeValue>{status.cluster}</CodeValue></div>
            <div className="spec-item"><span>本节点</span><CodeValue>{status.node_id}</CodeValue></div>
            <div className="spec-item"><span>当前角色</span><RoleChip role={status.role} /></div>
            <div className="spec-item"><span>主节点</span><CodeValue>{status.leader || (status.role === 'single' ? '单节点' : '未选出')}</CodeValue></div>
            <div className="spec-item"><span>任期</span><CodeValue>{status.term}</CodeValue></div>
            <div className="spec-item"><span>服务版本</span><CodeValue>{status.version || '—'}</CodeValue></div>
          </div>
          {view.opPaused && (
            <div className="action-strip is-muted">
              <div><b>管线处于人工暂停</b><span>位点已落盘；恢复后从位点继续同步，暂停期间修改的位点会被采用。</span></div>
              <Button type="primary" ghost loading={resuming} onClick={onResume}>恢复管线</Button>
            </div>
          )}
          {(status.role === 'leader' || status.role === 'single') && (status.pipeline_state === 'running' || status.pipeline_state === 'stock') && !view.opPaused && (
            <div className="action-strip is-muted">
              <div><b>运维窗口需要停止同步？</b><span>暂停后管线优雅停机、位点落盘，进程与 Web 保持在线；恢复从位点继续。</span></div>
              <Button onClick={onPause}>暂停管线</Button>
            </div>
          )}
          {status.role === 'leader' && (
            <div className="action-strip">
              <div><b>正在运行 Leader 节点</b><span>让位会触发集群重新选主，仅用于滚动发布或维护。</span></div>
              <Button danger ghost onClick={onResign}>主动让位</Button>
            </div>
          )}
          {status.role === 'follower' && (
            <div className="action-strip is-muted">
              <div><b>当前为热备节点</b><span>增量同步由 Leader 节点负责。</span></div>
              <Tooltip title="只有 Leader 节点可以主动让位"><Button disabled>主动让位</Button></Tooltip>
            </div>
          )}
        </DataCard>

        <div className="side-stack">
          <DataCard title={<b>快捷入口</b>}>
            <div className="quick-links">
              <Link to="/rules" className="quick-link">
                <span className="quick-index">01</span>
                <span className="quick-icon tone-info"><ApartmentOutlined /></span>
                <span className="quick-copy"><b>查看复制规则</b><small>生效规则与单表操作</small></span>
                <ArrowRightOutlined className="quick-arrow" />
              </Link>
              <Link to="/ddl" className="quick-link">
                <span className="quick-index">02</span>
                <span className="quick-icon tone-warn"><BranchesOutlined /></span>
                <span className="quick-copy"><b>处理结构变更</b><small>查看待决结构变更</small></span>
                <ArrowRightOutlined className="quick-arrow" />
              </Link>
              <Link to="/position" className="quick-link">
                <span className="quick-index">03</span>
                <span className="quick-icon tone-ok"><AimOutlined /></span>
                <span className="quick-copy"><b>管理同步位点</b><small>查看或危险修改位点</small></span>
                <ArrowRightOutlined className="quick-arrow" />
              </Link>
              <Link to="/config" className="quick-link">
                <span className="quick-index">04</span>
                <span className="quick-icon tone-signal"><SlidersOutlined /></span>
                <span className="quick-copy"><b>查看生效配置</b><small>已打码配置与有效规则</small></span>
                <ArrowRightOutlined className="quick-arrow" />
              </Link>
            </div>
          </DataCard>
          <div className="grafana-note">
            <span className="grafana-glyph"><LineChartOutlined /></span>
            <div><b>需要更深入的指标分析？</b><span>事件延迟、吞吐与队列趋势请使用 Grafana Dashboard。</span></div>
          </div>
        </div>
      </div>
    </>
  )
}

function Topology({ status, position, rules, pendingDDL }: {
  status: StatusData
  position?: PositionData | null
  rules: EffectiveRule[]
  pendingDDL: number
}) {
  const state = status.pipeline_state || 'idle'
  const stateMeta = state === 'paused' && status.pipeline_paused_by_op === true
    ? { label: '人工暂停', copy: '位点已落盘，恢复后从位点继续同步' }
    : TOPOLOGY_STATES[state] ?? { label: state, copy: '状态未知' }
  const sinks = [
    { label: 'MySQL', key: 'mysql' as const },
    { label: 'Kafka', key: 'kafka' as const },
    { label: 'RocketMQ', key: 'rocketmq' as const },
  ].map((sink) => {
    const count = rules.filter((rule) => rule.sinks?.[sink.key]).length
    return { ...sink, count, enabled: count > 0 }
  })

  return (
    <section className={`panel topology topology-${state}`}>
      <div className="panel-head">
        <div className="panel-title"><b>实时数据流</b></div>
        <div className="panel-extra topology-extra">
          {status.pipeline_state === 'running' && status.caught_up === true && <Chip tone="ok" dot>已追平</Chip>}
          {status.pipeline_state === 'running' && status.caught_up === false && <Chip tone="warn" dot>追赶中</Chip>}
          {status.pipeline_state === 'running' && typeof status.event_lag_seconds === 'number' && (
            <span className="mono">最近写入 {status.event_lag_seconds}s 前</span>
          )}
        </div>
      </div>
      <div className="topology-body">
        <div className="flow-node flow-source">
          <span className="node-tag">来源 · MYSQL</span>
          <b>BINLOG</b>
          <small>{position ? `${position.name || '—'} : ${position.pos.toLocaleString()}` : '等待持久化位点'}</small>
        </div>
        <div className="flow-link"><i /></div>
        <div className="flow-node flow-core">
          <span className="node-tag">GOCDC 管线</span>
          <b>{stateMeta.label}</b>
          <small>{stateMeta.copy}</small>
        </div>
        <div className="flow-link"><i /></div>
        <div className="flow-node flow-target">
          <span className="node-tag">下游</span>
          <div className="flow-sinks">
            {sinks.map((sink) => (
              <SinkTag key={sink.key} label={sink.count > 0 ? `${sink.label} · ${sink.count}` : sink.label} enabled={sink.enabled} />
            ))}
          </div>
          <small>{rules.length} 张表白名单</small>
        </div>
      </div>
      <div className="topology-foot">
        <span><CrownOutlined /> 主节点 <b>{status.leader || (status.role === 'single' ? '单节点' : '等待选主')}</b></span>
        <span>任期 <b>{status.term || 0}</b></span>
        <span>当前节点 <b>{status.node_id || '—'}</b></span>
        {pendingDDL > 0 && <Link to="/ddl" className="topology-alert"><WarningOutlined /> {pendingDDL} 条结构变更待决</Link>}
      </div>
    </section>
  )
}
