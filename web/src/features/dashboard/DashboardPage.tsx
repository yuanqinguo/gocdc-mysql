import { useEffect, useState } from 'react'
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
import { useSourceScope } from '@/app/sourceScope'
import { queryKeys, useDDLEvents, useDDLEventsAll, useEffectiveRules, usePosition, useSources, useStatus } from '@/shared/api/queries'
import type { EffectiveRule, PositionData, SourceStatus, StatusData } from '@/shared/api/types'
import { Callout } from '@/shared/components/Callout'
import { DataCard } from '@/shared/components/DataCard'
import { ErrorState, LoadingState } from '@/shared/components/PageState'
import { PageHeader } from '@/shared/components/PageHeader'
import { Chip, CodeValue, RoleChip, SinkTag, roleLabel } from '@/shared/components/presentation'
import { classifySource, isCatchingUp, isDDLWaiting, isOpPaused, sortSourceCards, type SourceCardData } from './sourceCards'

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

const CARD_STATE_CHIPS: Record<string, { label: string; tone: 'ok' | 'warn' | 'danger' | 'info' | 'muted' }> = {
  running: { label: '同步中', tone: 'ok' },
  stock: { label: '全量回放', tone: 'info' },
  paused: { label: '已暂停', tone: 'danger' },
  stopped: { label: '待机', tone: 'warn' },
  idle: { label: '空闲', tone: 'muted' },
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

function buildHealth(source: SourceStatus) {
  let health: PipelineHealth = source.phase === 'failed'
    ? { type: 'error', text: '源装配失败 —— 请检查配置与源库连通性' }
    : pipelineHealth(source.pipeline_state)
  const ddlPaused = isDDLWaiting(source)
  if (ddlPaused) health = { type: 'warning', text: 'DDL 等待人工决策 —— 管线已暂停' }
  const catchingUp = isCatchingUp(source)
  if (catchingUp) health = { type: 'warning', text: '同步追赶中 —— 落后于源库' }
  const opPaused = isOpPaused(source)
  if (opPaused) health = { type: 'info', text: '管线已人工暂停 —— 恢复后从位点继续同步' }

  const parts: string[] = []
  if (source.pipeline_state === 'running' && typeof source.event_lag_seconds === 'number') {
    parts.push(`距最近业务写入 ${source.event_lag_seconds}s`)
  }
  if (catchingUp && typeof source.master_lag_bytes === 'number') {
    parts.push(`落后 ${formatBytes(source.master_lag_bytes)}`)
  }
  if (source.pipeline_state === 'running' && source.caught_up === true) {
    parts.unshift('已追平')
  }
  return { health, ddlPaused, catchingUp, opPaused, lag: parts.join(' · ') }
}

export function DashboardPage() {
  const { currentSid } = useSourceScope()
  const { data: status, isPending: statusPending, error: statusError, refetch: refetchStatus } = useStatus()
  const [showResign, setShowResign] = useState(false)
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

  const sources = status?.sources ?? []
  const sourceCount = sources.length
  // 无选中源时：单源部署默认进入唯一源视图；多源部署进入聚合视图
  const effectiveSid = currentSid ?? (sourceCount === 1 ? sources[0]?.id ?? null : null)
  const resignModal = (
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
      <Callout tone="warning" title={`操作会切换全部 ${sourceCount} 个源`}>让位后全部源的管线将在新 Leader 上重新接管，请确认没有正在进行的维护冲突。</Callout>
    </Modal>
  )

  return (
    <>
      <PageHeader
        code="01"
        eyebrow={effectiveSid ? `实时监控 · 源 ${effectiveSid}` : '实时监控'}
        title="总控台"
        description={effectiveSid
          ? `实时观察源「${effectiveSid}」的同步管线、节点角色与持久化位点。复杂性能趋势请在 Grafana 查看。`
          : '实时观察全部数据源的同步管线、节点角色与持久化位点。复杂性能趋势请在 Grafana 查看。'}
        extra={<Button icon={<ReloadOutlined />} onClick={() => void refetchStatus()}>刷新</Button>}
      />
      {statusError ? <ErrorState error={statusError} onRetry={() => void refetchStatus()} /> : statusPending || !status ? <LoadingState rows={4} /> : (
        effectiveSid
          ? (sources.some((source) => source.id === effectiveSid)
              ? <SingleSourceDashboard sid={effectiveSid} status={status} onResign={() => setShowResign(true)} />
              : <ErrorState error={new Error(`源不存在：${effectiveSid}`)} onRetry={() => void refetchStatus()} />)
          : <AllSourcesDashboard status={status} onResign={() => setShowResign(true)} />
      )}
      {resignModal}
    </>
  )
}

/** 单源视图：现行总控台布局，数据全部为该源作用域。 */
function SingleSourceDashboard({ sid, status, onResign }: { sid: string; status: StatusData; onResign: () => void }) {
  const source = status.sources.find((item) => item.id === sid)
  const { data: position, isPending: posPending, error: posError } = usePosition(sid)
  const { data: rules = [] } = useEffectiveRules(sid)
  const { data: ddlEvents = [] } = useDDLEvents(sid)
  const { data: sourceList = [] } = useSources()
  const [showPause, setShowPause] = useState(false)
  const api = useApi()
  const queryClient = useQueryClient()
  const { message } = AntApp.useApp()

  // 多标签页值班区分：标题带源 id，离开时还原
  useEffect(() => {
    const previous = document.title
    document.title = `GOCDC · ${sid}`
    return () => { document.title = previous }
  }, [sid])

  const pausePipeline = useMutation({
    mutationFn: () => api.pausePipelineOf(sid),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.success('管线暂停中：位点已落盘，进程与 Web 保持在线。')
      setShowPause(false)
    },
    onError: (error) => void message.error(error.message),
  })
  const resumePipeline = useMutation({
    mutationFn: () => api.resumePipelineOf(sid),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.info('恢复请求已提交，管线将从持久化位点重新启动。')
    },
    onError: (error) => void message.error(error.message),
  })

  if (!source) return <LoadingState rows={4} />

  const summary = sourceList.find((item) => item.id === sid)
  const updatedAt = position ? Date.parse(position.updated_at) : Number.NaN
  const posAgeSec = Number.isFinite(updatedAt) ? Math.max(0, Math.round((Date.now() - updatedAt) / 1000)) : null
  const view = buildHealth(source)
  const withSource = (path: string) => `${path}?source=${encodeURIComponent(sid)}`

  return (
    <>
      <div className={`state-ribbon is-${healthTone(view.health.type)}`}>
        <span className="state-ribbon-led"><i /></span>
        <div className="state-ribbon-copy">
          <div className="state-ribbon-line">
            <b>{view.health.text}</b>
            {view.lag && <span className="state-ribbon-lag">{view.lag}</span>}
          </div>
          {source.pipeline_state === 'paused' && (
            <p>
              {source.pipeline_error || '管线致命错误退出，请排查服务日志，修复后重启进程恢复同步。'}
              {/gtid_set/.test(source.pipeline_error ?? '') && ' 切换 use_gtid 的位点迁移可在「位点管理」页执行。'}
            </p>
          )}
        </div>
        {view.ddlPaused && <Link to={withSource('/ddl')} className="state-ribbon-cta">处理结构变更 <ArrowRightOutlined /></Link>}
      </div>

      <Topology status={status} source={source} sourceAddr={summary?.source_addr} position={position} rules={rules} pendingDDL={ddlEvents.length} />

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
          title={<b>同步状态 · 源 {sid}</b>}
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
              <Button type="primary" ghost loading={resumePipeline.isPending} onClick={() => resumePipeline.mutateAsync()}>恢复管线</Button>
            </div>
          )}
          {(status.role === 'leader' || status.role === 'single') && (source.pipeline_state === 'running' || source.pipeline_state === 'stock') && !view.opPaused && (
            <div className="action-strip is-muted">
              <div><b>运维窗口需要停止同步？</b><span>暂停后管线优雅停机、位点落盘，进程与 Web 保持在线；恢复从位点继续。</span></div>
              <Button onClick={() => setShowPause(true)}>暂停管线</Button>
            </div>
          )}
          {status.role === 'leader' && (
            <div className="action-strip">
              <div><b>正在运行 Leader 节点</b><span>让位会触发集群重新选主并切换全部 {status.sources.length} 个源，仅用于滚动发布或维护。</span></div>
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
              <Link to={withSource('/rules')} className="quick-link">
                <span className="quick-index">01</span>
                <span className="quick-icon tone-info"><ApartmentOutlined /></span>
                <span className="quick-copy"><b>查看复制规则</b><small>生效规则与单表操作</small></span>
                <ArrowRightOutlined className="quick-arrow" />
              </Link>
              <Link to={withSource('/ddl')} className="quick-link">
                <span className="quick-index">02</span>
                <span className="quick-icon tone-warn"><BranchesOutlined /></span>
                <span className="quick-copy"><b>处理结构变更</b><small>查看待决结构变更</small></span>
                <ArrowRightOutlined className="quick-arrow" />
              </Link>
              <Link to={withSource('/position')} className="quick-link">
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

      <Modal
        title={`确认暂停管线 · 源 ${sid}`}
        open={showPause}
        onCancel={() => setShowPause(false)}
        onOk={() => pausePipeline.mutateAsync()}
        okText="确认暂停"
        okButtonProps={{ loading: pausePipeline.isPending }}
        cancelText="取消"
      >
        <p className="modal-copy">该源的管线将优雅停机：停止拉取、排空队列、位点终值落盘。进程与 Web 保持在线，集群保持当前 Leader。暂停状态在重启后仍然生效，需手动恢复。</p>
        <Callout tone="warning" title="此源的同步将中断">暂停期间源库变更持续积压在 binlog，恢复后继续消费；下游停写窗口请自行评估。</Callout>
      </Modal>
    </>
  )
}

/** 全部视图（多源部署）：汇总条 + 源卡片栅格。 */
function AllSourcesDashboard({ status, onResign }: { status: StatusData; onResign: () => void }) {
  const { data: sourceList = [] } = useSources()
  const { data: ddlAll = [] } = useDDLEventsAll()
  const [pauseSid, setPauseSid] = useState<string | null>(null)
  const api = useApi()
  const queryClient = useQueryClient()
  const { message } = AntApp.useApp()
  const pausePipeline = useMutation({
    mutationFn: (sid: string) => api.pausePipelineOf(sid),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.success('管线暂停中：位点已落盘，进程与 Web 保持在线。')
      setPauseSid(null)
    },
    onError: (error) => void message.error(error.message),
  })
  const resumePipeline = useMutation({
    mutationFn: (sid: string) => api.resumePipelineOf(sid),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.status })
      void message.info('恢复请求已提交，管线将从持久化位点重新启动。')
    },
    onError: (error) => void message.error(error.message),
  })

  const sources = status.sources
  const readyCount = sources.filter((source) => source.phase === 'ready').length
  const alertCount = sources.filter((source) => source.phase === 'failed' || source.pipeline_state === 'paused').length
  const pendingBySource = new Map<string, number>()
  for (const record of ddlAll) {
    if (record.source) pendingBySource.set(record.source, (pendingBySource.get(record.source) ?? 0) + 1)
  }
  const cards = sortSourceCards(sources.map((source): SourceCardData => ({
    source,
    summary: sourceList.find((item) => item.id === source.id),
    pendingDDL: pendingBySource.get(source.id) ?? 0,
  })))
  const isLeaderOrSingle = status.role === 'leader' || status.role === 'single'

  return (
    <>
      <div className="source-summary-bar">
        <div className="toolbar-stats">
          <span className="toolbar-stat"><b>{sources.length}</b><small>源总数</small></span>
          <span className="toolbar-divider" />
          <span className="toolbar-stat"><b>{readyCount}</b><small>就绪</small></span>
          <span className="toolbar-divider" />
          <span className={`toolbar-stat ${alertCount > 0 ? 'is-warn' : ''}`}><b>{alertCount}</b><small>告警</small></span>
          <span className="toolbar-divider" />
          <span className={`toolbar-stat ${ddlAll.length > 0 ? 'is-warn' : ''}`}><b>{ddlAll.length}</b><small>待决 DDL</small></span>
        </div>
        <div className="source-summary-meta">
          <span><CrownOutlined /> 主节点 <b>{status.leader || (status.role === 'single' ? '单节点' : '等待选主')}</b></span>
          <span>任期 <b>{status.term || 0}</b></span>
          <span>运行 <b>{formatUptime(status.uptime_seconds)}</b></span>
          {isLeaderOrSingle
            ? <Button danger ghost size="small" onClick={onResign}>主动让位</Button>
            : <Tooltip title="只有 Leader 节点可以主动让位"><Button size="small" disabled>主动让位</Button></Tooltip>}
        </div>
      </div>

      <div className="source-grid">
        {cards.map((card) => (
          <SourceCard
            key={card.source.id}
            card={card}
            role={status.role}
            pausing={pausePipeline.isPending && pauseSid === card.source.id}
            resuming={resumePipeline.isPending && resumePipeline.variables === card.source.id}
            onPause={() => setPauseSid(card.source.id)}
            onResume={() => resumePipeline.mutateAsync(card.source.id)}
          />
        ))}
      </div>

      <Modal
        title={`确认暂停管线 · 源 ${pauseSid ?? ''}`}
        open={pauseSid !== null}
        onCancel={() => setPauseSid(null)}
        onOk={() => pauseSid ? pausePipeline.mutateAsync(pauseSid) : undefined}
        okText="确认暂停"
        okButtonProps={{ loading: pausePipeline.isPending }}
        cancelText="取消"
      >
        <p className="modal-copy">该源的管线将优雅停机：停止拉取、排空队列、位点终值落盘。暂停状态在重启后仍然生效，需手动恢复。</p>
        <Callout tone="warning" title="此源的同步将中断">暂停期间源库变更持续积压在 binlog，恢复后继续消费；下游停写窗口请自行评估。</Callout>
      </Modal>
    </>
  )
}

function SourceCard({ card, role, pausing, resuming, onPause, onResume }: {
  card: SourceCardData
  role: string
  pausing: boolean
  resuming: boolean
  onPause: () => void
  onResume: () => void
}) {
  const { source, summary, pendingDDL } = card
  const severity = classifySource(source)
  const failed = severity === 'failed'
  const opPaused = isOpPaused(source)
  const ddlWaiting = isDDLWaiting(source)
  const catchingUp = isCatchingUp(source)
  const stateChip = failed ? { label: '装配失败 · 重试中', tone: 'danger' as const } : CARD_STATE_CHIPS[source.pipeline_state] ?? { label: source.pipeline_state, tone: 'info' as const }
  const position = source.position
  const canOperate = (role === 'leader' || role === 'single') && !failed

  return (
    <section className={`panel source-card ${failed ? 'is-failed' : ''}`}>
      <div className="source-card-head">
        <div className="source-card-title">
          <b>{source.id}</b>
          <small>{summary?.source_addr || '源地址未知'}</small>
        </div>
        <div className="chip-row">
          <Chip tone={stateChip.tone} dot>{stateChip.label}</Chip>
          {ddlWaiting && <Chip tone="warn" dot>DDL 等待</Chip>}
          {catchingUp && <Chip tone="warn" dot>追赶中</Chip>}
          {opPaused && <Chip tone="info">人工暂停</Chip>}
        </div>
      </div>
      <div className="source-card-body">
        <div className="source-route">
          <span className="source-route-node"><small>源</small><b>{summary?.source_addr || '—'}</b></span>
          <ArrowRightOutlined className="source-route-arrow" />
          <span className="source-route-node"><small>目标</small><b>{summary?.target_addrs?.length ? summary.target_addrs.join(', ') : '—'}</b></span>
        </div>
        {failed && (source.pipeline_error || summary?.error) && (
          <p className="source-card-error"><WarningOutlined /> {source.pipeline_error || summary?.error}</p>
        )}
        <div className="source-card-facts">
          <span><small>位点</small><code>{position ? `${position.name}:${position.pos.toLocaleString()}` : '未初始化'}</code></span>
          <span><small>更新</small><code>{position ? formatPosAge(position.updated_at) : '—'}</code></span>
          <span><small>事件延迟</small><code>{typeof source.event_lag_seconds === 'number' ? `${source.event_lag_seconds}s` : '—'}</code></span>
          <span><small>主库落后</small><code>{typeof source.master_lag_bytes === 'number' ? formatBytes(source.master_lag_bytes) : '—'}</code></span>
          <span><small>规则/表</small><code>{summary ? `${summary.rules}/${summary.tables}` : '—'}</code></span>
        </div>
      </div>
      <div className="source-card-foot">
        <div className="row-actions">
          {failed ? <span className="text-muted">装配失败 · 重试中</span> : opPaused
            ? <Button type="primary" ghost size="small" loading={resuming} disabled={!canOperate} onClick={onResume}>恢复</Button>
            : <Button size="small" loading={pausing} disabled={!canOperate || (source.pipeline_state !== 'running' && source.pipeline_state !== 'stock')} onClick={onPause}>暂停</Button>}
          {pendingDDL > 0 && (
            <Link to={`/ddl?source=${encodeURIComponent(source.id)}`} className="source-card-ddl"><WarningOutlined /> {pendingDDL} 待决 DDL</Link>
          )}
        </div>
        <Link to={`/?source=${encodeURIComponent(source.id)}`} className="source-card-detail">详情 <ArrowRightOutlined /></Link>
      </div>
    </section>
  )
}

function formatPosAge(value: string) {
  const parsed = Date.parse(value)
  if (!Number.isFinite(parsed)) return '—'
  const seconds = Math.max(0, Math.round((Date.now() - parsed) / 1000))
  return seconds < 3600 ? `${Math.round(seconds / 60)} 分钟前` : `${Math.round(seconds / 3600)} 小时前`
}

function Topology({ status, source, sourceAddr, position, rules, pendingDDL }: {
  status: StatusData
  source: SourceStatus
  sourceAddr?: string
  position?: PositionData | null
  rules: EffectiveRule[]
  pendingDDL: number
}) {
  const state = source.pipeline_state || 'idle'
  const stateMeta = state === 'paused' && source.pipeline_paused_by_op === true
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
        <div className="panel-title"><b>实时数据流 · 源 {source.id}</b></div>
        <div className="panel-extra topology-extra">
          {source.pipeline_state === 'running' && source.caught_up === true && <Chip tone="ok" dot>已追平</Chip>}
          {source.pipeline_state === 'running' && source.caught_up === false && <Chip tone="warn" dot>追赶中</Chip>}
          {source.pipeline_state === 'running' && typeof source.event_lag_seconds === 'number' && (
            <span className="mono">最近写入 {source.event_lag_seconds}s 前</span>
          )}
        </div>
      </div>
      <div className="topology-body">
        <div className="flow-node flow-source">
          <span className="node-tag">来源 · MYSQL</span>
          <b>{source.id}</b>
          <small>{sourceAddr || '源地址未知'} · {position ? `${position.name || '—'} : ${position.pos.toLocaleString()}` : '等待持久化位点'}</small>
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
        {pendingDDL > 0 && <Link to={`/ddl?source=${encodeURIComponent(source.id)}`} className="topology-alert"><WarningOutlined /> {pendingDDL} 条结构变更待决</Link>}
      </div>
    </section>
  )
}
