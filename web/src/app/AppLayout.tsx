import { useState, type ReactNode } from 'react'
import { Avatar, Drawer, Dropdown, Select, type MenuProps } from 'antd'
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom'
import {
  AimOutlined,
  ApartmentOutlined,
  BranchesOutlined,
  CaretDownOutlined,
  HistoryOutlined,
  LogoutOutlined,
  MenuOutlined,
  RadarChartOutlined,
  SlidersOutlined,
} from '@ant-design/icons'
import { useAuth } from './auth'
import { useSourceScope } from './sourceScope'
import { useDDLEventsAll, useStatus } from '@/shared/api/queries'
import type { SourceStatus } from '@/shared/api/types'
import { BrandMark } from '@/shared/components/BrandMark'
import { Chip, RoleChip, roleLabel } from '@/shared/components/presentation'

interface NavItem {
  key: string
  label: string
  icon: ReactNode
  badge?: 'ddl'
}

const NAV_ITEMS: NavItem[] = [
  { key: '/', label: '总控台', icon: <RadarChartOutlined /> },
  { key: '/rules', label: '复制规则', icon: <ApartmentOutlined /> },
  { key: '/position', label: '位点管理', icon: <AimOutlined /> },
  { key: '/ddl', label: '结构变更', icon: <BranchesOutlined />, badge: 'ddl' },
  { key: '/config', label: '生效配置', icon: <SlidersOutlined /> },
  { key: '/audit', label: '审计日志', icon: <HistoryOutlined /> },
]

const PIPELINE_STATES: Record<string, { label: string; tone: 'ok' | 'warn' | 'danger' | 'info' | 'muted' }> = {
  running: { label: '同步中', tone: 'ok' },
  stock: { label: '全量回放', tone: 'info' },
  paused: { label: '已暂停', tone: 'danger' },
  stopped: { label: '待机', tone: 'warn' },
  idle: { label: '空闲', tone: 'muted' },
}

/** 全部视图的顶栏聚合 Chip：任一 failed→故障；否则任一追赶中→warn；全 running→ok；混合→N/M。 */
function aggregatePipeline(sources: SourceStatus[]): { label: string; tone: 'ok' | 'warn' | 'danger' } | undefined {
  if (sources.length === 0) return undefined
  const failed = sources.filter((s) => s.phase === 'failed').length
  if (failed > 0) return { label: `${failed} 源故障`, tone: 'danger' }
  if (sources.some((s) => s.pipeline_state === 'running' && s.caught_up === false)) {
    return { label: '追赶中', tone: 'warn' }
  }
  const running = sources.filter((s) => s.pipeline_state === 'running').length
  if (running === sources.length) return { label: '全部同步中', tone: 'ok' }
  return { label: `${running}/${sources.length} 同步中`, tone: 'warn' }
}

export function AppLayout() {
  const { logout, session } = useAuth()
  const { data: status, isError: statusUnavailable } = useStatus()
  const { data: ddlEvents = [] } = useDDLEventsAll()
  const { currentSid, setSid, sourceCount } = useSourceScope()
  const location = useLocation()
  const navigate = useNavigate()
  const [mobileNavOpen, setMobileNavOpen] = useState(false)

  const sources = status?.sources ?? []
  const pendingDDL = ddlEvents.length
  const username = session?.credentials?.username ?? '匿名访问'
  const initial = username.slice(0, 1).toUpperCase()
  const active = NAV_ITEMS.find((item) => item.key === location.pathname)?.key ?? ''
  const apiState = statusUnavailable ? 'off' : status ? 'on' : 'wait'
  const apiLabel = statusUnavailable ? 'API 离线' : status ? 'API 在线' : 'API 连接中'
  const pipeline = currentSid
    ? PIPELINE_STATES[sources.find((s) => s.id === currentSid)?.pipeline_state ?? 'idle']
    : aggregatePipeline(sources)

  const userMenuItems: MenuProps['items'] = [
    {
      key: 'identity',
      disabled: true,
      label: (
        <div className="user-menu-card">
          <b>{username}</b>
          <span>{status ? roleLabel(status.role) : '未连接'}</span>
        </div>
      ),
    },
    { type: 'divider' },
    { key: 'logout', icon: <LogoutOutlined />, label: '断开连接' },
  ]

  function handleLogout() {
    logout()
    setMobileNavOpen(false)
    navigate('/', { replace: true })
  }

  function badgeText(count: number) {
    return count > 99 ? '99+' : String(count).padStart(2, '0')
  }

  return (
    <div className="app-shell">
      <header className="commandbar">
        <div className="command-top">
          <Link to="/" className="brand">
            <BrandMark />
            <span className="brand-copy"><strong>GOCDC</strong><small>MYSQL CDC 控制台</small></span>
          </Link>
          <div className="command-meta">
            <span className="meta-cell"><span>集群</span><b>{status?.cluster ?? '—'}</b></span>
            <span className="meta-cell"><span>节点</span><b>{status?.node_id ?? '—'}</b></span>
            <span className="meta-cell"><span>版本</span><b>{status?.version || '—'}</b></span>
            <span className={`health-pill is-${apiState}`}><i />{apiLabel}</span>
            {sourceCount > 1 && (
              <Select
                size="small"
                className="source-select"
                aria-label="切换数据源"
                value={currentSid ?? ''}
                onChange={(value) => setSid(value || null)}
                options={[{ value: '', label: '全部源' }, ...sources.map((source) => ({ value: source.id, label: source.id }))]}
              />
            )}
            {status && <RoleChip role={status.role} />}
            <span className="meta-divider" />
            <Dropdown trigger={['click']} menu={{ items: userMenuItems, onClick: ({ key }) => { if (key === 'logout') handleLogout() } }}>
              <button className="user-menu" type="button">
                <Avatar size={30} className="user-avatar">{initial}</Avatar>
                <span className="user-name">{username}</span>
                <CaretDownOutlined className="user-chevron" />
              </button>
            </Dropdown>
          </div>
          <button className="nav-toggle" type="button" aria-label="打开导航菜单" onClick={() => setMobileNavOpen(true)}>
            <MenuOutlined />
          </button>
        </div>
        <div className="command-nav">
          <nav className="primary-nav" aria-label="主导航">
            {NAV_ITEMS.map((item) => (
              <Link key={item.key} to={item.key} className={`nav-item ${active === item.key ? 'is-active' : ''}`}>
                <span className="nav-icon">{item.icon}</span>
                <span className="nav-label">{item.label}</span>
                {item.badge === 'ddl' && pendingDDL > 0 && <span className="nav-badge">{badgeText(pendingDDL)}</span>}
              </Link>
            ))}
          </nav>
          <div className="nav-side">
            {pipeline && <Chip tone={pipeline.tone} dot>{pipeline.label}</Chip>}
            {status && <span className="nav-side-node">任期 {status.term || 0}</span>}
          </div>
        </div>
      </header>

      <main className="shell-main">
        <div className="page-container"><Outlet /></div>
      </main>

      <Drawer
        rootClassName="nav-drawer"
        width={300}
        placement="left"
        open={mobileNavOpen}
        onClose={() => setMobileNavOpen(false)}
        title={<div className="drawer-brand"><BrandMark /><span className="brand-copy"><strong>GOCDC</strong><small>MYSQL CDC 控制台</small></span></div>}
      >
        <div className="drawer-status">
          <span className={`health-pill is-${apiState}`}><i />{apiLabel}</span>
          {status && <RoleChip role={status.role} />}
          {pipeline && <Chip tone={pipeline.tone} dot>{pipeline.label}</Chip>}
        </div>
        <div className="drawer-meta-grid">
          <div><span>集群</span><b>{status?.cluster ?? '—'}</b></div>
          <div><span>节点</span><b>{status?.node_id ?? '—'}</b></div>
          <div><span>版本</span><b>{status?.version || '—'}</b></div>
          <div><span>任期</span><b>{status?.term ?? '—'}</b></div>
        </div>
        <nav className="drawer-nav" aria-label="移动端导航">
          {NAV_ITEMS.map((item) => (
            <Link
              key={item.key}
              to={item.key}
              className={`drawer-link ${active === item.key ? 'is-active' : ''}`}
              onClick={() => setMobileNavOpen(false)}
            >
              <span className="nav-icon">{item.icon}</span>
              <span className="drawer-link-label">{item.label}</span>
              {item.badge === 'ddl' && pendingDDL > 0 && <span className="nav-badge">{badgeText(pendingDDL)}</span>}
            </Link>
          ))}
        </nav>
        <div className="drawer-foot">
          <div className="drawer-identity"><span>当前身份</span><b>{username}</b></div>
          <button type="button" className="drawer-logout" onClick={handleLogout}><LogoutOutlined /> 断开连接</button>
        </div>
      </Drawer>
    </div>
  )
}
