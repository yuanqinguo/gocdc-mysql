import { lazy, Suspense, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { App as AntApp, ConfigProvider, theme, type ThemeConfig } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import { AuthProvider } from './auth'
import { ApiProvider } from './api'
import { AuthGate } from './AuthGate'
import { AppLayout } from './AppLayout'

const DashboardPage = lazy(() => import('@/features/dashboard/DashboardPage').then((module) => ({ default: module.DashboardPage })))
const RulesPage = lazy(() => import('@/features/rules/RulesPage').then((module) => ({ default: module.RulesPage })))
const DDLPage = lazy(() => import('@/features/ddl/DDLPage').then((module) => ({ default: module.DDLPage })))
const PositionPage = lazy(() => import('@/features/position/PositionPage').then((module) => ({ default: module.PositionPage })))
const ConfigPage = lazy(() => import('@/features/config/ConfigPage').then((module) => ({ default: module.ConfigPage })))
const AuditPage = lazy(() => import('@/features/audit/AuditPage').then((module) => ({ default: module.AuditPage })))

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, refetchOnWindowFocus: false, staleTime: 10_000 },
    mutations: { retry: 0 },
  },
})

const appTheme: ThemeConfig = {
  algorithm: theme.darkAlgorithm,
  token: {
    colorPrimary: '#ff7a1a',
    colorPrimaryHover: '#ff9440',
    colorPrimaryActive: '#e8690f',
    colorPrimaryBg: 'rgba(255, 122, 26, 0.12)',
    colorPrimaryBgHover: 'rgba(255, 122, 26, 0.18)',
    colorPrimaryBorder: 'rgba(255, 122, 26, 0.35)',
    colorPrimaryText: '#ff9a4f',
    colorInfo: '#5cc8ff',
    colorSuccess: '#4bd37b',
    colorWarning: '#f2c14e',
    colorError: '#ff5c5c',
    colorLink: '#ff9a4f',
    colorLinkHover: '#ffb37a',
    colorLinkActive: '#e8690f',
    colorBgBase: '#161c25',
    colorBgLayout: '#161c25',
    colorBgContainer: '#1e2632',
    colorBgElevated: '#273241',
    colorBgSpotlight: '#2d3a4b',
    colorText: '#f3f7fb',
    colorTextSecondary: '#c0cbd8',
    colorTextTertiary: '#98a6b7',
    colorTextQuaternary: '#7b8ba0',
    colorTextHeading: '#f3f7fb',
    colorTextLabel: '#c0cbd8',
    colorTextDescription: '#98a6b7',
    colorBorder: '#374555',
    colorBorderSecondary: '#2b3644',
    colorSplit: '#2b3644',
    colorFill: 'rgba(255, 255, 255, 0.06)',
    colorFillSecondary: 'rgba(255, 255, 255, 0.04)',
    colorFillTertiary: 'rgba(255, 255, 255, 0.025)',
    colorFillQuaternary: 'rgba(255, 255, 255, 0.015)',
    borderRadius: 6,
    borderRadiusLG: 8,
    borderRadiusSM: 4,
    borderRadiusXS: 2,
    controlHeight: 36,
    controlHeightSM: 28,
    controlHeightLG: 44,
    fontFamily: '"PingFang SC", "Microsoft YaHei", "Noto Sans SC", "Segoe UI", sans-serif',
    fontFamilyCode: '"Cascadia Code", "JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace',
    fontSize: 13,
    fontSizeSM: 11,
    fontSizeLG: 15,
    lineHeight: 1.6,
    boxShadow: '0 12px 40px rgba(0, 0, 0, 0.45)',
    boxShadowSecondary: '0 18px 60px rgba(0, 0, 0, 0.55)',
  },
  components: {
    Layout: { headerBg: '#161c25', bodyBg: '#161c25', siderBg: '#161c25' },
    Button: {
      fontWeight: 600,
      primaryShadow: 'none',
      defaultShadow: 'none',
      dangerShadow: 'none',
      defaultBg: '#283342',
      defaultBorderColor: '#425264',
      defaultColor: '#c0cbd8',
      defaultHoverBg: '#2e3b4b',
      defaultHoverBorderColor: '#ff7a1a',
      defaultHoverColor: '#ff9a4f',
    },
    Table: {
      headerBg: 'rgba(14, 19, 26, 0.55)',
      headerColor: '#98a6b7',
      headerSplitColor: '#374555',
      rowHoverBg: 'rgba(255, 122, 26, 0.045)',
      borderColor: '#2b3644',
      cellPaddingBlock: 13,
      cellPaddingInline: 16,
    },
    Input: {
      activeBorderColor: '#ff7a1a',
      hoverBorderColor: '#4a5b6e',
      activeShadow: '0 0 0 2px rgba(255, 122, 26, 0.14)',
    },
    InputNumber: {
      activeBorderColor: '#ff7a1a',
      hoverBorderColor: '#4a5b6e',
      activeShadow: '0 0 0 2px rgba(255, 122, 26, 0.14)',
    },
    Segmented: {
      trackBg: '#10151d',
      itemColor: '#98a6b7',
      itemHoverBg: 'rgba(255, 255, 255, 0.03)',
      itemHoverColor: '#f3f7fb',
      itemSelectedBg: '#2b3747',
      itemSelectedColor: '#ff9a4f',
    },
    Modal: { contentBg: '#1e2836', headerBg: '#1e2836', titleColor: '#f3f7fb', footerBg: '#1e2836' },
  },
}

function PageLoader({ children }: { children: ReactNode }) {
  return <Suspense fallback={<div className="page-state"><div className="page-loading-line" /></div>}>{children}</Suspense>
}

function NotFound() {
  return (
    <div className="not-found">
      <span className="not-found-code">404 / ROUTE NOT FOUND</span>
      <h1>页面不存在</h1>
      <p>请检查地址，或从顶部导航选择一个管理页面。</p>
      <a className="not-found-action" href="/">返回总控台</a>
    </div>
  )
}

const router = createBrowserRouter([
  {
    path: '/',
    element: <AuthGate><AppLayout /></AuthGate>,
    children: [
      { index: true, element: <PageLoader><DashboardPage /></PageLoader> },
      { path: 'rules', element: <PageLoader><RulesPage /></PageLoader> },
      { path: 'ddl', element: <PageLoader><DDLPage /></PageLoader> },
      { path: 'position', element: <PageLoader><PositionPage /></PageLoader> },
      { path: 'config', element: <PageLoader><ConfigPage /></PageLoader> },
      { path: 'audit', element: <PageLoader><AuditPage /></PageLoader> },
      { path: '*', element: <NotFound /> },
    ],
  },
])

export function App() {
  return (
    <ConfigProvider locale={zhCN} theme={appTheme}>
      <QueryClientProvider client={queryClient}>
        <AuthProvider>
          <ApiProvider>
            <AntApp>
              <RouterProvider router={router} />
            </AntApp>
          </ApiProvider>
        </AuthProvider>
      </QueryClientProvider>
    </ConfigProvider>
  )
}
