import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiProvider } from './api'
import { AppLayout } from './AppLayout'
import { AuthProvider } from './auth'
import { clearStoredSession } from './session'
import { useSourceScope } from './sourceScope'

function stubStatus(sources: string[]) {
  vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({
    code: 0,
    data: {
      role: 'single',
      sources: sources.map((id) => ({ id, phase: 'ready', pipeline_state: 'running', use_gtid: false })),
      audit: [],
    },
  }), { status: 200 })))
}

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  clearStoredSession()
})

function ScopeProbe() {
  const { currentSid, setSid, sourceCount } = useSourceScope()
  const location = useLocation()
  return (
    <div>
      <output data-testid="sid">{currentSid ?? 'null'}</output>
      <output data-testid="count">{sourceCount}</output>
      <output data-testid="search">{location.search}</output>
      <button type="button" onClick={() => setSid('order-db')}>选源</button>
      <button type="button" onClick={() => setSid(null)}>清空</button>
    </div>
  )
}

function renderProbe(initialPath: string) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <ApiProvider>
          <MemoryRouter initialEntries={[initialPath]}>
            <ScopeProbe />
          </MemoryRouter>
        </ApiProvider>
      </AuthProvider>
    </QueryClientProvider>,
  )
}

describe('useSourceScope', () => {
  it('reads the sid from ?source= and falls back to null without the param', () => {
    stubStatus(['order-db', 'user-db'])
    const { unmount } = renderProbe('/?source=order-db')
    expect(screen.getByTestId('sid')).toHaveTextContent('order-db')
    unmount()

    renderProbe('/')
    expect(screen.getByTestId('sid')).toHaveTextContent('null')
  })

  it('updates the sid in place and keeps other query params intact', async () => {
    stubStatus(['order-db', 'user-db'])
    const user = userEvent.setup()
    renderProbe('/?tab=metrics&source=user-db')

    await user.click(screen.getByText('选源'))
    expect(screen.getByTestId('sid')).toHaveTextContent('order-db')
    expect(screen.getByTestId('search')).toHaveTextContent('tab=metrics')
    expect(screen.getByTestId('search')).toHaveTextContent('source=order-db')

    await user.click(screen.getByText('清空'))
    expect(screen.getByTestId('sid')).toHaveTextContent('null')
    expect(screen.getByTestId('search')).toHaveTextContent('tab=metrics')
    expect(screen.getByTestId('search')).not.toHaveTextContent('source=')
  })

  it('derives the source count from the status payload', async () => {
    stubStatus(['order-db', 'user-db'])
    renderProbe('/')
    await waitFor(() => expect(screen.getByTestId('count')).toHaveTextContent('2'))
  })

  it('keeps the sid parseable in single-source mode (selector hiding is a layout concern)', async () => {
    stubStatus(['only-db'])
    renderProbe('/?source=only-db')
    expect(screen.getByTestId('sid')).toHaveTextContent('only-db')
    await waitFor(() => expect(screen.getByTestId('count')).toHaveTextContent('1'))
  })
})

function LayoutProbe() {
  const location = useLocation()
  return (
    <div>
      页面内容
      <output data-testid="search">{location.search}</output>
    </div>
  )
}

function renderLayout(sources: string[]) {
  stubStatus(sources)
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <ApiProvider>
          <MemoryRouter initialEntries={['/']}>
            <Routes>
              <Route path="/" element={<AppLayout />}>
                <Route index element={<LayoutProbe />} />
              </Route>
            </Routes>
          </MemoryRouter>
        </ApiProvider>
      </AuthProvider>
    </QueryClientProvider>,
  )
}

describe('AppLayout source switcher', () => {
  it('hides the switcher for single-source deployments', async () => {
    renderLayout(['only-db'])
    await screen.findByText('页面内容')
    await waitFor(() => expect(screen.queryByText('全部源')).not.toBeInTheDocument())
  })

  it('shows the switcher in multi-source deployments and switches scope in place', async () => {
    renderLayout(['order-db', 'user-db'])
    await screen.findByText('页面内容')
    await waitFor(() => expect(screen.getByText('数据源')).toBeInTheDocument())
    expect(screen.getByText('全部源')).toBeInTheDocument()

    const user = userEvent.setup()
    await user.click(screen.getByText('user-db'))
    await waitFor(() => expect(screen.getByTestId('search')).toHaveTextContent('source=user-db'))

    await user.click(screen.getByText('全部源'))
    await waitFor(() => expect(screen.getByTestId('search')).not.toHaveTextContent('source='))
  })
})
