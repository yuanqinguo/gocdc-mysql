import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiProvider } from './api'
import { AuthGate } from './AuthGate'
import { AuthProvider, useAuth } from './auth'
import { clearStoredSession, storeSession } from './session'

function stubStatusOk() {
  vi.stubGlobal('fetch', vi.fn(async () =>
    new Response(JSON.stringify({ code: 0, message: 'ok', data: { role: 'leader' } }), { status: 200 }),
  ))
}

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  clearStoredSession()
})

function renderGate() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <ApiProvider>
          <AuthGate><div>已连接控制台</div></AuthGate>
        </ApiProvider>
      </AuthProvider>
    </QueryClientProvider>,
  )
}

describe('AuthGate', () => {
  it('does not show protected content before credentials are supplied', () => {
    stubStatusOk()
    renderGate()
    expect(screen.getByText('欢迎回来')).toBeInTheDocument()
    expect(screen.queryByText('已连接控制台')).not.toBeInTheDocument()
  })

  it('restores a credential session from the cookie after remount', async () => {
    stubStatusOk()
    storeSession({ username: 'admin', password: 'secret' })
    renderGate()
    expect(await screen.findByText('已连接控制台')).toBeInTheDocument()
    expect(screen.queryByText('欢迎回来')).not.toBeInTheDocument()
  })

  it('restores an anonymous session (auth disabled on server) from the cookie', async () => {
    stubStatusOk()
    storeSession(null)
    renderGate()
    expect(await screen.findByText('已连接控制台')).toBeInTheDocument()
  })

  it('clears the stored cookie on logout', async () => {
    stubStatusOk()
    storeSession({ username: 'admin', password: 'secret' })
    const user = userEvent.setup()
    function LogoutProbe() {
      const { logout } = useAuth()
      return <button type="button" onClick={logout}>断开连接</button>
    }
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(
      <QueryClientProvider client={queryClient}>
        <AuthProvider>
          <ApiProvider>
            <LogoutProbe />
            <AuthGate><div>已连接控制台</div></AuthGate>
          </ApiProvider>
        </AuthProvider>
      </QueryClientProvider>,
    )
    expect(await screen.findByText('已连接控制台')).toBeInTheDocument()

    await user.click(screen.getByText('断开连接'))

    expect(screen.getByText('欢迎回来')).toBeInTheDocument()
    expect(document.cookie).not.toContain('gocdc_session=')
  })
})
