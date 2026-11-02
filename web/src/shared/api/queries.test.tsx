import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiProvider } from '@/app/api'
import { AuthProvider } from '@/app/auth'
import { clearStoredSession } from '@/app/session'
import { queryKeys, usePosition, useSourceStatus } from './queries'

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  clearStoredSession()
})

/** fetch 桩：按 URL 里的 sid 返回不同数据——两个源若串缓存会立刻暴露。 */
function stubPerSource() {
  const fetcher = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input)
    const match = /\/sources\/([^/]+)\//.exec(url)
    const sid = decodeURIComponent(match?.[1] ?? 'unknown')
    if (url.endsWith('/status')) {
      return new Response(JSON.stringify({ code: 0, data: { id: sid, phase: 'ready', pipeline_state: 'running', use_gtid: false } }), { status: 200 })
    }
    return new Response(JSON.stringify({
      code: 0,
      data: { name: `bin-${sid}`, pos: 4, gtid_set: '', seq: 1, term: 1, node_id: 'n1', updated_at: '2026-01-01T00:00:00Z' },
    }), { status: 200 })
  })
  vi.stubGlobal('fetch', fetcher)
  return fetcher
}

function PositionProbe({ sid }: { sid: string }) {
  const { data } = usePosition(sid)
  return <output data-testid={`pos-${sid}`}>{data?.name ?? '加载中'}</output>
}

function SourceStatusProbe({ sid }: { sid: string }) {
  const { data } = useSourceStatus(sid)
  return <output data-testid={`state-${sid}`}>{data?.pipeline_state ?? '加载中'}</output>
}

function renderProbes() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <ApiProvider>
          <PositionProbe sid="order-db" />
          <PositionProbe sid="user-db" />
          <SourceStatusProbe sid="order-db" />
          <SourceStatusProbe sid="user-db" />
        </ApiProvider>
      </AuthProvider>
    </QueryClientProvider>,
  )
}

describe('source-scoped query keys', () => {
  it('gives every sid its own cache entry so switching sources never crosses data', async () => {
    const fetcher = stubPerSource()
    renderProbes()
    await waitFor(() => expect(screen.getByTestId('pos-order-db')).toHaveTextContent('bin-order-db'))
    await waitFor(() => expect(screen.getByTestId('pos-user-db')).toHaveTextContent('bin-user-db'))
    await waitFor(() => expect(screen.getByTestId('state-order-db')).toHaveTextContent('running'))
    await waitFor(() => expect(screen.getByTestId('state-user-db')).toHaveTextContent('running'))

    const calls = fetcher.mock.calls.map(([input]) => String(input))
    expect(calls).toContain('/api/v1/sources/order-db/position')
    expect(calls).toContain('/api/v1/sources/user-db/position')
    expect(calls).toContain('/api/v1/sources/order-db/status')
    expect(calls).toContain('/api/v1/sources/user-db/status')
  })

  it('builds distinct key tuples per sid for all scoped collections', () => {
    expect(queryKeys.position('a')).not.toEqual(queryKeys.position('b'))
    expect(queryKeys.position('a')).toEqual(['position', 'a'])
    expect(queryKeys.rules('a')).toEqual(['effective-rules', 'a'])
    expect(queryKeys.ddl('a')).toEqual(['ddl-events', 'a'])
    expect(queryKeys.sourceStatus('a')).toEqual(['source-status', 'a'])
    expect(queryKeys.ddlAll).toEqual(['ddl-events-all'])
  })
})
