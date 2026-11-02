import { describe, expect, it, vi } from 'vitest'
import { ApiClient, ApiError } from './client'

function response(status: number, payload: unknown) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('ApiClient', () => {
  it('adds Basic credentials and unwraps the API envelope', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: { role: 'leader' } }))
    const api = new ApiClient({ username: 'admin', password: 'p@ss' }, undefined, '/api/v1', fetcher)

    await expect(api.getStatus()).resolves.toEqual({ role: 'leader' })
    const [, init] = fetcher.mock.calls[0] as [string, RequestInit]
    expect(new Headers(init.headers).get('Authorization')).toBe(`Basic ${btoa('admin:p@ss')}`)
    expect(new Headers(init.headers).get('Accept')).toBe('application/json')
  })

  it('calls the browser fetch function through the global object', async () => {
    const fetcher = vi.fn(function (this: typeof globalThis) {
      expect(this).toBe(globalThis)
      return Promise.resolve(response(200, { code: 0, data: { role: 'leader' } }))
    })
    vi.stubGlobal('fetch', fetcher)
    try {
      await expect(new ApiClient(null).getStatus()).resolves.toMatchObject({ role: 'leader' })
      expect(fetcher).toHaveBeenCalledOnce()
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('calls an injected fetcher without binding it to the ApiClient', async () => {
    const fetcher = vi.fn(function (this: unknown) {
      expect(this).toBeUndefined()
      return Promise.resolve(response(200, { code: 0, data: { role: 'leader' } }))
    })
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)

    await expect(api.getStatus()).resolves.toMatchObject({ role: 'leader' })
    expect(fetcher).toHaveBeenCalledWith('/api/v1/status', expect.objectContaining({ credentials: 'same-origin' }))
  })

  it('includes confirm gates for dangerous endpoints', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: null }))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.resign()
    expect(fetcher.mock.calls[0]?.[0]).toBe('/api/v1/leader/resign?confirm=true')
  })

  it('scopes pipeline pause/resume to the source path with confirm gates and POST', async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(response(200, { code: 0, data: null })))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.pausePipelineOf('order-db')
    await api.resumePipelineOf('order-db')
    const [pausePath, pauseInit] = fetcher.mock.calls[0] as [string, RequestInit]
    const [resumePath, resumeInit] = fetcher.mock.calls[1] as [string, RequestInit]
    expect(pausePath).toBe('/api/v1/sources/order-db/pipeline/pause?confirm=true')
    expect(resumePath).toBe('/api/v1/sources/order-db/pipeline/resume?confirm=true')
    expect(pauseInit.method).toBe('POST')
    expect(resumeInit.method).toBe('POST')
  })

  it('encodes the source id so ids with special characters stay one path segment', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: { id: 'a b/c' } }))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.getSourceStatus('a b/c')
    expect(fetcher.mock.calls[0]?.[0]).toBe('/api/v1/sources/a%20b%2Fc/status')
  })

  it('uses separate restock path segments so dotted schemas stay intact', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: null }))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.restockOf('order-db', 'edu.mall.admin', 'users')
    expect(fetcher.mock.calls[0]?.[0]).toBe('/api/v1/sources/order-db/rules/by-schema/edu.mall.admin/users/restock?confirm=true')
  })

  it('routes DDL decisions through the source scope', async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(response(200, { code: 0, data: null })))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.approveDDLOf('order-db', 'ev/1')
    await api.skipDDLOf('order-db', 'ev/1')
    expect(fetcher.mock.calls[0]?.[0]).toBe('/api/v1/sources/order-db/ddl/ev%2F1/approve?confirm=true')
    expect(fetcher.mock.calls[1]?.[0]).toBe('/api/v1/sources/order-db/ddl/ev%2F1/skip?confirm=true')
  })

  it('normalizes nullable list payloads to empty arrays', async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(response(200, { code: 0, data: null })))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await expect(api.getEffectiveRulesOf('order-db')).resolves.toEqual([])
    await expect(api.getDDLEventsOf('order-db')).resolves.toEqual([])
    await expect(api.getDDLEventsAll()).resolves.toEqual([])
    await expect(api.getSources()).resolves.toEqual([])
    await expect(api.getAudit()).resolves.toEqual([])
  })

  it('sends the dedicated position confirmation in the body', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: null }))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.setPositionOf('order-db', { name: 'binlog.000001', pos: 123, confirm: 'SET-POSITION' })
    const [path, init] = fetcher.mock.calls[0] as [string, RequestInit]
    expect(path).toBe('/api/v1/sources/order-db/position')
    expect(init.method).toBe('POST')
    expect(JSON.parse(String(init.body))).toEqual({ name: 'binlog.000001', pos: 123, confirm: 'SET-POSITION' })
  })

  it('sends verify with the source scope and confirm gate', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: { src_count: 0, tgt_count: 0, checked: 0, mismatch: 0, diffs: [] } }))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.verifyOf('order-db', 'shop', 'users', 'id', 10)
    const [path, init] = fetcher.mock.calls[0] as [string, RequestInit]
    expect(path).toBe('/api/v1/sources/order-db/verify?confirm=true')
    expect(JSON.parse(String(init.body))).toEqual({ database: 'shop', table: 'users', pk: 'id', sample: 10 })
  })

  it('routes position mode migrations through the source scope', async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(response(200, { code: 0, data: null })))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.migrateGTIDOf('order-db')
    await api.migrateFilePosOf('order-db')
    expect(fetcher.mock.calls[0]?.[0]).toBe('/api/v1/sources/order-db/position/migrate-gtid?confirm=true')
    expect(fetcher.mock.calls[1]?.[0]).toBe('/api/v1/sources/order-db/position/migrate-filepos?confirm=true')
  })

  it('invalidates the session and exposes server errors on 401', async () => {
    const onUnauthorized = vi.fn()
    const api = new ApiClient(null, onUnauthorized, '/api/v1', vi.fn().mockResolvedValue(response(401, { code: 401, message: '凭据无效' })))
    const request = api.getStatus()
    await expect(request).rejects.toBeInstanceOf(ApiError)
    await expect(request).rejects.toMatchObject({ status: 401, message: '凭据无效' })
    expect(onUnauthorized).toHaveBeenCalledOnce()
  })

  it('does not silently accept malformed envelopes', async () => {
    const api = new ApiClient(null, undefined, '/api/v1', vi.fn().mockResolvedValue(response(200, { data: {} })))
    await expect(api.getStatus()).rejects.toThrow('无法识别')
  })
})
