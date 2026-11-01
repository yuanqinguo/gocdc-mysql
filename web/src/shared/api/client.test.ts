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

  it('sends pipeline pause/resume with confirm gates and POST', async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(response(200, { code: 0, data: null })))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.pausePipeline()
    await api.resumePipeline()
    const [pausePath, pauseInit] = fetcher.mock.calls[0] as [string, RequestInit]
    const [resumePath, resumeInit] = fetcher.mock.calls[1] as [string, RequestInit]
    expect(pausePath).toBe('/api/v1/pipeline/pause?confirm=true')
    expect(resumePath).toBe('/api/v1/pipeline/resume?confirm=true')
    expect(pauseInit.method).toBe('POST')
    expect(resumeInit.method).toBe('POST')
  })

  it('uses separate restock path segments so dotted schemas stay intact', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: null }))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.restock('edu.mall.admin', 'users')
    expect(fetcher.mock.calls[0]?.[0]).toBe('/api/v1/rules/by-schema/edu.mall.admin/users/restock?confirm=true')
  })

  it('normalizes nullable list payloads to empty arrays', async () => {
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(response(200, { code: 0, data: null })))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await expect(api.getEffectiveRules()).resolves.toEqual([])
    await expect(api.getDDLEvents()).resolves.toEqual([])
    await expect(api.getAudit()).resolves.toEqual([])
  })

  it('sends the dedicated position confirmation in the body', async () => {
    const fetcher = vi.fn().mockResolvedValue(response(200, { code: 0, data: null }))
    const api = new ApiClient(null, undefined, '/api/v1', fetcher)
    await api.setPosition({ name: 'binlog.000001', pos: 123, confirm: 'SET-POSITION' })
    const [, init] = fetcher.mock.calls[0] as [string, RequestInit]
    expect(JSON.parse(String(init.body))).toEqual({ name: 'binlog.000001', pos: 123, confirm: 'SET-POSITION' })
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
