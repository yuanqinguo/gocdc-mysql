import type {
  ApiCredentials,
  ApiEnvelope,
  AuditEntry,
  DDLRecord,
  EffectiveRule,
  MigrateFilePosResult,
  MigrateGTIDResult,
  PositionData,
  SetPositionRequest,
  StatusData,
  VerifyResult,
} from './types'

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

function encodeBasic(credentials: ApiCredentials): string {
  const bytes = new TextEncoder().encode(`${credentials.username}:${credentials.password}`)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return `Basic ${btoa(binary)}`
}

export class ApiClient {
  private readonly authorization: string | undefined
  private readonly fetcher: typeof fetch

  constructor(
    credentials: ApiCredentials | null,
    private readonly onUnauthorized: () => void = () => undefined,
    private readonly baseURL = '/api/v1',
    fetcher?: typeof fetch,
  ) {
    this.authorization = credentials ? encodeBasic(credentials) : undefined
    this.fetcher = fetcher
      ? (input, init) => fetcher(input, init)
      : (input, init) => globalThis.fetch(input, init)
  }

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers)
    headers.set('Accept', 'application/json')
    if (this.authorization) headers.set('Authorization', this.authorization)

    const response = await this.fetcher(`${this.baseURL}${path}`, {
      ...init,
      headers,
      credentials: 'same-origin',
    })
    const raw = await response.text()
    let payload: unknown
    if (raw) {
      try {
        payload = JSON.parse(raw) as unknown
      } catch {
        payload = undefined
      }
    }

    if (response.status === 401) this.onUnauthorized()
    if (!response.ok) {
      const message =
        payload && typeof payload === 'object' && 'message' in payload && typeof payload.message === 'string'
          ? payload.message
          : `请求失败（HTTP ${response.status}）`
      throw new ApiError(message, response.status)
    }

    const envelope = payload as ApiEnvelope<T> | undefined
    if (!envelope || typeof envelope !== 'object' || !('code' in envelope)) {
      throw new ApiError('服务端返回了无法识别的数据格式', response.status)
    }
    if (envelope.code !== 0) {
      throw new ApiError(envelope.message || `接口错误（${envelope.code}）`, response.status)
    }
    return envelope.data
  }

  getStatus(signal?: AbortSignal) {
    return this.request<StatusData>('/status', { signal })
  }

  async getEffectiveRules(signal?: AbortSignal): Promise<EffectiveRule[]> {
    const data = await this.request<EffectiveRule[] | null>('/rules/effective', { signal })
    return Array.isArray(data) ? data : []
  }

  getPosition(signal?: AbortSignal) {
    return this.request<PositionData | null>('/position', { signal })
  }

  async getDDLEvents(signal?: AbortSignal): Promise<DDLRecord[]> {
    const data = await this.request<DDLRecord[] | null>('/ddl-events', { signal })
    return Array.isArray(data) ? data : []
  }

  async getAudit(signal?: AbortSignal): Promise<AuditEntry[]> {
    const data = await this.request<AuditEntry[] | null>('/audit', { signal })
    return Array.isArray(data) ? data : []
  }

  async getConfig(signal?: AbortSignal): Promise<unknown> {
    const headers = new Headers({ Accept: 'application/json' })
    if (this.authorization) headers.set('Authorization', this.authorization)
    const response = await this.fetcher(`${this.baseURL}/config`, {
      headers,
      signal,
      credentials: 'same-origin',
    })
    if (response.status === 401) this.onUnauthorized()
    if (!response.ok) throw new ApiError(`请求失败（HTTP ${response.status}）`, response.status)
    return (await response.json()) as unknown
  }

  restock(schema: string, table: string) {
    return this.request<void>(`/rules/by-schema/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/restock?confirm=true`, {
      method: 'POST',
    })
  }

  approveDDL(id: string) {
    return this.request<void>(`/ddl/${encodeURIComponent(id)}/approve?confirm=true`, { method: 'POST' })
  }

  skipDDL(id: string) {
    return this.request<void>(`/ddl/${encodeURIComponent(id)}/skip?confirm=true`, { method: 'POST' })
  }

  verify(database: string, table: string, pk: string, sample: number) {
    return this.request<VerifyResult>('/verify?confirm=true', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ database, table, pk, sample }),
    })
  }

  resign() {
    return this.request<void>('/leader/resign?confirm=true', { method: 'POST' })
  }

  /** 人工暂停管线：优雅停管线（位点落盘）保持 Leader；etcd 持久标志跨重启。 */
  pausePipeline() {
    return this.request<void>('/pipeline/pause?confirm=true', { method: 'POST' })
  }

  /** 恢复人工暂停的管线：删标志并从 etcd 位点重启（支持暂停期间改位点）。 */
  resumePipeline() {
    return this.request<void>('/pipeline/resume?confirm=true', { method: 'POST' })
  }

  setPosition(body: SetPositionRequest) {
    return this.request<void>('/position', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  }

  /** file:pos 存量位点 → GTID 基线（use_gtid 切换后管线暂停的恢复路径）。 */
  migrateGTID() {
    return this.request<MigrateGTIDResult>('/position/migrate-gtid?confirm=true', { method: 'POST' })
  }

  /** GTID → file:pos 反向迁移（位点置最老 binlog 头，接受重放）。 */
  migrateFilePos() {
    return this.request<MigrateFilePosResult>('/position/migrate-filepos?confirm=true', { method: 'POST' })
  }
}
