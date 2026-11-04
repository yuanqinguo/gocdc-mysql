import type {
  ApiCredentials,
  ApiEnvelope,
  AuditEntry,
  DDLRecord,
  EffectiveRule,
  GetRulesResult,
  MigrateFilePosResult,
  MigrateGTIDResult,
  PositionData,
  PutRulesResult,
  RuleGroup,
  SetPositionRequest,
  SourceStatus,
  SourceSummary,
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

  async getSources(signal?: AbortSignal): Promise<SourceSummary[]> {
    const data = await this.request<SourceSummary[] | null>('/sources', { signal })
    return Array.isArray(data) ? data : []
  }

  /** 全集群 pending DDL 合并（记录带 source 字段，聚合视图/角标用）。 */
  async getDDLEventsAll(signal?: AbortSignal): Promise<DDLRecord[]> {
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

  resign() {
    return this.request<void>('/leader/resign?confirm=true', { method: 'POST' })
  }

  // ── 源作用域（sid 必经 encodeURIComponent；旧单源路由只服务外部脚本）──

  private sourcePath(sid: string, suffix: string) {
    return `/sources/${encodeURIComponent(sid)}${suffix}`
  }

  getSourceStatus(sid: string, signal?: AbortSignal) {
    return this.request<SourceStatus>(this.sourcePath(sid, '/status'), { signal })
  }

  getPositionOf(sid: string, signal?: AbortSignal) {
    return this.request<PositionData | null>(this.sourcePath(sid, '/position'), { signal })
  }

  async getEffectiveRulesOf(sid: string, signal?: AbortSignal): Promise<EffectiveRule[]> {
    const data = await this.request<EffectiveRule[] | null>(this.sourcePath(sid, '/rules/effective'), { signal })
    return Array.isArray(data) ? data : []
  }

  async getDDLEventsOf(sid: string, signal?: AbortSignal): Promise<DDLRecord[]> {
    const data = await this.request<DDLRecord[] | null>(this.sourcePath(sid, '/ddl-events'), { signal })
    return Array.isArray(data) ? data : []
  }

  restockOf(sid: string, schema: string, table: string) {
    return this.request<void>(this.sourcePath(sid, `/rules/by-schema/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/restock?confirm=true`), {
      method: 'POST',
    })
  }

  approveDDLOf(sid: string, id: string) {
    return this.request<void>(this.sourcePath(sid, `/ddl/${encodeURIComponent(id)}/approve?confirm=true`), { method: 'POST' })
  }

  skipDDLOf(sid: string, id: string) {
    return this.request<void>(this.sourcePath(sid, `/ddl/${encodeURIComponent(id)}/skip?confirm=true`), { method: 'POST' })
  }

  verifyOf(sid: string, database: string, table: string, pk: string, sample: number) {
    return this.request<VerifyResult>(this.sourcePath(sid, '/verify?confirm=true'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ database, table, pk, sample }),
    })
  }

  /** 人工暂停该源管线：优雅停管线（位点落盘）；etcd 持久标志跨重启。 */
  pausePipelineOf(sid: string) {
    return this.request<void>(this.sourcePath(sid, '/pipeline/pause?confirm=true'), { method: 'POST' })
  }

  /** 恢复该源人工暂停的管线：删标志并从 etcd 位点重启。 */
  resumePipelineOf(sid: string) {
    return this.request<void>(this.sourcePath(sid, '/pipeline/resume?confirm=true'), { method: 'POST' })
  }

  setPositionOf(sid: string, body: SetPositionRequest) {
    return this.request<void>(this.sourcePath(sid, '/position'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  }

  /** 该源 file:pos 存量位点 → GTID 基线（use_gtid 切换后的恢复路径）。 */
  migrateGTIDOf(sid: string) {
    return this.request<MigrateGTIDResult>(this.sourcePath(sid, '/position/migrate-gtid?confirm=true'), { method: 'POST' })
  }

  /** 该源 GTID → file:pos 反向迁移（位点置最老 binlog 头，接受重放）。 */
  migrateFilePosOf(sid: string) {
    return this.request<MigrateFilePosResult>(this.sourcePath(sid, '/position/migrate-filepos?confirm=true'), { method: 'POST' })
  }

  // ── 规则编辑（配置中心 CAS 写；file 形态 409 由调用方按 ApiError.status 处理）──

  /** 该源规则组原样（组形态；revision 作 PUT base）。 */
  getRulesOf(sid: string, signal?: AbortSignal) {
    return this.request<GetRulesResult>(this.sourcePath(sid, '/rules'), { signal })
  }

  /**
   * 保存该源规则组（整组提交）。
   * @param base 读到的 revision——服务端 CAS，不一致返回 409（ApiError）。
   * @param checkOnly true = 预检预演（免 confirm、不写）。
   */
  putRulesOf(sid: string, base: number, rules: RuleGroup[], checkOnly: boolean) {
    const query = checkOnly ? 'check_only=true' : `confirm=true&base=${base}`
    return this.request<PutRulesResult>(this.sourcePath(sid, `/rules?${query}`), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rules }),
    })
  }

  /** 优雅排空退出（位点终值+resign），由编排拉起以应用新配置。 */
  applyRestart() {
    return this.request<void>('/config/apply-restart?confirm=true', { method: 'POST' })
  }
}
