export interface ApiEnvelope<T> {
  code: number
  message?: string
  data: T
}

export interface ApiCredentials {
  username: string
  password: string
}

export type NodeRole = 'leader' | 'follower' | 'single' | string

export type PipelineState = 'idle' | 'stock' | 'running' | 'paused' | 'stopped' | string

export interface StatusData {
  role: NodeRole
  leader: string
  term: number
  node_id: string
  version: string
  cluster: string
  config_source: string
  uptime_seconds: number
  /** 全部源运行摘要（配置序）；前端一律读此数组 */
  sources: SourceStatus[]
  /** @deprecated 单源部署的兼容镜像；多源为 idle 占位——前端读 sources[] */
  pipeline_state: PipelineState
  /** @deprecated 同上，读 sources[].pipeline_error */
  pipeline_error?: string
  /** @deprecated 同上，读 sources[].pipeline_since */
  pipeline_since?: string
  /** @deprecated 同上，读 sources[].pipeline_paused_by_op */
  pipeline_paused_by_op?: boolean
  /** @deprecated 同上，读 sources[].event_lag_seconds */
  event_lag_seconds?: number
  /** @deprecated 同上，读 sources[].caught_up */
  caught_up?: boolean
  /** @deprecated 同上，读 sources[].master_lag_bytes */
  master_lag_bytes?: number
  /** @deprecated 同上，读 sources[].use_gtid */
  use_gtid: boolean
}

/** /status sources[] 单源运行摘要（源作用域状态/遥测） */
export interface SourceStatus {
  id: string
  phase: string // starting | ready | failed
  pipeline_state: PipelineState
  pipeline_error?: string
  pipeline_since?: string
  pipeline_paused_by_op?: boolean
  event_lag_seconds?: number
  caught_up?: boolean
  master_lag_bytes?: number
  use_gtid: boolean
  position?: PositionData
}

/** GET /sources 源清单项（地址/规则统计/状态摘要） */
export interface SourceSummary {
  id: string
  source_addr: string
  target_addrs: string[]
  sinks: string[]
  rules: number
  tables: number
  state: string
  error?: string
}

export interface MigrateGTIDResult {
  name: string
  pos: number
  gtid_set: string
  gap_bytes: number
  next: string
}

export interface MigrateFilePosResult {
  name: string
  pos: number
  next: string
}

export interface EffectiveRuleSinks {
  mysql: boolean
  kafka: boolean
  rocketmq: boolean
  kafka_topic: string
  rocketmq_topic: string
  rocketmq_tag: string
}

export interface EffectiveMaskSpec {
  type: string
  keep?: number
}

export interface EffectiveRule {
  key: string
  schema: string
  table: string
  target_database: string
  do_stock: boolean
  ddl_policy: string
  order_by_column: string
  exclude_columns: string[]
  mask_columns: Record<string, EffectiveMaskSpec>
  sinks: EffectiveRuleSinks
}

export interface PositionData {
  name: string
  pos: number
  gtid_set: string
  seq: number
  term: number
  node_id: string
  updated_at: string
}

export interface DDLRecord {
  id: string
  schema: string
  table: string
  sql: string
  position: string
  policy: string
  status: string
  created_at: string
  decided_at?: string
  note?: string
  source?: string
}

export interface AuditEntry {
  who: string
  what: string
  result: string
  node: string
  at: string
  source?: string
}

export interface VerifyResult {
  src_count: number
  tgt_count: number
  checked: number
  mismatch: number
  diffs: string[]
  truncated?: boolean
}

export interface SetPositionRequest {
  name: string
  pos: number
  confirm: 'SET-POSITION'
}
