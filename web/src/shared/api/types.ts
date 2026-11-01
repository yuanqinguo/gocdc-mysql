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
  pipeline_state: PipelineState
  pipeline_error?: string
  pipeline_since?: string
  pipeline_paused_by_op?: boolean
  event_lag_seconds?: number
  caught_up?: boolean
  master_lag_bytes?: number
  use_gtid: boolean
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
}

export interface AuditEntry {
  who: string
  what: string
  result: string
  node: string
  at: string
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
