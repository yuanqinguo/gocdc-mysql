import type { ApiCredentials } from '@/shared/api/types'

// 2026-11-16 review P3-2（过渡方案）：凭据改存 sessionStorage——
// 原实现写入 7 天持久 Cookie（JS 可读），管理员口令长期落盘且任何 XSS
// 可直接窃取。sessionStorage 生命周期 = 标签页会话：关闭标签即清除，
// 不落盘、不跨标签共享，显著缩小暴露面。
// 长期建议（报告 P3-2 主方案）：服务端签发 HttpOnly 会话 Cookie，前端
// 不再持有原始口令；生产部署强制 TLS 反代（见 deploy/README.md）。
const SESSION_KEY = 'gocdc_session'

interface StoredSession {
  credentials: ApiCredentials | null
}

/** 从 sessionStorage 恢复登录态；返回 undefined 表示没有有效会话（未登录或数据损坏）。 */
export function readStoredSession(): StoredSession | undefined {
  const raw = globalThis.sessionStorage?.getItem(SESSION_KEY)
  if (!raw) return undefined

  let payload: unknown
  try {
    payload = JSON.parse(raw) as unknown
  } catch {
    return undefined
  }
  if (!payload || typeof payload !== 'object') return undefined
  const { username, password } = payload as Record<string, unknown>
  if (typeof username !== 'string' || typeof password !== 'string') return undefined
  // 用户名与密码均为空代表匿名会话（服务端关闭鉴权时选择"无凭据继续"）
  if (!username && !password) return { credentials: null }
  return { credentials: { username, password } }
}

/** 登录成功后写入会话存储（随标签页会话存活，关闭标签即清除）。 */
export function storeSession(credentials: ApiCredentials | null): void {
  const payload = credentials ?? { username: '', password: '' }
  globalThis.sessionStorage?.setItem(SESSION_KEY, JSON.stringify(payload))
}

/** 退出登录或会话失效时清除存储。 */
export function clearStoredSession(): void {
  globalThis.sessionStorage?.removeItem(SESSION_KEY)
}
