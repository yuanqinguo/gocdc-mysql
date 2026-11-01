import type { ApiCredentials } from '@/shared/api/types'

const SESSION_COOKIE = 'gocdc_session'
const SESSION_TTL_SECONDS = 7 * 24 * 60 * 60

interface StoredSession {
  credentials: ApiCredentials | null
}

/** 从 Cookie 恢复登录态；返回 undefined 表示没有有效会话（未登录或数据损坏）。 */
export function readStoredSession(): StoredSession | undefined {
  const raw = document.cookie
    .split('; ')
    .find((entry) => entry.startsWith(`${SESSION_COOKIE}=`))
    ?.slice(SESSION_COOKIE.length + 1)
  if (!raw) return undefined

  let payload: unknown
  try {
    payload = JSON.parse(decodeURIComponent(raw)) as unknown
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

/** 登录成功后写入会话 Cookie，过期时间由 Max-Age 控制。 */
export function storeSession(credentials: ApiCredentials | null): void {
  const payload = credentials ?? { username: '', password: '' }
  document.cookie = [
    `${SESSION_COOKIE}=${encodeURIComponent(JSON.stringify(payload))}`,
    `Max-Age=${SESSION_TTL_SECONDS}`,
    'Path=/',
    'SameSite=Lax',
    ...(globalThis.location?.protocol === 'https:' ? ['Secure'] : []),
  ].join('; ')
}

/** 退出登录或会话失效时清除 Cookie。 */
export function clearStoredSession(): void {
  document.cookie = `${SESSION_COOKIE}=; Max-Age=0; Path=/; SameSite=Lax`
}
