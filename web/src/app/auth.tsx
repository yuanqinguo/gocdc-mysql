/* eslint-disable react-refresh/only-export-components */
import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import type { ApiCredentials } from '@/shared/api/types'
import { clearStoredSession, readStoredSession, storeSession } from './session'

export interface AuthSession {
  credentials: ApiCredentials | null
}

interface AuthContextValue {
  session: AuthSession | null
  login: (credentials: ApiCredentials | null) => void
  logout: () => void
  invalidate: () => void
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient()
  // 刷新页面时从会话 Cookie 恢复登录态，过期后 Cookie 被浏览器清除则回到登录页
  const [session, setSession] = useState<AuthSession | null>(() => readStoredSession() ?? null)

  const login = useCallback((credentials: ApiCredentials | null) => {
    storeSession(credentials)
    queryClient.clear()
    setSession({ credentials })
  }, [queryClient])
  const logout = useCallback(() => {
    clearStoredSession()
    queryClient.clear()
    setSession(null)
  }, [queryClient])
  const invalidate = useCallback(() => {
    clearStoredSession()
    queryClient.clear()
    setSession(null)
  }, [queryClient])

  const value = useMemo(() => ({ session, login, logout, invalidate }), [session, login, logout, invalidate])
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const value = useContext(AuthContext)
  if (!value) throw new Error('useAuth 必须在 AuthProvider 内使用')
  return value
}
