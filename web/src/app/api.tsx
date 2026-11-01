/* eslint-disable react-refresh/only-export-components */
import { createContext, useContext, useMemo, type ReactNode } from 'react'
import { ApiClient } from '@/shared/api/client'
import { useAuth } from './auth'

const ApiContext = createContext<ApiClient | null>(null)

export function ApiProvider({ children }: { children: ReactNode }) {
  const { session, invalidate } = useAuth()
  const api = useMemo(
    () => new ApiClient(session?.credentials ?? null, invalidate),
    [session, invalidate],
  )
  return <ApiContext.Provider value={api}>{children}</ApiContext.Provider>
}

export function useApi() {
  const api = useContext(ApiContext)
  if (!api) throw new Error('useApi 必须在 ApiProvider 内使用')
  return api
}
