import { useQuery } from '@tanstack/react-query'
import { useApi } from '@/app/api'

export const queryKeys = {
  status: ['status'] as const,
  rules: ['effective-rules'] as const,
  position: ['position'] as const,
  ddl: ['ddl-events'] as const,
  audit: ['audit'] as const,
  config: ['config'] as const,
}

export function useStatus() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.status, queryFn: ({ signal }) => api.getStatus(signal), refetchInterval: 15_000 })
}

export function usePosition() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.position, queryFn: ({ signal }) => api.getPosition(signal), refetchInterval: 15_000 })
}

export function useEffectiveRules() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.rules, queryFn: ({ signal }) => api.getEffectiveRules(signal), staleTime: 30_000 })
}

export function useDDLEvents() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.ddl, queryFn: ({ signal }) => api.getDDLEvents(signal), refetchInterval: 15_000 })
}

export function useAudit() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.audit, queryFn: ({ signal }) => api.getAudit(signal), refetchInterval: 30_000 })
}

export function useConfig() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.config, queryFn: ({ signal }) => api.getConfig(signal), staleTime: 60_000 })
}
