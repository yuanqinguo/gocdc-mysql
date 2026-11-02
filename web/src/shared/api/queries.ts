import { useQuery } from '@tanstack/react-query'
import { useApi } from '@/app/api'

/** key 必须含 sid：切换源时命中各自的缓存，否则会串数据。 */
export const queryKeys = {
  status: ['status'] as const,
  sources: ['sources'] as const,
  sourceStatus: (sid: string) => ['source-status', sid] as const,
  rules: (sid: string) => ['effective-rules', sid] as const,
  position: (sid: string) => ['position', sid] as const,
  ddl: (sid: string) => ['ddl-events', sid] as const,
  ddlAll: ['ddl-events-all'] as const,
  audit: ['audit'] as const,
  config: ['config'] as const,
}

export function useStatus() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.status, queryFn: ({ signal }) => api.getStatus(signal), refetchInterval: 15_000 })
}

export function useSources() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.sources, queryFn: ({ signal }) => api.getSources(signal), staleTime: 30_000 })
}

export function useSourceStatus(sid: string | null) {
  const api = useApi()
  return useQuery({
    queryKey: queryKeys.sourceStatus(sid ?? ''),
    queryFn: ({ signal }) => api.getSourceStatus(sid as string, signal),
    enabled: sid !== null,
    refetchInterval: 15_000,
  })
}

export function usePosition(sid: string | null) {
  const api = useApi()
  return useQuery({
    queryKey: queryKeys.position(sid ?? ''),
    queryFn: ({ signal }) => api.getPositionOf(sid as string, signal),
    enabled: sid !== null,
    refetchInterval: 15_000,
  })
}

export function useEffectiveRules(sid: string | null) {
  const api = useApi()
  return useQuery({
    queryKey: queryKeys.rules(sid ?? ''),
    queryFn: ({ signal }) => api.getEffectiveRulesOf(sid as string, signal),
    enabled: sid !== null,
    staleTime: 30_000,
  })
}

export function useDDLEvents(sid: string | null) {
  const api = useApi()
  return useQuery({
    queryKey: queryKeys.ddl(sid ?? ''),
    queryFn: ({ signal }) => api.getDDLEventsOf(sid as string, signal),
    enabled: sid !== null,
    refetchInterval: 15_000,
  })
}

/** 全集群 pending DDL 合并列表（聚合视图/角标用，记录带 source 字段）。 */
export function useDDLEventsAll() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.ddlAll, queryFn: ({ signal }) => api.getDDLEventsAll(signal), refetchInterval: 30_000 })
}

export function useAudit() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.audit, queryFn: ({ signal }) => api.getAudit(signal), refetchInterval: 30_000 })
}

export function useConfig() {
  const api = useApi()
  return useQuery({ queryKey: queryKeys.config, queryFn: ({ signal }) => api.getConfig(signal), staleTime: 60_000 })
}
