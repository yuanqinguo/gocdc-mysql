import { useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useStatus } from '@/shared/api/queries'

/** 源作用域唯一入口：全部页面经此 hook 取 ?source={sid}，禁止各自解析 query。 */
export function useSourceScope() {
  const [searchParams, setSearchParams] = useSearchParams()
  const { data: status } = useStatus()
  const raw = searchParams.get('source')
  const currentSid = raw && raw !== '' ? raw : null

  const setSid = useCallback((sid: string | null) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev)
      if (sid === null || sid === '') next.delete('source')
      else next.set('source', sid)
      return next
    }, { replace: false })
  }, [setSearchParams])

  return { currentSid, setSid, sourceCount: status?.sources?.length ?? 0 }
}
